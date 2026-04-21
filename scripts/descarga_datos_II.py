import pandas as pd
import requests
import logging
import shutil
import os
from datetime import date, timedelta


# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────
URL            = 'https://www.datos.gov.co/resource/jbjy-vk9h.json'
ARCHIVO        = '/data/secop2.parquet'
VENTANA_DIAS   = 15
LIMIT          = 1000

COLS_FECHA = [
    'fecha_de_firma',
    'fecha_de_inicio_del_contrato',
    'fecha_de_fin_del_contrato',
    'ultima_actualizacion',
    'fecha_inicio_liquidacion',
    'fecha_fin_liquidacion',
    'fecha_de_notificaci_n_de_prorrogaci_n',
]

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  HELPER: variables de tiempo
# ─────────────────────────────────────────────
def agregar_variables_tiempo(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLS_FECHA:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    if 'fecha_de_inicio_del_contrato' in df.columns and 'fecha_de_fin_del_contrato' in df.columns:
        df['duracion_dias'] = (
            df['fecha_de_fin_del_contrato'] - df['fecha_de_inicio_del_contrato']
        ).dt.days

    if 'fecha_de_firma' in df.columns:
        df['firma_anio']       = df['fecha_de_firma'].dt.year
        df['firma_mes']        = df['fecha_de_firma'].dt.month
        df['firma_dia_semana'] = df['fecha_de_firma'].dt.day_name()
        df['firma_trimestre']  = df['fecha_de_firma'].dt.quarter

    if 'ultima_actualizacion' in df.columns:
        df['actualizacion_anio'] = df['ultima_actualizacion'].dt.year
        df['actualizacion_mes']  = df['ultima_actualizacion'].dt.month

    return df


# ─────────────────────────────────────────────
#  1. LEER ARCHIVO LOCAL
# ─────────────────────────────────────────────
hoy         = date.today()
fecha_hoy   = hoy.strftime('%Y-%m-%d')
fecha_corte = hoy - timedelta(days=VENTANA_DIAS)   # límite inferior de la ventana

if os.path.exists(ARCHIVO):
    log.info(f'Leyendo archivo existente: {ARCHIVO}')
    df_local = pd.read_parquet(ARCHIVO)

    if 'ultima_actualizacion' in df_local.columns:
        df_local['ultima_actualizacion'] = pd.to_datetime(
            df_local['ultima_actualizacion'], errors='coerce'
        )
        ultima_fecha = df_local['ultima_actualizacion'].dt.date.max()
    else:
        ultima_fecha = None

    # Desde el día siguiente a lo que ya tenemos, pero nunca antes de la ventana
    if ultima_fecha:
        fecha_desde = max(ultima_fecha + timedelta(days=1), fecha_corte)
    else:
        fecha_desde = fecha_corte

    log.info(f'Última fecha en archivo: {ultima_fecha} → descarga desde: {fecha_desde} hasta: {fecha_hoy}')
else:
    log.warning(f'No existe {ARCHIVO}. Primera carga: últimos {VENTANA_DIAS} días ({fecha_corte} → {fecha_hoy})')
    fecha_desde = fecha_corte
    df_local    = pd.DataFrame()

fecha_desde_str = fecha_desde.strftime('%Y-%m-%d') if isinstance(fecha_desde, date) else str(fecha_desde)


# ─────────────────────────────────────────────
#  2. DESCARGAR — con tope superior = hoy
# ─────────────────────────────────────────────
log.info(f'Descargando registros con ultima_actualizacion entre {fecha_desde_str} y {fecha_hoy}...')
data_nuevos = []
offset = 0

# El WHERE tiene tope superior explícito para evitar bajar registros con fechas raras
where = (
    f"ultima_actualizacion >= '{fecha_desde_str}T00:00:00' "
    f"AND ultima_actualizacion <= '{fecha_hoy}T23:59:59'"
)

while True:
    params = {
        '$offset': offset,
        '$limit' : LIMIT,
        '$where' : where,
    }

    try:
        response = requests.get(URL, params=params, timeout=30)
    except requests.exceptions.RequestException as e:
        log.error(f'Error de conexión: {e}')
        raise

    if response.status_code != 200:
        log.error(f'HTTP {response.status_code}: {response.text}')
        raise Exception(f'Falló la descarga con código {response.status_code}')

    batch = response.json()
    if not batch:
        break

    data_nuevos.extend(batch)
    log.info(f'  {len(data_nuevos):,} filas descargadas...')
    offset += LIMIT


# ─────────────────────────────────────────────
#  3. COMBINAR: solo nuevos + cambios de estado
# ─────────────────────────────────────────────
if not data_nuevos:
    log.info('No hay nuevos datos. El archivo está al día ✅')
    df_completo = df_local
else:
    df_nuevos = pd.DataFrame(data_nuevos)
    log.info(f'Registros recibidos de la API: {len(df_nuevos):,}')

    if df_local.empty:
        df_completo = df_nuevos
        log.info('Primera carga completa.')
    else:
        # Detectar cambios de estado
        estados_locales = (
            df_local[['id_contrato', 'estado_contrato']]
            .dropna(subset=['id_contrato'])
            .drop_duplicates(subset=['id_contrato'], keep='last')
            .set_index('id_contrato')['estado_contrato']
        )

        cambios = []
        for _, row in df_nuevos.iterrows():
            id_c   = row.get('id_contrato')
            estado = row.get('estado_contrato')
            if id_c and id_c in estados_locales.index:
                ant = estados_locales[id_c]
                if ant != estado:
                    cambios.append({'id_contrato': id_c, 'anterior': ant, 'nuevo': estado})

        if cambios:
            log.warning(f'⚠️  {len(cambios)} cambio(s) de estado detectado(s):')
            for c in cambios:
                log.warning(f'   {c["id_contrato"]} | {c["anterior"]} → {c["nuevo"]}')
        else:
            log.info('Sin cambios de estado.')

        # Solo agregar lo que es nuevo o cambió
        ids_locales    = set(df_local['id_contrato'].dropna().unique())
        ids_con_cambio = {c['id_contrato'] for c in cambios}

        df_a_agregar = df_nuevos[
            df_nuevos['id_contrato'].apply(
                lambda x: (x not in ids_locales) or (x in ids_con_cambio)
            )
        ]
        omitidos = len(df_nuevos) - len(df_a_agregar)
        log.info(f'Omitidos (ya existían, sin cambios): {omitidos:,} | A agregar: {len(df_a_agregar):,}')

        df_completo = pd.concat([df_local, df_a_agregar], ignore_index=True)


# ─────────────────────────────────────────────
#  4. RECORTAR A VENTANA DE 3 MESES
# ─────────────────────────────────────────────
corte_ts = pd.Timestamp(fecha_corte)
if 'ultima_actualizacion' in df_completo.columns:
    df_completo['ultima_actualizacion'] = pd.to_datetime(
        df_completo['ultima_actualizacion'], errors='coerce'
    )
    antes = len(df_completo)
    df_completo = df_completo[
        df_completo['ultima_actualizacion'].isna() |
        (df_completo['ultima_actualizacion'] >= corte_ts)
    ]
    if (antes - len(df_completo)) > 0:
        log.info(f'Registros descartados por ventana de {VENTANA_DIAS} días: {antes - len(df_completo):,}')


# ─────────────────────────────────────────────
#  5. VARIABLES DE TIEMPO
# ─────────────────────────────────────────────
df_completo = agregar_variables_tiempo(df_completo)


# ─────────────────────────────────────────────
#  6. GUARDADO ATÓMICO
# ─────────────────────────────────────────────
archivo_tmp = ARCHIVO + '.tmp'
try:
    df_completo.to_parquet(archivo_tmp, index=False, engine='pyarrow')
    shutil.move(archivo_tmp, ARCHIVO)
    log.info(f'Guardado ✅ | {len(df_completo):,} registros | {df_completo.shape[1]} columnas')
except Exception as e:
    log.error(f'Error guardando: {e}')
    if os.path.exists(archivo_tmp):
        os.remove(archivo_tmp)
    raise

log.info('Proceso finalizado ✅')

import pandas as pd
import requests
import os
from datetime import datetime, timedelta

# =====================================================
# 🔹 CONFIGURACIÓN
# =====================================================

URL = "https://www.datos.gov.co/resource/f789-7hwg.json"
ARCHIVO = "data/secop.parquet"
LIMIT = 50000

ID_COL = "uid"
COL_VALOR = "cuantia_contrato"
COL_ESTADO = "estado_del_proceso"

os.makedirs("data", exist_ok=True)

# =====================================================
# 🔹 1. CARGAR HISTÓRICO
# =====================================================

if not os.path.exists(ARCHIVO):
    print("⚠️ No existe histórico. Ejecuta primero una descarga inicial.")
    exit()

df_local = pd.read_parquet(ARCHIVO)

df_local["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
    df_local["fecha_de_cargue_en_el_secop"], errors="coerce"
)

# última fecha conocida
ultima_fecha = df_local["fecha_de_cargue_en_el_secop"].max()
fecha_desde = ultima_fecha.strftime('%Y-%m-%dT%H:%M:%S')

print("=" * 60)
print(f"📂 Registros locales: {len(df_local):,}")
print(f"📅 Última fecha local: {ultima_fecha}")
print(f"🔍 Buscando desde: {fecha_desde}")
print("=" * 60)

# =====================================================
# 🔹 2. DESCARGA API
# =====================================================

offset = 0
all_data = []

while True:
    params = {
        "$where": f"fecha_de_cargue_en_el_secop >= '{fecha_desde}'",
        "$limit": LIMIT,
        "$offset": offset
    }

    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(f"❌ Error API: {response.status_code}")

    data = response.json()

    if not data:
        break

    all_data.extend(data)
    print(f"📦 Descargando... {len(all_data):,}", end="\r")

    offset += LIMIT

print()

# =====================================================
# 🔴 SOLUCIÓN CLAVE: SI NO HAY DATOS
# =====================================================

if not all_data:
    print("✅ No hay datos nuevos. Dataset actualizado.")
    exit()

print(f"📥 {len(all_data):,} registros descargados")

df_api = pd.DataFrame(all_data)

df_api["fecha_de_cargue_en_el_secop"] = pd.to_datetime(
    df_api["fecha_de_cargue_en_el_secop"], errors="coerce"
)

# =====================================================
# 🔹 3. ANÁLISIS CAMBIOS
# =====================================================

print("\n🔍 ANALIZANDO CAMBIOS...\n")

ids_local = set(df_local[ID_COL].dropna())
ids_api = set(df_api[ID_COL].dropna())

ids_nuevos = ids_api - ids_local
ids_actualizados = ids_api & ids_local

print(f"🆕 Nuevos: {len(ids_nuevos):,}")
print(f"🔄 Posibles actualizados: {len(ids_actualizados):,}")

cambios_valor = []
cambios_estado = []

if ids_actualizados:
    df_local_idx = df_local[df_local[ID_COL].isin(ids_actualizados)].set_index(ID_COL)
    df_api_idx = df_api[df_api[ID_COL].isin(ids_actualizados)].set_index(ID_COL)

    df_local_idx = df_local_idx[~df_local_idx.index.duplicated(keep="first")]
    df_api_idx = df_api_idx[~df_api_idx.index.duplicated(keep="last")]

    ids_comunes = set(df_local_idx.index) & set(df_api_idx.index)

    for uid in ids_comunes:

        # cambio valor
        if COL_VALOR in df_local_idx.columns and COL_VALOR in df_api_idx.columns:
            v_old = str(df_local_idx.loc[uid, COL_VALOR])
            v_new = str(df_api_idx.loc[uid, COL_VALOR])

            if v_old != v_new:
                cambios_valor.append({
                    "uid": uid,
                    "valor_anterior": v_old,
                    "valor_nuevo": v_new
                })

        # cambio estado
        if COL_ESTADO in df_local_idx.columns and COL_ESTADO in df_api_idx.columns:
            e_old = str(df_local_idx.loc[uid, COL_ESTADO])
            e_new = str(df_api_idx.loc[uid, COL_ESTADO])

            if e_old != e_new:
                cambios_estado.append({
                    "uid": uid,
                    "estado_anterior": e_old,
                    "estado_nuevo": e_new
                })

print(f"\n💰 Cambios de valor: {len(cambios_valor)}")
print(f"📋 Cambios de estado: {len(cambios_estado)}")

# =====================================================
# 🔹 4. REEMPLAZAR ACTUALIZADOS
# =====================================================

df_base = df_local[~df_local[ID_COL].isin(ids_actualizados)]

# =====================================================
# 🔹 5. UNIR
# =====================================================

df_total = pd.concat([df_base, df_api], ignore_index=True)

# =====================================================
# 🔹 6. FILTRO 3 MESES + CONVOCADO
# =====================================================

hoy = datetime.today()
hace_3_meses = hoy - timedelta(days=90)

fechas = pd.to_datetime(df_total["fecha_de_cargue_en_el_secop"], errors="coerce")

df_total = df_total[
    (fechas >= hace_3_meses) |
    (df_total["estado_del_proceso"] == "CONVOCADO")
]

# =====================================================
# 🔹 7. GUARDAR
# =====================================================

df_total.to_parquet(ARCHIVO, engine="pyarrow")

print("\n" + "=" * 60)
print(f"✅ DATASET ACTUALIZADO: {len(df_total):,} filas")
print("=" * 60)
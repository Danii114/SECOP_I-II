import pandas as pd
import requests
import os
from datetime import date, timedelta
 
URL      = "https://www.datos.gov.co/resource/p6dx-8zbt.json"
ARCHIVO  = "data/secop2.parquet"
LIMIT    = 1000
DIAS_MAX = 90
 
ESTADOS_INACTIVOS = {"terminado", "liquidado", "cancelado", "cerrado", "no vigente"}
 
ID_COL       = "id_del_proceso"
COL_VALOR    = "valor_del_contrato"
COL_ESTADO   = "estado_del_procedimiento"
COL_URL      = "urlproceso"
 
os.makedirs("data", exist_ok=True)
 
if not os.path.exists(ARCHIVO):
    print(f"No existe {ARCHIVO}. Se necesita descarga inicial.")
    raise SystemExit(1)
 
# ── 1. Cargar local ───────────────────────────────────────────────────────────
df_local = pd.read_parquet(ARCHIVO)
df_local["fecha_de_publicacion"] = pd.to_datetime(df_local["fecha_de_publicacion"], errors="coerce")
 
# CORRECCIÓN: usar date.today() como tope para nunca quedar atascado
# en un día sin datos. Buscamos desde la última fecha conocida (inclusive).
hoy            = date.today()
ultima_local   = df_local["fecha_de_publicacion"].dt.date.max()
fecha_desde    = ultima_local.strftime("%Y-%m-%d")
 
print("=" * 60)
print(f"📂 Registros locales  : {len(df_local):,}")
print(f"   Última fecha local : {ultima_local}")
print(f"   Hoy                : {hoy}")
print(f"   Buscando desde     : {fecha_desde}")
print("=" * 60)
 
# ── 2. Descargar desde API ────────────────────────────────────────────────────
offset, data_nuevos = 0, []
while True:
    try:
        resp = requests.get(URL, params={
            "$offset": offset,
            "$limit":  LIMIT,
            "$where":  f"fecha_de_publicacion >= '{fecha_desde}'"
        }, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"❌ Error API: {e}")
        raise SystemExit(1)
 
    batch = resp.json()
    if not batch:
        break
    data_nuevos.extend(batch)
    print(f"   Descargando... {len(data_nuevos):,} filas", end="\r")
    offset += LIMIT
 
print()
 
if not data_nuevos:
    print("✅ Sin datos nuevos. El archivo está al día.")
    raise SystemExit(0)
 
print(f"📥 {len(data_nuevos):,} registros descargados desde la API")
 
df_api = pd.DataFrame(data_nuevos)
df_api["fecha_de_publicacion"] = pd.to_datetime(df_api["fecha_de_publicacion"], errors="coerce")
 
# ── 3. Clasificar: nuevos vs actualizados ─────────────────────────────────────
print()
print("=" * 60)
print("🔍 ANÁLISIS DE CAMBIOS")
print("=" * 60)
 
if ID_COL not in df_local.columns or ID_COL not in df_api.columns:
    print(f"⚠️  No se encontró '{ID_COL}'. Concatenando sin análisis.")
    df_base = df_local
else:
    ids_locales = set(df_local[ID_COL].dropna())
    ids_api     = set(df_api[ID_COL].dropna())
 
    ids_nuevos      = ids_api - ids_locales
    ids_actualizados = ids_api & ids_locales
 
    print(f"🆕 Contratos NUEVOS       : {len(ids_nuevos):,}")
    print(f"🔄 Contratos en SECOP API : {len(ids_actualizados):,} (ya existían localmente)")
 
    # ── Detectar cambios de valor y estado ───────────────────────────────────
    cambios_valor  = []
    cambios_estado = []
 
    if ids_actualizados:
        df_local_idx = df_local[df_local[ID_COL].isin(ids_actualizados)].set_index(ID_COL)
        df_api_idx   = df_api[df_api[ID_COL].isin(ids_actualizados)].set_index(ID_COL)
 
        # Manejar duplicados en el índice tomando el primero
        df_local_idx = df_local_idx[~df_local_idx.index.duplicated(keep="first")]
        df_api_idx   = df_api_idx[~df_api_idx.index.duplicated(keep="last")]
 
        ids_comunes = set(df_local_idx.index) & set(df_api_idx.index)
 
        for pid in ids_comunes:
            url = df_api_idx.loc[pid, COL_URL] if COL_URL in df_api_idx.columns else "—"
 
            # Cambio de VALOR
            if COL_VALOR in df_local_idx.columns and COL_VALOR in df_api_idx.columns:
                v_old = str(df_local_idx.loc[pid, COL_VALOR]).strip()
                v_new = str(df_api_idx.loc[pid, COL_VALOR]).strip()
                if v_old != v_new:
                    cambios_valor.append({
                        "id_del_proceso":        pid,
                        "valor_anterior":        v_old,
                        "valor_nuevo":           v_new,
                        "urlproceso":            url,
                    })
 
            # Cambio de ESTADO
            if COL_ESTADO in df_local_idx.columns and COL_ESTADO in df_api_idx.columns:
                e_old = str(df_local_idx.loc[pid, COL_ESTADO]).strip()
                e_new = str(df_api_idx.loc[pid, COL_ESTADO]).strip()
                if e_old != e_new:
                    cambios_estado.append({
                        "id_del_proceso":        pid,
                        "estado_anterior":       e_old,
                        "estado_nuevo":          e_new,
                        "urlproceso":            url,
                    })
 
    # ── Reporte de cambios ────────────────────────────────────────────────────
    print()
    print(f"💰 Contratos con cambio de VALOR  : {len(cambios_valor):,}")
    if cambios_valor:
        df_cv = pd.DataFrame(cambios_valor)
        print(df_cv.to_string(index=False))
 
    print()
    print(f"📋 Contratos con cambio de ESTADO : {len(cambios_estado):,}")
    if cambios_estado:
        df_ce = pd.DataFrame(cambios_estado)
        print(df_ce.to_string(index=False))
 
    if cambios_valor or cambios_estado:
        print()
        print("⚠️  ATENCIÓN: Contratos con modificaciones detectadas.")
        print("   Revisar manualmente si corresponde a correcciones legítimas")
        print("   o posibles indicios de irregularidades.")
 
    # Quitar versiones viejas de actualizados y reemplazar con los de la API
    df_base = df_local[~df_local[ID_COL].isin(ids_actualizados)]
 
# ── 4. Combinar ───────────────────────────────────────────────────────────────
df_total = pd.concat([df_base, df_api], ignore_index=True)
df_total["fecha_de_publicacion"] = pd.to_datetime(df_total["fecha_de_publicacion"], errors="coerce")
 
# ── 5. Limpiar inactivos viejos (> DIAS_MAX) ──────────────────────────────────
fecha_corte = pd.Timestamp(date.today() - timedelta(days=DIAS_MAX))
 
if COL_ESTADO in df_total.columns:
    mask_inactivo = df_total[COL_ESTADO].fillna("").str.lower().str.strip().isin(ESTADOS_INACTIVOS)
    mask_viejo    = df_total["fecha_de_publicacion"] < fecha_corte
    antes = len(df_total)
    df_total = df_total[~(mask_inactivo & mask_viejo)]
    print()
    print(f"🧹 Eliminados (inactivos + >{DIAS_MAX}d) : {antes - len(df_total):,}")
 
# ── 6. Guardar ────────────────────────────────────────────────────────────────
df_total.to_parquet(ARCHIVO, index=False, engine="pyarrow")
print()
print("=" * 60)
print(f"✅ SECOP 2 guardado: {len(df_total):,} registros → {ARCHIVO}")
print("=" * 60)
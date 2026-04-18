import pandas as pd
import requests
import os
from datetime import date, timedelta

URL      = "https://www.datos.gov.co/resource/jbjy-vk9h.json"
ARCHIVO  = "data/secop2.parquet"
LIMIT    = 1000
DIAS_MAX = 90

ESTADOS_INACTIVOS = {"terminado", "liquidado", "cancelado", "cerrado", "no vigente"}

# 🔴 COLUMNAS CORREGIDAS
ID_COL       = "id_contrato"
COL_VALOR    = "valor_del_contrato"
COL_ESTADO   = "estado_contrato"
COL_URL      = "urlproceso"
COL_FECHA    = "fecha_de_firma"

os.makedirs("data", exist_ok=True)

if not os.path.exists(ARCHIVO):
    print(f"No existe {ARCHIVO}. Se necesita descarga inicial.")
    raise SystemExit(1)

# ── 1. Cargar local ───────────────────────────────────────────────────────────
df_local = pd.read_parquet(ARCHIVO)
df_local[COL_FECHA] = pd.to_datetime(df_local[COL_FECHA], errors="coerce")

hoy          = date.today()
ultima_local = df_local[COL_FECHA].dt.date.max()
fecha_desde  = ultima_local.strftime("%Y-%m-%d")

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
            "$where":  f"{COL_FECHA} >= '{fecha_desde}'"
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
df_api[COL_FECHA] = pd.to_datetime(df_api[COL_FECHA], errors="coerce")

# asegurar tipo string para IDs
df_api[ID_COL] = df_api[ID_COL].astype(str)
df_local[ID_COL] = df_local[ID_COL].astype(str)

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

    ids_nuevos       = ids_api - ids_locales
    ids_actualizados = ids_api & ids_locales

    print(f"🆕 Contratos NUEVOS       : {len(ids_nuevos):,}")
    print(f"🔄 Contratos en SECOP API : {len(ids_actualizados):,} (ya existían localmente)")

    cambios_valor  = []
    cambios_estado = []

    if ids_actualizados:
        df_local_idx = df_local[df_local[ID_COL].isin(ids_actualizados)].set_index(ID_COL)
        df_api_idx   = df_api[df_api[ID_COL].isin(ids_actualizados)].set_index(ID_COL)

        df_local_idx = df_local_idx[~df_local_idx.index.duplicated(keep="first")]
        df_api_idx   = df_api_idx[~df_api_idx.index.duplicated(keep="last")]

        ids_comunes = set(df_local_idx.index) & set(df_api_idx.index)

        for pid in ids_comunes:
            url = df_api_idx.loc[pid, COL_URL] if COL_URL in df_api_idx.columns else "—"

            # CAMBIO DE VALOR
            if COL_VALOR in df_local_idx.columns and COL_VALOR in df_api_idx.columns:
                v_old = str(df_local_idx.loc[pid, COL_VALOR]).strip()
                v_new = str(df_api_idx.loc[pid, COL_VALOR]).strip()
                if v_old != v_new:
                    cambios_valor.append({
                        "id_contrato": pid,
                        "valor_anterior": v_old,
                        "valor_nuevo": v_new,
                        "urlproceso": url,
                    })

            # CAMBIO DE ESTADO
            if COL_ESTADO in df_local_idx.columns and COL_ESTADO in df_api_idx.columns:
                e_old = str(df_local_idx.loc[pid, COL_ESTADO]).strip()
                e_new = str(df_api_idx.loc[pid, COL_ESTADO]).strip()
                if e_old != e_new:
                    cambios_estado.append({
                        "id_contrato": pid,
                        "estado_anterior": e_old,
                        "estado_nuevo": e_new,
                        "urlproceso": url,
                    })

    # ── Reporte ───────────────────────────────────────────────────────────────
    print()
    print(f"💰 Cambios VALOR  : {len(cambios_valor):,}")
    if cambios_valor:
        print(pd.DataFrame(cambios_valor).to_string(index=False))

    print()
    print(f"📋 Cambios ESTADO : {len(cambios_estado):,}")
    if cambios_estado:
        print(pd.DataFrame(cambios_estado).to_string(index=False))

    if cambios_valor or cambios_estado:
        print("\n⚠️ Contratos con modificaciones detectadas")

    df_base = df_local[~df_local[ID_COL].isin(ids_actualizados)]

# ── 4. Combinar ───────────────────────────────────────────────────────────────
df_total = pd.concat([df_base, df_api], ignore_index=True)
df_total[COL_FECHA] = pd.to_datetime(df_total[COL_FECHA], errors="coerce")

# eliminar duplicados finales
df_total = df_total.drop_duplicates(subset=[ID_COL], keep="last")

# ── 5. Guardar ────────────────────────────────────────────────────────────────
df_total.to_parquet(ARCHIVO, index=False, engine="pyarrow")

print()
print("=" * 60)
print(f"✅ SECOP II guardado: {len(df_total):,} registros → {ARCHIVO}")
print("=" * 60)

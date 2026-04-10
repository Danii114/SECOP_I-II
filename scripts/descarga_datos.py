import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# =====================================================
# 🔹 CONFIGURACIÓN
# =====================================================

URL = "https://www.datos.gov.co/resource/f789-7hwg.json"
ARCHIVO = "data/secop.parquet"

# =====================================================
# 🔹 1. FECHA (AYER)
# =====================================================

hoy = datetime.today()
ayer = hoy - timedelta(days=1)

fecha_inicio = ayer.strftime('%Y-%m-%dT00:00:00')
fecha_fin = ayer.strftime('%Y-%m-%dT23:59:59')

print(f"📅 Descargando: {fecha_inicio} → {fecha_fin}")

# =====================================================
# 🔹 2. DESCARGA CON PAGINACIÓN
# =====================================================

limit = 50000
offset = 0
all_data = []

while True:
    params = {
        "$where": f"fecha_de_cargue_en_el_secop BETWEEN '{fecha_inicio}' AND '{fecha_fin}'",
        "$limit": limit,
        "$offset": offset
    }

    response = requests.get(URL, params=params)

    if response.status_code != 200:
        raise Exception(f"❌ Error API: {response.status_code}")

    data = response.json()

    if not data:
        break

    all_data.extend(data)
    print(f"📦 Descargados: {len(all_data)}")

    offset += limit

df_nuevo = pd.DataFrame(all_data)

# =====================================================
# 🔹 3. CARGAR HISTÓRICO
# =====================================================

if os.path.exists(ARCHIVO):
    df_hist = pd.read_parquet(ARCHIVO)
    print("📂 Histórico cargado")
else:
    df_hist = pd.DataFrame()
    print("⚠️ No hay histórico")

# =====================================================
# 🔹 4. DETECTAR NUEVOS
# =====================================================

if not df_hist.empty and "uid" in df_nuevo.columns:

    nuevos = df_nuevo[~df_nuevo["uid"].isin(df_hist["uid"])]

    print(f"\n🆕 CONTRATOS NUEVOS: {len(nuevos)}")

else:
    nuevos = df_nuevo.copy()
    print(f"\n🆕 Todo es nuevo: {len(nuevos)}")

# =====================================================
# 🔹 5. DETECTAR ACTUALIZADOS
# =====================================================

actualizados = pd.DataFrame()

if not df_hist.empty:

    df_merge = df_nuevo.merge(
        df_hist,
        on="uid",
        how="inner",
        suffixes=("_nuevo", "_viejo")
    )

    mask_cambio = (
        (df_merge["cuantia_contrato_nuevo"] != df_merge["cuantia_contrato_viejo"]) |
        (df_merge["estado_del_proceso_nuevo"] != df_merge["estado_del_proceso_viejo"])
    )

    actualizados = df_merge[mask_cambio]

    print(f"🔄 CONTRATOS ACTUALIZADOS: {len(actualizados)}")

# =====================================================
# 🔹 6. UNIR HISTÓRICO + NUEVO
# =====================================================

df_total = pd.concat([df_hist, df_nuevo], ignore_index=True)

if "uid" in df_total.columns:
    df_total = df_total.drop_duplicates(subset="uid", keep="last")

# =====================================================
# 🔹 7. FILTRO 3 MESES + CONVOCADO (TU VERSIÓN)
# =====================================================

hace_3_meses = hoy - timedelta(days=90)

fechas = pd.to_datetime(df_total["fecha_de_cargue_en_el_secop"], errors="coerce")

df_total = df_total[
    (fechas >= hace_3_meses) |
    (df_total["estado_del_proceso"] == "CONVOCADO")
]

# =====================================================
# 🔹 8. CREAR CARPETA
# =====================================================

os.makedirs("data", exist_ok=True)

# =====================================================
# 🔹 9. GUARDAR
# =====================================================

df_total.to_parquet(ARCHIVO, engine="pyarrow")

nuevos.to_parquet("data/nuevos.parquet", engine="pyarrow")
actualizados.to_parquet("data/actualizados.parquet", engine="pyarrow")

# =====================================================
# 🔹 10. RESUMEN
# =====================================================

print("\n📊 RESUMEN FINAL")
print("🆕 Nuevos:", len(nuevos))
print("🔄 Actualizados:", len(actualizados))
print("📦 Total dataset:", len(df_total))
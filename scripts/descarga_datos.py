import requests
import pandas as pd
from datetime import datetime, timedelta
import os

# =====================================================
# 🔹 CONFIGURACIÓN
# =====================================================

URL = "https://www.datos.gov.co/resource/f789-7hwg.json"
ARCHIVO = "data/secop_2026-04.parquet"

# =====================================================
# 🔹 1. FECHA ÚLTIMA SEMANA
# =====================================================

hoy = datetime.today()
hace_una_semana = hoy - timedelta(days=7)

fecha_inicio = hace_una_semana.strftime('%Y-%m-%dT%H:%M:%S')

print(f"📅 Descargando desde última semana: {fecha_inicio}")

# =====================================================
# 🔹 2. DESCARGA CON PAGINACIÓN
# =====================================================

limit = 50000
offset = 0
all_data = []

while True:
    params = {
        "$where": f"fecha_de_cargue_en_el_secop >= '{fecha_inicio}'",
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
    print(f"📦 Nuevos registros acumulados: {len(all_data)}")

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
    print("⚠️ No hay histórico, creando nuevo dataset")

# =====================================================
# 🔹 4. UNIR DATOS
# =====================================================

df_total = pd.concat([df_hist, df_nuevo], ignore_index=True)

# =====================================================
# 🔹 5. ELIMINAR DUPLICADOS (CLAVE)
# =====================================================

df_total = df_total.drop_duplicates(subset="uid")

# =====================================================
# 🔹 6. FILTRO 3 MESES + CONVOCADO (SIN LIMPIEZA EXTRA)
# =====================================================

hace_3_meses = hoy - timedelta(days=90)

# solo convertimos la fecha para poder filtrar (no es limpieza)
fechas = pd.to_datetime(df_total["fecha_de_cargue_en_el_secop"], errors="coerce")

df_total = df_total[
    (fechas >= hace_3_meses) |
    (df_total["estado_del_proceso"] == "CONVOCADO")
]

# =====================================================
# 🔹 7. CREAR CARPETA SI NO EXISTE
# =====================================================

os.makedirs("data", exist_ok=True)

# =====================================================
# 🔹 8. GUARDAR
# =====================================================

df_total.to_parquet(ARCHIVO, engine="pyarrow")

# =====================================================
# 🔹 9. RESUMEN
# =====================================================

print("\n✅ DATASET ACTUALIZADO")
print("📊 Filas finales:", df_total.shape[0])
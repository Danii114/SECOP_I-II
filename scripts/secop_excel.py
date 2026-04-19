import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import os
from datetime import date
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
#Configurando
palabras_clave = [
    "Construcción",
    "construcción de vías",
    "obra pública",
    "infraestructura",
    "mejoramiento vial",
    "construcción de puentes"
]

COLUMNAS = [
    "nit_de_la_entidad",
    "estado_del_proceso",
    "detalle_del_objeto_a_contratar",
    "cuantia_proceso",
    "ruta_proceso_en_secop_i",
    "score",
]

NOMBRE_ARCHIVO = f"secop_construccion_{date.today()}.xlsx"

# =========================================================
Cargando datos
# =========================================================

print("🔄 Cargando modelo y datos...")

model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
embeddings = np.load("data/embeddings.npy")

df = pd.read_parquet(
    "https://raw.githubusercontent.com/Danii114/SECOP_I-II/main/data/secop_2026-04.parquet"
)

# =========================================================
 FUNCIÓN DE BÚSQUEDA
# =========================================================

def buscar_secop(query, top_k=15):
    query_emb = model.encode([query.lower().strip()])
    similitudes = cosine_similarity(query_emb, embeddings)[0]

    top_idx = similitudes.argsort()[-top_k:][::-1]

    # 🔥 quitar score antes de seleccionar
    columnas_base = [c for c in COLUMNAS if c != "score"]

    resultado = df.iloc[top_idx][columnas_base].copy()
    resultado["score"] = similitudes[top_idx]

    return resultado

# =========================================================
 EJECUTAR BÚSQUEDA
# =========================================================

print("🔎 Buscando contratos...")

resultados = [buscar_secop(p) for p in palabras_clave]
todo = pd.concat(resultados, ignore_index=True)

# quitar duplicados
todo = todo.drop_duplicates(subset=["ruta_proceso_en_secop_i"])

# =========================================================
 LIMPIEZA
# =========================================================

print("🧹 Limpiando datos...")

# convertir a número
todo["cuantia_proceso"] = pd.to_numeric(
    todo["cuantia_proceso"], errors="coerce"
)

# ordenar por score
todo = todo.sort_values(by="score", ascending=False)

# =========================================================
# 🔹 6. EXPORTAR EXCEL
# =========================================================

print("📁 Generando Excel...")

# copia para formato bonito
excel_df = todo.copy()

excel_df["cuantia_proceso"] = excel_df["cuantia_proceso"].apply(
    lambda x: f"${x:,.0f}" if pd.notnull(x) else ""
)

excel_df.to_excel(NOMBRE_ARCHIVO, index=False)

print(f"✅ Excel guardado: {NOMBRE_ARCHIVO}")

# =========================================================
 ENVIAR CORREO
# =========================================================

print("📧 Enviando correo...")

EMAIL_SENDER = os.environ.get("EMAIL_SENDER", "danimorav05@gmail.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "exum hoyz gcvy nsli")
EMAIL_TO = [EMAIL_SENDER]

msg = MIMEMultipart()
msg["Subject"] = f"SECOP Construcción - {date.today()}"
msg["From"] = EMAIL_SENDER
msg["To"] = ", ".join(EMAIL_TO)

cuerpo = f"""
Hola,

Adjunto el Excel con contratos SECOP relacionados con construcción.

Palabras clave:
{', '.join(palabras_clave)}

Generado automáticamente el {date.today()}
"""

msg.attach(MIMEText(cuerpo, "plain"))

# adjuntar archivo
with open(NOMBRE_ARCHIVO, "rb") as f:
    part = MIMEBase("application", "octet-stream")
    part.set_payload(f.read())

encoders.encode_base64(part)
part.add_header(
    "Content-Disposition",
    f"attachment; filename={NOMBRE_ARCHIVO}"
)
msg.attach(part)

try:
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.sendmail(EMAIL_SENDER, EMAIL_TO, msg.as_string())

    print("✅ Correo enviado correctamente")

except Exception as e:
    print("❌ Error al enviar correo:", e)

print("🏁 Proceso finalizado")


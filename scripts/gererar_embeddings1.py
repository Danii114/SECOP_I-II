import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

print("🔄 Cargando parquet...")
df = pd.read_parquet("data/secop_2026-04.parquet")

print(f"📐 {len(df)} filas — generando embeddings...")
model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

textos = df["detalle_del_objeto_a_contratar"].fillna("").str.lower().str.strip().tolist()
embeddings = model.encode(textos, show_progress_bar=True, batch_size=64)

np.save("data/embeddingsI.npy", embeddings)
print(f"✅ Guardado: {embeddings.shape}")

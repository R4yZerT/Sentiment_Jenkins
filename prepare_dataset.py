import pandas as pd
from datetime import datetime, timedelta
import random

# 1. Cargar el dataset original
df = pd.read_csv('dataset_sentimientos_500.csv')

# 2. Crear IDs únicos (1 a 500)
df['id'] = range(1, len(df) + 1)

# 3. Simular fechas de streaming (empezando desde ahora)
base_date = datetime.now()
df['fecha'] = [base_date + timedelta(seconds=i * random.randint(1, 5)) for i in range(len(df))]

# 4. Reordenar columnas según el PDF
df = df[['id', 'texto', 'sentimiento', 'fecha']]

# 5. Guardar el nuevo dataset enriquecido
df.to_csv('dataset_enriquecido.csv', index=False)
print("Dataset enriquecido guardado como 'dataset_enriquecido.csv'")
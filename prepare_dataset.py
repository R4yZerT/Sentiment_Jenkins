import pandas as pd
from datetime import datetime, timedelta
import random

# 1. Cargar el dataset original
df = pd.read_csv('dataset_sentimientos_500.csv')

# 2. Crear IDs únicos (1 a 500)
df['id'] = range(1, len(df) + 1)

# 3. Simular fechas distribuidas en un lapso de 3 meses
start_date = datetime(2025, 11, 1)  # Fecha de inicio
end_date = datetime(2026, 2, 1)     # Fecha fin (~3 meses)
total_seconds = int((end_date - start_date).total_seconds())

# Generar timestamps aleatorios dentro del rango y ordenarlos
timestamps = sorted([
    start_date + timedelta(seconds=random.randint(0, total_seconds))
    for _ in range(len(df))
])

df['fecha'] = timestamps

# 4. Reordenar columnas
df = df[['id', 'texto', 'etiqueta', 'fecha']]

# 5. Guardar
df.to_csv('dataset_enriquecido.csv', index=False)
print("Dataset enriquecido guardado como 'dataset_enriquecido.csv'")
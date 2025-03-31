import pandas as pd
import kagglehub
import os
from datetime import datetime

# Configurar la ruta del archivo kaggle.json
token = os.environ.get("SESSION_TOKEN")

# Ruta del dataset limpio
ruta_dataset_limpio = "src/bigdata/static/limpieza/books.csv"
# Ruta de salida para el archivo auditado
ruta_output_ratings = "src/bigdata/static/xlsx/books_transformacion_data.csv"
# Ruta del archivo de auditoría
ruta_auditoria = "src/bigdata/static/auditoria_transformacion.txt"

# Abrir archivo de auditoría
auditoria = []
auditoria.append("Auditoría de transformación de datos")
auditoria.append("=" * 60)
auditoria.append(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

# Cargar el dataset limpio
try:
    df_cleaned = pd.read_csv(ruta_dataset_limpio) 
    registros_antes = len(df_cleaned)
    auditoria.append(f"Registros en dataset limpio antes de transformación: {registros_antes}")
except Exception as e:
    auditoria.append(f"Error al cargar el dataset limpio: {e}")
    raise

# Cargar el dataset adicional desde Kaggle
try:
    path = kagglehub.dataset_download("thedevastator/books-sales-and-ratings")
    auditoria.append("Dataset adicional descargado correctamente.")
except Exception as e:
    auditoria.append(f"Error al descargar el dataset adicional: {e}")
    raise

# Ruta del archivo dentro del dataset descargado
ruta_dataset_transformacion = os.path.join(path, "Books_Data_Clean.csv")

# Cargar el archivo adicional
try:
    df_additional = pd.read_csv(ruta_dataset_transformacion)
    auditoria.append(f"Registros en dataset adicional antes de transformación: {len(df_additional)}")
except Exception as e:
    auditoria.append(f"Error al cargar el dataset adicional: {e}")
    raise

# Generar claves compuestas
df_cleaned['clave_compuesta'] = df_cleaned['title'].str.strip().str.lower() + "_" + df_cleaned['publishedDate'].astype(str).str.strip()
df_additional['clave_compuesta'] = df_additional['Book Name'].str.strip().str.lower() + "_" + df_additional['Publishing Year'].astype(str).str.strip()

# Combinar ambos datasets
try:
    df_merged = pd.merge(df_cleaned, df_additional, on='clave_compuesta', how='inner')
    auditoria.append(f"Registros combinados después de transformación: {len(df_merged)}")
except Exception as e:
    auditoria.append(f"Error al combinar los datasets: {e}")
    raise

# Añadir columnas de ratings
df_cleaned = df_cleaned.reset_index(drop=True)
ratings_data = df_additional[['Book_ratings_count', 'Book_average_rating', 'gross sales']].reset_index(drop=True)

try:
    df_cleaned['Book_ratings_count'] = ratings_data['Book_ratings_count']
    df_cleaned['Book_average_rating'] = ratings_data['Book_average_rating']
    df_cleaned['gross sales'] = ratings_data['gross sales']
    auditoria.append("Columnas de ratings añadidas correctamente.")
except Exception as e:
    auditoria.append(f"Error al añadir las columnas de ratings: {e}")
    raise

# Guardar dataset transformado
try:
    df_cleaned.to_csv(ruta_output_ratings, index=False)
    auditoria.append(f"Archivo transformado guardado en: {ruta_output_ratings}")
except Exception as e:
    auditoria.append(f"Error al guardar el archivo transformado: {e}")
    raise

# Guardar auditoría en archivo
with open(ruta_auditoria, "w", encoding="utf-8") as file:
    file.write("\n".join(auditoria))

print(f"Auditoría generada en {ruta_auditoria}")
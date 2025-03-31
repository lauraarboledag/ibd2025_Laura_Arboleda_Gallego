import pandas as pd
import kagglehub
import os

# Configurar la ruta del archivo kaggle.json
token = os.environ.get("SESSION_TOKEN")

# Ruta del dataset limpio
ruta_dataset_limpio = "src/bigdata/static/limpieza/books.csv"

# Cargar el dataset limpio
try:
    df_cleaned = pd.read_csv(ruta_dataset_limpio) 
    print("Dataset limpio cargado correctamente.")
except Exception as e:
    print(f"Error al cargar el dataset limpio: {e}")
    raise

# Cargar el dataset adicional desde Kaggle
try:
    path = kagglehub.dataset_download("thedevastator/books-sales-and-ratings")
    print("Dataset adicional descargado correctamente.")
except Exception as e:
    print(f"Error al descargar el dataset adicional: {e}")
    raise

# Ruta del archivo dentro del dataset descargado
ruta_dataset_transformacion = os.path.join(path, "Books_Data_Clean.csv")

# Cargar el archivo adicional
try:
    df_additional = pd.read_csv(ruta_dataset_transformacion)
    print("Dataset adicional cargado correctamente.")
except Exception as e:
    print(f"Error al cargar el dataset adicional: {e}")
    raise

# Generar claves compuestas en el dataset limpio
df_cleaned['clave_compuesta'] = df_cleaned['title'].str.strip().str.lower() + "_" + df_cleaned['publishedDate'].astype(str).str.strip()

# Generar claves compuestas en el dataset adicional
df_additional['clave_compuesta'] = df_additional['Book Name'].str.strip().str.lower() + "_" + df_additional['Publishing Year'].astype(str).str.strip()

# Combinar ambos datasets usando la clave compuesta
try:
    df_merged = pd.merge(df_cleaned, df_additional, on='clave_compuesta', how='inner')
    print("Datasets combinados correctamente.")
except Exception as e:
    print(f"Error al combinar los datasets: {e}")
    raise

# Seleccionamos solo las columnas relevantes del dataset adicional
ratings_data = df_additional[['Book_ratings_count', 'Book_average_rating', 'gross sales']]

# Resetear índices para asegurar alineación secuencial
df_cleaned = df_cleaned.reset_index(drop=True)
ratings_data = ratings_data.reset_index(drop=True)

# Añadir las columnas de ratings al dataset limpio
try:
    df_cleaned['Book_ratings_count'] = ratings_data['Book_ratings_count']
    df_cleaned['Book_average_rating'] = ratings_data['Book_average_rating']
    df_cleaned['gross sales'] = ratings_data['gross sales']
    print("Columnas de ratings añadidas correctamente.")
except Exception as e:
    print(f"Error al añadir las columnas de ratings: {e}")
    raise

# Exportar el dataset enriquecido a un nuevo archivo CSV
ruta_output_ratings = "src/bigdata/static/xlsx/books_transformacion_data.csv"
try:
    df_cleaned.to_csv(ruta_output_ratings, index=False)
    print(f"Archivo con ratings añadido guardado en: {ruta_output_ratings}")
except Exception as e:
    print(f"Error al guardar el archivo enriquecido: {e}")
    raise

# Crear un archivo de auditoría vacío
ruta_output_auditoria = "src/bigdata/static/auditoria/auditoria_transformacion.txt"
try:
    with open(ruta_output_auditoria, "w", encoding="utf-8") as file:
        file.write("")  # Archivo vacío
    print(f"Archivo de auditoría vacío guardado en: {ruta_output_auditoria}")
except Exception as e:
    print(f"Error al guardar el archivo de auditoría: {e}")
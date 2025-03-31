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

# Seleccionar solo las columnas relevantes del dataset adicional
ratings_data = df_additional[['Book_ratings_count', 'Book_average_rating', 'gross sales']]

# Resetear índices para asegurar alineación secuencial
df_cleaned = df_cleaned.reset_index(drop=True)
ratings_data = ratings_data.reset_index(drop=True)

# Guardar una copia de un registro antes de la transformación
ejemplo_antes = df_cleaned.iloc[0].to_dict()

# Obtener las columnas antes de agregar las nuevas
columnas_antes = set(df_cleaned.columns)

# Añadir las columnas de ratings al dataset limpio
try:
    df_cleaned['Book_ratings_count'] = ratings_data['Book_ratings_count']
    df_cleaned['Book_average_rating'] = ratings_data['Book_average_rating']
    df_cleaned['gross sales'] = ratings_data['gross sales']
    print("Columnas de ratings añadidas correctamente.")
except Exception as e:
    print(f"Error al añadir las columnas de ratings: {e}")
    raise

# Contar cuántos registros se actualizaron efectivamente
registros_actualizados = df_cleaned[['Book_ratings_count', 'Book_average_rating', 'gross sales']].notnull().all(axis=1).sum()

# Obtener las columnas después de la transformación
columnas_despues = set(df_cleaned.columns)
columnas_agregadas = columnas_despues - columnas_antes
columnas_eliminadas = columnas_antes - columnas_despues

# Guardar un registro después de la transformación
ejemplo_despues = df_cleaned.iloc[0].to_dict()

# Ruta de salida para el dataset enriquecido
ruta_output_ratings = "src/bigdata/static/xlsx/books_transformacion_data.csv"

# Exportar el dataset enriquecido a un nuevo archivo CSV
try:
    df_cleaned.to_csv(ruta_output_ratings, index=False)
    print(f"Archivo con ratings añadido guardado en: {ruta_output_ratings}")
except Exception as e:
    print(f"Error al guardar el archivo enriquecido: {e}")
    raise

# Ruta del archivo de auditoría
ruta_output_auditoria = "src/bigdata/static/auditoria/auditoria_transformacion.txt"

# Formatear el contenido de la auditoría
contenido_auditoria = f"""
AUDITORÍA DE TRANSFORMACIÓN DE DATOS

Columnas Agregadas:
{', '.join(columnas_agregadas) if columnas_agregadas else 'Ninguna'}

Columnas Eliminadas:
{', '.join(columnas_eliminadas) if columnas_eliminadas else 'Ninguna'}

Total de registros en el dataset final: {len(df_cleaned)}

Registros efectivamente actualizados con nuevas columnas: {registros_actualizados}

Ejemplo de registro ANTES de la transformación:
{ejemplo_antes}

Ejemplo de registro DESPUÉS de la transformación:
{ejemplo_despues}
"""

# Guardar el archivo de auditoría
try:
    with open(ruta_output_auditoria, "w", encoding="utf-8") as file:
        file.write(contenido_auditoria)
    print(f"Auditoría guardada en: {ruta_output_auditoria}")
except Exception as e:
    print(f"Error al guardar la auditoría: {e}")
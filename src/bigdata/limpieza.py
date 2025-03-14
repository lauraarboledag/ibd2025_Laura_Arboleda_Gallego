import sqlite3
import pandas as pd
import json
import os
import pandas as pd
from pathlib import Path
import unicodedata
import re

#Configuración preventiva de rutas

ruta_actual = str(Path.cwd())
ruta_db = f"{ruta_actual}/src/bigdata/static/db/ingesta.sqlite3"
ruta_excel = f"{ruta_actual}/src/bigdata/static/xlsx/export_books.xlsx"
ruta_csv = f"{ruta_actual}/src/bigdata/static/xlsx/export_books.csv"
ruta_auditoria = f"{ruta_actual}/src/bigdata/static/auditoria/auditoria.txt"


# Crear directorios si no existen
directorio_db = os.path.dirname(ruta_db)
directorio_xlsx = os.path.dirname(ruta_excel)
directorio_auditoria = os.path.dirname(ruta_auditoria)
os.makedirs(directorio_db, exist_ok=True)
os.makedirs(directorio_xlsx, exist_ok=True)
os.makedirs(directorio_auditoria, exist_ok=True)

#Llamada a la base de datos
conexion = sqlite3.connect(ruta_db)
cursor = conexion.cursor()

#Carga de las tablas

df_books = pd.read_sql_query("SELECT * FROM books", conexion)
df_authors = pd.read_sql_query("SELECT * FROM authors", conexion)
df_categories = pd.read_sql_query("SELECT * FROM categories", conexion)
df_books_authors = pd.read_sql_query("SELECT * FROM books_authors", conexion)
df_books_categories = pd.read_sql_query("SELECT * FROM books_categories", conexion)

conexion.close()

#Garantizar conexión

print("Libros:", df_books.head())
print("Autores:", df_authors.head())
print("Categorías:", df_categories.head())

#Limpieza de datos

# Eliminar valores nulos o vacíos en las columnas clave
df_books.dropna(subset=["id", "title"], inplace=True)
df_authors.dropna(subset=["id", "name"], inplace=True)
df_categories.dropna(subset=["id", "name"], inplace=True)

# Eliminar duplicados en autores y categorías
df_authors.drop_duplicates(subset=["name"], inplace=True)
df_categories.drop_duplicates(subset=["name"], inplace=True)

# Normalizar texto: Convertir nombres de autores y categorías a minúsculas y quitar espacios extras
df_authors["name"] = df_authors["name"].str.strip().str.lower()
df_categories["name"] = df_categories["name"].str.strip().str.lower()
df_books["title"] = df_books["title"].str.strip()

# Verificar integridad de relaciones
# Filtrar registros de books_authors que no tengan un libro o autor en las tablas principales
df_books_authors = df_books_authors[
    df_books_authors["book_id"].isin(df_books["id"]) &
    df_books_authors["author_id"].isin(df_authors["id"])
]

# Filtrar registros de books_categories que no tengan un libro o categoría en las tablas principales
df_books_categories = df_books_categories[
    df_books_categories["book_id"].isin(df_books["id"]) &
    df_books_categories["category_id"].isin(df_categories["id"])
]

# Normalizar nombres de autores con mayúscula inicial
df_authors["name"] = df_authors["name"].str.title()

# Normalizar nombres de autores con mayúscula inicial
df_categories["name"] = df_categories["name"].str.title()

# Función para limpiar y normalizar correctamente el texto

def normalizar_texto(texto):
    if isinstance(texto, str):  
        texto = texto.strip()  
        texto = unicodedata.normalize("NFC", texto) 
        texto = texto.lower().title() 
    return texto

# Aplicar la función de normalización a los títulos de los libros
df_books["title"] = df_books["title"].apply(normalizar_texto)

# Verificar si quedan títulos incorrectos
print(df_books["title"].unique())  

# Reemplazar valores vacíos o nulos en la columna 'description' por "Desconocido"
df_books["description"] = df_books["description"].fillna("Desconocido").replace(r'^\s*$', "Desconocido", regex=True)

# Verificar si quedan valores vacíos
print(df_books[df_books["description"] == "Desconocido"])

#Corregir valores de fecha por sólo el año

def extraer_anio(fecha):
    if pd.isna(fecha) or fecha in ["None", "null", ""]:
        return "Desconocido"
    match = re.search(r"\d{4}", str(fecha))  # Busca un patrón de 4 dígitos (año)
    return match.group(0) if match else "Desconocido"

# Aplicar la función a la columna
df_books["publishedDate"] = df_books["publishedDate"].apply(extraer_anio)

# Verificar resultados
print(df_books["publishedDate"].value_counts())

# Mostrar resultados de la limpieza
print("Limpieza completada")
print("Libros después de la limpieza:", df_books.shape)
print("Autores después de la limpieza:", df_authors.shape)
print("Categorías después de la limpieza:", df_categories.shape)
print("Fechas convertidas a solo año correctamente.")
print(df_books[["id", "title", "publishedDate"]].head())

#-------------Exportación de archivos a CSV--------------

# Definir rutas de exportación
ruta_salida = f"{ruta_actual}/src/bigdata/static/limpieza"
os.makedirs(ruta_salida, exist_ok=True)

# Exportar cada DataFrame a CSV
df_books.to_csv(f"{ruta_salida}/books.csv", index=False, encoding="utf-8")
df_authors.to_csv(f"{ruta_salida}/authors.csv", index=False, encoding="utf-8")
df_categories.to_csv(f"{ruta_salida}/categories.csv", index=False, encoding="utf-8")
df_books_authors.to_csv(f"{ruta_salida}/books_authors.csv", index=False, encoding="utf-8")
df_books_categories.to_csv(f"{ruta_salida}/books_categories.csv", index=False, encoding="utf-8")

print("Exportación completada. Archivos CSV generados en:", ruta_salida)




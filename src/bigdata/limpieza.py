import sqlite3
import pandas as pd
import json
import os
import pandas as pd
from pathlib import Path

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

# 1️⃣ Eliminar valores nulos o vacíos en las columnas clave
df_books.dropna(subset=["id", "title"], inplace=True)
df_authors.dropna(subset=["id", "name"], inplace=True)
df_categories.dropna(subset=["id", "name"], inplace=True)

# 2️⃣ Eliminar duplicados en autores y categorías
df_authors.drop_duplicates(subset=["name"], inplace=True)
df_categories.drop_duplicates(subset=["name"], inplace=True)

# 3️⃣ Normalizar texto: Convertir nombres de autores y categorías a minúsculas y quitar espacios extras
df_authors["name"] = df_authors["name"].str.strip().str.lower()
df_categories["name"] = df_categories["name"].str.strip().str.lower()
df_books["title"] = df_books["title"].str.strip()

# 4️⃣ Verificar integridad de relaciones
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

# Mostrar resultados de la limpieza
print("Limpieza completada")
print("Libros después de la limpieza:", df_books.shape)
print("Autores después de la limpieza:", df_authors.shape)
print("Categorías después de la limpieza:", df_categories.shape)

#Exportación de archivos a CSV


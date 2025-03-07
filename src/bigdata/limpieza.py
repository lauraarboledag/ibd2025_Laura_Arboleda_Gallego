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

print("Libros:", df_books.head())
print("Autores:", df_authors.head())
print("Categorías:", df_categories.head())
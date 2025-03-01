import requests
import sqlite3
import json
import os
import pandas as pd
from pathlib import Path

def obtener_datos_api(url="", params={}):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(f"Error al hacer la solicitud: {error}")
        return {}

# Configuración de rutas
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

# API de Google Books
url = "https://www.googleapis.com/books/v1/volumes"
parametros = {
    "q": "Juego de tronos",
    "maxResults": 10,
    "printType": "books"
}

datos = obtener_datos_api(url, parametros)

# Conectar a SQLite
conexion = sqlite3.connect(ruta_db)
cursor = conexion.cursor()

# Crear tablas
cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id TEXT PRIMARY KEY,
        title TEXT,
        description TEXT,
        publishedDate TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS books_authors (
        book_id TEXT,
        author_id INTEGER,
        PRIMARY KEY (book_id, author_id),
        FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE CASCADE,
        FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS books_categories (
        book_id TEXT,
        category_id INTEGER,
        PRIMARY KEY (book_id, category_id),
        FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE
    )
''')

# Contar registros antes de la inserción
cursor.execute("SELECT COUNT(*) FROM books")
registros_antes = cursor.fetchone()[0]

# Limpiar datos previos
cursor.execute("DELETE FROM books")
cursor.execute("DELETE FROM authors")
cursor.execute("DELETE FROM books_authors")
cursor.execute("DELETE FROM books_categories")
conexion.commit()

# Insertar datos de la API
datos_insertados = False
registros_insertados = 0
if datos and "items" in datos:
    for item in datos["items"]:
        try:
            book_id = item["id"]
            title = item["volumeInfo"].get("title", "Desconocido")
            description = item["volumeInfo"].get("description", "Desconocido")
            published_date = item["volumeInfo"].get("publishedDate", "Desconocido")
            
            cursor.execute('INSERT OR REPLACE INTO books (id, title, description, publishedDate) VALUES (?, ?, ?, ?)',
                           (book_id, title, description, published_date))
            registros_insertados += 1

            # Insertar autores
            authors = item["volumeInfo"].get("authors", [])
            for author in authors:
                cursor.execute('INSERT OR IGNORE INTO authors (name) VALUES (?)', (author,))
                cursor.execute('SELECT id FROM authors WHERE name = ?', (author,))
                author_id = cursor.fetchone()[0]
                cursor.execute('INSERT OR IGNORE INTO books_authors (book_id, author_id) VALUES (?, ?)',
                               (book_id, author_id))

            # Insertar categorías
            categories = item["volumeInfo"].get("categories", [])
            for category in categories:
                cursor.execute('INSERT OR IGNORE INTO categories (name) VALUES (?)', (category,))
                cursor.execute('SELECT id FROM categories WHERE name = ?', (category,))
                category_id = cursor.fetchone()[0]
                cursor.execute('INSERT OR IGNORE INTO books_categories (book_id, category_id) VALUES (?, ?)',
                               (book_id, category_id))
            
            datos_insertados = True
        except KeyError as e:
            print(f"Error al procesar un libro: {e}")

    conexion.commit()
    print("Datos insertados correctamente en SQLite.")
else:
    print("No se obtuvieron datos para insertar en la base de datos.")

# Contar registros después de la inserción
cursor.execute("SELECT COUNT(*) FROM books")
registros_despues = cursor.fetchone()[0]

# Exportar datos a Excel y CSV si hay datos
if datos_insertados:
    query = """
        SELECT b.id, b.title, b.description, b.publishedDate, GROUP_CONCAT(a.name, ', ') AS authors
        FROM books b
        LEFT JOIN books_authors ba ON b.id = ba.book_id
        LEFT JOIN authors a ON ba.author_id = a.id
        GROUP BY b.id
    """
    df_books = pd.read_sql_query(query, conexion)
    df_books.to_excel(ruta_excel, index=False)
    df_books.to_csv(ruta_csv, index=False)
    print("Datos exportados correctamente a Excel y CSV.")
else:
    print("No hay datos para exportar.")

# Generar informe de auditoría
with open(ruta_auditoria, "w") as auditoria:
    auditoria.write(f"Auditoría de extracción de datos\n")
    auditoria.write(f"---------------------------------\n")
    auditoria.write(f"Registros en BD antes de la extracción: {registros_antes}\n")
    auditoria.write(f"Registros extraídos de la API: {registros_insertados}\n")
    auditoria.write(f"Registros en BD después de la extracción: {registros_despues}\n")
    auditoria.write(f"Diferencia de registros antes y despues de la extracción: {registros_despues - registros_antes}\n")

print("Informe de auditoría generado correctamente.")

conexion.close()

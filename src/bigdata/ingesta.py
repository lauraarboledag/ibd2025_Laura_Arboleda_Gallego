import requests
import sqlite3
import json

def obtener_datos_api(url="", params={}):
    try:
        response = requests.get(url, params=params)  
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(f"Error al hacer la solicitud: {error}")
        return {}

# API de Google Books
url = "https://www.googleapis.com/books/v1/volumes"

# Parámetros para buscar libros
parametros = {
    "q": "Percy Jackson",   
    "maxResults": 20,     
    "printType": "books" 
}

# Llamada a la API
datos = obtener_datos_api(url, parametros)

# Mostrar resultados
if datos:
    print(json.dumps(datos, indent=4, ensure_ascii=False))
else:
    print("No se obtuvo respuesta de la API")

#Creación de la base de datos

conexion = sqlite3.connect("ingesta.db")
cursor = conexion.cursor()

# Creación de tablas

cursor.execute('''
    CREATE TABLE IF NOT EXISTS books (
        id TEXT PRIMARY KEY,
        title TEXT,
        authors TEXT,
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

#cursor.execute('''
    #CREATE TABLE IF NOT EXISTS books_authors (
        #book_id TEXT,
        #author_id INTEGER,
        #PRIMARY KEY (book_id, author_id),
        #FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE CASCADE,
        #FOREIGN KEY (author_id) REFERENCES authors (id) ON DELETE CASCADE
    #)
#''')

#cursor.execute('''
    #CREATE TABLE IF NOT EXISTS books_categories (
        #book_id TEXT,
        #category_id INTEGER,
        #PRIMARY KEY (book_id, category_id),
        #FOREIGN KEY (book_id) REFERENCES books (id) ON DELETE CASCADE,
        #FOREIGN KEY (category_id) REFERENCES categories (id) ON DELETE CASCADE
    #)
#''')


# Recolectar los datos desde la API y rellenar la base de datos
datos_db = obtener_datos_api(url, parametros)

if datos_db and "items" in datos_db:
    for item in datos_db["items"]:
        try:
            book_id = item["id"]
            title = item["volumeInfo"].get("title", "Desconocido")
            authors = ", ".join(item["volumeInfo"].get("authors", ["Desconocido"]))
            description = item["volumeInfo"].get("description", "Desconocido")
            published_date = item["volumeInfo"].get("publishedDate", "Desconocido")

            cursor.execute('INSERT OR IGNORE INTO books (id, title, authors, description, publishedDate) VALUES (?, ?, ?, ?, ?)',
                           (book_id, title, authors, description, published_date))
        except KeyError as e:
            print(f"Error al procesar un libro: {e}")

    # Guardar datos.
    conexion.commit()
    print("Datos insertados correctamente en SQLite.")
else:
    print("No se obtuvieron datos para insertar en la base de datos.")

conexion.close()
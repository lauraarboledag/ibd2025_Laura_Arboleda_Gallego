import sqlite3
import pandas as pd
from pathlib import Path

# Configuración de la ruta de la base de datos
ruta_actual = str(Path.cwd())
ruta_db = f"{ruta_actual}/src/bigdata/static/db/ingesta.sqlite3"

# Verificar si la base de datos existe
if not Path(ruta_db).is_file():
    print(f"Error: La base de datos no se encuentra en {ruta_db}")
else:
    # Conectar a SQLite
    conexion = sqlite3.connect(ruta_db)

    # Cargar datos de libros y autores
    query = """
        SELECT b.id, b.title, b.description, b.publishedDate, GROUP_CONCAT(a.name, ', ') AS authors
        FROM books b
        LEFT JOIN books_authors ba ON b.id = ba.book_id
        LEFT JOIN authors a ON ba.author_id = a.id
        GROUP BY b.id
    """
    df_books = pd.read_sql_query(query, conexion)

    # Seleccionar una muestra representativa (por ejemplo, las primeras 5 filas)
    df_sample = df_books.head(10)

    # Exportar a Excel y CSV
    ruta_excel = f"{ruta_actual}/src/bigdata/static/xlsx/export_books.xlsx"
    ruta_csv = f"{ruta_actual}/src/bigdata/static/xlsx/export_books.csv"
    df_sample.to_excel(ruta_excel, index=False)
    df_sample.to_csv(ruta_csv, index=False)

    print("Datos exportados correctamente a Excel y CSV.")

    # Cerrar conexión
    conexion.close()

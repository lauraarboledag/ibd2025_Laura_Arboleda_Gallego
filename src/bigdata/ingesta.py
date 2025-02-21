import requests
import json

def obtener_datos_api(url="", params={}):
    try:
        response = requests.get(url, params=params)  # Pasar params correctamente
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as error:
        print(f"Error al hacer la solicitud: {error}")
        return {}

# API de Google Books
url = "https://www.googleapis.com/books/v1/volumes"

# Parámetros para buscar libros
parametros = {
    "q": "harry potter",   
    "maxResults": 5,     
    "printType": "books" 
}

# Llamada a la API
datos = obtener_datos_api(url, parametros)

# Mostrar resultados
if datos:
    print(json.dumps(datos, indent=4, ensure_ascii=False))
else:
    print("No se obtuvo respuesta de la API")

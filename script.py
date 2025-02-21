import pandas as pd
import json 

def main():
    #Leer el archivo JSON:
    with open ('data.json', 'r', encoding= 'utf-8') as f:
        data = json.load(f)

    #Si el Json es un diccionario único, convertirlo en una lista:

    if isinstance (data, dict):
        data = [data]
    #Prueba 
    #Crear Dataframe en un archivo excel 

    df = pd.DataFrame(data)
    df.to_excel("output.xlsx", index=False)
    print("Archivo 'output.xlsx' generado exitosamente")

if __name__ == '__main__':
    main()
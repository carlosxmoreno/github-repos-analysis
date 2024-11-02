import json
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import time

# Definir la variable para la contraseña de la base de datos
db_password = "ContraY01"

# Fichero de datos
# Leer el archivo JSON desde el sistema de archivos
json_file_path = "/home/20839394Carlos/Datasets/df_clean.json"

# Configurar la URI de conexión usando la variable db_password
uri = f"mongodb+srv://carlosxmoreno:{db_password}@cluster0.pxgloyz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Crear un nuevo cliente y conectarse al servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Definir el nombre de la base de datos y la colección
db_name = "db_repos_analysis"
collection_name = "raw_data_json"

# Conectar a la base de datos y la colección
db = client[db_name]
collection = db[collection_name]

# Comenzar a medir el tiempo de ejecución
start_time = time.time()

# Cargar y procesar el archivo JSON
try:
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = [json.loads(line) for line in file]  # Si el archivo tiene múltiples líneas de JSON
        
        total_documents = len(data)  # Contar el total de documentos
        print(f"Total de documentos a insertar: {total_documents}")

        # Insertar los datos en la colección uno por uno para mostrar el progreso
        inserted_count = 0
        for i, document in enumerate(data):
            collection.insert_one(document)  # Insertar un único documento
            inserted_count += 1
            # Mostrar el progreso cada 10 documentos
            if inserted_count % 10 == 0 or inserted_count == total_documents:
                print(f"{inserted_count}/{total_documents} documentos insertados...")

        print(f"{inserted_count} documentos insertados exitosamente en la colección '{collection_name}' en la base de datos '{db_name}'")

except FileNotFoundError:
    print(f"No se encontró el archivo en la ruta: {json_file_path}")
except json.JSONDecodeError as e:
    print(f"Error al procesar el archivo JSON: {e}")
except Exception as e:
    print(f"Error al insertar los datos: {e}")

# Calcular el tiempo total de ejecución
end_time = time.time()
execution_time = end_time - start_time
print(f"Tiempo total de ejecución: {execution_time:.2f} segundos")

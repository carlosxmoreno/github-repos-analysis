from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# connection_string mongodb+srv://carlosxmoreno:ContraY01@cluster0.pxgloyz.mongodb.net/?authSource=admin

# Definir la variable para la contraseña de la base de datos
db_password = "ContraY01"

# Configurar la URI de conexión usando la variable db_password
uri = f"mongodb+srv://carlosxmoreno:{db_password}@cluster0.pxgloyz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Crear un nuevo cliente y conectarse al servidor
client = MongoClient(uri, server_api=ServerApi('1'))

# Enviar un ping para confirmar la conexión exitosa
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

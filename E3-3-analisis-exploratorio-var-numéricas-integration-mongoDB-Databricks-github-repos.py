# Databricks notebook source
# Instalar pymongo
!pip install pymongo

# COMMAND ----------

# Leer datos ya cargados en mongoDB, y analizar. Resultdos se insertan en mongoDB

# Importar las bibliotecas necesarias
import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Definir la variable para la contraseña de la base de datos
db_password = "ContraY01"

# Conectar a MongoDB
uri = f"mongodb+srv://carlosxmoreno:{db_password}@cluster0.pxgloyz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(uri, server_api=ServerApi('1'))

# Definir la base de datos y la colección de los datos originales
db_name = "db_repos_analysis"
collection_name = "raw_data_json"

# Conectar a la base de datos y la colección
db = client[db_name]
collection = db[collection_name]

# Leer los datos desde MongoDB en un DataFrame y convertir el campo _id a string
df_copy = pd.DataFrame(list(collection.find()))
df_copy['_id'] = df_copy['_id'].astype(str)

# Mostrar la estructura del DataFrame
display(df_copy)


# COMMAND ----------

# análisis de oultiers
# Seleccionar solo las columnas numéricas
numeric_columns = df_copy.select_dtypes(include=['number'])

# Definir el límite para detectar outliers
LIM = 3

# Función para detectar valores extremos graves en una columna
def count_extremes(column):
    Q1 = column.quantile(0.25)
    Q3 = column.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - LIM * IQR
    upper_bound = Q3 + LIM * IQR
    # Contar los valores extremos graves
    return ((column < lower_bound) | (column > upper_bound)).sum()

# Total de registros (filas)
total_registros = len(df_copy)

# Crear una lista para almacenar resultados
results = []

# Aplicar la función a todas las columnas numéricas y recopilar resultados
for column in numeric_columns.columns:
    extremes_count = count_extremes(numeric_columns[column])
    percentage = (extremes_count / total_registros) * 100
    results.append({
        'Atributo': column,
        'Total Outliers': extremes_count,
        'Porcentaje sobre Total de Registros (%)': round(percentage, 2)
    })

# Convertir la lista de resultados a un DataFrame
outliers_table = pd.DataFrame(results)

# Mostrar la tabla de outliers
display(outliers_table)

# COMMAND ----------

# Escritura de Resultados en MongoDB

# Definir la colección para almacenar los resultados de outliers
outliers_collection_name = "outliers_numeric_vars_analysis_results"

# Conectar a la colección donde se guardarán los resultados
outliers_collection = db[outliers_collection_name]

# Convertir la tabla de outliers a un formato que MongoDB puede entender
data_to_insert = outliers_table.to_dict(orient='records')

# Insertar los resultados en la colección
try:
    result = outliers_collection.insert_many(data_to_insert)
    print(f"Resultados de outliers insertados correctamente en la colección '{outliers_collection_name}'")
except Exception as e:
    print(f"Error al insertar los resultados: {e}")

# COMMAND ----------

# Esto es esútpido hacerlo así, pero es para comprobar que accede a resultados intermedios y los puede usar

collection_name = "outliers_numeric_vars_analysis_results"

# Conectar a la base de datos y la colección
db = client[db_name]
collection = db[collection_name]

# Leer los datos desde MongoDB en un DataFrame y convertir el campo _id a string
outliers_results_df = pd.DataFrame(list(collection.find()))
outliers_results_df['_id'] = outliers_results_df['_id'].astype(str)

# Importar las bibliotecas para visualización
import matplotlib.pyplot as plt
import seaborn as sns

# Configurar el estilo de seaborn
sns.set(style="whitegrid")

# Crear un gráfico de barras para visualizar el total de outliers por atributo
plt.figure(figsize=(12, 6))
sns.barplot(data=outliers_results_df, x='Atributo', y='Total Outliers', palette='viridis')

# Configurar el título y las etiquetas
plt.title('Total de Outliers por Atributo Numérico', fontsize=16)
plt.xlabel('Atributo', fontsize=14)
plt.ylabel('Total de Outliers', fontsize=14)
plt.xticks(rotation=45)  # Rotar las etiquetas del eje x para mejor visibilidad

# Mostrar el gráfico
plt.tight_layout()
plt.show()

# COMMAND ----------

# SQl sobre agregado en mongoDB

# Importar las librerías necesarias
from pyspark.sql import SparkSession
import pandas as pd
from pymongo import MongoClient

# Definir la contraseña de la base de datos
db_password = "ContraY01"

# Conectar a la base de datos MongoDB
uri = f"mongodb+srv://carlosxmoreno:{db_password}@cluster0.pxgloyz.mongodb.net/"
client = MongoClient(uri)

# Nombre de la base de datos y colección
db_name = "db_repos_analysis"
collection_name = "outliers_numeric_vars_analysis_results"

# Conectar a la base de datos y la colección
db = client[db_name]
collection = db[collection_name]

# Leer los datos desde MongoDB en un DataFrame y convertir el campo _id a string
outliers_results_df = pd.DataFrame(list(collection.find()))
outliers_results_df['_id'] = outliers_results_df['_id'].astype(str)

# Convertir el DataFrame de Pandas a un DataFrame de Spark
spark_df = spark.createDataFrame(outliers_results_df)

# Crear una vista temporal para ejecutar consultas SQL
spark_df.createOrReplaceTempView("outliers_table")

# Definir la consulta SQL
query = """
SELECT *
FROM outliers_table
WHERE `Total Outliers` > 20000
ORDER BY `Porcentaje sobre Total de Registros (%)` DESC
"""

# Ejecutar la consulta y mostrar los resultados
result = spark.sql(query)
result.show()


# COMMAND ----------



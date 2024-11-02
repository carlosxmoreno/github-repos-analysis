# Databricks notebook source
table = "githubarchive.year.2023"
project_id = "github-repo-analysis-392013"
parent_project_id = project_id
materialization_dataset = "dataset_repo_analysis"  # Usa solo el nombre del dataset

# Comprobar acceso con una consulta usando "query" y "materializationDataset"
df = spark.read.format("bigquery") \
    .option("query", f"SELECT * FROM `{table}` LIMIT 0") \
    .option("project", project_id) \
    .option("parentProject", parent_project_id) \
    .option("materializationDataset", materialization_dataset) \
    .load()

print("Acceso confirmado a la tabla, pero sin leer los datos.")

# COMMAND ----------

df.show()

# COMMAND ----------

# Esto no va a funcionar por el límite de quota en Google BigQuery

# Definir la consulta SQL
query = """
WITH repo_contributors AS (
  SELECT '2018' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2018`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
  UNION ALL
  SELECT '2019' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2019`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
  UNION ALL
  SELECT '2020' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2020`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
  UNION ALL
  SELECT '2021' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2021`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
  UNION ALL
  SELECT '2022' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2022`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
  UNION ALL
  SELECT '2023' AS year, repo.id AS repo_id, COUNT(DISTINCT actor.id) AS contributor_count
  FROM `githubarchive.year.2023`
  WHERE type = 'CreateEvent' AND public = TRUE
  GROUP BY repo_id
)
SELECT year, COUNT(*) AS repo_count
FROM repo_contributors
WHERE contributor_count <= 3
GROUP BY year
ORDER BY year;
"""

# Cargar los datos utilizando Spark y la consulta
project_id = "github-repo-analysis-392013"
parent_project_id = project_id
materialization_dataset = "dataset_repo_analysis"  # Usa solo el nombre del dataset

# Ejecutar la consulta
df = spark.read.format("bigquery") \
    .option("query", query) \
    .option("project", project_id) \
    .option("parentProject", parent_project_id) \
    .option("materializationDataset", materialization_dataset) \
    .load()

# Mostrar el DataFrame en el notebook
display(df)


# COMMAND ----------

# integración con MongoDB. Librerías
%pip install pymongo

# COMMAND ----------

# Probando a insertar en mongoDB una colección con el df
# Importar las bibliotecas necesarias
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Definir las credenciales de conexión
db_password = "ContraY01"
uri = f"mongodb+srv://carlosxmoreno:{db_password}@cluster0.pxgloyz.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Crear el cliente de MongoDB
client = MongoClient(uri, server_api=ServerApi('1'))

# Definir la base de datos y la colección
db_name = "db_repos_analysis"

# Definir el nombre de la colección basado en la consulta
collection_name = "repos_contributors_per_year_gharchive_2018-2023"

# Conectar a la base de datos y la colección
db = client[db_name]
collection = db[collection_name]

# Convertir el DataFrame a un formato que MongoDB pueda entender (lista de diccionarios)
data_to_insert = df.toPandas().to_dict(orient='records')

# Insertar los datos en la colección
try:
    result = collection.insert_many(data_to_insert)  # Insertar múltiples documentos
    print(f"Se insertaron {len(result.inserted_ids)} documentos en la colección '{collection_name}'.")

    # Verificar la inserción
    inserted_count = len(result.inserted_ids)
    if inserted_count == len(data_to_insert):
        print("Todos los documentos se insertaron correctamente.")
    else:
        print("No todos los documentos se insertaron correctamente.")

except Exception as e:
    print(f"Error al insertar los datos en MongoDB: {e}")


# COMMAND ----------



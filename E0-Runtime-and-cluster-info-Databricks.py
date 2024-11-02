# Databricks notebook source
# Obtener la versión del Databricks Runtime desde la configuración de Spark
runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "Desconocido")

# Mostrar la versión
print(f"Databricks Runtime Version: {runtime_version}")


# Obtener el proveedor de infraestructura desde la configuración de Spark
cloud_provider = spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider", "Desconocido")

# Mostrar el proveedor
print(f"Cloud Provider: {cloud_provider}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.getOrCreate()

# Obtener la información básica del cluster a través de la SparkContext
sc = spark.sparkContext

# Obtener el nombre del cluster
cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "Desconocido")

# Obtener el proveedor de infraestructura (AWS, Azure, GCP)
cloud_provider = spark.conf.get("spark.databricks.clusterUsageTags.cloudProvider", "Desconocido")

# Extraer la versión del Databricks Runtime
runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "Desconocido")

# Extraer la versión de Scala
scala_version = runtime_version.split('-')[-1] if runtime_version else "Desconocido"

# Obtener información de los nodos
num_executors = sc.defaultParallelism  # Número de particiones, que normalmente corresponde a los ejecutores
node_type = spark.conf.get("spark.databricks.clusterUsageTags.nodeType", "Desconocido")
driver_node_type = spark.conf.get("spark.databricks.clusterUsageTags.driverNodeType", "Desconocido")
cluster_mode = spark.conf.get("spark.databricks.clusterUsageTags.clusterType", "Desconocido")

# Mostrar la información del cluster
print(f"Cluster Name: {cluster_name}")
print(f"Cloud Provider: {cloud_provider}")
print(f"Runtime Version: {runtime_version}")
print(f"Scala Version: {scala_version}")
print(f"Number of Executors: {num_executors}")  # Este es un estimado
print(f"Node Type for Workers: {node_type}")
print(f"Driver Node Type: {driver_node_type}")
print(f"Cluster Mode: {cluster_mode}")


# COMMAND ----------



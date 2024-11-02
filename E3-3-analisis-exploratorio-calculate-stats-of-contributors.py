"""
Script para calcular sctats de los repos en el dataset de análisis
"""

import json
import numpy as np
import csv

# Archivos de entrada y salida
data_file = 'merge-xdata-with-contributors-v2.json'
output_csv_file = 'contributors_stats.csv'

# Lista para almacenar el número de contribuyentes y nombres de repositorios
contributors_counts = []
repos = []

# Leer el archivo JSON y extraer los datos de contribuyentes
with open(data_file, 'r') as f:
    for line in f:
        line = line.strip()
        if line:  # Ignorar líneas vacías
            repo = json.loads(line)
            # Añadir el número de contribuyentes a la lista
            if 'contributors' in repo:
                contributors_counts.append(repo['contributors'])
                repos.append(f"{repo['owner']['login']}/{repo['name']}")  # Formato 'owner/name'

# Calcular estadísticas
if contributors_counts:
    mean = np.mean(contributors_counts)
    median = np.median(contributors_counts)
    variance = np.var(contributors_counts)
    std_dev = np.std(contributors_counts)
    minimum = np.min(contributors_counts)
    maximum = np.max(contributors_counts)
    q1 = np.percentile(contributors_counts, 25)
    q2 = np.median(contributors_counts)  # Mediana (también Q2)
    q3 = np.percentile(contributors_counts, 75)

    # Imprimir estadísticas
    print(f"Número total de repositorios: {len(contributors_counts)}")
    print(f"Media: {mean:.2f}")
    print(f"Mediana: {median:.2f}")
    print(f"Varianza: {variance:.2f}")
    print(f"Desviación estándar: {std_dev:.2f}")
    print(f"Mínimo: {minimum}")
    print(f"Máximo: {maximum}")
    print(f"Q1 (25th percentile): {q1:.2f}")
    print(f"Q2 (50th percentile / Mediana): {q2:.2f}")
    print(f"Q3 (75th percentile): {q3:.2f}")

    # Escribir datos a un archivo CSV auxiliar
    with open(output_csv_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Repository', 'Contributors'])  # Escribir cabecera
        for repo, contributors in zip(repos, contributors_counts):
            csv_writer.writerow([repo, contributors])

    print(f"Datos escritos en el archivo '{output_csv_file}'.")
else:
    print("No se encontraron datos.")

"""
Script que fusiona dataset de metadata de repos conseguido con graphQL (sin contributors) con el número de contributors de cada repo (REST API)
Lee de ficheros con repos leidos y procesados previemente por otros scripts, y realiza un merge
"""

import json
import csv
import os

# Archivos de entrada y salida
json_ampliado_file = 'xmetadata.json'  # Archivo JSON ampliado sin el campo contributors
json_original_file = 'xmerge-metadata-original.json'  # Archivo JSON original con el campo contributors
csv_file = 'get_contributors.csv'  # Archivo CSV con contributors

# Configuración de logging y fichero de salid
script_name = os.path.splitext(os.path.basename(__file__))[0]
log_file = f"{script_name}.log"
output_file = f"{script_name}.json"  # Archivo de salida

# Leer los datos de contribuyentes desde el archivo CSV
contributors_data = {}
with open(csv_file, newline='') as cf:
    reader = csv.DictReader(cf)
    for row in reader:
        # Almacenar el número de contribuyentes para cada repositorio
        contributors_data[row['Repository']] = int(row['Contributors'])

# Leer los datos del archivo JSON original en un diccionario para acceso rápido
original_repos = {}
with open(json_original_file, 'r') as json_original:
    for line in json_original:
        line = line.strip()
        if line:  # Si la línea no está vacía
            repo = json.loads(line)
            # Crear el nombre del repo en formato 'owner/name'
            repo_name = f"{repo['owner']['login']}/{repo['name']}"
            original_repos[repo_name] = repo  # Guardar el repositorio en el diccionario

# Preparar el archivo de salida
with open(output_file, 'w') as outfile:
    # Leer el archivo JSON ampliado y procesar cada repositorio
    with open(json_ampliado_file, 'r') as json_ampliado:
        for line in json_ampliado:
            line = line.strip()
            if line:  # Si la línea no está vacía
                repo = json.loads(line)
                # Crear el nombre del repo en formato 'owner/name'
                repo_name = f"{repo['owner']['login']}/{repo['name']}"
                
                # Verificar si el repositorio tiene contribuyentes en el CSV
                if repo_name in contributors_data:
                    repo['contributors'] = contributors_data[repo_name]
                # Si no se encuentra en el CSV, buscar en el JSON original
                elif repo_name in original_repos:
                    # Tomar el registro del JSON original y añadirlo al archivo de salida
                    repo['contributors'] = original_repos[repo_name]['contributors']
                else:
                    # Si el repositorio no está en ninguno de los archivos, continuar
                    print(f"No se encontró información para el repositorio: {repo_name}")
                    continue
                
                # Escribir el repositorio modificado en el archivo de salida
                outfile.write(json.dumps(repo) + '\n')

print(f"Proceso completado. Los datos se han guardado en '{output_file}'.")

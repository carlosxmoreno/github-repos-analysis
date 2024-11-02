"""
script para conseguir una muestra de repos en un periodo de años y contabilizar el número de contributors en esos repos
Utiliza query graphQL a github para repos
Utiliza REST API para contributors por cada repo

El limite de número de repos es define por la API (1000 a fecha de ejecución)
"""

import requests
import time
import logging
import csv
import os

# Configuración de logging
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_FILE = f'{script_name}.log'
RESULT_FILE = f'{script_name}.csv'

logger = logging.getLogger()
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(formatter)

file_handler = logging.FileHandler(LOG_FILE)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Configuración de la API de GitHub
GITHUB_TOKEN = open("github_token").read().strip()
HEADERS = {'Authorization': f'token {GITHUB_TOKEN}'}


def get_repos_and_contributors(year):
    """Obtiene los repositorios y contribuidores del año especificado, respetando el rate limit."""
    query = f"created:{year}-01-01..{year}-12-31 is:public language:*"
    page = 1
    with open(RESULT_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        while True:
            try:
                url = f"https://api.github.com/search/repositories?q={query}&per_page=100&page={page}"
                response = requests.get(url, headers=HEADERS)
                response.raise_for_status()  # Lanzar excepción si hay error HTTP
                repos = response.json().get('items', [])
                if not repos:  # Salir si no hay repositorios
                    break
                
                for repo in repos:
                    owner = repo['owner']['login']
                    name = repo['name']
                    contributors_count = get_contributors(owner, name)
                    writer.writerow([year, name, contributors_count])
                    logger.info(f"{year} - {owner}/{name}: {contributors_count} contribuidores guardados.")
                
                # Verificar rate limit
                respect_rate_limit(response)
                
                page += 1
            except requests.exceptions.RequestException as e:
                logger.error(f"Error al obtener repositorios en la página {page}: {e}")
                break

def get_contributors(owner, repo):
    """Obtiene el número de contribuidores de un repositorio."""
    contributors_count = 0
    page = 1
    while True:
        try:
            url = f"https://api.github.com/repos/{owner}/{repo}/contributors?per_page=100&page={page}"
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()  # Lanzar excepción si hay error HTTP
            contributors = response.json()
            if not contributors:
                break

            contributors_count += len(contributors)

            # Verificar rate limit
            respect_rate_limit(response)

            page += 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener contribuidores de {owner}/{repo}: {e}")
            break
    return contributors_count

def respect_rate_limit(response):
    """Verifica si se alcanzó el rate limit y espera hasta que se pueda hacer una nueva solicitud."""
    remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
    if remaining == 0:
        reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
        wait_time = max(0, reset_time - time.time())
        logger.warning(f"Rate limit alcanzado. Esperando {wait_time:.2f} segundos.")
        time.sleep(wait_time)

def main():
    # Crear archivo CSV con encabezados
    with open(RESULT_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Año", "Repositorio", "Contribuidores"])

    # Procesar años del 2018 al 2023
    for year in range(2018, 2024):
        logger.info(f"Procesando repositorios del año {year}...")
        get_repos_and_contributors(year)

if __name__ == "__main__":
    main()

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
from google.cloud import bigquery
import base64

# Configuración de BigQuery
BQ_DATASET = 'dataset'  # Nombre del dataset en BigQuery
BQ_PROJECT = 'proyecto_id'  # ID del proyecto en Google Cloud

# Tokens de acceso y refresh para la API de Spotify
ACCESS_TOKEN = 'tu_token_de_acceso'
REFRESH_TOKEN = 'tu_token_actualizado'

# Credenciales de la API de Spotify
CLIENT_ID = 'tu_cliente_id'  # Reemplaza con tu CLIENT_ID
CLIENT_SECRET = 'tu_cliente_secreto'  # Reemplaza con tu CLIENT_SECRET

def refresh_access_token():
    """Refresca el token de acceso utilizando el refresh token."""
    global ACCESS_TOKEN, REFRESH_TOKEN
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode(),
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': REFRESH_TOKEN,
    }
    response = requests.post(url, headers=headers, data=data)
    
    if response.status_code == 200:
        token_info = response.json()
        ACCESS_TOKEN = token_info['access_token']
        REFRESH_TOKEN = token_info.get('refresh_token', REFRESH_TOKEN)  # Actualiza el refresh token si se proporciona
    else:
        raise Exception(f'Error refreshing token: {response.status_code}, {response.text}')

def clear_table():
    """Limpia la tabla en BigQuery borrándola y recreándola."""
    bq_client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.top_artists_raw"

    # Borrar la tabla si existe
    bq_client.delete_table(table_id, not_found_ok=True)
    
    # Esquema de la tabla a crear
    schema = [
        bigquery.SchemaField("Rank", "INTEGER"),
        bigquery.SchemaField("Artist_Name", "STRING"),
        bigquery.SchemaField("Genres", "STRING"),
        bigquery.SchemaField("Popularity", "INTEGER"),
        bigquery.SchemaField("Followers", "INTEGER"),
        bigquery.SchemaField("External_URL", "STRING"),
        bigquery.SchemaField("Image_URL", "STRING"),
        bigquery.SchemaField("Load_Date", "TIMESTAMP"),  # Añadida la columna de fecha de carga
    ]
    
    # Crear la nueva tabla
    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)
    print(f"Table {table_id} created.")

def get_top_artists_to_bigquery():
    """Obtiene los artistas más escuchados de mi cuenta Spotify y los carga en BigQuery."""
    refresh_access_token()  # Refresca el token antes de hacer la solicitud
    
    # Configura la URL para obtener los artistas más escuchados
    url = 'https://api.spotify.com/v1/me/top/artists?limit=20&time_range=short_term'
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        results = response.json()
        
        artists = []
        load_date = datetime.now()  # Obtener la fecha y hora actual
        for idx, artist in enumerate(results['items']):
            artists.append({
                'Rank': idx + 1,
                'Artist_Name': artist['name'],
                'Genres': ', '.join(artist['genres']),
                'Popularity': artist['popularity'],
                'Followers': artist['followers']['total'],
                'External_URL': artist['external_urls']['spotify'],
                'Image_URL': artist['images'][0]['url'] if artist['images'] else None,
                'Load_Date': load_date,  # Añadiendo la fecha de carga
            })
        
        df = pd.DataFrame(artists)

        # Asegurarte de que Load_Date sea del tipo correcto
        df['Load_Date'] = pd.to_datetime(df['Load_Date'])  # Convertir a datetime

        # Cargar el DataFrame en BigQuery
        bq_client = bigquery.Client()
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.top_artists_raw"
        job = bq_client.load_table_from_dataframe(df, table_id)
        job.result()  # Espera a que el trabajo se complete
        print(f"Loaded {job.output_rows} rows into {table_id}.")
    else:
        print(f'Error: {response.status_code}, {response.text}')

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spotify_top_artists_bigquery',
    default_args=default_args,
    description='DAG para guardar los artistas más escuchados de Spotify en BigQuery',
    schedule_interval='0 13 * * *',  # A las 10 AM todos los días
)

# Crear las tareas
clear_table_task = PythonOperator(
    task_id='clear_table',
    python_callable=clear_table,
    dag=dag,
)

top_artists_task = PythonOperator(
    task_id='get_top_artists_to_bigquery',
    python_callable=get_top_artists_to_bigquery,
    dag=dag,
)

# Definir el orden de las tareas
clear_table_task >> top_artists_task  # Primero limpiar la tabla, luego cargar los datos

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
from google.cloud import bigquery
import base64

# Configuración de BigQuery
BQ_DATASET = 'dataset'
BQ_PROJECT = 'proyecto_id'

# Tokens de acceso y refresh para la API de Spotify
ACCESS_TOKEN = 'tu_token_de_acceso'
CLIENT_ID = 'tu_cliente_id'  # Reemplaza con tu CLIENT_ID
CLIENT_SECRET = 'tu_cliente_secreto'  # Reemplaza con tu CLIENT_SECRET

def refresh_access_token():
    """Refresca el token de acceso utilizando el refresh token."""
    global ACCESS_TOKEN
    url = 'https://accounts.spotify.com/api/token'
    headers = {
        'Authorization': 'Basic ' + base64.b64encode(f'{CLIENT_ID}:{CLIENT_SECRET}'.encode()).decode(),
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    data = {
        'grant_type': 'client_credentials',  # Usar client_credentials para este caso
    }
    response = requests.post(url, headers=headers, data=data)

    if response.status_code == 200:
        token_info = response.json()
        ACCESS_TOKEN = token_info['access_token']
    else:
        raise Exception(f'Error refreshing token: {response.status_code}, {response.text}')

def clear_table():
    """Limpia la tabla en BigQuery borrándola y recreándola."""
    bq_client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.top_songs_argentina_raw"

    # Verificar si la tabla ya existe
    try:
        bq_client.get_table(table_id)  # Intenta obtener la tabla
        table_exists = True
    except Exception:
        table_exists = False

    if table_exists:
        # Borrar la tabla si existe
        bq_client.delete_table(table_id, not_found_ok=True)
        print(f"Table {table_id} deleted.")

    # Esquema de la tabla a crear
    schema = [
        bigquery.SchemaField("Rank", "INTEGER"),
        bigquery.SchemaField("Song_Name", "STRING"),
        bigquery.SchemaField("Artist", "STRING"),
        bigquery.SchemaField("Album", "STRING"),
        bigquery.SchemaField("Release_Date", "STRING"),
        bigquery.SchemaField("Popularity", "INTEGER"),
        bigquery.SchemaField("Duration_ms", "INTEGER"),
        bigquery.SchemaField("External_URL", "STRING"),
        bigquery.SchemaField("Load_Date", "TIMESTAMP"),  # Añadida la columna de fecha de carga
        bigquery.SchemaField("Album_Image", "STRING"),
    ]
    
    # Crear la nueva tabla con el esquema definido
    table = bigquery.Table(table_id, schema=schema)
    bq_client.create_table(table)
    print(f"Table {table_id} created.")

def get_top_songs_argentina_to_bigquery():
    """Obtiene las canciones más escuchadas en Argentina y las carga en BigQuery."""
    refresh_access_token()  # Refresca el token antes de hacer la solicitud
    
    # URL de la playlist de las canciones más populares en Argentina
    url = 'https://api.spotify.com/v1/playlists/37i9dQZEVXbMMy2roB9myp'  
    headers = {
        'Authorization': f'Bearer {ACCESS_TOKEN}',
        'Content-Type': 'application/json',
    }

    # Realiza la solicitud a la API de Spotify
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        results = response.json()
        songs = []
        load_date = datetime.now()  # Cambiado a datetime, no a ISO string
        for idx, track in enumerate(results['tracks']['items']):
            songs.append({
                'Rank': idx + 1,
                'Song_Name': track['track']['name'],
                'Artist': ', '.join(artist['name'] for artist in track['track']['artists']),
                'Album': track['track']['album']['name'],
                'Release_Date': track['track']['album']['release_date'],
                'Popularity': track['track']['popularity'],
                'Duration_ms': track['track']['duration_ms'],
                'External_URL': track['track']['external_urls']['spotify'],
                'Load_Date': load_date,  # Ahora es un objeto datetime
                'Album_Image': track['track']['album']['images'][0]['url'],
            })

        df = pd.DataFrame(songs)
        
        # Convertir Load_Date a tipo datetime
        df['Load_Date'] = pd.to_datetime(df['Load_Date'])  # Asegúrate de que Load_Date sea del tipo correcto

        # Cargar el DataFrame a BigQuery
        bq_client = bigquery.Client()
        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.top_songs_argentina_raw"

        # Cargar datos a BigQuery
        job = bq_client.load_table_from_dataframe(df, table_id, job_id_prefix='load_job_')
        job.result()  # Espera a que el trabajo se complete
        print(f"Loaded {job.output_rows} rows into {table_id}.")
    else:
        print(f'Error: {response.status_code}, {response.text}')

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 25),  # Fecha de inicio del DAG
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # Número de reintentos en caso de falla
    'retry_delay': timedelta(minutes=5),  # Tiempo de espera entre reintentos
}

# Definición del DAG
dag = DAG(
    'spotify_top_tracks_argentina_bigquery',
    default_args=default_args,
    description='DAG para guardar el top de canciones en Argentina en BigQuery',
    schedule_interval='0 13 * * *',  # A las 10 AM todos los días
)

# Crear las tareas
clear_table_task = PythonOperator(
    task_id='clear_table',  # Tarea para limpiar la tabla
    python_callable=clear_table,
    dag=dag,
)

top_songs_argentina_task = PythonOperator(
    task_id='get_top_songs_argentina_to_bigquery',  # Tarea para obtener y cargar las canciones
    python_callable=get_top_songs_argentina_to_bigquery,
    dag=dag,
)

# Definir el orden de las tareas
clear_table_task >> top_songs_argentina_task  # Primero limpiar la tabla, luego cargar los datos

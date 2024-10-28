# Proyecto de Integración de Datos con Airflow y Spotify API

Este proyecto utiliza **Apache Airflow** dockerizado para automatizar la extracción de datos de la API de **Spotify**, cargarlos a **BigQuery** y procesarlos en **Pandas** para obtener insights que finalmente se visualizan en **Looker**. El objetivo principal fue capturar mis artistas y canciones más escuchadas, junto con el top 50 de canciones en Argentina, para luego analizarlas y visualizarlas en un tablero de Looker.

## Índice

1. [Descripción del Proyecto](#descripción-del-proyecto)
2. [Objetivos](#objetivos)
3. [Arquitectura del Proyecto](#arquitectura-del-proyecto)
4. [Configuración y Entorno](#configuración-y-entorno)
5. [Extracción de Datos con Airflow](#extracción-de-datos-con-airflow)
6. [Transformación de Datos con Pandas](#transformación-de-datos-con-pandas)
7. [Cargas de Datos en BigQuery](#cargas-de-datos-en-bigquery)
8. [Visualización en Looker](#visualización-en-looker)
9. [Capturas de Pantalla](#capturas-de-pantalla)
10. [Conclusiones](#conclusiones)

---
### Descripción del Proyecto

Este proyecto integra varios servicios en un flujo de trabajo automatizado para realizar las siguientes tareas:

1. Extracción de datos mediante **API de Spotify** con Airflow.
2. Carga inicial de datos en **BigQuery** en un esquema crudo (*raw*) en las tablas.
3. Transformación y procesamiento de datos en **Pandas** para limpiar y organizar la información en tablas finales en BigQuery.
4. Visualización de resultados en **Looker** para crear un tablero de análisis.

### Objetivos

- Automatizar la extracción de datos desde la API de Spotify.
- Almacenar y estructurar los datos en BigQuery para su posterior procesamiento.
- Realizar transformaciones y análisis de los datos con Pandas.
- Crear un tablero visual y accesible en Looker para presentar los datos de manera clara y amigable.

### Arquitectura del Proyecto

La arquitectura del proyecto sigue el flujo de ETL (Extracción, Transformación, Carga):

1. **Extracción**: Se utiliza un DAG en Airflow para conectarse a la API de Spotify y descargar datos sobre las canciones y artistas.
2. **Transformación**: Los datos crudos se procesan en un Jupyter Notebook usando Pandas para limpiar y organizar la información.
3. **Carga**: Se cargan los datos transformados nuevamente en BigQuery, listos para el análisis en Looker.

**Diagrama de Arquitectura:**
![Diagrama](img\Diagrama_arquitactura.png)

### Configuración y Entorno

Este proyecto utiliza **Docker** para mantener un entorno controlado, y **Airflow** para la orquestación de tareas. La configuración básica del entorno incluye:

- **Docker Compose**: Configura contenedores para Airflow, PostgreSQL, y BigQuery.
- **Spotify API**: Se necesita una clave de acceso para obtener los datos (asegúrate de tener tus credenciales CLIENT_ID, CLIENT_SECRET, URI, ACCESS_TOKEN). Para poder trabajar con el proyecto **Spotify Data Pipeline**, es necesario realizar las configuraciones en tu cuenta de desarrollador de Spotify (Client ID, App Status, App Name, etc.)
- **Airflow**: Configuración de DAGs para la automatización de las tareas de extracción y carga. 

**Importante:** Todo el desarrollo se ha realizado localmente, así que asegúrate de tener el entorno configurado correctamente antes de ejecutar el proyecto.

#### Instrucciones de Configuración:

1. Clonar el repositorio y extraer los archivos de la carpeta.
2. Crear un archivo `.env` con tus credenciales de Spotify y BigQuery.
3. Ejecutar el comando `docker-compose up -d` para iniciar los contenedores.
4. Una vez que Airflow esté en ejecución, ir a Admin -> Connections y agregar la conexión a BigQuery completando todos los campos requeridos.
5. Ejecutar el archivo `get_spotify_token.py` para obtener el token de acceso y refresh token de Spotify. Reemplazar estos tokens en el código del DAG para asegurar la renovación automática sin necesidad de actualización manual cada 40 minutos.
6. Ejecutar manualmente los DAG por primera vez y activarlos para que se ejecuten diariamente a las 10 a.m.
7. (Opcional) Ejecutar el notebook de pandas para transformar las tablas cargadas por los DAG y volver a subir las tablas limpias a BigQuery.

### Extracción de Datos con Airflow

Para extraer los datos, creé varios DAGs en Airflow:

1. **Extracción de Artistas y Canciones Más Escuchadas**: Utiliza la API de Spotify para obtener mis artistas y canciones más escuchadas.
2. **Top 50 de Canciones en Argentina**: Extrae el top 50 de canciones populares en Argentina.

Cada DAG se ejecuta periódicamente para mantener la base de datos actualizada con la última información disponible en Spotify.

```python
# Ejemplo de un DAG en Airflow
with DAG('spotify_top_tracks',
         default_args=default_args,
         description='Extracción de Top 50 Canciones Argentina',
         schedule_interval='@daily') as dag:
    
    # Task 1: Conectar con API de Spotify
    ...
    
    # Task 2: Cargar datos en BigQuery
    ...
```

### Transformación de Datos con Pandas

Una vez cargados los datos en su forma cruda (raw) en BigQuery, se utilizó un Jupyter Notebook para procesar la información y realizar las siguientes transformaciones:

- **Creación de categorias**
- **Normalización de seguidores**
- **Cambio de zona horaria de fecha carga de tablas.**

#### Ejemplo de Código en Pandas

```python
bins = [0, 20, 50, 80, 100]
labels = ['Baja', 'Media', 'Alta', 'Muy Alta']
df_tracks['Popularidad_Categorizada'] = pd.cut(df_tracks['Popularidad'], bins=bins, labels=labels)
df_tracks
```

### Carga de datos en BigQuery

Los datos transformados se cargan nuevamente en BigQuery, listos para ser utilizados en Looker. Esto se realiza mediante el siguiente script en el notebook:

```python
# Cargar el DataFrame a BigQuery, reemplazando si la tabla ya existe
df_tracks.to_gbq(
    destination_table='rock-atlas-435913-t7.dataset_spotify.top_tracks', 
    project_id='rock-atlas-435913-t7', 
    if_exists='replace'  # 'replace' creará la tabla si no existe
)
```

### Visualización en Looker

Se creó un tablero en Looker para visualizar los datos transformados. El tablero incluye métricas clave, como los artistas más escuchados y las canciones más populares en Argentina.

- **Link del Tablero:** [Tablero en Looker](https://lookerstudio.google.com/reporting/0be178a7-92f0-42d2-9e6b-a00e9d6b8042)

### Capturas de Pantalla

1. **DAGs en Airflow**
![DAGs_en_airflow](img\DAG.png)
![DAG_detalles](img\DAG_Detalles.png)

2. **Notebook de Transformación en Pandas**
![Notebook_pandas](img\Notebook_pandas.png)

3. **Tablero en Looker**
Página 1: Top Songs
En esta sección se presenta la canción más reproducida junto con el ranking de las canciones más populares. Los gráficos incluyen:
- Gráfico de barras vertical: muestra la distribución de canciones según el año de lanzamiento.
- Gráfico de dispersión de popularidad: visualiza la popularidad normalizada de las canciones y las clasifica en distintas categorías. Categorías de Popularidad: 1 = Muy alta, 2 = Alta, 3 = Media.
- Gráfico de torta: analiza el porcentaje de canciones explícitas versus no explícitas.
- Gráfico de barras: compara la popularidad por artista, incluyendo una barra que indica el promedio general.
![Top_Songs](img\Top_Songs.png)

Página 2: Top Artists
Esta sección presenta información sobre los artistas más escuchados, incluyendo:
Artista más escuchado: destaca al artista con mayor número de reproducciones.
- Gráfico de barras vertical: muestra la popularidad de cada artista.
- Ranking de artistas más reproducidos: lista los artistas con más reproducciones.
- Gráfico de barras: ilustra el porcentaje de géneros más escuchados según cada artista.
- Gráfico de dispersión: analiza la relación entre la popularidad de los artistas y su número de seguidores.
- Gráfico de burbujas: representa la relación entre la popularidad de los artistas y la cantidad de géneros que interpretan.
![Top_Artists](img\Top_Artists.png)

Página 3: Top global 50 Argentina
En esta sección se presenta:
- Top 50: un listado de las 50 canciones más populares en Argentina.
- Top 3 canciones más populares: destaca las tres canciones que han alcanzado mayor popularidad en el país.
![Top_Global](img\Top_global_50_Argentina.png)

### Conclusiones

Este proyecto demuestra el poder de la integración de múltiples tecnologías modernas, como Apache Airflow, Docker, Python, Pandas, BigQuery y APIs. La automatización de la ingesta y el procesamiento de datos a través de Airflow no solo optimiza el flujo de trabajo, sino que también garantiza la consistencia y la puntualidad de la información extraída de la API de Spotify.

El uso de Docker proporciona un entorno controlado y reproducible que facilita la implementación y la escalabilidad de las aplicaciones, mientras que Python y Pandas ofrecen herramientas potentes para el análisis y la transformación de datos, permitiendo obtener insights significativos de manera eficiente.

Al almacenar los datos en BigQuery, se aprovechan las capacidades de análisis en tiempo real y la potencia de procesamiento de Google Cloud, lo que permite a los usuarios acceder y visualizar rápidamente la información a través de Looker.

En conjunto, estas tecnologías no solo optimizan la toma de decisiones basadas en datos, sino que también permiten construir soluciones robustas y escalables que se adaptan a las necesidades cambiantes del negocio, facilitando una analítica avanzada y proactiva en el análisis de tendencias musicales.



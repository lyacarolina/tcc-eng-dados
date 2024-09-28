from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import requests
import zipfile
import os
import pandas as pd
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'pipeline_climaticas',
    default_args=default_args,
    description='Pipeline Climaticas Bronze-Silver-Gold',
    schedule_interval='@monthly',  # Para rodar todo mês
    start_date=days_ago(1),
    catchup=False
)

PROJECT_ID = 'tcc-pos-eng-dados'
BUCKET_NAME = 'tcc_eng_dados'
TABLE_ID = 'tcc-pos-eng-dados.dados_meteorologicos.media_climaticas'
KEYFILE_PATH = '/tmp/keyfile.json'

def get_previous_month():
    today = datetime.today()
    first = today.replace(day=1)
    previous_month = first - timedelta(days=1)
    return previous_month.strftime("%Y-%m")

def bronze_to_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)

    previous_month = get_previous_month()
    year, month = previous_month.split("-")

    # URLs para os anos de 2020 até o ano atual
    base_url = 'https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip'
    current_year = int(year)
    years = range(2020, current_year + 1)
    urls = [base_url.format(year) for year in years]

    # Diretório para extrair os arquivos
    extracted_dir = '/tmp/extracted'

    # Cria o diretório se não existir
    if not os.path.exists(extracted_dir):
        os.makedirs(extracted_dir)

    def download_and_extract_zip(url, bucket):
        local_filename = url.split('/')[-1]
        local_path = os.path.join(extracted_dir, local_filename)

        print(f"Downloading {url} to {local_path}")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        print(f"Extracting {local_path} to {extracted_dir}")
        with zipfile.ZipFile(local_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_dir)

        for root, _, files in os.walk(extracted_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                if os.path.exists(local_file_path):
                    blob = bucket.blob(f"0. land/{file}")
                    blob.upload_from_filename(local_file_path)
                    os.remove(local_file_path)  # Remove o arquivo após upload

        if os.path.exists(local_path):
            os.remove(local_path)  # Remove o arquivo zip após extração

    for url in urls:
        print(f"Processing URL: {url}")
        download_and_extract_zip(url, bucket)

    def format_date(date):
        return date.strftime('%d-%m-%Y')

    cidade_fixa = 'SE_SP_A713_SOROCABA'
    data_inicio = datetime.strptime('01-01-2020', '%d-%m-%Y')
    data_fim = datetime.strptime(f'{year}-{month}-01', '%Y-%m')
    data_fim = data_fim + timedelta(days=31)
    data_fim = data_fim.replace(day=1) - timedelta(days=1)

    file_path_template = 'gs://tcc_eng_dados/0. land/INMET_{CIDADE}_{DATA_INICIO}_A_{DATA_FIM}.CSV'
    df_list = []

    current_date = data_inicio
    while current_date <= data_fim:
        if current_date.year == data_fim.year:
            next_year_date = data_fim
        else:
            next_year_date = datetime(current_date.year, 12, 31)

        file_path = file_path_template.format(
            CIDADE=cidade_fixa,
            DATA_INICIO=format_date(current_date),
            DATA_FIM=format_date(next_year_date)
        )

        blob_name = f"0. land/INMET_{cidade_fixa}_{format_date(current_date)}_A_{format_date(next_year_date)}.CSV"
        blob = bucket.blob(blob_name)

        if blob.exists():
            print(f"File found: {file_path}")
            try:
                df = pd.read_csv(f'gs://{bucket_name}/{blob_name}', sep=';', header=None, dtype=str, encoding='latin-1', skiprows=8, on_bad_lines='skip', keep_default_na=False, engine='python')
                df['cidade'] = cidade_fixa
                df_list.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        else:
            print(f"File not found: {file_path}")

        current_date = next_year_date + timedelta(days=1)

    cidade_2024 = 'SE_SP_A713_IPERO'
    data_inicio_2024 = datetime.strptime('01-01-2024', '%d-%m-%Y')
    data_fim_2024 = datetime.strptime('30-06-2024', '%d-%m-%Y')
    file_path_2024 = file_path_template.format(
        CIDADE=cidade_2024,
        DATA_INICIO=format_date(data_inicio_2024),
        DATA_FIM=format_date(data_fim_2024)
    )

    blob_name_2024 = f"0. land/INMET_{cidade_2024}_{format_date(data_inicio_2024)}_A_{format_date(data_fim_2024)}.CSV"
    blob_2024 = bucket.blob(blob_name_2024)

    if blob_2024.exists():
        print(f"File found: {file_path_2024}")
        try:
            df_2024 = pd.read_csv(f'gs://{bucket_name}/{blob_name_2024}', sep=';', header=None, dtype=str, encoding='latin-1', skiprows=8, on_bad_lines='skip', keep_default_na=False, engine='python')
            df_2024['cidade'] = cidade_2024
            df_list.append(df_2024)
        except Exception as e:
            print(f"Error reading {file_path_2024}: {e}")
    else:
        print(f"File not found: {file_path_2024}")

    if df_list:
        df_climaticas = pd.concat(df_list, ignore_index=True)
    else:
        print("No files were found, so no DataFrame was created.")

    df_climaticas_selecao = df_climaticas.rename(columns={
        0: 'data',
        1: 'hora_utc',
        2: 'precipitacao_total',
        4: 'pressao_atmosferica_max',
        5: 'pressao_atmosferica_min',
        9: 'temp_max',
        10: 'temp_min',
        15: 'umidade_relativa_ar'
    })[['data', 'precipitacao_total', 'pressao_atmosferica_max', 'pressao_atmosferica_min', 'temp_max', 'temp_min', 'umidade_relativa_ar']].iloc[1:].reset_index(drop=True)

    df_climaticas_selecao.dropna(subset=['precipitacao_total', 'pressao_atmosferica_max', 'pressao_atmosferica_min', 'temp_max', 'temp_min', 'umidade_relativa_ar'], inplace=True)

    climaticas = df_climaticas_selecao[df_climaticas_selecao['data'] != 'Data']
    climaticas = climaticas.applymap(lambda x: x.replace(',', '.') if isinstance(x, str) else x)

    climaticas['precipitacao_total'] = pd.to_numeric(climaticas['precipitacao_total'], errors='coerce')
    climaticas['pressao_atmosferica_max'] = pd.to_numeric(climaticas['pressao_atmosferica_max'], errors='coerce')
    climaticas['pressao_atmosferica_min'] = pd.to_numeric(climaticas['pressao_atmosferica_min'], errors='coerce')
    climaticas['temp_max'] = pd.to_numeric(climaticas['temp_max'], errors='coerce')
    climaticas['temp_min'] = pd.to_numeric(climaticas['temp_min'], errors='coerce')
    climaticas['umidade_relativa_ar'] = pd.to_numeric(climaticas['umidade_relativa_ar'], errors='coerce')

    file_path = 'gs://tcc_eng_dados/1. bronze/bronze_climaticas.csv'
    climaticas.to_csv(file_path, index=False)
    print(f'Dados bronze salvos em {file_path}.')

def silver_process():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)
    file_path = f"gs://{BUCKET_NAME}/1. bronze/bronze_climaticas.csv"

    df_climaticas = pd.read_csv(file_path)

    # Convertendo a coluna 'data' para datetime
    df_climaticas['data'] = pd.to_datetime(df_climaticas['data'], errors='coerce')

    # Aplicando a agregação
    df_climaticas_agregado = df_climaticas.groupby('data').agg({
        'precipitacao_total': 'sum',
        'pressao_atmosferica_max': 'max',
        'pressao_atmosferica_min': 'min',
        'temp_max': 'max',
        'temp_min': 'min',
        'umidade_relativa_ar': 'mean'
    }).reset_index()

    # Calculando as médias mensais
    df_climaticas_agregado['data'] = pd.to_datetime(df_climaticas_agregado['data'])
    df_climaticas_agregado.set_index('data', inplace=True)
    df_climaticas_medias_mensais = df_climaticas_agregado.resample('M').mean().reset_index()

    # Ajustando a data para o primeiro dia do mês
    df_climaticas_medias_mensais['data'] = df_climaticas_medias_mensais['data'].apply(lambda x: x.replace(day=1))

    df_climaticas_medias_mensais.fillna(0, inplace=True)

    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_climaticas.csv"
    df_climaticas_medias_mensais.to_csv(file_path_silver, index=False)
    print(f'Dados silver salvos em {file_path_silver}.')

def gold_to_bq():
    # Download do arquivo de credenciais do GCS
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.blob('auth_keys/keyfile.json')
    blob.download_to_filename(KEYFILE_PATH)
    
    # Configuração das credenciais
    credentials = service_account.Credentials.from_service_account_file(KEYFILE_PATH)
    bigquery_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_climaticas.csv"
    climaticas_gold = pd.read_csv(file_path_silver)

    climaticas_gold.to_gbq(destination_table=TABLE_ID, project_id=PROJECT_ID, credentials=credentials, if_exists='replace')
    print(f'Dados carregados para a tabela {TABLE_ID} no BigQuery.')

    # Remove o arquivo temporário
    os.remove(KEYFILE_PATH)

bronze_task = PythonOperator(
    task_id='bronze_to_gcs',
    python_callable=bronze_to_gcs,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_process',
    python_callable=silver_process,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='gold_to_bq',
    python_callable=gold_to_bq,
    dag=dag,
)

bronze_task >> silver_task >> gold_task
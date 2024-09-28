from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG(
    'pipeline_dengue',
    default_args=default_args,
    description='Pipeline Dengue Bronze-Silver-Gold',
    schedule_interval='@monthly',  # Para rodar todo mês
    start_date=days_ago(1),
    catchup=False
)

PROJECT_ID = 'tcc-pos-eng-dados'
BUCKET_NAME = 'tcc_eng_dados'
TABLE_ID = 'tcc-pos-eng-dados.casos_notificados.dengue'
KEYFILE_PATH = '/tmp/keyfile.json'

def bronze_to_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)

    # Dicionário com as URLs de cada ano
    urls = {
        "2020": "2020/dengue20_import_autoc_mes.htm",
        "2022": "2022/dengue22_import_autoc_mes.htm",
        "2023": "2023/dengue23_mes.htm"
    }

    # URL base
    url_base = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/dengue/"

    # Lista para armazenar os DataFrames
    dataframes = []

    # Lista para armazenar os anos
    anos = []

    # Loop para iterar sobre cada ano e criar o DataFrame correspondente
    for year, path in urls.items():
        url = url_base + path
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Encontrar a tabela específica
        table = soup.find('table')

        # Converter a tabela HTML em um DataFrame do Pandas
        df = pd.read_html(str(table))[0].iloc[5:, :43].reset_index(drop=True).astype(str)

        # Filtrar o DataFrame pela coluna 5 contendo a palavra 'sorocaba'
        df_filtered = df[df.iloc[:, 5].str.upper() == 'SOROCABA']

        # Adicionar o ano correspondente à lista de anos
        anos.extend([int(year)] * len(df_filtered))

        # Armazenar o DataFrame na lista
        dataframes.append(df_filtered)

    # Concatenar todos os DataFrames em um único DataFrame
    df_final_dengue = pd.concat(dataframes, ignore_index=True)

    # Adicionar a coluna 'Ano' ao DataFrame final
    df_final_dengue['Ano'] = anos

    # Selecionando colunas e aplicando transformações no Pandas
    selecao_colunas = df_final_dengue.columns[6:48]

    # Selecionando apenas as colunas desejadas
    df_selecao = df_final_dengue[selecao_colunas]

    # Aplicando a transformação na coluna 6 (substituição de padrão) - use integer indexing
    df_selecao.iloc[:, 0] = df_selecao.iloc[:, 0].str.replace(r'^[0-9]+ ', '', regex=True)

    # Lista com os novos nomes das colunas
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao.columns))]

    # Renomeando as colunas
    df_resetado = df_selecao.rename(columns=dict(zip(df_selecao.columns, novos_nomes)))

    # Processamento para 2021
    url_21 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/dengue/2021/dengue21_import_autoc_mes.htm"
    response_21 = requests.get(url_21)
    soup_21 = BeautifulSoup(response_21.text, 'html.parser')
    table_21 = soup_21.find('table')
    dengue_2021 = pd.read_html(str(table_21))[0].iloc[5:, 2:43].reset_index(drop=True).astype(str)
    df_2021_filtro = dengue_2021[dengue_2021.iloc[:, 0].str.upper() == 'SOROCABA'].copy()
    df_2021_filtro['ano'] = '2021'
    selecao_colunas_2021 = df_2021_filtro.columns[1:38].tolist() + ['ano']
    df_selecao_2021 = df_2021_filtro[selecao_colunas_2021]
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2021.columns))]
    df_selecao_2021.columns = novos_nomes

    # Processamento para 2024
    url_24 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/dengue/2024/dengue24_mes.htm"
    response_24 = requests.get(url_24)
    soup_24 = BeautifulSoup(response_24.text, 'html.parser')
    table_24 = soup_24.find('table')
    dengue_2024 = pd.read_html(str(table_24))[0].iloc[5:, 5:22].reset_index(drop=True).astype(str)
    df_2024_filtro = dengue_2024[dengue_2024.iloc[:, 0].str.upper() == 'SOROCABA'].copy()
    selecao_colunas_2024 = df_2024_filtro.columns[1:]
    df_2024_filtro.loc[:, 'ano'] = '2024'
    column_to_replace = df_2024_filtro.columns[1]
    df_2024_filtro.loc[:, column_to_replace] = df_2024_filtro[column_to_replace].str.replace(r'^[0-9]+ ', '', regex=True)
    df_selecao_2024 = df_2024_filtro[selecao_colunas_2024.tolist() + ['ano']]
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2024.columns))]
    df_selecao_2024.columns = novos_nomes
    df_selecao_2024 = df_selecao_2024.rename(columns={'coluna_16': 'coluna_37'})

    # Concatenando todos os DataFrames
    df_final_dengue = pd.concat([df_resetado, df_selecao_2021, df_selecao_2024], ignore_index=True, sort=False).fillna('0').replace('nan', '0')

    # Salvando no GCS (Bronze Layer)
    file_path = 'gs://tcc_eng_dados/1. bronze/bronze_dengue.csv'
    df_final_dengue.to_csv(file_path, index=False)
    print(f'Dados bronze salvos em {file_path}.')

def silver_process():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)
    file_path = f"gs://{BUCKET_NAME}/1. bronze/bronze_dengue.csv"

    dengue = pd.read_csv(file_path)

    # Renomeando colunas e selecionando apenas as colunas desejadas em Pandas
    dengue_selecao = dengue.rename(columns={
        'coluna_0': 'municipio_resid',
        'coluna_1': 'jan',
        'coluna_4': 'fev',
        'coluna_7': 'mar',
        'coluna_10': 'abr',
        'coluna_13': 'mai',
        'coluna_16': 'jun',
        'coluna_19': 'jul',
        'coluna_22': 'ago',
        'coluna_25': 'set',
        'coluna_28': 'out',
        'coluna_31': 'nov',
        'coluna_34': 'dez',
        'coluna_37': 'ano'
    })[['municipio_resid', 'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez', 'ano']]

    # Dicionário de mapeamento de meses
    mes_convert = {
        'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05',
        'jun': '06', 'jul': '07', 'ago': '08', 'set': '09', 'out': '10',
        'nov': '11', 'dez': '12'
    }

    # Converte o DataFrame para formato longo
    dengue_completo = dengue_selecao.melt(id_vars=['municipio_resid', 'ano'],
                                          value_vars=list(mes_convert.keys()),
                                          var_name='mes',
                                          value_name='casos')

    # Substitui os nomes dos meses pelos números correspondentes
    dengue_completo['mes'] = dengue_completo['mes'].map(mes_convert)

    # Cria a coluna 'ano_mes' com o formato desejado
    dengue_completo['ano_mes'] = pd.to_datetime(dengue_completo['ano'].astype(str) + '-' + dengue_completo['mes'])

    # Remove linhas onde 'casos' é NaN ou null
    dengue_completo = dengue_completo.dropna(subset=['casos'])

    # Converte os valores em 'casos' para inteiros
    dengue_completo['casos'] = dengue_completo['casos'].astype(int)

    dengue_silver = dengue_completo[['municipio_resid', 'ano_mes', 'casos']].sort_values('ano_mes').reset_index(drop=True)

    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_dengue.csv"
    dengue_silver.to_csv(file_path_silver, index=False)
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

    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_dengue.csv"
    dengue_gold = pd.read_csv(file_path_silver)

    dengue_gold['caso'] = 'dengue'
    dengue_final = dengue_gold.rename(columns={'casos': 'qtd_casos'})

    # Convertendo 'ano_mes' para datetime (se ainda não for)
    dengue_final['ano_mes'] = pd.to_datetime(dengue_final['ano_mes'])

    dengue_final.to_gbq(destination_table=TABLE_ID, project_id=PROJECT_ID, credentials=credentials, if_exists='replace')
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
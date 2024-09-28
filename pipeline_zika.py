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
    'pipeline_zika',
    default_args=default_args,
    description='Pipeline Zika Bronze-Silver-Gold',
    schedule_interval='@monthly',  # Para rodar todo mês
    start_date=days_ago(1),
    catchup=False
)

PROJECT_ID = 'tcc-pos-eng-dados'
BUCKET_NAME = 'tcc_eng_dados'
TABLE_ID = 'tcc-pos-eng-dados.casos_notificados.zika'
KEYFILE_PATH = '/tmp/keyfile.json'

def bronze_to_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)

    # URL do ano de 2020
    url_20 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/zika/2020/zika20_autoc_import_se_arquivos/sheet001.htm"

    # Fazendo a requisição HTTP e parseando o HTML
    response_20 = requests.get(url_20)
    soup_20 = BeautifulSoup(response_20.text, 'html.parser')

    # Encontrando a tabela específica
    table_20 = soup_20.find('table')

    # Lendo a tabela HTML para um DataFrame Pandas
    zika_2020 = pd.read_html(str(table_20))[0].iloc[4:649, 5:43].reset_index(drop=True).astype(str)

    # Filtrando o DataFrame pela coluna 2 contendo a palavra 'sorocaba'
    df_2020_filtro = zika_2020[zika_2020.iloc[:, 0].str.upper() == 'SOROCABA'].copy()

    # Adicionando a coluna 'ano'
    df_2020_filtro['ano'] = '2020'

    # Selecionando as colunas desejadas
    selecao_colunas_2020 = df_2020_filtro.columns[1:38].tolist() + ['ano']
    df_selecao_2020 = df_2020_filtro[selecao_colunas_2020]

    # Aplicando a transformação na coluna 6 (substituição de padrão)
    df_selecao_2020.iloc[:, 0] = df_selecao_2020.iloc[:, 0].str.replace(r'^[0-9]+ ', '', regex=True)

    # Lista com os nomes das colunas
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2020.columns))]

    # Renomeando as colunas no DataFrame Pandas
    df_selecao_2020.columns = novos_nomes

    # Dicionário com as URLs de cada ano
    urls = {
        "2021": "2021/zika21_autoc_import_mes.htm",
        "2022": "2022/zika22_autoc_import_mes.fld/sheet001.htm",
        "2023": "2023/zika23_mes.htm"
    }

    # URL base
    url_base = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/zika/"

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
        df = pd.read_html(str(table))[0].iloc[5:, 2:40].reset_index(drop=True).astype(str)

        # Filtrar o DataFrame pela coluna 0 contendo a palavra 'sorocaba'
        df_filtered = df[df.iloc[:, 0].str.upper() == 'SOROCABA']

        # Adicionar o ano correspondente à lista de anos
        anos.extend([int(year)] * len(df_filtered))

        # Armazenar o DataFrame na lista
        dataframes.append(df_filtered)

    # Concatenar todos os DataFrames em um único DataFrame
    df_final_zika = pd.concat(dataframes, ignore_index=True)

    # Adicionar a coluna 'Ano' ao DataFrame final
    df_final_zika['Ano'] = anos

    # Selecionando colunas e aplicando transformações no Pandas
    selecao_colunas = df_final_zika.columns[1:48]

    # Selecionando apenas as colunas desejadas
    df_selecao = df_final_zika[selecao_colunas]

    # Aplicando a transformação na coluna 6 (substituição de padrão)
    df_selecao.iloc[:, 0] = df_selecao.iloc[:, 0].str.replace(r'^[0-9]+ ', '', regex=True)

    # Lista com os novos nomes das colunas
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao.columns))]

    # Renomeando as colunas
    df_resetado = df_selecao.rename(columns=dict(zip(df_selecao.columns, novos_nomes)))

    # Processamento para 2024
    url_24 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/zika/2024/zika24_mes.htm"
    response_24 = requests.get(url_24)
    soup_24 = BeautifulSoup(response_24.text, 'html.parser')
    table_24 = soup_24.find('table')
    zika_2024 = pd.read_html(str(table_24))[0].iloc[6:, 2:19].reset_index(drop=True).astype(str)
    df_2024_filtro = zika_2024[zika_2024.iloc[:, 0].str.upper() == 'SOROCABA'].copy()
    selecao_colunas_2024 = df_2024_filtro.columns[1:]
    df_2024_filtro.loc[:, 'ano'] = '2024'
    df_selecao_2024 = df_2024_filtro[selecao_colunas_2024.tolist() + ['ano']]
    novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2024.columns))]
    df_selecao_2024.columns = novos_nomes
    df_selecao_2024 = df_selecao_2024.rename(columns={'coluna_16': 'coluna_37'})

    # Concatenando todos os DataFrames
    df_final_zika = pd.concat([df_selecao_2020, df_resetado, df_selecao_2024], ignore_index=True, sort=False).fillna('0').replace('nan', '0')

    # Dicionário de substituições
    substituicoes = {
        'AlumÃ­nio': 'Alumínio',
        'AraÃ§ariguama': 'Araçariguama',
        'AraÃ§oiaba da Serra': 'Araçoiaba da Serra',
        'IbiÃºna': 'Ibiúna',
        'IperÃ³': 'Iperó',
        'SÃ£o Roque': 'São Roque',
        'TapiraÃ­': 'Tapiraí',
        'TietÃª': 'Tietê'
    }

    # Aplicando as substituições
    df_final_zika.replace(substituicoes, inplace=True)

    # Salvando no GCS (Bronze Layer)
    file_path = 'gs://tcc_eng_dados/1. bronze/bronze_zika.csv'
    df_final_zika.to_csv(file_path, index=False)
    print(f'Dados bronze salvos em {file_path}.')

def silver_process():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.get_bucket(BUCKET_NAME)
    file_path = f"gs://{BUCKET_NAME}/1. bronze/bronze_zika.csv"

    zika = pd.read_csv(file_path)

    # Renomeando colunas e selecionando apenas as colunas desejadas em Pandas
    zika_selecao = zika.rename(columns={
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
    zika_completo = zika_selecao.melt(id_vars=['municipio_resid', 'ano'],
                                      value_vars=list(mes_convert.keys()),
                                      var_name='mes',
                                      value_name='casos')

    # Substitui os nomes dos meses pelos números correspondentes
    zika_completo['mes'] = zika_completo['mes'].map(mes_convert)

    # Cria a coluna 'ano_mes' com o formato desejado
    zika_completo['ano_mes'] = pd.to_datetime(zika_completo['ano'].astype(str) + '-' + zika_completo['mes'])

    # Remove linhas onde 'casos' é NaN ou null
    zika_completo = zika_completo.dropna(subset=['casos'])

    # Converte os valores em 'casos' para inteiros
    zika_completo['casos'] = zika_completo['casos'].astype(int)

    zika_silver = zika_completo[['municipio_resid', 'ano_mes', 'casos']].sort_values('ano_mes').reset_index(drop=True)

    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_zika.csv"
    zika_silver.to_csv(file_path_silver, index=False)
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
    
    file_path_silver = f"gs://{BUCKET_NAME}/2. silver/silver_zika.csv"
    zika_gold = pd.read_csv(file_path_silver)

    zika_gold['caso'] = 'zika'
    zika_final = zika_gold.rename(columns={'casos': 'qtd_casos'})

    # Convertendo 'ano_mes' para datetime (se ainda não for)
    zika_final['ano_mes'] = pd.to_datetime(zika_final['ano_mes'])

    zika_final.to_gbq(destination_table=TABLE_ID, project_id=PROJECT_ID, credentials=credentials, if_exists='replace')
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
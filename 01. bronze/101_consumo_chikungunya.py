# Databricks notebook source
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("ChikungunyaData").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dados de Chikungunya Notificados da Região de Saúde de Sorocaba

# COMMAND ----------

# MAGIC %md
# MAGIC A região de saúde de Sorocaba engloba as cidades de Alumínio, Araçariguama, Araçoiaba da Serra, Boituva, Capela do Alto, Ibiúna, Iperó, Itu, Jumirim, Mairinque, Piedade, Pilar do Sul, Porto Feliz, Salto, Salto de Pirapora, São Roque, Sorocaba, iraí, Tietê, Votorantim

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumo dados dos anos 2020, 2022 e 2023

# COMMAND ----------

# Dicionário com as URLs de cada ano
urls = {
    "2020": "chikung/2020/chikung20_import_autoc_se_arquivos/sheet001.htm",
    "2022": "chikung/2022/chikung22_import_autoc_res.htm",
    "2023": "chikung/2023/chikung23_mes.htm"
}

# URL base
url_base = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/"

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
    df =  pd.read_html(str(table))[0].iloc[5:, :46].reset_index(drop=True).astype(str)

    # Filtrar o DataFrame pela coluna 5 contendo a palavra 'sorocaba'
    df_filtered = df[df.iloc[:, 5].str.upper() == 'SOROCABA']

    # Adicionar o ano correspondente à lista de anos
    anos.extend([int(year)] * len(df_filtered))

    # Armazenar o DataFrame na lista
    dataframes.append(df_filtered)

# Concatenar todos os DataFrames em um único DataFrame
df_final_chikung = pd.concat(dataframes, ignore_index=True)

# Adicionar a coluna 'Ano' ao DataFrame final
df_final_chikung['Ano'] = anos

df_final = spark.createDataFrame(df_final_chikung)


# COMMAND ----------

selecao_colunas = df_final.columns[6:47]
df_selecao = (df_final
    .select(*selecao_colunas)
    .withColumn('6', regexp_replace('6', '^[0-9]+ ', ''))
)

# COMMAND ----------

# Lista com os nomes das colunas
novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao.columns))]

# Resetando o índice das colunas
df_resetado = df_selecao.toDF(*novos_nomes)

# COMMAND ----------

df_resetado.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumo dados do ano de 2021

# COMMAND ----------

url_21 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/chikung/2021/chikung21_import_autoc_res.htm"

response_21 = requests.get(url_21)
soup_21 = BeautifulSoup(response_21.text, 'html.parser')

# Encontrar a tabela específica
table_21 = soup_21.find('table')

chikungunya_2021 = pd.read_html(str(table_21))[0].iloc[5:, :43].reset_index(drop=True).astype(str)

# Filtrar o DataFrame pela coluna 2 contendo a palavra 'sorocaba'
df_2021_filtro = chikungunya_2021[chikungunya_2021.iloc[:, 2].str.upper() == 'SOROCABA']


df_chikungunya_2021 = spark.createDataFrame(df_2021_filtro)

# COMMAND ----------

selecao_colunas_2021 = df_chikungunya_2021.columns[3:43]
df_selecao_2021 = (df_chikungunya_2021
    .withColumn('ano', lit(2021))               
    .select(*selecao_colunas_2021, 'ano')
)

# COMMAND ----------

# Lista com os nomes das colunas
novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2021.columns))]

# Resetando o índice das colunas
df_resetado_2021 = df_selecao_2021.toDF(*novos_nomes)

# COMMAND ----------

df_resetado_2021.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consumo dados do ano de 2024 até abril

# COMMAND ----------

url_24 = "https://www.saude.sp.gov.br/resources/cve-centro-de-vigilancia-epidemiologica/areas-de-vigilancia/doencas-de-transmissao-por-vetores-e-zoonoses/dados/chikung/2024/chikung24_mes.htm"

response_24 = requests.get(url_24)
soup_24 = BeautifulSoup(response_24.text, 'html.parser')

# Encontrar a tabela específica
table_24 = soup_24.find('table')

chikungunya_2024 = pd.read_html(str(table_24))[0].iloc[5:, 5:17].reset_index(drop=True).astype(str)

# Filtrar o DataFrame pela coluna 2 contendo a palavra 'sorocaba'
df_2024_filtro = chikungunya_2024[chikungunya_2024.iloc[:, 0].str.upper() == 'SOROCABA']


df_chikungunya_2024 = spark.createDataFrame(df_2024_filtro)

# COMMAND ----------

df_2024_filtro.display()

# COMMAND ----------

selecao_colunas_2024 = df_chikungunya_2021.columns[6:17]
df_selecao_2024 = (df_chikungunya_2024
    .withColumn('ano', lit(2024))
    .withColumn('6', regexp_replace('6', '^[0-9]+ ', ''))               
    .select(*selecao_colunas_2024, 'ano')
)

# COMMAND ----------

df_selecao_2024.display()

# COMMAND ----------

# Lista com os nomes das colunas
novos_nomes = [f'coluna_{i}' for i in range(len(df_selecao_2024.columns))]

# Resetando o índice das colunas
df_resetado_2024 = (df_selecao_2024
    .toDF(*novos_nomes)
    .withColumnRenamed('coluna_11', 'coluna_40')
)

# COMMAND ----------

df_resetado_2024.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## União dos DFs

# COMMAND ----------

df_chikung_20_23 = (df_resetado
                    .union(df_resetado_2021)
                    .fillna('0')
                    .replace('nan', '0')
                    )

# COMMAND ----------

df_final_chikung = (df_chikung_20_23.unionByName(df_resetado_2024, allowMissingColumns=True))

# COMMAND ----------

df_final_chikung.display()

# COMMAND ----------

df_final_chikung.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save('dbfs:/mnt/dbstoragehnadwxeer7qfg/bronze/chikungunya_jan2020_abr2024')

# COMMAND ----------

# dbutils.fs.rm('dbfs:/mnt/dbstoragehnadwxeer7qfg/bronze/chikungunya_jan2020_abr2024', True)

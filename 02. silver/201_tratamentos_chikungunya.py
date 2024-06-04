# Databricks notebook source
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark = SparkSession.builder.appName("TransformDataFrame").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação estrutura

# COMMAND ----------

# path_delta = 'dbfs:/mnt/dbstoragehnadwxeer7qfg/bronze/chikungunya_jan2020_abr2024'

# df_chikung_20 = spark.read.format('delta').load(path_delta_20)

# COMMAND ----------

# df_chikun_20_renom = (df_chikung_20
#  .withColumnRenamed('0', 'drs_cod_ depart_regional_saude')    
#  .withColumnRenamed('1', 'drs_nome')        
#  .withColumnRenamed('2', 'gve_cod_grupos_vigilancia_epidemiologica')  
#  .withColumnRenamed('3', 'gve_nome')  
#  .withColumnRenamed('4', 'rs_cod')  
#  .withColumnRenamed('5', 'rs_nome')  
#  .withColumnRenamed('6', 'municipio_resid')  
#  .withColumnRenamed('7', '01/2020')  
#  .withColumnRenamed('10', '02/2020')  
#  .withColumnRenamed('13', '03/2020')  
#  .withColumnRenamed('16', '04/2020')  
#  .withColumnRenamed('19', '05/2020')
#  .withColumnRenamed('22', '06/2020') 
#  .withColumnRenamed('25', '07/2020') 
#  .withColumnRenamed('28', '08/2020') 
#  .withColumnRenamed('31', '09/2020') 
#  .withColumnRenamed('34', '10/2020') 
#  .withColumnRenamed('37', '11/2020')     
#  .withColumnRenamed('40', '12/2020')
#  .filter(upper(col('rs_nome'))== 'SOROCABA')    
#  .select('municipio_resid', '01/2020', '02/2020', '03/2020', '04/2020', '05/2020',  
#     '06/2020', '07/2020', '08/2020', '09/2020', '10/2020', '11/2020', '12/2020' )           
# )

# COMMAND ----------

# df_chikun_20_pivot = (df_chikun_20_renom
#     .selectExpr("municipio_resid", "stack(12, '01/2020', 01/2020, '02/2020', 02/2020, '03/2020',  mar_notificados,  '04/2020', abr_notificados,  '05/2020', mai_notificados,'06/2020', jun_notificados, '07/2020', jul_notificados, '08/2020', ago_notificados, '09/2020', set_notificados, '10/2020',  out_notificados, '11/2020', nov_notificados, '12/2020', dez_notificados) as (mes, valor)")
#     .withColumn('valor', col('valor').cast('integer'))
#     .groupBy("mes").agg(sum("valor").alias("casos_notificados_total"))
# )

# COMMAND ----------

# df_chikun_20_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Tratamentos dados Chikungunya de jan/2020 à abr/2024

# COMMAND ----------

path_delta = 'dbfs:/mnt/dbstoragehnadwxeer7qfg/bronze/chikungunya_jan2020_abr2024'

df_chikung = spark.read.format('delta').load(path_delta)

# COMMAND ----------

df_chikung.display()

# COMMAND ----------

df_chikung_selecao = (df_chikung
    .withColumnRenamed('coluna_0', 'municipio_resid')  
    .withColumnRenamed('coluna_1', 'jan')  
    .withColumnRenamed('coluna_4', 'fev')  
    .withColumnRenamed('coluna_7', 'mar')  
    .withColumnRenamed('coluna_10', 'abr')  
    .withColumnRenamed('coluna_13', 'mai')
    .withColumnRenamed('coluna_16', 'jun') 
    .withColumnRenamed('coluna_19', 'jul') 
    .withColumnRenamed('coluna_22', 'ago') 
    .withColumnRenamed('coluna_25', 'set') 
    .withColumnRenamed('coluna_28', 'out') 
    .withColumnRenamed('coluna_31', 'nov')     
    .withColumnRenamed('coluna_34', 'dez')
    .withColumnRenamed('coluna_40', 'ano')
    .select('municipio_resid', 'jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez', 'ano')
)

# COMMAND ----------

df_chikung_selecao.display()

# COMMAND ----------

# Define o schema do DataFrame de resultado
schema = StructType([
    StructField("municipio_resid", StringType(), True),
    StructField("Ano", IntegerType(), True),
    StructField("mes", StringType(), True),
    StructField("casos", IntegerType(), True),
])

# Cria um DataFrame vazio para armazenar o resultado
df_long = spark.createDataFrame([], schema=schema)

# Seleciona as colunas de meses e converte para formato longo
meses = ['jan', 'fev', 'mar', 'abr', 'mai', 'jun', 'jul', 'ago', 'set', 'out', 'nov', 'dez']

# Dicionário de mapeamento de meses
mes_convert = {
    'jan': '01', 'fev': '02', 'mar': '03', 'abr': '04', 'mai': '05',
    'jun': '06', 'jul': '07', 'ago': '08', 'set': '09', 'out': '10',
    'nov': '11', 'dez': '12'
}

# meses = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

for mes in meses:
    df_mes = df_chikung_selecao.select(
        col('municipio_resid'),
        col('Ano'),
        lit(mes).alias('mes'),
        col(mes).alias('casos')
    )
    df_long = df_long.union(df_mes)

# Remove linhas onde 'casos' é NaN ou null
# df_long = df_long.filter(col('casos').isNotNull())

# Converte os valores em 'casos' para inteiros
df_long = df_long.withColumn('casos', col('casos').cast('int'))


# Substitui os nomes dos meses pelos números correspondentes
for nome_mes, num_mes in mes_convert.items():
    df_long = df_long.withColumn('mes', when(col('mes') == nome_mes, num_mes).otherwise(col('mes')))

# Cria a coluna 'mes_ano' com o formato desejado
df_result = (df_long
        # .withColumn('ano_mes', concat(expr("lpad(mes, 2, '0')"), lit('-'), col('Ano')))
        .withColumn('ano_mes', to_date(concat(col('Ano'), lit('-'), expr("lpad(mes, 2, '0')")), 'yyyy-MM'))
        .groupBy('ano_mes').sum('casos').withColumnRenamed('sum(casos)', 'total_casos')
        .orderBy('ano_mes')
)        

# COMMAND ----------

df_result.display()

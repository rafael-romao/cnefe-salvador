from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
import random



spark = SparkSession.builder.getOrCreate()    
spark.sparkContext.setLogLevel("ERROR")
# Checkpoint
spark.sparkContext.setCheckpointDir("/content/checkpoint/")
sc = spark
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "7hgOYRYF7bQvG6toul8I")
hadoop_conf.set("fs.s3a.secret.key", "cG8S9jo2c9rY6ynCp8rqZ5bNuY1WsZbzTepLHBcG")
hadoop_conf.set("fs.s3a.path.style.access", "True")
# hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Seleciona colunas aleatorias
def select_random_column(ids, columns, id):
  if id in ids:
    return random.sample(columns, 1)[0]
  else:
    return None

# Registrar a função UDF no Spark
select_random_column_udf = F.udf(lambda ids, columns, id: select_random_column(ids, columns, id), StringType())


# Suprime palavras da coluna
def sup_words(word, n):
  if word is None or word.isspace():
    return word
  word_list = word.split()
  if len(word_list) < n:
    return word
  for _ in range(n):
    if word_list and len(word_list):
      word_list.pop(random.randint(0, len(word_list)-1))
  return ' '.join(word_list)


# Registrar a função UDF no Spark
sup_words_udf = F.udf(lambda word, n: sup_words(word, n), StringType())



# set variables
base_a_path = "s3a://cleaned/base_a"
output_path = "s3a://cleaned/base_b"

_PERCT_MISSING_VALUE = 0.01
_PERCT_ONE_SUP = 0.02
_PERCT_TWO_SUP = 0.03
_RANDOM_SEED = 2

    
# Read Dataframe
base_a = spark.read.format("parquet").load(base_a_path)

print(f"Quantidade de registros: {base_a.count()}")

# Coleta todos os ids da base de dados
#ids = [id[0] for id in base_b.select("id").collect()]
ids_with_three_words = [id[0] for id in base_a.filter((F.size(F.split(F.col("nome_logradouro"), ' ')) > 2) | (F.size(F.split(F.col("complemento_concatenado"), ' ')) > 2)).collect()]
ids_with_two_words = [id[0] for id in base_a.filter((F.size(F.split(F.col("nome_logradouro"), ' ')) > 1) | (F.size(F.split(F.col("complemento_concatenado"), ' ')) > 1)).collect()]
ids_notnull = [id[0] for id in base_a.filter((F.col("nome_logradouro").isNotNull()) | (F.col("complemento_concatenado").isNotNull())).collect()]

tot = base_a.count()

# Define a quantidade de registros que possuirá cada tipo de ruído
qt_missing_value = int(tot*_PERCT_MISSING_VALUE)
qt_one_sup = int(tot*_PERCT_ONE_SUP)
qt_two_sup = int(tot*_PERCT_TWO_SUP)

# Inserindo seed para garantir reprodutibilidade
random.seed(_RANDOM_SEED)

# Selecionando ids para ruído de dados faltantes
### Candidatos: todos os ids com mais de duas palavras
ids_two_sup = random.sample(ids_with_three_words, qt_two_sup)
### Candidatos: Todos os ids com mais de uma palavra, exceto os já escolhidos para supressao de duas palavras
ids_one_sup = random.sample(list(set(ids_with_two_words)-set(ids_two_sup)), qt_one_sup)
### Candidatos: Todos os ids com o campo preenchido, exceto os já escolhidos para supressao de uma e duas palavras
ids_missing_value = random.sample(list(set(ids_notnull)-set(ids_one_sup)-set(ids_two_sup)), qt_missing_value)

# Lista com nome de colunas que terão seus valores apagados
linkage_columns = ['nome_logradouro', 'complemento_concatenado']


# Definindo aleatoriamente qual a coluna - entre logradouro e complemento - o ruído será adicionado
base_b = base_a.withColumn("column_for_missing_value", select_random_column_udf(F.lit(ids_missing_value), F.lit(linkage_columns), F.col('id')))
base_b = base_b.withColumn("column_for_one_sup_value", select_random_column_udf(F.lit(ids_one_sup), F.lit(linkage_columns), F.col('id')))
base_b = base_b.withColumn("column_for_two_sup_value", select_random_column_udf(F.lit(ids_two_sup), F.lit(linkage_columns), F.col('id')))

base_b.checkpoint()

# Adicionando coluna na base identificando qual tipo de ruido inserido em cada registro
base_b = base_b.withColumn("noise_type", F.when(F.col("id").isin(ids_missing_value), F.lit("Valores ausentes"))
                                     .when(F.col("id").isin(ids_one_sup), F.lit("Supressão de uma palavra"))
                                     .when(F.col("id").isin(ids_two_sup), F.lit("Supressão de duas palavras"))
                                     .otherwise(F.lit("Sem ruído"))
                          )

base_b = base_b.withColumn("noise_column", F.coalesce(F.col("column_for_missing_value"),
                                             F.col("column_for_one_sup_value"),
                                             F.col("column_for_two_sup_value")))



# Inserindo registros com valores ausentes na variável logradouro e complemento
base_b = base_b.withColumn("nome_logradouro", F.when(F.col("column_for_missing_value") == "nome_logradouro", F.lit(None)).otherwise(F.col("nome_logradouro")))
base_b = base_b.withColumn("complemento_concatenado", F.when(F.col("column_for_missing_value") == "complemento_concatenado", F.lit(None)).otherwise(F.col("complemento_concatenado")))

# Criando variável com supressão de uma palavra
base_b = base_b.withColumn("nome_logradouro_one_sup", sup_words_udf(F.col('nome_logradouro'), F.lit(1)))
base_b = base_b.withColumn("complemento_concatenado_one_sup", sup_words_udf(F.col('complemento_concatenado'), F.lit(1)))

# Inserindo  registros com supressão de uma palavra (aleatória) nas variáveis LOGRADOURO ou COMPLEMENTO
base_b = base_b.withColumn("nome_logradouro", F.when(F.col("column_for_one_sup_value") == "nome_logradouro", F.col('nome_logradouro_one_sup')).otherwise(F.col("nome_logradouro")))
base_b = base_b.withColumn("complemento_concatenado", F.when(F.col("column_for_one_sup_value") == "complemento_concatenado", F.col('complemento_concatenado_one_sup')).otherwise(F.col("complemento_concatenado")))

# Criando variável com supressão de duas palavras
base_b = base_b.withColumn("nome_logradouro_two_sup", sup_words_udf(F.col('nome_logradouro'), F.lit(2)))
base_b = base_b.withColumn("complemento_concatenado_two_sup", sup_words_udf(F.col('complemento_concatenado'), F.lit(2)))

# Inserindo  registros com supressão de duas palavras (aleatória) nas variáveis LOGRADOURO ou COMPLEMENTO
base_b = base_b.withColumn("nome_logradouro", F.when(F.col("column_for_two_sup_value") == "nome_logradouro", F.col('nome_logradouro_two_sup')).otherwise(F.col("nome_logradouro")))
base_b = base_b.withColumn("complemento_concatenado", F.when(F.col("column_for_two_sup_value") == "complemento_concatenado", F.col('complemento_concatenado_two_sup')).otherwise(F.col("complemento_concatenado")))

drop_columns = ['column_for_missing_value', 'column_for_one_sup_value', 'column_for_two_sup_value',
                'nome_logradouro_one_sup', 'complemento_concatenado_one_sup', 'nome_logradouro_two_sup',
                'complemento_concatenado_two_sup']
base_b = base_b.drop(*drop_columns)

print(f"Quantidade de registros: {base_a.count()}")

# Escrevendo a base A
base_b.write.format("parquet") \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .save(f"{output_path}")

print("Base escrita!")


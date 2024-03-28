from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType
from jellyfish import jaro_similarity
from random import sample, randint



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
    return sample(columns, 1)[0]
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
      word_list.pop(randint(0, len(word_list)-1))
  return ' '.join(word_list)


# Registrar a função UDF no Spark
sup_words_udf = F.udf(lambda word, n: sup_words(word, n), StringType())



# set variables
base_a_path = "s3a://cleaned/base_a"
base_b_path = "s3a://cleaned/base_b"
cross_join_output_path = "s3a://cleaned/base_a_base_b_cross_join"
matrix_distance_output_path = "s3a://cleaned/matrix_distance"

_METHOD = 1

# Função para acrescentar sufixo às colunas em comum nas duas bases de dados
def rename_columns(df, columns, df_name):
  for column in columns:
    df = df.withColumnRenamed(column, column+'_'+df_name)
  return df

# Função para calcular a distância entre os registros das duas bases de dados usando Jaro Winkler
def jaro_similarity_function(col1, col2):
  # Se uma das duas colunas forem nulas, devemos considerar o score como 0
  if col1 is None or col2 is None:
    return 0.0

  return jaro_similarity(str(col1), str(col2))

# Registro da função UDF
jaro_similarity_udf = F.udf(lambda col1, col2: jaro_similarity_function(col1, col2), DoubleType())
    
# Read Dataframe
base_a = spark.read.format("parquet").load(base_a_path)
base_b = spark.read.format("parquet").load(base_b_path)


# Renomeando colunas em comum nas duas bases de dados
columns = ['id', 'nome_logradouro', 'complemento_concatenado']
base_a = rename_columns(base_a.select(columns), columns, 'a')
base_b = rename_columns(base_b.select(columns+['noise_type', 'noise_column']), columns, 'b')

# Combinação entre todos os registros das bases para calcular as distâncias
df = base_a.crossJoin(base_b)


# Checkpoint da base de dados para redução do custo computacional
df.checkpoint()

# Calculo de similaridade entre os dois registros

# Método 1: Score é calculado com base na média da similaridade do logradouro e complemento
if _METHOD == 1:
  df = (
        df.withColumn("nome_logradouro_similarity", jaro_similarity_udf(F.col('nome_logradouro_a'), F.col('nome_logradouro_b')))
          .withColumn("complemento_concatenado_similarity", jaro_similarity_udf(F.col('complemento_concatenado_a'), F.col('complemento_concatenado_b')))
          .withColumn("score", (F.col("nome_logradouro_similarity") + F.col("complemento_concatenado_similarity"))/2)
  )




# Escrevendo a base A
df.write.format("parquet") \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .save(f"{cross_join_output_path}")

print("Base matrix_distance escrita!")

df.select('id_a', 'id_b', 'score')\
  .write.format("parquet") \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .save(matrix_distance_output_path)



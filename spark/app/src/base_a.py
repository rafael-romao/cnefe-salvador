import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import trim, col, monotonically_increasing_id, current_timestamp, regexp_replace, concat_ws


spark = SparkSession.builder.getOrCreate()    
spark.sparkContext.setLogLevel("ERROR")
sc = spark
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "7hgOYRYF7bQvG6toul8I")
hadoop_conf.set("fs.s3a.secret.key", "cG8S9jo2c9rY6ynCp8rqZ5bNuY1WsZbzTepLHBcG")
hadoop_conf.set("fs.s3a.path.style.access", "True")
# hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# Função para remover espaços em branco
def trim_df(df):
    return df.select([trim(col(c)).alias(c) for c in df.columns])


# set variables
cnefe_bahia_raw = "s3a://raw/cnefe_bahia"
output_path = "s3a://cleaned/base_a"
codigo_municipio = "27408"
limit_amostra = 100000
    
# Read Dataframe
cnefe_bahia = spark.read.format("parquet").load(cnefe_bahia_raw)
print(f"Quantidade de registros: {cnefe_bahia.count()}")

# Limpeza e filtragem
clean_cnefe_ssa = trim_df(cnefe_bahia).filter(col("codigo_municipio") == codigo_municipio)

# Adição de colunas ID e EXTRACTED_AT
clean_cnefe_ssa = clean_cnefe_ssa.withColumn("id", monotonically_increasing_id())
clean_cnefe_ssa = clean_cnefe_ssa.withColumn("extracted_at", current_timestamp())

# Define as colunas a serem selecionadas
columns = [
    "id",
    "setor_censitario",
    "tipo_logradouro",
    "titulo_logradouro",
    "nome_logradouro",
]

# Colunas de complemento
for i in range(1, 7):
    columns.append(f"complemento_elemento_{i}")
    columns.append(f"complemento_valor_{i}")

# Realiza a amostragem
base_a = clean_cnefe_ssa.select(columns).sample(fraction=0.1, seed=7).limit(limit_amostra)

complemento_cols = []
for i in range(1, 7):
    complemento_cols.append(f"complemento_elemento_{i}")
    complemento_cols.append(f"complemento_valor_{i}")

# Concatena as colunas de complemento

base_a = base_a.withColumn(
    "complemento_concatenado", concat_ws(" ", *[col(col_name) for col_name in complemento_cols])
)

# Remove as colunas de complemento duplicadas
base_a = base_a.drop(*complemento_cols)

# Remove espaços em branco em excesso na coluna de complemento
# base_a = base_a.withColumn("complemento_concatenado", trim(col("complemento_concatenado")))

regex = r"\s{2,}"
replace_with = " "

base_a = (base_a.withColumn("complemento_concatenado", trim("complemento_concatenado"))
                .withColumn("complemento_concatenado", regexp_replace(col("complemento_concatenado"), regex, replace_with)))


print(f"Quantidade de registros: {base_a.count()}")

# Escrevendo a base A
base_a.write.format("parquet") \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .save(f"{output_path}")

print("Base escrita!")


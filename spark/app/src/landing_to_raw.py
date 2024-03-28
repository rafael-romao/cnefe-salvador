from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os


spark = SparkSession.builder.getOrCreate()    
spark.sparkContext.setLogLevel("ERROR")
sc = spark
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "7hgOYRYF7bQvG6toul8I")
hadoop_conf.set("fs.s3a.secret.key", "cG8S9jo2c9rY6ynCp8rqZ5bNuY1WsZbzTepLHBcG")
hadoop_conf.set("fs.s3a.path.style.access", "True")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

# get the cnefe data set file name
cnefe_file = "s3a://landing/29.txt"
output_path = "s3a://raw/cnefe_bahia"

# print(f"MINIO URL: {os.environ['MINIO_URL']}")


# Parse database
def parse_dataframe(df: DataFrame, schema: dict) -> DataFrame:
    return df.select([
        substring(col("_c0"), start, length).alias(field) for field, (start, length) in schema.items()
    ])

# Read Dataframe
cnefe_df = spark.read.format("csv").load(cnefe_file)
print(f"Quantidade de registros: {cnefe_df.count()}")

cnefe_uf_schema = {
    "setor_censitario":(0,15),
    "codigo_uf":(0,2),
    "codigo_municipio":(3,5),
    "codigo_distrito": (8,2),
    "codigo_subdistrito": (10,2),
    "codigo_setor": (12,4),
    "situacao_setor": (16,1),
    "tipo_logradouro": (17,20),
    "titulo_logradouro": (37,30),
    "nome_logradouro": (67,60),
    "numero_logradouro": (127,8),
    "modificador_numero": (135,6),
    "complemento_elemento_1": (142,20),
    "complemento_valor_1": (162,10),
    "complemento_elemento_2": (172,20),
    "complemento_valor_2": (192,10),
    "complemento_elemento_3": (202,20),
    "complemento_valor_3": (222,10),
    "complemento_elemento_4": (232,20),
    "complemento_valor_4": (252,10),
    "complemento_elemento_5": (262,20),
    "complemento_valor_5": (282,10),
    "complemento_elemento_6": (292,20),
    "complemento_valor_6": (312,10),
    "latiturde": (322,15),
    "longitude": (337,15),
    "localicade": (352,60),
    "nulo": (412,60),
    "especie_endereco": (472,2),
    "identificacao_estabelecimento": (474,40),
    "indicador_endereco": (514,1),
    "identificacao_domicilio_coletivo": (515,30),
    "numero_quadra": (545,3),
    "numero_face": (548,3),
    "cep": (551,8)
}


parsed_cnefe = parse_dataframe(cnefe_df, cnefe_uf_schema)

print(f"Quantidade de registros depois: {parsed_cnefe.count()}")

# Escrevendo a base A
parsed_cnefe.write.format("parquet") \
        .mode("overwrite") \
        .option("parquet.compression", "snappy") \
        .save(f"{output_path}")

print("Base escrita!")


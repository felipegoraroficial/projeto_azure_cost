from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, date_format
import os


def processar_dados():

    DELTA_LAKE_PACKAGE = "io.delta:delta-core_2.12:3.3.2"

    spark = SparkSession.builder \
        .appName("Bronze") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "2g") \
        .config("spark.cores.max", "2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("KEY_ACCESS")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("KEY_SECRETS")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026,io.delta:delta-core_2.12:2.4.0") \
        .getOrCreate()

    # Caminho inbound para os arquivos JSON no MinIO
    inbound_path = "s3a://azurecost/inbound/*.json"

    # Lendo os arquivos JSON como DataFrame
    df_new = spark.read.json(inbound_path)

    # Adiciona a coluna 'data_ref' com a data atual formatada como 'yyyy-MM-dd'
    df_new = df_new.withColumn("data_ref", date_format(current_date(), "yyyy-MM-dd"))

    df_new.show()

    # Caminho para a tabela Delta (no seu MinIO)
    bronze_path = "s3a://azurecost/bronze"

    # Lê os dados somente da última partição
    df_old = spark.read.format("delta").load(bronze_path)

    df_old.show(truncate=False)

    df_combined = df_new.unionByName(df_old)

    df_incremental = df_combined.dropDuplicates()


    df_incremental.show(truncate=False)

    df_incremental.write.format("delta") \
        .mode("append") \
        .partitionBy("data_ref") \
        .save(bronze_path)
    
    spark.stop()


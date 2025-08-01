import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, col, row_number, concat_ws
from pyspark.sql.window import Window

def load_data_in_resource():
    DELTA_LAKE_PACKAGE = "io.delta:delta-core_2.12:3.3.2"

    spark = SparkSession.builder \
        .appName("Load data in resource table") \
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
        .config(
             "spark.jars.packages",
             "org.apache.hadoop:hadoop-aws:3.3.4,"
             "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
             "io.delta:delta-core_2.12:2.4.0,"
             "org.postgresql:postgresql:42.6.0"
        ) \
        .getOrCreate()

    gold_path = "s3a://azurecost/gold"

    df = spark.read.format("delta").load(gold_path)

    df_id = df.withColumn('concat', concat_ws("_", col("ResourceName"), col("SubscriptionId"), col("ResourceGroup")))
    df_id = df_id.select(
        "concat",
        "ResourceName",
        "SubscriptionId",
        "ResourceGroup"
    ).distinct()

    window_spec = Window.orderBy("concat")
    df_id = df_id.withColumn("Id", row_number().over(window_spec))
    df_id = df_id.select("concat", "Id").distinct()

    df = df.select(
        "ResourceName",
        "SubscriptionId",
        "ResourceGroup",
        "Provider",
        "StatusRecourse",
        "PreTaxCost",
        "Pct_Change",
        "Currency",
        "UsageDate",
        "TendenciaCusto",
        "PrevisaoProxima"
    )

    latest_dates_df = df.groupBy("ResourceName").agg(max("UsageDate").alias("MaxUsageDate"))

    window_spec = Window.partitionBy("ResourceName").orderBy(col("UsageDate").desc())
    df_with_rank = df.withColumn("rank", row_number().over(window_spec))

    df_latest_per_resource = df_with_rank.filter(col("rank") == 1).drop("rank")

    df_resources = df_latest_per_resource.select(
        "ResourceName",
        "SubscriptionId",
        "ResourceGroup",
        "Provider",
        "StatusRecourse",
        "Currency",
        "TendenciaCusto"
    )

    df_resources = df_resources.withColumn('concat', concat_ws("_", col("ResourceName"), col("SubscriptionId"), col("ResourceGroup")))

    df_resources = df_resources.join(df_id, on="concat", how="left").drop("concat")

    df_resources = df_resources.select(
        "Id",
        "ResourceName",
        "SubscriptionId",
        "ResourceGroup",
        "Provider",
        "StatusRecourse",
        "Currency",
        "TendenciaCusto"
    )

    df_resources.show(truncate=False)

    # Informações de conexão PostgreSQL
    DB_HOST = "airflow-postgres"
    DB_PORT = "5432"
    DB_USER = os.getenv('POSTGRES_USER')
    DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    DB_NAME = "azurecost"

    # Conecta ao PostgreSQL para executar TRUNCATE
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    conn.autocommit = True

    try:
        # Trunca tabela 'resources' antes de inserir dados novos
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE resources;")

        conn.close()
    except:
        pass

    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Grava os dados usando append (já que a tabela foi truncada)
    df_resources.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "resources") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    spark.stop()
    print("Dados inseridos com sucesso na tabela 'resources'.")

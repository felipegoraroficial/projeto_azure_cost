from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, regexp_extract, max, date_add, lit, lag, when, round
from pyspark.sql import Window
from delta.tables import DeltaTable
import os



def normalizar_dados():

    DELTA_LAKE_PACKAGE = "io.delta:delta-core_2.12:3.3.2"

    spark = SparkSession.builder \
        .appName("Silver") \
        .master("spark://spark-master:7077") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "8g") \
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
    
    # Caminho para a tabela Delta (no seu MinIO)
    bronze_path = "s3a://azurecost/bronze"

    # Inicializa objeto DeltaTable
    delta_table = DeltaTable.forPath(spark, bronze_path)

    # Obtém todos os valores únicos da partição
    partitions_df = delta_table.toDF().select("data_ref").distinct()

    # Obtém o valor mais recente da partição
    max_partition = partitions_df.agg({"data_ref": "max"}).collect()[0][0]
    print(f"Última partição disponível: {max_partition}")

    # Lê os dados somente da última partição
    df = spark.read.format("delta").load(bronze_path).filter(f"data_ref = '{max_partition}'")

    df.show(truncate=False)

    df_transformado = (
        df
        .withColumn("PreTaxCost", col("PreTaxCost").cast("double"))
        .withColumn("UsageDate", to_timestamp(col("UsageDate"), "yyyy-MM-dd'T'HH:mm"))
        .withColumn("data_ref", to_date(col("data_ref"), "yyyy-MM-dd"))
    )

    df_transformado.show(truncate=False)
    df_transformado.printSchema()

    # Regex patterns
    regex_subscription = r"/subscriptions/([^/]+)/resourcegroups"
    regex_provider = r"/providers/([^/]+)/"
    regex_resource = r".*/([^/]+)$"

    df_ordenado = (
        df_transformado
        .withColumn("SubscriptionId", regexp_extract("ResourceId", regex_subscription, 1))
        .withColumn("Provider", regexp_extract("ResourceId", regex_provider, 1))
        .withColumn("ResourceName", regexp_extract("ResourceId", regex_resource, 1))
        .select(
            "SubscriptionId",
            "ResourceGroup",
            "Provider",
            "ResourceName",
            "PreTaxCost",
            "Currency",
            "UsageDate",
            "data_ref"
        )
    )

    max_date = df_ordenado.select(max("UsageDate").alias("max_date")).collect()[0]["max_date"]

    ativos_hoje = df_ordenado.filter(col("UsageDate") == max_date).select("ResourceName").distinct()

    ativos_hoje_list = [row.ResourceName for row in ativos_hoje.collect()]

    w = Window.partitionBy("ResourceName")
    df_last = df_ordenado.withColumn("max_usage", max("UsageDate").over(w))

    df_last = df_last.filter(col("UsageDate") == col("max_usage"))

    df_obsoletos = df_last.join(ativos_hoje, on="ResourceName", how="left_anti")

    df_novos = df_obsoletos.withColumn("UsageDate", date_add(col("UsageDate"), 1)) \
                        .withColumn("PreTaxCost", lit(0.0)) \
                        .withColumn("data_ref", to_date(col("UsageDate")))

    df_novos = df_novos.select(*df_ordenado.columns)

    df_com_status = df_ordenado.unionByName(df_novos)

    df_com_status = df_com_status.withColumn(
        "StatusRecourse",
        when(col("ResourceName").isin(ativos_hoje_list), lit("Ativo")).otherwise(lit("Inativo"))
    )

    w = Window.partitionBy("ResourceName").orderBy("UsageDate")

    df_temp  = df_com_status.withColumn("PreTaxCost_prev", lag(col("PreTaxCost")).over(w))

    df_final = df_temp.withColumn(
        "Pct_Change",
        when(
            col("PreTaxCost_prev").isNull(), lit(0.0)
        ).when(
            col("PreTaxCost_prev") == 0, lit(0.0)
        ).otherwise(
            round(
                ((col("PreTaxCost") - col("PreTaxCost_prev")) / col("PreTaxCost_prev")) * 100, 2
            )
        )
    ).drop("PreTaxCost_prev") \
    .orderBy(col("UsageDate").desc()) \
    .select(
        "SubscriptionId",
        "ResourceGroup",
        "Provider",
        "ResourceName",
        "StatusRecourse",
        "PreTaxCost",
        "Pct_Change",
        "Currency",
        "UsageDate",
        "data_ref"
    )

    df_final.show(truncate=False)

    # Caminho S3A para os dados no formato delta na camada silver
    silver_path = "s3a://azurecost/silver"

    df_final.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .save(silver_path)
    
    spark.stop()
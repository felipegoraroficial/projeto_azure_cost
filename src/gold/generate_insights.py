from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, unix_timestamp, lit
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import DenseVector
import os


def calcular_dados_futuro():

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
    silver_path = "s3a://azurecost/silver"

    # Inicializa objeto DeltaTable
    delta_table = DeltaTable.forPath(spark, silver_path)

    # Obtém todos os valores únicos da partição
    partitions_df = delta_table.toDF().select("data_ref").distinct()

    # Obtém o valor mais recente da partição
    max_partition = partitions_df.agg({"data_ref": "max"}).collect()[0][0]
    print(f"Última partição disponível: {max_partition}")

    # Lê os dados somente da última partição
    df = spark.read.format("delta").load(silver_path).filter(f"data_ref = '{max_partition}'")

    df_tend = df.withColumn(
        "TendenciaCusto",
        when(col("Pct_Change") > 0, "Subindo")
        .when(col("Pct_Change") < 0, "Descendo")
        .otherwise("Estável")
    )

    # 1. Converter UsageDate em número
    df_ts = df_tend.withColumn("UsageDate_num", unix_timestamp(col("UsageDate")))

    # 2. Listar todos os recursos únicos
    resource_list = [row["ResourceName"] for row in df_ts.select("ResourceName").distinct().collect()]

    previsoes = []

    # 3. Loop para treinar e prever para cada ResourceName
    for resource in resource_list:
        df_recurso = df_ts.filter(col("ResourceName") == resource)

        assembler = VectorAssembler(inputCols=["UsageDate_num"], outputCol="features")
        df_feat = assembler.transform(df_recurso)

        if df_feat.count() < 2:
            previsoes.append((resource, None))
            continue

        lr = LinearRegression(featuresCol="features", labelCol="PreTaxCost")
        model = lr.fit(df_feat)

        last_ts = df_feat.agg({"UsageDate_num": "max"}).first()[0]
        future_ts = last_ts + 600

        row_prediction = df_feat.sql_ctx.createDataFrame([
            (DenseVector([float(future_ts)]),)
        ], ["features"])

        result = model.transform(row_prediction).select("prediction").collect()[0][0]
        previsoes.append((resource, result))

    # 4. Criar DataFrame com as previsões
    schema = StructType([
        StructField("ResourceName", StringType(), True),
        StructField("PrevisaoProxima", DoubleType(), True),
    ])

    df_previsao = spark.createDataFrame(previsoes, schema)

    # ⚠️ 5. Remover coluna PrevisaoProxima anterior (se existir) para evitar ambiguidade
    if "PrevisaoProxima" in df_ts.columns:
        df_ts = df_ts.drop("PrevisaoProxima")

    # 6. Join com as previsões
    df_final = df_ts.join(df_previsao, on="ResourceName", how="left")

    # 7. Seleciona e exibe
    df_final.orderBy("UsageDate").show(truncate=False)

    # Caminho S3A para os dados no formato delta na camada silver
    gold_path = "s3a://azurecost/gold"

    df_final.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("data_ref") \
        .save(gold_path)
    
    spark.stop()
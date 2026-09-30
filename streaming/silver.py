from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = (
    SparkSession.builder.appName("silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("card_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_category", StringType()),
    StructField("country", StringType()),
    StructField("home_country", StringType()),
    StructField("event_time", StringType()),
    StructField("is_fraud", IntegerType()),
])

bronze = spark.readStream.format("delta").load("/data/bronze/transactions")

silver = (
    bronze.select(from_json(col("value"), schema).alias("t"))
    .select("t.*")
    .filter(
        col("transaction_id").isNotNull()
        & col("card_id").isNotNull()
        & col("is_fraud").isNotNull()
        & (col("amount") > 0)
    )
    .withColumn("event_time", col("event_time").cast("timestamp"))
)

query = (
    silver.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/data/checkpoints/silver")
    .trigger(availableNow=True)
    .start("/data/silver/transactions")
)
query.awaitTermination()
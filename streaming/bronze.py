from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 50000)
    .load()
)

bronze = raw.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp"),
)

query = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/data/checkpoints/bronze")
    .trigger(processingTime="10 seconds")
    .start("/data/bronze/transactions")
)
query.awaitTermination()
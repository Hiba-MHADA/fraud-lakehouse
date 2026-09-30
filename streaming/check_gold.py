from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, round as sround

spark = (
    SparkSession.builder.appName("check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/gold/features")
df.groupBy("is_fraud").agg(
    count("*").alias("n"),
    sround(avg("amount_ratio"), 2).alias("avg_amount_ratio"),
    sround(avg("is_foreign"), 3).alias("avg_is_foreign"),
    sround(avg("tx_1h"), 2).alias("avg_tx_1h"),
).orderBy("is_fraud").show()
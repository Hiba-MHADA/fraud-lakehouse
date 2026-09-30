from pyspark.sql import SparkSession
from pyspark.sql.functions import count, get_json_object, max as smax

spark = (
    SparkSession.builder.appName("check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/bronze/transactions")
print("TOTAL:", df.count())
df.groupBy("partition").agg(count("*").alias("n"), smax("offset").alias("max_offset")).orderBy("partition").show()
df.select(get_json_object("value", "$.is_fraud").alias("is_fraud")).groupBy("is_fraud").count().show()
df.select("key", "value").show(3, truncate=False)
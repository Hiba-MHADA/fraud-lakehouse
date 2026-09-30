from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/silver/transactions")
print("TOTAL:", df.count())
df.printSchema()
df.groupBy("is_fraud").count().show()
df.filter("is_fraud = 1").show(3, truncate=False)
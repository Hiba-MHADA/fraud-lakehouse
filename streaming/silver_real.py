from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

spark = (
    SparkSession.builder.appName("silver_real")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [StructField("row_id", LongType())]
    + [StructField(f"V{i}", DoubleType()) for i in range(1, 29)]
    + [
        StructField("Amount", DoubleType()),
        StructField("is_fraud", IntegerType()),
        StructField("event_time", StringType()),
    ]
)

bronze = spark.read.format("delta").load("/data/bronze/transactions_real")
parsed = bronze.select(from_json(col("value"), schema).alias("t")).select("t.*")

silver = (
    parsed.filter(
        col("row_id").isNotNull()
        & col("Amount").isNotNull()
        & (col("Amount") >= 0)
        & col("is_fraud").isin(0, 1)
    )
    .dropDuplicates(["row_id"])
    .withColumn("event_time", col("event_time").cast("timestamp"))
)

silver.write.format("delta").mode("overwrite").save("/data/silver/transactions_real")

out = spark.read.format("delta").load("/data/silver/transactions_real")
print("BRONZE:", bronze.count())
print("SILVER:", out.count())
print("FRAUDES:", out.filter("is_fraud = 1").count())
out.select("row_id", "V1", "V2", "Amount", "is_fraud").orderBy("row_id").show(5)
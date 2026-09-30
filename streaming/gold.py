from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, avg, count, lag, when, coalesce, lit, unix_timestamp, round as sround
)

spark = (
    SparkSession.builder.appName("gold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/data/silver/transactions")
df = df.withColumn("ts", unix_timestamp("event_time"))

w_order = Window.partitionBy("card_id").orderBy("ts", "transaction_id")
w_prev = w_order.rowsBetween(Window.unboundedPreceding, -1)
w_1h = Window.partitionBy("card_id").orderBy("ts").rangeBetween(-3600, -1)

avg_prev = avg("amount").over(w_prev)

gold = (
    df.withColumn("n_prev", count(lit(1)).over(w_prev))
    .withColumn("amount_ratio", when(avg_prev.isNull(), lit(1.0)).otherwise(col("amount") / avg_prev))
    .withColumn("tx_1h", count(lit(1)).over(w_1h))
    .withColumn("sec_since_prev", coalesce(col("ts") - lag("ts").over(w_order), lit(-1)))
    .withColumn("is_foreign", (col("country") != col("home_country")).cast("int"))
    .select(
        "transaction_id", "card_id", "event_time", "amount", "merchant_category",
        "n_prev", "amount_ratio", "tx_1h", "sec_since_prev", "is_foreign", "is_fraud",
    )
)

gold.write.format("delta").mode("overwrite").save("/data/gold/features")

out = spark.read.format("delta").load("/data/gold/features")
print("TOTAL:", out.count())
out.groupBy("is_fraud").agg(
    count("*").alias("n"),
    sround(avg("amount_ratio"), 2).alias("avg_amount_ratio"),
    sround(avg("is_foreign"), 3).alias("avg_is_foreign"),
    sround(avg("tx_1h"), 2).alias("avg_tx_1h"),
).show()
out.show(5, truncate=False)
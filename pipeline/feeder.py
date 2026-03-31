# coding: utf-8
from datetime import date
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("feeder").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

today = date.today()
year  = today.year
month = today.month
day   = today.day

# Les 3 CSV ingeres vers HDFS raw
sources = [
    ("file:///source/olist_order_items_dataset.csv",   "order_items"),
    ("file:///source/olist_order_reviews_dataset.csv", "reviews"),
    ("file:///source/olist_orders_dataset.csv",        "orders"),
]

for path, name in sources:
    df = spark.read.option("header","true").option("inferSchema","true").csv(path)
    df2 = (df.withColumn("year",  F.lit(year))
              .withColumn("month", F.lit(month))
              .withColumn("day",   F.lit(day)))
    df2.cache()
    n = df2.count()
    print("%s : %d lignes" % (name, n))
    out = "hdfs://namenode:9000/data/raw/olist/%s" % name
    df2.repartition(4).write.mode("overwrite").partitionBy("year","month","day").parquet(out)
    print("%s ecrit OK" % name)

print("Feeder termine avec succes !")
spark.stop()
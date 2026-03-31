# coding: utf-8
from datetime import date
from pyspark.sql import SparkSession, functions as F, Window

spark = SparkSession.builder.appName("processor").config("spark.sql.shuffle.partitions","4").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

today = date.today()
year  = today.year
month = today.month
day   = today.day

# Lecture des 3 sources depuis HDFS raw (pas de SQLite, pas de Python en memoire)
base = "hdfs://namenode:9000/data/raw/olist"
df_items   = spark.read.parquet("%s/order_items/year=%d/month=%d/day=%d"   % (base, year, month, day))
df_reviews = spark.read.parquet("%s/reviews/year=%d/month=%d/day=%d"       % (base, year, month, day))
df_orders  = spark.read.parquet("%s/orders/year=%d/month=%d/day=%d"        % (base, year, month, day))

print("items   : %d" % df_items.count())
print("reviews : %d" % df_reviews.count())
print("orders  : %d" % df_orders.count())

# Typage
df_items   = df_items.withColumn("price", F.col("price").cast("double")).withColumn("freight_value", F.col("freight_value").cast("double"))
df_reviews = df_reviews.withColumn("review_score", F.col("review_score").cast("int"))
df_orders  = (df_orders
    .withColumn("order_purchase_timestamp",      F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
    .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date")))

# Validation 5 regles minimum
df_items = df_items.filter(F.col("order_id").isNotNull())
print("R1 order_id non null : %d" % df_items.count())
df_items = df_items.filter(F.col("price") > 0)
print("R2 price > 0 : %d" % df_items.count())
df_items = df_items.filter(F.col("freight_value") >= 0)
print("R3 freight >= 0 : %d" % df_items.count())
df_reviews = df_reviews.filter(F.col("review_score").between(1, 5))
print("R4 score 1-5 : %d" % df_reviews.count())
df_orders = df_orders.filter(F.col("order_status").isNotNull())
print("R5 status non null : %d" % df_orders.count())

# Jointure des 3 sources sur order_id
df_joined = (df_items
    .join(df_orders,  on="order_id", how="inner")
    .join(df_reviews.select("order_id","review_score"), on="order_id", how="left")
    .withColumn("total_amount",        F.col("price") + F.col("freight_value"))
    .withColumn("delivery_delay_days", F.datediff("order_delivered_customer_date","order_estimated_delivery_date")))

print("Jointure : %d lignes" % df_joined.count())

# Window function 1 : RANK sellers par revenu
df_seller = (df_joined.groupBy("seller_id")
    .agg(F.count("order_id").alias("nb_orders"), F.sum("price").alias("total_revenue"), F.avg("review_score").alias("avg_review_score"))
    .withColumn("revenue_rank", F.rank().over(Window.orderBy(F.desc("total_revenue")))))

# Window function 2 : revenu cumule mensuel
df_monthly = (df_joined
    .withColumn("order_month", F.date_trunc("month","order_purchase_timestamp"))
    .groupBy("order_month")
    .agg(F.count("order_id").alias("nb_orders"), F.sum("total_amount").alias("monthly_revenue"))
    .withColumn("cumulative_revenue", F.sum("monthly_revenue").over(Window.orderBy("order_month"))))

print("Sellers : %d" % df_seller.count())
print("Monthly : %d" % df_monthly.count())

# Ecriture Silver
def write_silver(df, name):
    path = "hdfs://namenode:9000/data/silver/olist/%s" % name
    (df.withColumn("year",F.lit(year)).withColumn("month",F.lit(month)).withColumn("day",F.lit(day))
       .repartition(2).write.mode("overwrite").partitionBy("year","month","day").parquet(path))
    print("%s OK" % name)

write_silver(df_joined,  "orders_enriched")
write_silver(df_seller,  "seller_ranking")
write_silver(df_monthly, "monthly_stats")

print("Processor termine avec succes !")
spark.stop()
# -*- coding: utf-8 -*-
import argparse
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("datamart")
    .getOrCreate()
)

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", required=True)
parser.add_argument("--jdbc_url",   required=True)
parser.add_argument("--jdbc_user",  required=True)
parser.add_argument("--jdbc_pass",  required=True)
parser.add_argument("--year",  type=int, required=True)
parser.add_argument("--month", type=int, required=True)
parser.add_argument("--day",   type=int, required=True)
args = parser.parse_args()

props = {
    "user":     args.jdbc_user,
    "password": args.jdbc_pass,
    "driver":   "org.postgresql.Driver"
}

# Lecture Silver
path_enriched = "{}/orders_enriched/year={}/month={}/day={}".format(args.input_path, args.year, args.month, args.day)
path_seller   = "{}/seller_ranking/year={}/month={}/day={}".format(args.input_path, args.year, args.month, args.day)
path_monthly  = "{}/monthly_stats/year={}/month={}/day={}".format(args.input_path, args.year, args.month, args.day)

df_enriched = spark.read.parquet(path_enriched)
df_seller   = spark.read.parquet(path_seller)
df_monthly  = spark.read.parquet(path_monthly)

df_enriched.cache()
df_seller.cache()
df_monthly.cache()

print("enriched : {} lignes".format(df_enriched.count()))
print("seller   : {} lignes".format(df_seller.count()))
print("monthly  : {} lignes".format(df_monthly.count()))

# DM1 : statuts commandes
dm1 = (
    df_enriched
    .groupBy("order_status")
    .agg(
        F.count("order_id").alias("nb_orders"),
        F.avg("total_amount").alias("avg_amount"),
        F.avg("review_score").alias("avg_review_score"),
        F.avg("delivery_delay_days").alias("avg_delay_days"),
        F.sum("total_amount").alias("total_revenue")
    )
    .orderBy(F.desc("nb_orders"))
)
dm1.write.mode("overwrite").jdbc(args.jdbc_url, "dm_order_status_performance", properties=props)
print("dm_order_status_performance OK")

# DM2 : performance sellers
dm2 = df_seller.select(
    "seller_id", "nb_orders", "total_revenue",
    "avg_review_score", "revenue_rank"
)
dm2.write.mode("overwrite").jdbc(args.jdbc_url, "dm_seller_performance", properties=props)
print("dm_seller_performance OK")

# DM3 : revenus mensuels
dm3 = df_monthly.select(
    F.col("order_month").cast("string").alias("order_month"),
    "nb_orders",
    "monthly_revenue",
    "cumulative_revenue"
).orderBy("order_month")
dm3.write.mode("overwrite").jdbc(args.jdbc_url, "dm_monthly_revenue", properties=props)
print("dm_monthly_revenue OK")

# DM4 : distribution avis
dm4 = (
    df_enriched
    .filter(F.col("review_score").isNotNull())
    .groupBy("review_score", "order_status")
    .agg(
        F.count("order_id").alias("nb_orders"),
        F.avg("delivery_delay_days").alias("avg_delay_days")
    )
    .orderBy("review_score")
)
dm4.write.mode("overwrite").jdbc(args.jdbc_url, "dm_review_distribution", properties=props)
print("dm_review_distribution OK")

print("Datamart termine avec succes !")
spark.stop()

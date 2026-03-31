#!/bin/bash
set -e

YEAR=$(date +%Y)
MONTH=$(date +%-m)
DAY=$(date +%-d)
LOG_DIR="/opt/pipeline/logs"
mkdir -p "$LOG_DIR"

HDFS_RAW="hdfs://namenode:9000/data/raw/olist"
HDFS_SILVER="hdfs://namenode:9000/data/silver/olist"
JDBC_URL="jdbc:postgresql://postgres:5432/olist_dw"
SOURCE="/source"
PIPELINE="/opt/pipeline"
PG_JAR="$PIPELINE/jars/postgresql-42.7.3.jar"
SQLITE_JAR="$PIPELINE/jars/sqlite-jdbc-3.45.3.0.jar"

echo "======================================"
echo " OLIST PIPELINE – $(date)"
echo " NOTE : csv_to_sql.py déjà exécuté sur le Mac"
echo " orders.db présent dans /source/"
echo "======================================"

echo ">>> ÉTAPE 1 : feeder.py"
/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --num-executors 2 --executor-cores 2 \
  --executor-memory 1g --driver-memory 512m \
  $PIPELINE/feeder.py \
  --input_items   file://$SOURCE/olist_order_items_dataset.csv \
  --input_reviews file://$SOURCE/olist_order_reviews_dataset.csv \
  --output_path   $HDFS_RAW

echo ">>> ÉTAPE 2 : processor.py"
/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --num-executors 2 --executor-cores 2 \
  --executor-memory 1g --driver-memory 512m \
  --jars $SQLITE_JAR \
  --conf spark.sql.shuffle.partitions=8 \
  $PIPELINE/processor.py \
  --input_path  $HDFS_RAW \
  --sqlite_path $SOURCE/orders.db \
  --output_path $HDFS_SILVER \
  --year $YEAR --month $MONTH --day $DAY

echo ">>> ÉTAPE 3 : datamart.py"
/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --num-executors 2 --executor-cores 2 \
  --executor-memory 1g --driver-memory 512m \
  --jars $PG_JAR \
  $PIPELINE/datamart.py \
  --input_path $HDFS_SILVER \
  --jdbc_url   $JDBC_URL \
  --jdbc_user  olist --jdbc_pass olist123 \
  --year $YEAR --month $MONTH --day $DAY

echo "======================================"
echo " PIPELINE TERMINÉ !"
echo " Spark Master : http://localhost:8080"
echo " HDFS         : http://localhost:9870"
echo " Dashboard    : http://localhost:8501"
echo " API          : http://localhost:8000/docs"
echo "======================================"

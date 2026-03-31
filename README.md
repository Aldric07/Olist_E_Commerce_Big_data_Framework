# Olist Data Platform – M1 Data Engineering

## Problematique business

Analyse de la performance e-commerce **Olist** (Bresil) :
- Quels vendeurs generent le plus de revenus ?
- Comment evolue le chiffre d'affaires mensuel ?
- Quel est l'impact des delais de livraison sur la satisfaction client ?

## Donnees (3 sources)

| # | Dataset | Format | Traitement | Lignes |
|---|---------|--------|------------|--------|
| 1 | olist_order_items_dataset.csv | CSV | feeder.py | ~112 000 |
| 2 | olist_order_reviews_dataset.csv | CSV | feeder.py | ~104 000 |
| 3 | olist_orders_dataset.csv | CSV → SQLite | csv_to_sql.py | ~99 000 |
| | **Total** | | | **~315 000 lignes** |

## Architecture Medallion

```
3 CSV (source)
     │
     ▼  csv_to_sql.py  → orders.db (SQLite)
     ▼  feeder.py      → HDFS /raw/olist/ (Parquet partitionne year/month/day)
     ▼  processor.py   → HDFS /silver/olist/ (jointure + validation + window functions)
     ▼  datamart.py    → PostgreSQL (4 datamarts)
     ▼
API FastAPI (JWT) + Dashboard Streamlit
```

## Structure du projet

```
olist-data-platform/
├── docker-compose.yml
├── hadoop.env
├── hadoop-hive.env
├── source/                    ← Deposer les CSV ici
├── pipeline/
│   ├── csv_to_sql.py          ← Etape 0 : CSV orders → SQLite
│   ├── feeder.py              ← Etape 1 : 2 CSV → HDFS /raw
│   ├── processor.py           ← Etape 2 : RAW + SQLite → /silver
│   ├── datamart.py            ← Etape 3 : Silver → PostgreSQL
│   ├── run.sh
│   ├── logs/
│   └── jars/
├── api/
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
├── dashboard/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
└── sql/
    └── init.sql
```

## Setup

### 1. Prerequis
- Docker Desktop (16 Go RAM recommandes)
- Python 3 (sur le Mac pour csv_to_sql.py)

### 2. Cloner et preparer

```bash
git clone https://github.com/TON_USERNAME/olist-data-platform.git
cd olist-data-platform

mkdir -p source pipeline/logs pipeline/jars

# Copier les CSV Olist dans source/
cp /chemin/vers/*.csv source/

# Telecharger les drivers JDBC
curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -o pipeline/jars/postgresql-42.7.3.jar
```

### 3. Lancer l'infrastructure

```bash
docker compose up -d
```

### 4. Executer le pipeline

```bash
# Etape 0 : Transformer orders CSV en SQLite (sur le Mac)
python3 pipeline/csv_to_sql.py \
  --input source/olist_orders_dataset.csv \
  --output source/orders.db

# Etape 1 : Feeder (CSV → HDFS raw)
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 --executor-cores 2 --executor-memory 1g \
  /opt/pipeline/feeder.py

# Etape 2 : Processor (RAW → Silver)
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 --executor-cores 2 --executor-memory 1g \
  /opt/pipeline/processor.py

# Etape 3 : Datamart (Silver → PostgreSQL)
docker exec -it spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --num-executors 2 --executor-cores 2 --executor-memory 1g \
  --jars /opt/pipeline/jars/postgresql-42.7.3.jar \
  /opt/pipeline/datamart.py \
  --input_path hdfs://namenode:9000/data/silver/olist \
  --jdbc_url jdbc:postgresql://postgres:5432/olist_dw \
  --jdbc_user olist --jdbc_pass olist123 \
  --year $(date +%Y) --month $(date +%-m) --day $(date +%-d)
```

## Interfaces

| Service | URL |
|---------|-----|
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| YARN Resource Manager | http://localhost:8088 |
| API Swagger | http://localhost:8000/docs |
| Dashboard Streamlit | http://localhost:8501 |

## API (FastAPI + JWT)

```bash
# Obtenir un token
curl -X POST http://localhost:8000/token \
  -d "username=admin&password=secret"

# Interroger un datamart
curl -H "Authorization: Bearer TON_TOKEN" \
  "http://localhost:8000/datamarts/seller-performance?page=1&page_size=20"
```

## Choix techniques

### Partitionnement
Partitionnement par `year/month/day` sur raw et silver pour le **partition pruning** et l'idempotence des traitements quotidiens.

### Configuration Spark
- `--num-executors 2` : 2 workers disponibles dans Docker
- `--executor-cores 2` : correspond a SPARK_WORKER_CORES=2
- `--executor-memory 1g` : laisse de la marge au systeme (workers configures a 2g)
- `spark.sql.shuffle.partitions=4` : adapte au volume de 315k lignes

### Datamarts
4 tables PostgreSQL couvrant les axes business :
- `dm_order_status_performance` : performance par statut
- `dm_seller_performance` : classement des vendeurs
- `dm_monthly_revenue` : evolution mensuelle
- `dm_review_distribution` : satisfaction clients

### Pagination API
LIMIT/OFFSET PostgreSQL, page_size par defaut 20, maximum 100.

## Regles de validation (processor.py)

| # | Source | Regle |
|---|--------|-------|
| R1 | items | order_id non null |
| R2 | items | price > 0 |
| R3 | items | freight_value >= 0 |
| R4 | reviews | review_score entre 1 et 5 |
| R5 | orders | order_status non null |

## Resultats

- **Revenu total** : R$ 15,915,872
- **Commandes traitees** : 113,314
- **Vendeurs analyses** : 3,095
- **Periode** : Oct 2016 – Aug 2018

## Bareme couvert

- Ingestion raw (2 pts) : feeder.py avec partitionnement year/month/day
- Traitement silver (4 pts) : 5 regles de validation, jointure 3 sources, 2 window functions
- Logs (1 pt) : feeder.txt, processor.txt, datamart.txt
- Problematique business (1 pt) : performance e-commerce Olist
- Analyse business (1.5 pts) : 4 axes d'analyse
- Datamarts (4 pts) : 4 tables PostgreSQL via JDBC
- API (2 pts) : FastAPI, JWT, pagination
- Visualisation (1.5 pts) : Streamlit, 4 graphiques Plotly
- Architecture modulaire (1 pt) : 4 scripts paramétrables
- Video (2 pts) : demonstration complete du pipeline

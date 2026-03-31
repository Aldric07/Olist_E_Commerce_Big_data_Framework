"""
csv_to_sql.py – Transforme olist_orders_dataset.csv en base SQLite orders.db
Usage:
    python3 pipeline/csv_to_sql.py \
        --input  /source/olist_orders_dataset.csv \
        --output /source/orders.db
"""
import argparse, logging, os, sqlite3, sys
import pandas as pd

LOG_DIR = os.environ.get("LOG_DIR", "/opt/pipeline/logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/csv_to_sql.txt"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("csv_to_sql")

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input",  required=True)
    p.add_argument("--output", default="/source/orders.db")
    return p.parse_args()

def main():
    args = parse_args()
    log.info("=== Démarrage csv_to_sql.py ===")
    log.info(f"Input: {args.input} | Output: {args.output}")
    try:
        df = pd.read_csv(args.input)
        log.info(f"CSV chargé : {len(df)} lignes")

        # Nettoyage
        initial = len(df)
        df = df.drop_duplicates(subset=["order_id"])
        df = df.dropna(subset=["order_id", "customer_id"])
        df["order_status"] = df["order_status"].str.strip().str.lower()
        valid = ["delivered","shipped","canceled","processing","invoiced","unavailable","approved","created"]
        df = df[df["order_status"].isin(valid)]

        date_cols = ["order_purchase_timestamp","order_approved_at",
                     "order_delivered_carrier_date","order_delivered_customer_date",
                     "order_estimated_delivery_date"]
        for c in date_cols:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        log.info(f"Après nettoyage : {len(df)} lignes (rejetées : {initial - len(df)})")

        if os.path.exists(args.output):
            os.remove(args.output)

        conn = sqlite3.connect(args.output)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("DROP TABLE IF EXISTS orders")
        conn.execute("""
            CREATE TABLE orders (
                order_id                       TEXT PRIMARY KEY,
                customer_id                    TEXT NOT NULL,
                order_status                   TEXT NOT NULL,
                order_purchase_timestamp       TEXT,
                order_approved_at              TEXT,
                order_delivered_carrier_date   TEXT,
                order_delivered_customer_date  TEXT,
                order_estimated_delivery_date  TEXT
            )
        """)
        conn.execute("CREATE INDEX idx_status ON orders(order_status)")
        conn.commit()

        df.to_sql("orders", conn, if_exists="append", index=False, chunksize=10000)
        conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        log.info(f"SQLite créée : {count} lignes dans orders")
        conn.close()
        log.info("=== csv_to_sql.py terminé avec succès ===")
    except Exception as e:
        log.error(f"Erreur : {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

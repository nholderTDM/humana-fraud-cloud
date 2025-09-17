"""
pipeline.py
-----------
Fraud detection ETL that:
 • Connects to Neon Postgres using NEON_CONN (from GitHub Actions secrets)
 • Ensures fraud_alerts and transactions_all tables exist (idempotent)
 • Drains Upstash Redis queue if REDIS_URL is set (consuming items)
 • Loads CSV (if present) or generates synthetic transactions
 • Flags suspicious transactions (amount >= 10,000, risk_score=90)
 • Inserts:
      – all transactions into transactions_all
      – only flagged ones into fraud_alerts
"""

import os, sys, time, json
from typing import List, Dict
import psycopg2, psycopg2.extras
import pandas as pd

NEON_CONN = os.getenv("NEON_CONN")
if not NEON_CONN:
    print("ERROR: NEON_CONN environment variable is missing.")
    sys.exit(1)

CSV_PATH = "data/transactions_sample.csv"
REDIS_URL = os.getenv("REDIS_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")


def get_conn():
    return psycopg2.connect(NEON_CONN, sslmode="require")

def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        # store ALL transactions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions_all (
              transaction_id TEXT PRIMARY KEY,
              amount DOUBLE PRECISION NOT NULL,
              location TEXT,
              device TEXT,
              processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
              is_flagged BOOLEAN DEFAULT FALSE,
              risk_score INT,
              flagged_reason TEXT
            );
        """)
        # store only fraudulent ones
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
              alert_id SERIAL PRIMARY KEY,
              transaction_id TEXT UNIQUE REFERENCES transactions_all(transaction_id),
              amount DOUBLE PRECISION NOT NULL,
              risk_score INT NOT NULL,
              flagged_reason TEXT NOT NULL,
              created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

def drain_queue() -> pd.DataFrame:
    if not REDIS_URL:
        return pd.DataFrame(columns=["transaction_id","amount","location","device"])
    import redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
    drained = []
    for _ in range(5000):
        item = r.lpop(QUEUE_NAME)
        if item is None:
            break
        drained.append(json.loads(item))
    return pd.DataFrame(drained) if drained else pd.DataFrame(
        columns=["transaction_id","amount","location","device"])

def load_file_or_synthetic() -> pd.DataFrame:
    if os.path.exists(CSV_PATH):
        return pd.read_csv(CSV_PATH)
    base_id = int(time.time())
    rows=[]
    for i in range(1,51):
        rows.append({
            "transaction_id": f"TXN{base_id+i}",
            "amount": float(25*i if i%7 else 25000),
            "location": "USA" if i%3 else "CAN",
            "device": "Web" if i%2 else "Mobile"
        })
    return pd.DataFrame(rows)

def main():
    conn = get_conn()
    try:
        ensure_schema(conn)
        qdf = drain_queue()
        base = load_file_or_synthetic()
        df = pd.concat([qdf, base], ignore_index=True) if not qdf.empty else base

        with conn, conn.cursor() as cur:
            for _, row in df.iterrows():
                tid   = str(row["transaction_id"])
                amt   = float(row["amount"])
                loc   = row.get("location","USA")
                dev   = row.get("device","Web")
                flagged = amt >= 10000
                risk   = 90 if flagged else None
                reason = "high_amount" if flagged else None

                # Upsert into transactions_all
                cur.execute("""
                  INSERT INTO transactions_all
                    (transaction_id, amount, location, device,
                     processed_at, is_flagged, risk_score, flagged_reason)
                  VALUES (%s,%s,%s,%s,NOW(),%s,%s,%s)
                  ON CONFLICT (transaction_id) DO UPDATE
                    SET amount=EXCLUDED.amount,
                        location=EXCLUDED.location,
                        device=EXCLUDED.device,
                        processed_at=EXCLUDED.processed_at,
                        is_flagged=EXCLUDED.is_flagged,
                        risk_score=EXCLUDED.risk_score,
                        flagged_reason=EXCLUDED.flagged_reason;
                """,(tid,amt,loc,dev,flagged,risk,reason))

                # Insert into fraud_alerts if flagged
                if flagged:
                    cur.execute("""
                      INSERT INTO fraud_alerts
                        (transaction_id, amount, risk_score, flagged_reason)
                      VALUES (%s,%s,%s,%s)
                      ON CONFLICT (transaction_id) DO NOTHING;
                    """,(tid,amt,risk,reason))
        print(f"Processed {len(df)} total transactions.")
    finally:
        conn.close()

if __name__=="__

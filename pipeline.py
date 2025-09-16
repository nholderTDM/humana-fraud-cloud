"""
pipeline.py (queue-aware)
-------------------------
End-to-end fraud detection pipeline:
 • Connects to Neon Postgres using NEON_CONN (from GitHub Actions secret)
 • Ensures the fraud_alerts table exists (idempotent)
 • Optionally drains Redis queue (if REDIS_URL is set)
 • Loads transactions (CSV if present, else synthetic)
 • Flags suspicious transactions (amount >= 10,000, risk_score=90)
 • Inserts alerts into the database using ON CONFLICT DO NOTHING
"""

import os
import sys
import time
import json
from typing import List, Dict

import psycopg2
import psycopg2.extras
import pandas as pd


# --- 1. Config ---------------------------------------------------------------
NEON_CONN = os.getenv("NEON_CONN")
if not NEON_CONN:
    print("ERROR: NEON_CONN environment variable is missing.")
    sys.exit(1)

CSV_PATH = "data/transactions_sample.csv"  # optional
REDIS_URL = os.getenv("REDIS_URL")         # only needed if using the queue
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")


# --- 2. Database helpers ------------------------------------------------------
def get_conn():
    return psycopg2.connect(NEON_CONN, sslmode="require")

def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
              alert_id        SERIAL PRIMARY KEY,
              transaction_id  TEXT                NOT NULL,
              amount          DOUBLE PRECISION    NOT NULL,
              risk_score      INT                 NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
              flagged_reason  TEXT                NOT NULL,
              created_at      TIMESTAMP           DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_fraud_alerts_txn
              ON fraud_alerts (transaction_id);
        """)


# --- 3. Optional queue drain --------------------------------------------------
def drain_queue() -> pd.DataFrame:
    """Return a DataFrame of queued transactions, or empty DF if none or no REDIS_URL."""
    if not REDIS_URL:
        return pd.DataFrame(columns=["transaction_id", "amount", "location", "device"])

    print(f"Draining Redis queue '{QUEUE_NAME}'...")
    import redis  # lazy import
    r = redis.from_url(REDIS_URL, decode_responses=True)
    drained = []
    # Drain up to a sensible cap per run to avoid huge batches on free tiers
    max_to_drain = 5000
    for _ in range(max_to_drain):
        item = r.lpop(QUEUE_NAME)
        if item is None:
            break
        obj = json.loads(item)
        drained.append(obj)

    print(f"Drained {len(drained)} queued transactions.")
    if not drained:
        return pd.DataFrame(columns=["transaction_id", "amount", "location", "device"])
    return pd.DataFrame(drained)


# --- 4. Data loading ----------------------------------------------------------
def load_file_or_synthetic() -> pd.DataFrame:
    if os.path.exists(CSV_PATH):
        print(f"Loading transactions from {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        required = {"transaction_id", "amount"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        return df

    print("No CSV found. Generating synthetic transactions...")
    base_id = int(time.time())
    synthetic: List[Dict] = []
    for i in range(1, 51):
        tid = f"TXN{base_id + i}"
        amt = 25 * i if i % 7 else 25000  # spike every 7th transaction
        loc = "USA" if i % 3 else "CAN"
        dev = "Web" if i % 2 else "Mobile"
        synthetic.append({
            "transaction_id": tid,
            "amount": float(amt),
            "location": loc,
            "device": dev
        })
    return pd.DataFrame(synthetic)


# --- 5. Fraud detection -------------------------------------------------------
def detect_fraud(df: pd.DataFrame) -> List[Dict]:
    alerts: List[Dict] = []
    for _, r in df.iterrows():
        amt = float(r["amount"])
        if amt >= 10000:
            alerts.append({
                "transaction_id": str(r["transaction_id"]),
                "amount": amt,
                "risk_score": 90,
                "flagged_reason": "high_amount"
            })
    return alerts


# --- 6. Insert alerts ---------------------------------------------------------
def write_alerts(conn, alerts: List[Dict]) -> int:
    if not alerts:
        return 0
    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO fraud_alerts (transaction_id, amount, risk_score, flagged_reason)
            VALUES (%(transaction_id)s, %(amount)s, %(risk_score)s, %(flagged_reason)s)
            ON CONFLICT (transaction_id) DO NOTHING;
            """,
            alerts,
            page_size=100
        )
    return len(alerts)


# --- 7. Main ------------------------------------------------------------------
def main():
    print("Connecting to Neon...")
    conn = get_conn()
    try:
        print("Ensuring schema...")
        ensure_schema(conn)

        # Drain queue first (if enabled), then merge with file/synthetic
        qdf = drain_queue()
        base = load_file_or_synthetic()
        df = pd.concat([qdf, base], ignore_index=True) if not qdf.empty else base
        print(f"Total transactions to evaluate: {len(df)}")

        print("Detecting fraud...")
        alerts = detect_fraud(df)
        print(f"Detected {len(alerts)} alerts")

        print("Writing alerts to database...")
        inserted = write_alerts(conn, alerts)
        print(f"Inserted {inserted} new alerts (duplicates skipped).")
    finally:
        conn.close()
    print("Pipeline run complete.")


if __name__ == "__main__":
    main()

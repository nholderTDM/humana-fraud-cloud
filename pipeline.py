"""
pipeline.py
------------
End-to-end fraud detection pipeline:
 • Connects to Neon Postgres using NEON_CONN
 • Ensures the fraud_alerts table exists (idempotent)
 • Loads transactions (from CSV if present, otherwise synthetic)
 • Flags suspicious transactions (amount >= 10,000)
 • Inserts alerts into the database using ON CONFLICT DO NOTHING
"""

import os
import sys
import time
from typing import List, Dict

import psycopg2
import psycopg2.extras
import pandas as pd

# --- 1. Configuration ---------------------------------------------------------
NEON_CONN = os.getenv("NEON_CONN")  # GitHub Actions secret
if not NEON_CONN:
    print("ERROR: NEON_CONN environment variable is missing.")
    sys.exit(1)

CSV_PATH = "data/transactions_sample.csv"  # optional CSV file

# --- 2. Database helpers ------------------------------------------------------
def get_conn():
    """Establish a secure SSL connection to Neon Postgres."""
    return psycopg2.connect(NEON_CONN, sslmode="require")

def ensure_schema(conn):
    """Create the fraud_alerts table and unique index if they don't exist."""
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

# --- 3. Data loading ----------------------------------------------------------
def load_transactions() -> pd.DataFrame:
    """
    Load transactions from CSV if present; otherwise generate synthetic demo data.
    Required columns: transaction_id, amount
    """
    if os.path.exists(CSV_PATH):
        print(f"Loading transactions from {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        required_cols = {"transaction_id", "amount"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        return df

    # No CSV present → create synthetic sample
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

# --- 4. Fraud detection -------------------------------------------------------
def detect_fraud(df: pd.DataFrame) -> List[Dict]:
    """
    Simple fraud logic:
     - Flag any transaction with amount >= 10,000
     - Assign a high risk score (90)
    """
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

# --- 5. Insert alerts ---------------------------------------------------------
def write_alerts(conn, alerts: List[Dict]) -> int:
    """
    Insert detected fraud alerts into Neon.
    Uses ON CONFLICT DO NOTHING to avoid duplicates.
    """
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

# --- 6. Main execution --------------------------------------------------------
def main():
    print("Connecting to Neon...")
    conn = get_conn()
    try:
        print("Ensuring schema...")
        ensure_schema(conn)

        print("Loading transactions...")
        df = load_transactions()
        print(f"Loaded {len(df)} transactions")

        print("Detecting fraud...")
        alerts = detect_fraud(df)
        print(f"Detected {len(alerts)} alerts")

        print("Writing alerts to database...")
        inserted = write_alerts(conn, alerts)
        print(f"Inserted {inserted} new alerts (duplicates skipped).")
    finally:
        conn.close()
    print("Pipeline run complete.")

# Run as a script
if __name__ == "__main__":
    main()

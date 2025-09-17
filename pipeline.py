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

import pandas as pd
import psycopg2
import psycopg2.extras


# --------------------------------------------------------------------
# 1. Configuration
# --------------------------------------------------------------------
NEON_CONN = os.getenv("NEON_CONN")
if not NEON_CONN:
    print("❌ Missing NEON_CONN environment variable.")
    sys.exit(1)

CSV_PATH  = "data/transactions_sample.csv"
REDIS_URL = os.getenv("REDIS_URL")
QUEUE_NAME = os.getenv("QUEUE_NAME", "transactions_queue")


# --------------------------------------------------------------------
# 2. Database Helpers
# --------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(NEON_CONN, sslmode="require")

def ensure_schema(conn):
    with conn, conn.cursor() as cur:
        # Fraud alerts table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fraud_alerts (
                alert_id        SERIAL PRIMARY KEY,
                transaction_id  TEXT UNIQUE NOT NULL,
                amount          DOUBLE PRECISION NOT NULL,
                risk_score      INT NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
                flagged_reason  TEXT NOT NULL,
                created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        # All processed transactions
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transactions_all (
                transaction_id  TEXT PRIMARY KEY,
                amount          DOUBLE PRECISION NOT NULL,
                location        TEXT,
                device          TEXT,
                processed_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                is_flagged      BOOLEAN NOT NULL,
                risk_score      INT,
                flagged_reason  TEXT
            );
        """)


# --------------------------------------------------------------------
# 3. Optional Redis Queue Drain
# --------------------------------------------------------------------
def drain_queue() -> pd.DataFrame:
    """Consume and return queued transactions as a DataFrame."""
    if not REDIS_URL:
        return pd.DataFrame(columns=["transaction_id", "amount", "location", "device"])

    import redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
    drained: List[Dict] = []
    max_to_drain = 5000
    for _ in range(max_to_drain):
        item = r.lpop(QUEUE_NAME)  # removes item so queue does not grow forever
        if item is None:
            break
        drained.append(json.loads(item))

    print(f"Drained {len(drained)} transactions from Redis.")
    return pd.DataFrame(drained)


# --------------------------------------------------------------------
# 4. Load Transactions
# --------------------------------------------------------------------
def load_file_or_synthetic() -> pd.DataFrame:
    if os.path.exists(CSV_PATH):
        print(f"Loading sample transactions from {CSV_PATH}")
        df = pd.read_csv(CSV_PATH)
        required = {"transaction_id", "amount"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        return df

    # Generate synthetic demo data
    base_id = int(time.time())
    synthetic: List[Dict] = []
    for i in range(1, 51):
        tid = f"TXN{base_id + i}"
        amt = 25 * i if i % 7 else 25000  # big spike every 7th
        loc = "USA" if i % 3 else "CAN"
        dev = "Web" if i % 2 else "Mobile"
        synthetic.append({
            "transaction_id": tid,
            "amount": float(amt),
            "location": loc,
            "device": dev
        })
    return pd.DataFrame(synthetic)


# --------------------------------------------------------------------
# 5. Fraud Detection
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# 6. Database Inserts
# --------------------------------------------------------------------
def write_all_transactions(conn, df: pd.DataFrame, alerts: List[Dict]) -> None:
    """
    Insert every transaction into transactions_all.
    Mark is_flagged, risk_score, and flagged_reason appropriately.
    """
    if df.empty:
        return

    alert_lookup = {a["transaction_id"]: a for a in alerts}
    rows: List[Dict] = []
    for _, r in df.iterrows():
        tid = str(r["transaction_id"])
        amt = float(r["amount"])
        loc = r.get("location", None)
        dev = r.get("device", None)
        flagged = tid in alert_lookup
        risk   = alert_lookup[tid]["risk_score"] if flagged else None
        reason = alert_lookup[tid]["flagged_reason"] if flagged else None
        rows.append({
            "transaction_id": tid,
            "amount": amt,
            "location": loc,
            "device": dev,
            "is_flagged": flagged,
            "risk_score": risk,
            "flagged_reason": reason
        })

    with conn, conn.cursor() as cur:
        psycopg2.extras.execute_batch(
            cur,
            """
            INSERT INTO transactions_all
              (transaction_id, amount, location, device,
               is_flagged, risk_score, flagged_reason)
            VALUES (%(transaction_id)s, %(amount)s, %(location)s, %(device)s,
                    %(is_flagged)s, %(risk_score)s, %(flagged_reason)s)
            ON CONFLICT (transaction_id) DO UPDATE
              SET amount        = EXCLUDED.amount,
                  location      = EXCLUDED.location,
                  device        = EXCLUDED.device,
                  processed_at  = CURRENT_TIMESTAMP,
                  is_flagged    = EXCLUDED.is_flagged,
                  risk_score    = EXCLUDED.risk_score,
                  flagged_reason= EXCLUDED.flagged_reason;
            """,
            rows,
            page_size=100
        )


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


# --------------------------------------------------------------------
# 7. Main ETL
# --------------------------------------------------------------------
def main():
    print("Connecting to Neon...")
    conn = get_conn()
    try:
        print("Ensuring tables exist...")
        ensure_schema(conn)

        print("Collecting transactions...")
        qdf = drain_queue()
        base = load_file_or_synthetic()
        df = pd.concat([qdf, base], ignore_index=True) if not qdf.empty else base
        print(f"Total transactions this run: {len(df)}")

        print("Running fraud detection...")
        alerts = detect_fraud(df)
        print(f"Flagged {len(alerts)} transactions.")

        print("Writing all transactions to database...")
        write_all_transactions(conn, df, alerts)

        print("Writing fraud alerts to database...")
        inserted = write_alerts(conn, alerts)
        print(f"Inserted {inserted} new fraud alerts.")
    finally:
        conn.close()

    print("✅ Pipeline run complete.")


if __name__ == "__main__":
    main()

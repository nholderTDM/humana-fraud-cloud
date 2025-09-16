import os
import datetime as dt

import streamlit as st
import pandas as pd
import psycopg2


# -----------------------------
# 1) PAGE CONFIG & HEADER
# -----------------------------
st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")
st.caption("Read-only view of flagged transactions. Data updates after each pipeline run.")

# NEON Console Connection
NEON_CONN = st.secrets.get("NEON_CONN", None)


# -----------------------------
# 2) DATA ACCESS HELPERS
# -----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_alerts(conn_str: str) -> pd.DataFrame:
    """
    Connect to Neon and return the fraud_alerts table sorted by newest first.
    Cached for 60 seconds to keep the app snappy.
    """
    if not conn_str:
        # This means secret has not be set yet in Streamlit Cloud.
        return pd.DataFrame()

    conn = psycopg2.connect(conn_str, sslmode="require")
    try:
        df = pd.read_sql(
            """
            SELECT
                transaction_id,
                amount,
                risk_score,
                flagged_reason,
                created_at
            FROM fraud_alerts
            ORDER BY created_at DESC
            """,
            conn
        )
        # Format for nicer display
        if not df.empty:
            df["created_at"] = pd.to_datetime(df["created_at"])
        return df
    finally:
        conn.close()


# -----------------------------
# 3) REFRESH / EMPTY / ERROR UI
# -----------------------------
col_a, col_b = st.columns([1, 3])

with col_a:
    if st.button("🔄 Refresh data", use_container_width=True):
        load_alerts.clear()  # clear the cache
        st.experimental_rerun()

with col_b:
    st.write(
        "If you don't see data yet, run the GitHub Actions pipeline to insert alerts, then click **Refresh data**."
    )

# Load the data
try:
    df = load_alerts(NEON_CONN)
except Exception as e:
    st.error(
        "We couldn't load data from the database. "
        "Please verify the NEON_CONN secret in Streamlit Cloud (Step 8) and try again."
    )
    st.exception(e)
    st.stop()

# If no rows yet, show a friendly empty state
if df.empty:
    st.info(
        "No alerts yet. Run your GitHub Action (pipeline) to create alerts, "
        "then come back and click **Refresh data**."
    )
    st.stop()


# -----------------------------
# 4) KPI STRIP
# -----------------------------
left, right = st.columns(2)

# KPI 1: Total alerts
left.metric("Total Fraud Alerts", f"{len(df):,}")

# KPI 2: Latest Alert (transaction_id + amount + reason + time)
latest = df.iloc[0]
right.write(
    f"**Latest Alert:** `{latest['transaction_id']}` • "
    f"${latest['amount']:,.2f} • {latest['flagged_reason']} • "
    f"{latest['created_at']}"
)


# -----------------------------
# 5) OPTIONAL FILTERS (simple)
# -----------------------------
with st.expander("Filters (optional)"):
    min_amt = st.slider("Minimum amount", 0, int(df["amount"].max()), 0, step=500)
    reasons = st.multiselect(
        "Flagged reason(s)",
        sorted(df["flagged_reason"].unique().tolist()),
        default=sorted(df["flagged_reason"].unique().tolist()),
    )
    # Apply filters
    m = (df["amount"] >= min_amt) & (df["flagged_reason"].isin(reasons))
    filtered = df.loc[m].copy()
    st.caption(f"Showing {len(filtered):,} of {len(df):,} alerts after filters.")


# -----------------------------
# 6) TABLE
# -----------------------------
st.subheader("Alerts (newest first)")
# Optional: nice formatting for display
display = filtered.copy()
display["amount"] = display["amount"].map(lambda x: f"${x:,.2f}")
display.rename(
    columns={
        "transaction_id": "Transaction ID",
        "amount": "Amount",
        "risk_score": "Risk Score",
        "flagged_reason": "Reason",
        "created_at": "Created",
    },
    inplace=True,
)

st.dataframe(display, use_container_width=True)
st.caption(f"Last refreshed: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

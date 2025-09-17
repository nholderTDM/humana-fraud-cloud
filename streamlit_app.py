import os
import datetime as dt

import streamlit as st
import pandas as pd
import psycopg2
import altair as alt

# -----------------------------
# 1) PAGE CONFIG & HEADER
# -----------------------------
st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")
st.caption("Real-time fraud KPIs and detailed alert records.")

# NEON connection string from Streamlit secrets
NEON_CONN = st.secrets.get("NEON_CONN", None)


# -----------------------------
# 2) DATA ACCESS HELPERS
# -----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def _connect(conn_str: str):
    if not conn_str:
        return None
    return psycopg2.connect(conn_str, sslmode="require")

@st.cache_data(ttl=60, show_spinner=False)
def load_alerts(conn_str: str) -> pd.DataFrame:
    """
    Load fraud alerts only (flagged transactions).
    """
    conn = _connect(conn_str)
    if conn is None:
        return pd.DataFrame()

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
        if not df.empty:
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df
    finally:
        conn.close()

@st.cache_data(ttl=60, show_spinner=False)
def load_all_transactions(conn_str: str) -> pd.DataFrame:
    """
    Load ALL transactions processed by the pipeline (flagged and non-flagged).
    Looks for a table named 'transactions_all'. If not found, returns empty DF.
    """
    conn = _connect(conn_str)
    if conn is None:
        return pd.DataFrame()

    try:
        # If your table name differs, change it here.
        df = pd.read_sql(
            """
            SELECT
                transaction_id,
                amount,
                location,
                device,
                processed_at,     -- or created_at; adjust to your column name
                is_flagged,       -- boolean
                risk_score,
                flagged_reason
            FROM transactions_all
            ORDER BY processed_at DESC
            """,
            conn
        )
        if not df.empty:
            # Normalize timestamp column name to created_at for consistency below
            if "processed_at" in df.columns:
                df.rename(columns={"processed_at": "created_at"}, inplace=True)
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df
    except Exception:
        # Table probably doesn't exist yet
        return pd.DataFrame()
    finally:
        conn.close()


# -----------------------------
# 3) REFRESH / EMPTY / ERROR UI
# -----------------------------
cta_col, help_col = st.columns([1, 3])
with cta_col:
    if st.button("🔄 Refresh data", use_container_width=True):
        load_alerts.clear()
        load_all_transactions.clear()
        st.experimental_rerun()

with help_col:
    st.write("Data updates automatically after each pipeline run (and instantly when triggered).")

# Load data
try:
    alerts_df = load_alerts(NEON_CONN)
    all_df    = load_all_transactions(NEON_CONN)
except Exception as e:
    st.error("Unable to load data from Neon. Check NEON_CONN secret.")
    st.exception(e)
    st.stop()

if alerts_df.empty and all_df.empty:
    st.info("No data yet. Trigger the pipeline and refresh.")
    st.stop()

# -----------------------------
# 4) KPIs
# -----------------------------
k1, k2, k3, k4 = st.columns(4)

total_alerts = len(alerts_df)
k1.metric("Total Fraud Alerts", f"{total_alerts:,}")

avg_risk = round(alerts_df["risk_score"].mean(), 2) if not alerts_df.empty else 0.0
k2.metric("Average Risk Score", avg_risk)

avg_amt = alerts_df["amount"].mean() if not alerts_df.empty else 0.0
k3.metric("Average Amount", f"${avg_amt:,.2f}")

# Fraud % = (# alerts) / (# all transactions processed)
if not all_df.empty:
    total_processed = len(all_df)
    fraud_pct = (total_alerts / max(total_processed, 1)) * 100
    k4.metric("Fraud % (of monitored txns)", f"{fraud_pct:.1f}%")
else:
    # Fallback when transactions_all doesn't exist (or is empty)
    k4.metric("Fraud % (of monitored txns)", "—")
    st.info(
        "Fraud % is shown as em dash because the denominator table "
        "`transactions_all` is missing or empty. "
        "Add this table in the pipeline to track ALL processed transactions."
    )

# -----------------------------
# 5) Trends (Alerts over Time)
# -----------------------------
st.subheader("Fraud Alerts Over Time")
if not alerts_df.empty:
    # Bucket by hour for a smoother time trend
    tmp = alerts_df.copy()
    tmp["bucket"] = tmp["created_at"].dt.floor("H")
    daily = tmp.groupby("bucket").size().reset_index(name="count")

    chart = (
        alt.Chart(daily)
        .mark_area()
        .encode(
            x=alt.X("bucket:T", title="Time"),
            y=alt.Y("count:Q", title="Count"),
            tooltip=[alt.Tooltip("bucket:T", title="Time"), alt.Tooltip("count:Q", title="Count")],
        )
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No alerts yet to plot.")

# -----------------------------
# 6) Filters
# -----------------------------
with st.expander("Filters (optional)"):
    base = alerts_df.copy()
    max_amt_slider = int(base["amount"].max()) if not base.empty else 0
    min_amt = st.slider("Minimum amount", 0, max_amt_slider, 0, step=100)
    reasons = st.multiselect(
        "Flagged reasons",
        sorted(base["flagged_reason"].dropna().unique().tolist()) if not base.empty else [],
        default=sorted(base["flagged_reason"].dropna().unique().tolist()) if not base.empty else [],
    )
    if not base.empty:
        mask = (base["amount"] >= min_amt) & (base["flagged_reason"].isin(reasons))
        filtered = base.loc[mask].copy()
        st.caption(f"Showing {len(filtered):,} of {len(base):,} alerts.")
    else:
        filtered = base

# -----------------------------
# 7) Detail Table
# -----------------------------
st.subheader("Detailed Fraud Alerts (newest first)")
display = filtered.copy()
if not display.empty:
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

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

# NEON Console Connection
NEON_CONN = st.secrets.get("NEON_CONN", None)

# -----------------------------
# 2) DATA ACCESS HELPERS
# -----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def load_alerts(conn_str: str) -> pd.DataFrame:
    """Fetch fraud_alerts table sorted by newest first."""
    if not conn_str:
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
        load_alerts.clear()
        st.experimental_rerun()
with col_b:
    st.write("Data updates automatically after each pipeline run or instantly on manual refresh.")

try:
    df = load_alerts(NEON_CONN)
except Exception as e:
    st.error("Unable to load data from Neon. Check NEON_CONN secret.")
    st.exception(e)
    st.stop()

if df.empty:
    st.info("No alerts yet. Trigger the pipeline and refresh.")
    st.stop()

# -----------------------------
# 4) KPI STRIP
# -----------------------------
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Total Fraud Alerts", f"{len(df):,}")
kpi2.metric("Average Risk Score", round(df["risk_score"].mean(), 2))
kpi3.metric("Average Amount", f"${df['amount'].mean():,.2f}")
fraud_ratio = (len(df) / max(len(df), 1)) * 100
kpi4.metric("Fraud % (of monitored txns)", f"{fraud_ratio:.1f}%")

# Trend chart
st.subheader("Fraud Alerts Over Time")
daily = df.groupby(df["created_at"].dt.date).size().reset_index(name="count")
chart = alt.Chart(daily).mark_area().encode(
    x="created_at:T",
    y="count:Q",
    tooltip=["created_at", "count"]
)
st.altair_chart(chart, use_container_width=True)

# -----------------------------
# 5) Filters
# -----------------------------
with st.expander("Filters"):
    min_amt = st.slider("Minimum amount", 0, int(df["amount"].max()), 0, step=100)
    reasons = st.multiselect(
        "Flagged reasons",
        sorted(df["flagged_reason"].unique()),
        default=sorted(df["flagged_reason"].unique()),
    )
    mask = (df["amount"] >= min_amt) & (df["flagged_reason"].isin(reasons))
    filtered = df.loc[mask].copy()
    st.caption(f"Showing {len(filtered):,} of {len(df):,} alerts.")

# -----------------------------
# 6) Table of Alerts
# -----------------------------
st.subheader("Detailed Fraud Alerts")
display = filtered.copy()
display["amount"] = display["amount"].map(lambda x: f"${x:,.2f}")
display.rename(columns={
    "transaction_id": "Transaction ID",
    "amount": "Amount",
    "risk_score": "Risk Score",
    "flagged_reason": "Reason",
    "created_at": "Created"
}, inplace=True)
st.dataframe(display, use_container_width=True)
st.caption(f"Last refreshed: {dt.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")

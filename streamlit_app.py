import os, datetime as dt
import streamlit as st
import pandas as pd
import psycopg2
import altair as alt

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Fraud Detection Dashboard")
st.caption("Real-time fraud KPIs and detailed alert records.")

NEON_CONN = st.secrets.get("NEON_CONN", None)

@st.cache_data(ttl=60, show_spinner=False)
def _connect(conn_str):
    return psycopg2.connect(conn_str, sslmode="require") if conn_str else None

@st.cache_data(ttl=60, show_spinner=False)
def load_table(conn_str, query: str) -> pd.DataFrame:
    conn = _connect(conn_str)
    if not conn:
        return pd.DataFrame()
    try:
        df = pd.read_sql(query, conn)
        if "created_at" in df.columns:
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        if "processed_at" in df.columns:
            df.rename(columns={"processed_at": "created_at"}, inplace=True)
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df
    finally:
        conn.close()

@st.cache_data(ttl=60, show_spinner=False)
def load_alerts(conn_str):   # flagged only
    return load_table(conn_str, """
        SELECT transaction_id, amount, risk_score, flagged_reason, created_at
        FROM fraud_alerts ORDER BY created_at DESC
    """)

@st.cache_data(ttl=60, show_spinner=False)
def load_all(conn_str):      # all processed transactions
    return load_table(conn_str, """
        SELECT transaction_id, amount, location, device,
               processed_at, is_flagged, risk_score, flagged_reason
        FROM transactions_all ORDER BY processed_at DESC
    """)

# refresh button
r1, r2 = st.columns([1,3])
with r1:
    if st.button("🔄 Refresh data", use_container_width=True):
        load_alerts.clear(); load_all.clear()
        st.experimental_rerun()
with r2:
    st.write("Data updates after every pipeline run or instant trigger.")

alerts_df = load_alerts(NEON_CONN)
all_df    = load_all(NEON_CONN)
if alerts_df.empty and all_df.empty:
    st.info("No data yet. Trigger the pipeline and refresh.")
    st.stop()

# KPIs
c1, c2, c3, c4 = st.columns(4)
total_alerts = len(alerts_df)
c1.metric("Total Fraud Alerts", f"{total_alerts:,}")
c2.metric("Average Risk Score", round(alerts_df["risk_score"].mean(),2) if not alerts_df.empty else 0)
c3.metric("Average Amount", f"${alerts_df['amount'].mean():,.2f}" if not alerts_df.empty else "$0.00")
fraud_pct = (total_alerts / len(all_df))*100 if not all_df.empty else 0
c4.metric("Fraud % (of all transactions)", f"{fraud_pct:.1f}%" if all_df.any().any() else "—")

# Trend chart
st.subheader("Fraud Alerts Over Time")
if not alerts_df.empty:
    tmp = alerts_df.copy()
    tmp["bucket"] = tmp["created_at"].dt.floor("H")
    grouped = tmp.groupby("bucket").size().reset_index(name="Count")
    chart = (
        alt.Chart(grouped)
        .mark_area()
        .encode(
            x=alt.X("bucket:T", title="Time"),
            y=alt.Y("Count:Q", title="Count"),
            tooltip=["bucket:T", "Count:Q"]
        )
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No fraud alerts yet to plot.")

# Filters
with st.expander("Filters (optional)"):
    max_amt = int(alerts_df["amount"].max()) if not alerts_df.empty else 0
    min_amt = st.slider("Minimum amount", 0, max_amt, 0, step=100)
    reasons = st.multiselect(
        "Flagged reasons",
        sorted(alerts_df["flagged_reason"].dropna().unique()) if not alerts_df.empty else [],
        default=sorted(alerts_df["flagged_reason"].dropna().unique()) if not alerts_df.empty else []
    )
    mask = (alerts_df["amount"] >= min_amt) & (alerts_df["flagged_reason"].isin(reasons))
    filtered = alerts_df.loc[mask].copy()

# Detail table
st.subheader("Detailed Fraud Alerts (newest first)")
if not filtered.empty:
    filtered["amount"] = filtered["amount"].map(lambda x: f"${x:,.2f}")
    filtered.rename(columns={
        "transaction_id":"Transaction ID","amount":"Amount",
        "risk_score":"Risk Score","flagged_reason":"Reason",
        "created_at":"Created"
    }, inplace=True)
st.dataframe(filtered, use_container_width=True)
st.caption(f"Last refreshed: {dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")

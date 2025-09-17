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

# Neon connection string
NEON_CONN = st.secrets.get("NEON_CONN", None)


# -----------------------------
# 2) CONNECTION + DATA HELPERS
# -----------------------------

@st.cache_resource(show_spinner=False)
def get_conn(conn_str: str):
    """Create a reusable DB connection (not pickled)."""
    if not conn_str:
        return None
    return psycopg2.connect(conn_str, sslmode="require")


@st.cache_data(ttl=60, show_spinner=False)
def load_alerts() -> pd.DataFrame:
    """Load flagged transactions (fraud_alerts)."""
    conn = get_conn(NEON_CONN)
    if not conn:
        return pd.DataFrame()
    query = """
        SELECT transaction_id, amount, risk_score,
               flagged_reason, created_at
        FROM fraud_alerts
        ORDER BY created_at DESC
    """
    df = pd.read_sql(query, conn)
    if not df.empty:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df


@st.cache_data(ttl=60, show_spinner=False)
def load_all_transactions() -> pd.DataFrame:
    """Load ALL processed transactions (transactions_all)."""
    conn = get_conn(NEON_CONN)
    if not conn:
        return pd.DataFrame()
    try:
        query = """
            SELECT transaction_id, amount, location, device,
                   processed_at AS created_at,
                   is_flagged, risk_score, flagged_reason
            FROM transactions_all
            ORDER BY processed_at DESC
        """
        df = pd.read_sql(query, conn)
        if not df.empty:
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df
    except Exception:
        return pd.DataFrame()


# -----------------------------
# 3) REFRESH / EMPTY / ERROR UI
# -----------------------------
left, right = st.columns([1, 3])
with left:
    if st.button("🔄 Refresh data", use_container_width=True):
        load_alerts.clear()
        load_all_transactions.clear()
        st.experimental_rerun()
with right:
    st.write("Data updates after every pipeline run or instant trigger.")

try:
    alerts_df = load_alerts()
    all_df = load_all_transactions()
except Exception as e:
    st.error("Unable to load data from Neon. Check NEON_CONN secret.")
    st.exception(e)
    st.stop()

if alerts_df.empty and all_df.empty:
    st.info("No data yet. Trigger the pipeline and refresh.")
    st.stop()


# -----------------------------
# 4) KPI Strip
# -----------------------------
c1, c2, c3, c4 = st.columns(4)

total_alerts = len(alerts_df)
c1.metric("Total Fraud Alerts", f"{total_alerts:,}")

avg_risk = round(alerts_df["risk_score"].mean(), 2) if not alerts_df.empty else 0.0
c2.metric("Average Risk Score", avg_risk)

avg_amt = alerts_df["amount"].mean() if not alerts_df.empty else 0.0
c3.metric("Average Amount", f"${avg_amt:,.2f}")

if not all_df.empty:
    fraud_pct = (total_alerts / len(all_df)) * 100
    c4.metric("Fraud % (of all transactions)", f"{fraud_pct:.1f}%")
else:
    c4.metric("Fraud % (of all transactions)", "—")


# -----------------------------
# 5) Fraud Alerts Over Time
# -----------------------------
st.subheader("Fraud Alerts Over Time")
if not alerts_df.empty:
    tmp = alerts_df.copy()
    tmp["bucket"] = tmp["created_at"].dt.floor("H")
    # rename the count column exactly as used in Altair encodings
    grouped = tmp.groupby("bucket").size().reset_index(name="No. Transactions")
    chart = (
        alt.Chart(grouped)
        .mark_area()
        .encode(
            x=alt.X("bucket:T", title="Time"),
            y=alt.Y("No. Transactions:Q", title="No. Transactions"),
            tooltip=[alt.Tooltip("bucket:T", title="Time"),
                     alt.Tooltip("No. Transactions:Q", title="No. Transactions")],
        )
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.caption("No fraud alerts yet to plot.")


# -----------------------------
# 6) Filters
# -----------------------------
with st.expander("Filters (optional)"):
    base = alerts_df.copy()
    max_amt = int(base["amount"].max()) if not base.empty else 0
    min_amt = st.slider("Minimum amount", 0, max_amt, 0, step=100)
    reasons = st.multiselect(
        "Flagged reasons",
        sorted(base["flagged_reason"].dropna().unique()) if not base.empty else [],
        default=sorted(base["flagged_reason"].dropna().unique()) if not base.empty else [],
    )
    mask = (base["amount"] >= min_amt) & (base["flagged_reason"].isin(reasons))
    filtered = base.loc[mask].copy()

# -----------------------------
# 7) Detailed Table
# -----------------------------
st.subheader("Detailed Fraud Alerts (newest first)")
if not filtered.empty:
    filtered["amount"] = filtered["amount"].map(lambda x: f"${x:,.2f}")
    filtered.rename(
        columns={
            "transaction_id": "Transaction ID",
            "amount": "Amount",
            "risk_score": "Risk Score",
            "flagged_reason": "Reason",
            "created_at": "Created",
        },
        inplace=True,
    )
st.dataframe(filtered, use_container_width=True)
st.caption(f"Last refreshed: {dt.datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")

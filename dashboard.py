"""Streamlit dashboard for real-time OHLCV data quality monitoring."""

import json
import time
from pathlib import Path

import pandas as pd
import streamlit as st

from src.storage import DataStorage

st.set_page_config(
    page_title="OHLCV Validation Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
.metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border-radius: 12px; padding: 20px;
    border: 1px solid rgba(255,255,255,0.1); text-align: center;
}
.metric-value { font-size: 2.5rem; font-weight: 700; margin: 8px 0; }
.metric-label { font-size: 0.9rem; color: rgba(255,255,255,0.6); text-transform: uppercase; letter-spacing: 1px; }
.status-valid { color: #00d4aa; }
.status-invalid { color: #ff4757; }
.status-warning { color: #ffa502; }
.header-gradient {
    background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    font-size: 2.5rem; font-weight: 800;
}
</style>
""", unsafe_allow_html=True)


def load_quality_report(symbol: str) -> dict | None:
    fp = Path("data/reports") / f"quality_{symbol.replace('/', '_')}.json"
    if not fp.exists():
        return None
    try:
        with open(fp) as f:
            return json.load(f)
    except Exception:
        return None


def metric_card(label: str, value: str, css: str = "") -> str:
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {css}">{value}</div></div>'


def main():
    st.markdown('<div class="header-gradient">📊 OHLCV Validation Dashboard</div>', unsafe_allow_html=True)
    st.markdown("*Real-time market data quality monitoring*")
    st.markdown("---")

    with st.sidebar:
        st.header("⚙️ Configuration")
        symbol = st.text_input("Symbol", value="BTC/USDT")
        interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h"])
        auto_refresh = st.checkbox("Auto-refresh (10s)", value=True)
        st.markdown("---")
        st.markdown("### 📖 How to Use")
        st.markdown("1. Run: `python -m src.main`\n2. Dashboard reads from `data/`\n3. Metrics auto-update")

    storage = DataStorage(base_dir="data")
    report = load_quality_report(symbol)
    clean_df = storage.get_clean_data(symbol, interval)
    anomaly_df = storage.get_anomaly_log(symbol)

    st.subheader("🏥 Data Health")
    if report:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.markdown(metric_card("Total Bars", str(report.get("total_bars", 0))), unsafe_allow_html=True)
        c2.markdown(metric_card("Valid", str(report.get("valid_bars", 0)), "status-valid"), unsafe_allow_html=True)
        c3.markdown(metric_card("Invalid", str(report.get("invalid_bars", 0)), "status-invalid"), unsafe_allow_html=True)
        c4.markdown(metric_card("Warnings", str(report.get("warning_bars", 0)), "status-warning"), unsafe_allow_html=True)
        rate = report.get("validity_rate", 0)
        rate_css = "status-valid" if rate >= 95 else "status-warning" if rate >= 80 else "status-invalid"
        c5.markdown(metric_card("Validity Rate", f"{rate:.1f}%", rate_css), unsafe_allow_html=True)
    else:
        st.info("📡 No data yet. Start the pipeline: `python -m src.main`")

    st.markdown("---")

    col_chart, col_anom = st.columns([2, 1])
    with col_chart:
        st.subheader("📈 Validated Price Data")
        if clean_df is not None and len(clean_df) > 0:
            clean_df["timestamp"] = pd.to_datetime(clean_df["timestamp"])
            clean_df = clean_df.sort_values("timestamp")
            st.line_chart(clean_df.tail(100).set_index("timestamp")[["close"]])
            st.markdown(f"**Latest:** ${clean_df['close'].iloc[-1]:,.2f} | **Bars:** {len(clean_df)}")
            st.markdown(f"**Range:** {clean_df['timestamp'].iloc[0]} → {clean_df['timestamp'].iloc[-1]}")
        else:
            st.info("No validated data yet.")

    with col_anom:
        st.subheader("🚨 Anomaly Log")
        if anomaly_df is not None and len(anomaly_df) > 0:
            st.metric("Total Anomalies", len(anomaly_df))
            st.dataframe(anomaly_df.tail(20)[["timestamp", "status", "checks_failed"]].iloc[::-1], height=400)
        else:
            st.success("✅ No anomalies detected!")

    st.markdown("---")
    st.subheader("📋 Recent Bars")
    if clean_df is not None and len(clean_df) > 0:
        st.dataframe(clean_df.tail(50).iloc[::-1], height=300)

    st.markdown("---")
    st.markdown("<div style='text-align:center;color:rgba(255,255,255,0.4);'>Live OHLCV Validation Framework</div>", unsafe_allow_html=True)

    if auto_refresh:
        time.sleep(10)
        st.rerun()


if __name__ == "__main__":
    main()

import streamlit as st
import time
import sys
import os
import pandas as pd

# --- FIX MODULE PATH ---
# Menggunakan __file__ untuk mendapatkan direktori saat ini
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# --- IMPORT MODULE ---
# Pastikan file analytics.py dan alerts.py berada di folder yang benar
try:
    from analytics import transportation_analytics as ta
    from alerts import transportation_alert as alert
except ImportError as e:
    st.error(f"Gagal mengimpor modul: {e}")
    st.stop()

# --- CONFIG ---
DATA_PATH = "data/serving/transportation"
REFRESH_INTERVAL = 5 # dalam detik

st.set_page_config(
    page_title="Smart Transportation Dashboard",
    layout="wide"
)

st.title("🚀 Smart Transportation Real-Time Analytics")
st.markdown(f"**Data Source:** `{DATA_PATH}`")

# --- PLACEHOLDER UNTUK AUTO-REFRESH ---
placeholder = st.empty()

# --- MAIN LOOP ---
while True:
    with placeholder.container():
        # 1. LOAD DATA
        df = ta.load_data(DATA_PATH)

        if df.empty:
            st.warning("Waiting for streaming transportation data...")
            time.sleep(REFRESH_INTERVAL)
            st.rerun() # Menggantikan loop manual agar UI tetap responsif

        # 2. PREPROCESS
        df = ta.preprocess(df)

        # 3. METRICS
        try:
            metrics = ta.compute_metrics(df)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Trips", f"{metrics['total_trips']:,}")
            with col2:
                st.metric("Total Fare", f"Rp {int(metrics['total_fare']):,}")
            with col3:
                st.metric("Top Location", metrics["top_location"])
        except Exception as e:
            st.error(f"Error computing metrics: {e}")

        st.divider()

        # 4. PEAK HOUR & ALERTS
        col_info, col_alert = st.columns(2)
        
        with col_info:
            try:
                peak_hour = ta.detect_peak_hour(df)
                st.info(f"📍 **Peak Traffic Hour:** {peak_hour:02d}:00")
            except Exception:
                st.warning("Tidak dapat menghitung peak hour")

        with col_alert:
            try:
                alerts = alert.generate_alert(df)
                if alerts:
                    for a in alerts:
                        st.error(f"⚠️ {a}")
            except Exception as e:
                st.warning(f"Alert error: {e}")

        st.divider()

        # 5. VISUALISASI
        try:
            v_col1, v_col2 = st.columns(2)
            
            with v_col1:
                st.subheader("Fare per Location")
                st.bar_chart(ta.fare_per_location(df))
            
            with v_col2:
                st.subheader("Vehicle Distribution")
                st.bar_chart(ta.vehicle_distribution(df))

            st.subheader("Mobility Trend (Revenue)")
            st.line_chart(ta.mobility_trend(df))
        except Exception as e:
            st.warning(f"Visualization error: {e}")

        st.divider()

        # 6. ANOMALY DETECTION
        try:
            st.subheader("🚨 Abnormal Trips Detection")
            anomaly_df = ta.detect_anomaly(df)
            if not anomaly_df.empty:
                st.dataframe(anomaly_df.tail(20), use_container_width=True)
            else:
                st.success("No anomalies detected")
        except Exception as e:
            st.warning(f"Anomaly error: {e}")

        st.divider()

        # 7. LIVE DATA TABLE
        st.subheader("📋 Live Trip Data (Last 50 Records)")
        st.dataframe(df.tail(50), use_container_width=True)

        # Footer timestamp
        st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    time.sleep(REFRESH_INTERVAL)
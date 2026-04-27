import streamlit as st
import time
import sys
import os
import pandas as pd

# =========================
# FIX MODULE PATH
# =========================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR,".."))

if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# =========================
# IMPORT MODULE
# =========================
try:
    from analytics import transportation_analytics as ta
    from alerts import transportation_alert as alert
except ImportError as e:
    st.error(f"Gagal import modul: {e}")
    st.stop()


# =========================
# CONFIG
# =========================
DATA_PATH="data/serving/transportation"
REFRESH_INTERVAL=5

st.set_page_config(
    page_title="Smart Transportation Dashboard",
    layout="wide"
)

st.title("🚀 Smart Transportation Real-Time Analytics (Big Data Optimized)")
st.markdown(f"Data Source : `{DATA_PATH}`")

placeholder = st.empty()


# =========================
# MAIN LOOP
# =========================
while True:

    with placeholder.container():

        # =========================
        # LOAD DATA
        # =========================
        df = ta.load_data(DATA_PATH)

        if df.empty:
            st.warning("Waiting for streaming transportation data...")
            time.sleep(REFRESH_INTERVAL)
            st.rerun()

        df = ta.preprocess(df)

        # optimasi big data (downsampling)
        df_sample = df.tail(1000)


        # =========================
        # METRICS
        # =========================
        try:
            metrics = ta.compute_metrics(df)

            c1,c2,c3 = st.columns(3)

            c1.metric(
                "Total Trips",
                f"{metrics['total_trips']:,}"
            )

            c2.metric(
                "Total Fare",
                f"Rp {int(metrics['total_fare']):,}"
            )

            c3.metric(
                "Top Location",
                metrics["top_location"]
            )

        except Exception as e:
            st.error(f"Metrics error : {e}")


        st.divider()


        # =========================
        # PEAK HOUR + ALERT
        # =========================
        info1,info2 = st.columns(2)

        with info1:
            try:
                peak_hour = ta.detect_peak_hour(df)
                st.info(
                    f"📍 Peak Traffic Hour : {peak_hour:02d}:00"
                )
            except:
                st.warning("Peak hour tidak tersedia")


        with info2:
            try:
                alerts = alert.generate_alert(df)

                if alerts:
                    for a in alerts:
                        st.error(f"⚠️ {a}")

            except Exception as e:
                st.warning(
                    f"Alert error : {e}"
                )

        st.divider()


        # =========================
        # PRAKTIKUM 6 VISUALISASI
        # =========================

        # 1 TRAFFIC WINDOW (NEW)
        st.subheader("📈 Real-Time Traffic (Windowed)")

        try:
            traffic_window = ta.traffic_per_window(df)

            if traffic_window is not None:
                st.line_chart(traffic_window)

        except Exception as e:
            st.warning(
                f"Traffic window error: {e}"
            )


        st.divider()


        # 2 TRAFFIC DENSITY + VEHICLE DISTRIBUTION
        v1,v2 = st.columns(2)

        with v1:
            try:
                st.subheader("🚖 Traffic Density Per Location")
                st.bar_chart(
                    ta.fare_per_location(df_sample)
                )
            except Exception as e:
                st.warning(e)


        with v2:
            try:
                st.subheader("🚗 Vehicle Distribution")
                st.bar_chart(
                    ta.vehicle_distribution(df_sample)
                )
            except Exception as e:
                st.warning(e)


        st.divider()


        # 3 MOBILITY TREND (DOWNSAMPLED)
        try:
            st.subheader("📉 Mobility Trend")
            st.line_chart(
                df_sample["fare"]
            )

        except Exception as e:
            st.warning(
                f"Mobility trend error: {e}"
            )


        st.divider()


        # =========================
        # ANOMALY
        # =========================
        try:
            st.subheader(
                "🚨 Abnormal Trips Detection"
            )

            anomaly_df = ta.detect_anomaly(df_sample)

            if not anomaly_df.empty:
                st.dataframe(
                    anomaly_df.tail(20),
                    use_container_width=True
                )

            else:
                st.success(
                    "No anomalies detected"
                )

        except Exception as e:
            st.warning(
                f"Anomaly error : {e}"
            )


        st.divider()


        # =========================
        # LIVE TABLE
        # =========================
        st.subheader(
            "📋 Live Trip Data (50 Records)"
        )

        st.dataframe(
            df.tail(50),
            use_container_width=True
        )


        # footer
        st.caption(
            f"Last Updated : {time.strftime('%Y-%m-%d %H:%M:%S')}"
        )


    time.sleep(REFRESH_INTERVAL)
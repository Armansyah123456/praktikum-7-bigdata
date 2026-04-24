import streamlit as st
import pandas as pd
import plotly.express as px
import os
import time

# 1. Konfigurasi Halaman
st.set_page_config(
    page_title="Real-Time E-Commerce Analytics Dashboard",
    page_icon="🛍️",
    layout="wide"
)

# Path data
DATA_PATH = "data/serving/stream"

@st.cache_data(ttl=5)
def load_data(path):
    if not os.path.exists(path):
        return pd.DataFrame()
    files = [f for f in os.listdir(path) if f.endswith('.parquet')]
    if not files:
        return pd.DataFrame()
    return pd.read_parquet(path)

# --- JUDUL UTAMA ---
st.title("Real-Time E-Commerce Analytics Dashboard")

# Placeholder untuk update real-time
placeholder = st.empty()

while True:
    df = load_data(DATA_PATH)
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        with placeholder.container():
            # --- 1. KEY METRICS ---
            st.markdown("### Key Metrics")
            m1, m2, m3, m4 = st.columns(4)
            
            total_trans = len(df)
            total_rev = df['price'].sum()
            avg_trans = total_rev / total_trans if total_trans > 0 else 0
            num_cities = df['city'].nunique()
            
            m1.metric("Total Transactions", f"{total_trans}")
            m2.metric("Total Revenue", f"${total_rev:,.0f}")
            m3.metric("Avg Transaction", f"${avg_trans:,.0f}")
            m4.metric("Cities", f"{num_cities}")
            
            st.markdown("---")
            
            # --- 2. BAR CHARTS (Revenue by City & Top Products) ---
            col_a, col_b = st.columns(2)
            
            with col_a:
                city_rev = df.groupby('city')['price'].sum().reset_index()
                # Menggunakan warna biru solid agar mirip modul
                fig_city = px.bar(city_rev, x='city', y='price', 
                                 title="Revenue by City",
                                 color_discrete_sequence=['#0072B2']) 
                fig_city.update_layout(xaxis_title="", yaxis_title="", height=350)
                st.plotly_chart(fig_city, use_container_width=True, key=f"city_{time.time()}")
                
            with col_b:
                prod_rev = df.groupby('product')['price'].sum().sort_values(ascending=False).head(10).reset_index()
                fig_prod = px.bar(prod_rev, x='product', y='price', 
                                 title="Top Products",
                                 color_discrete_sequence=['#0072B2'])
                fig_prod.update_layout(xaxis_title="", yaxis_title="", height=350)
                st.plotly_chart(fig_prod, use_container_width=True, key=f"prod_{time.time()}")
            
            # --- 3. REVENUE TREND ---
            st.markdown("### Revenue Trend")
            # Perbaikan '5min' agar tidak error
            df_trend = df.set_index('timestamp').resample('5min')['price'].sum().reset_index()
            fig_trend = px.line(df_trend, x='timestamp', y='price',
                               color_discrete_sequence=['#0072B2'])
            fig_trend.update_layout(xaxis_title="", yaxis_title="", height=300)
            st.plotly_chart(fig_trend, use_container_width=True, key=f"trend_{time.time()}")
            
            # --- 4. LIVE TRANSACTIONS TABLE ---
            st.markdown("### Live Transactions")
            live_df = df.sort_values('timestamp', ascending=False).head(10)
            # Pilih kolom sesuai gambar modul
            st.dataframe(live_df[['user_id', 'product', 'price', 'city', 'timestamp']], 
                         use_container_width=True, hide_index=True)

    else:
        placeholder.warning("⚠️ Menunggu data dari Spark Serving Layer...")

    time.sleep(5)
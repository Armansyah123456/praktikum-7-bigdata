import streamlit as st
import pandas as pd
import joblib
import matplotlib.pyplot as plt

# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="Smart Traffic AI",
    layout="wide"
)

st.title("🚦 Smart Traffic Prediction Dashboard")


# =========================
# LOAD DATA
# =========================
df = pd.read_csv(
"data/clean/traffic_smartcity_clean_v1.csv"
)

model = joblib.load(
"models/traffic_model_v1.pkl"
)


# =========================
# PREPROCESS
# =========================
df["datetime"] = pd.to_datetime(
df["datetime"]
)

df["hour"] = df["datetime"].dt.hour
df["day"] = df["datetime"].dt.dayofweek
df["lag1"] = df["traffic"].shift(1)

df = df.dropna()


# =========================
# KPI (sesuai modul)
# =========================
c1,c2 = st.columns(2)

with c1:
    st.metric(
      "Average Traffic",
      int(df["traffic"].mean())
    )

with c2:
    st.metric(
      "Max Traffic",
      int(df["traffic"].max())
    )


# =========================
# TRAFFIC TREND (RAPI, TIDAK BESAR)
# =========================
st.subheader("📈 Traffic Trend")

trend = df["traffic"].rolling(
5
).mean()

fig,ax = plt.subplots(
figsize=(8,3)
)

ax.plot(
trend.tail(60),
linewidth=2
)

ax.set_title(
"Traffic Volume Trend"
)

ax.set_xlabel(
"Time"
)

ax.set_ylabel(
"Traffic"
)

plt.tight_layout()

st.pyplot(fig)



# =========================
# PREDICTION (sesuai modul)
# =========================
st.subheader(
"🔮 Traffic Prediction"
)

hour = st.slider(
"Hour",
0,23,17
)

day = st.slider(
"Day of Week",
0,6,2
)

lag1 = st.number_input(
"Previous Traffic",
50,300,120
)

if st.button(
"Predict"
):

    pred = model.predict(
      [[hour,day,lag1]]
    )

    st.success(
f"Predicted traffic: {int(pred[0])} vehicles"
)
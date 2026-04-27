import pandas as pd
import os
import glob

# --- LOAD DATA ---
def load_data(path):
    if not os.path.exists(path):
        return pd.DataFrame()

    # Mengambil semua file parquet dengan cara yang lebih stabil
    files = glob.glob(os.path.join(path, "*.parquet"))
    
    if not files:
        return pd.DataFrame()

    try:
        # Membaca semua file dan menggabungkannya
        df_list = [pd.read_parquet(f) for f in files]
        df = pd.concat(df_list, ignore_index=True)
        return df
    except Exception as e:
        print(f"Error reading parquet: {e}")
        return pd.DataFrame()

# --- PREPROCESS ---
def preprocess(df):
    if df.empty:
        return df
    
    # Memastikan kolom penting ada dan bertipe benar
    # Spark sering mengirim data dalam tipe yang butuh konversi manual di Pandas
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    
    if "fare" in df.columns:
        df["fare"] = pd.to_numeric(df["fare"], errors="coerce").fillna(0)
        
    if "distance" in df.columns:
        df["distance"] = pd.to_numeric(df["distance"], errors="coerce").fillna(0)

    # Hapus data yang rusak
    df = df.dropna(subset=["timestamp"])
    return df

# --- METRICS ---
def compute_metrics(df):
    if df.empty:
        return {"total_trips": 0, "total_fare": 0, "top_location": "-"}

    return {
        "total_trips": int(len(df)),
        "total_fare": float(df["fare"].sum()),
        "top_location": str(df.groupby("location")["fare"].sum().idxmax()) if "location" in df.columns else "-"
    }

# --- PEAK HOUR ---
def detect_peak_hour(df):
    if df.empty or "timestamp" not in df.columns:
        return 0
    
    df["hour"] = df["timestamp"].dt.hour
    return int(df.groupby("hour").size().idxmax())

# --- VISUALIZATION DATA ---
def fare_per_location(df):
    if df.empty or "location" not in df.columns:
        return pd.Series(dtype=float)
    return df.groupby("location")["fare"].sum().sort_values(ascending=False)

def vehicle_distribution(df):
    if df.empty or "vehicle_type" not in df.columns:
        return pd.Series(dtype=int)
    return df.groupby("vehicle_type").size().sort_values(ascending=False)

def mobility_trend(df):
    if df.empty or "timestamp" not in df.columns:
        return pd.Series(dtype=float)
    
    df_trend = df.set_index("timestamp")
    # Resample per 1 menit agar terlihat pergerakan real-time nya
    return df_trend["fare"].resample("1min").sum().fillna(0)

# --- ANOMALY DETECTION ---
def detect_anomaly(df):
    if df.empty or "fare" not in df.columns:
        return pd.DataFrame()
    # Anomali jika fare > 90.000
    return df[df["fare"] > 90000]
import pandas as pd
import os
import glob


# ==================================
# LOAD DATA
# ==================================
def load_data(path):

    if not os.path.exists(path):
        return pd.DataFrame()

    files = glob.glob(
        os.path.join(path,"*.parquet")
    )

    if not files:
        return pd.DataFrame()

    try:
        df_list = [
            pd.read_parquet(f)
            for f in files
        ]

        df = pd.concat(
            df_list,
            ignore_index=True
        )

        return df

    except Exception as e:
        print(
            f"Error reading parquet : {e}"
        )
        return pd.DataFrame()



# ==================================
# PREPROCESS
# ==================================
def preprocess(df):

    if df.empty:
        return df


    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )


    if "fare" in df.columns:
        df["fare"] = pd.to_numeric(
            df["fare"],
            errors="coerce"
        ).fillna(0)


    if "distance" in df.columns:
        df["distance"] = pd.to_numeric(
            df["distance"],
            errors="coerce"
        ).fillna(0)


    df = df.dropna(
        subset=["timestamp"]
    )

    return df



# ==================================
# METRICS
# ==================================
def compute_metrics(df):

    if df.empty:
        return {
            "total_trips":0,
            "total_fare":0,
            "top_location":"-"
        }


    top_loc="-"

    if "location" in df.columns:
        top_loc = (
            df.groupby("location")["fare"]
              .sum()
              .idxmax()
        )


    return {
        "total_trips": int(len(df)),
        "total_fare": float(
            df["fare"].sum()
        ),
        "top_location": str(top_loc)
    }



# ==================================
# PEAK HOUR
# ==================================
def detect_peak_hour(df):

    if df.empty:
        return 0

    if "timestamp" not in df.columns:
        return 0


    df["hour"] = df["timestamp"].dt.hour

    return int(
        df.groupby("hour")
          .size()
          .idxmax()
    )



# ==================================
# VISUALIZATION DATA
# ==================================

# Traffic Density
def fare_per_location(df):

    if df.empty:
        return pd.Series(
            dtype=float
        )

    if "location" not in df.columns:
        return pd.Series(
            dtype=float
        )


    return (
      df.groupby("location")["fare"]
        .sum()
        .sort_values(
            ascending=False
        )
    )



# Vehicle Distribution
def vehicle_distribution(df):

    if df.empty:
        return pd.Series(
            dtype=int
        )

    if "vehicle_type" not in df.columns:
        return pd.Series(
            dtype=int
        )


    return (
      df.groupby("vehicle_type")
        .size()
        .sort_values(
            ascending=False
        )
    )



# Mobility Trend
def mobility_trend(df):

    if df.empty:
        return pd.Series(
            dtype=float
        )

    if "timestamp" not in df.columns:
        return pd.Series(
            dtype=float
        )


    df_trend = df.set_index(
        "timestamp"
    )


    return (
      df_trend["fare"]
        .resample("10s")
        .sum()
        .fillna(0)
    )



# ==================================
# PRAKTIKUM 6
# WINDOW AGGREGATION
# ==================================
def traffic_per_window(df):

    if df.empty:
        return None

    if "timestamp" not in df.columns:
        return None


    df["timestamp"]=pd.to_datetime(
        df["timestamp"]
    )


    return (
      df.set_index("timestamp")
        .resample("1min")
        .size()
    )



# ==================================
# ANOMALY DETECTION
# ==================================
def detect_anomaly(df):

    if df.empty:
        return pd.DataFrame()

    if "fare" not in df.columns:
        return pd.DataFrame()


    # fare tinggi dianggap anomali
    return df[
        df["fare"] > 80000
    ]



# ==================================
# OPTIONAL EXTRA INSIGHT
# ==================================

def average_fare(df):

    if df.empty:
        return 0

    return df["fare"].mean()



def average_distance(df):

    if df.empty:
        return 0

    return df["distance"].mean()



def traffic_volume(df):

    if df.empty:
        return pd.Series(dtype=int)

    if "timestamp" not in df.columns:
        return pd.Series(dtype=int)


    df2 = df.set_index(
        "timestamp"
    )


    return (
      df2.resample("1min")
         .size()
    )
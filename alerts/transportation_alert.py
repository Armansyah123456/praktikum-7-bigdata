def generate_alert(df):
    # Menggunakan kurung siku [] untuk mendefinisikan list
    alerts = []

    if df.empty:
        return alerts

    # Cek volume trafik (jumlah baris dalam dataframe)
    if len(df) > 100:
        alerts.append("High traffic volume")

    # Cek apakah ada tarif yang sangat tinggi
    # Menggunakan .max() dan operator perbandingan >= atau >
    if df["fare"].max() >= 90000:
        alerts.append("High fare detected")

    return alerts
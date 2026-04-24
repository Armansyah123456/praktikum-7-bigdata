import json
import random
import time
import os
from datetime import datetime

# 1. Konfigurasi
output_folder = "stream_data"
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam"]
cities = ["Jakarta", "Bandung", "Surabaya", "Medan", "Yogyakarta"]

# Membuat folder jika belum ada
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

counter = 1

print(f"Memulai pengiriman data ke folder: {output_folder}...")

# 2. Loop Generator
try:
    while True:
        # Membuat data transaksi
        transaction = {
            "user_id": random.randint(100, 200),
            "product": random.choice(products),
            "price": float(random.randint(50, 2000)), # Disamakan ke Float/Double untuk Spark
            "city": random.choice(cities),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        # Penamaan file (menggunakan counter agar unik)
        filename = f"transaction_{counter}.json"
        filepath = os.path.join(output_folder, filename)

        # Menulis data ke file JSON
        with open(filepath, "w") as f:
            json.dump(transaction, f)

        print(f"[{counter}] Berhasil membuat: {filename} -> {transaction['product']}")

        counter += 1
        time.sleep(3) # Jeda 3 detik agar simulasi streaming terlihat

except KeyboardInterrupt:
    print("\nGenerator dihentikan oleh pengguna.")
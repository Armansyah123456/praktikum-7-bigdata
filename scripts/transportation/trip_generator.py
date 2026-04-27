import json
import time
import random
import os
from datetime import datetime

# Jalur ini disesuaikan agar Spark bisa membaca dari folder yang sama
OUTPUT_PATH = "data/raw/transportation"
os.makedirs(OUTPUT_PATH, exist_ok=True)

locations = ["Jakarta", "Bandung", "Surabaya"]
vehicles = ["Car", "Motorbike", "Taxi"]

i = 1
print(f"Memulai pengiriman data ke: {OUTPUT_PATH}...")

try:
    while True:
        data = {
            "trip_id": f"TRX-{i:04d}",
            "vehicle_type": random.choice(vehicles),
            "location": random.choice(locations),
            "distance": round(random.uniform(1, 20), 2),
            "fare": random.randint(10000, 100000),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        file_name = os.path.join(OUTPUT_PATH, f"trip_{i}.json")
        with open(file_name, "w") as f:
            json.dump(data, f)
        
        print(f"✅ Data {i} berhasil dibuat di {file_name}")
        i += 1
        time.sleep(2)
except KeyboardInterrupt:
    print("Berhenti...")
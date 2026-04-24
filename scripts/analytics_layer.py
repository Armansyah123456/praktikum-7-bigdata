from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

# Konfigurasi Path
INPUT_FOLDER = "stream_data"
OUTPUT_FOLDER = "data/serving/stream"
CHECKPOINT_FOLDER = "data/serving/checkpoint"

# Membuat folder output jika belum ada
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("EcommerceAnalyticsServingLayer") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Definisi Schema (harus cocok dengan JSON dari generator)
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print(f"👀 Memulai monitoring folder: {INPUT_FOLDER}...")

# 2. Baca Data Streaming (Streaming DF)
# Spark akan terus memantau folder INPUT_FOLDER untuk file JSON baru
raw_stream_df = spark.readStream \
    .schema(schema) \
    .json(INPUT_FOLDER)

# 3. Transformasi Data
# - Ubah string timestamp menjadi tipe timestamp sungguhan
# - Bersihkan nama produk (opsional, contoh)
processed_stream_df = raw_stream_df \
    .withColumn("timestamp", to_timestamp(col("timestamp"), "Y-M-d H:M:S"))

# 4. Tulis Data ke Serving Layer (Format Parquet)
# Data akan disimpan secara berkala (append) ke folderOUTPUT_FOLDER
query = processed_stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", OUTPUT_FOLDER) \
    .option("checkpointLocation", CHECKPOINT_FOLDER) \
    .start()

# Biarkan Spark berjalan terus sampai dihentikan
query.awaitTermination()
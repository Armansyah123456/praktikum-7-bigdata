from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# 1. Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("StreamingPipeline") \
    .getOrCreate()

# Mengatur log level agar console tidak terlalu penuh
spark.sparkContext.setLogLevel("ERROR")

# 2. Mendefinisikan Schema
# Menggunakan StructType lebih stabil untuk pembacaan JSON di Spark
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("timestamp", StringType(), True)
])  

# 3. Membaca Data Stream
stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("stream_data")

# 4. Menulis Data Stream (Sink) ke Parquet
query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/stream") \
    .option("checkpointLocation", "logs/stream_checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

# Menjaga agar aplikasi tetap berjalan
query.awaitTermination()
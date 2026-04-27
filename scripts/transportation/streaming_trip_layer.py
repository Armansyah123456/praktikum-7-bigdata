from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.appName("TransportationStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("distance", DoubleType(), True),
    StructField("fare", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Membaca data dari generator
stream_df = spark.readStream \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .json("data/raw/transportation")

# Menulis hasil ke folder yang dibaca dashboard
query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "data/serving/transportation") \
    .option("checkpointLocation", "logs/checkpoint_transportation") \
    .start()

print("🚀 Spark Streaming sedang memproses data...")
query.awaitTermination()
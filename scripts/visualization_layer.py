from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, desc
import matplotlib.pyplot as plt
import os

os.makedirs("reports", exist_ok=True)

spark = SparkSession.builder \
    .appName("VisualizationLayer") \
    .master("local[*]") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet("data/clean/parquet/")

# Chart 1: Revenue per Category
category_df = df.groupBy("category") \
    .agg(_sum("total_amount").alias("category_revenue")) \
    .orderBy(desc("category_revenue")) \
    .toPandas()

plt.figure(figsize=(8, 5))
plt.bar(category_df["category"], category_df["category_revenue"], color=["#4F8EF7", "#F7A44F", "#4FF7A4"])
plt.title("Revenue per Category")
plt.ylabel("Total Revenue (Rp)")
plt.tight_layout()
plt.savefig("reports/category_revenue.png")
print("✔ Saved: reports/category_revenue.png")

# Chart 2: Top Products
top_df = df.groupBy("product") \
    .agg(_sum("quantity").alias("total_quantity")) \
    .orderBy(desc("total_quantity")) \
    .limit(10) \
    .toPandas()

plt.figure(figsize=(8, 5))
plt.barh(top_df["product"], top_df["total_quantity"], color="#6C63FF")
plt.gca().invert_yaxis()
plt.title("Top Products by Quantity")
plt.xlabel("Total Quantity")
plt.tight_layout()
plt.savefig("reports/top_products.png")
print("✔ Saved: reports/top_products.png")

spark.stop()
print("✔ Visualization Layer selesai!")
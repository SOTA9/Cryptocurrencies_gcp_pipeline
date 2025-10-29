from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Spark session for Dataproc
spark = SparkSession.builder \
    .appName("CoinCap Batch Assets to BigQuery") \
    .getOrCreate()

# GCS bucket with raw JSON files
GCS_BUCKET = os.getenv("GCS_BUCKET", "your-gcs-bucket")
RAW_PATH = f"gs://{GCS_BUCKET}/coincap_assets/*.json"

# BigQuery target table
BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
BQ_DATASET = os.getenv("BQ_BATCH_DATASET", "coincap_batch")
BQ_TABLE = "assets"

# Read raw JSON from GCS
df = spark.read.json(RAW_PATH)

# Select and transform fields
transformed_df = df.select(
    col("id"),
    col("symbol"),
    col("name"),
    col("priceUsd").cast("double"),
    col("marketCapUsd").cast("double"),
    col("volumeUsd24Hr").cast("double"),
    col("rank").cast("int")
)

# Write to BigQuery (overwrite or append)
transformed_df.write \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}") \
    .mode("append") \
    .save()

print("✅ Batch assets written to BigQuery successfully.")

spark.stop()

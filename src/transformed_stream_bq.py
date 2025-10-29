from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

# Spark session for Structured Streaming
spark = SparkSession.builder \
    .appName("CoinCap Streaming Assets to BigQuery") \
    .getOrCreate()

# Pub/Sub subscription
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project")
PUBSUB_SUB = os.getenv("PUBSUB_SUBSCRIPTION", "projects/your-gcp-project/subscriptions/your-sub")
BQ_PROJECT = PROJECT_ID
BQ_DATASET = os.getenv("BQ_STREAM_DATASET", "coincap_stream")
BQ_TABLE = "assets_stream"

# Define schema matching CoinCap asset JSON
asset_schema = StructType([
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("name", StringType(), True),
    StructField("priceUsd", StringType(), True),
    StructField("marketCapUsd", StringType(), True),
    StructField("volumeUsd24Hr", StringType(), True),
    StructField("rank", StringType(), True)
])

# Read from Pub/Sub as streaming
raw_df = spark.readStream \
    .format("pubsub") \
    .option("subscription", PUBSUB_SUB) \
    .load()

# Convert message payload from bytes to string
df_str = raw_df.selectExpr("CAST(data AS STRING) as json_str")

# Parse JSON
df_parsed = df_str.select(from_json(col("json_str"), asset_schema).alias("data")).select("data.*")

# Transform numeric fields
df_transformed = df_parsed.select(
    col("id"),
    col("symbol"),
    col("name"),
    col("priceUsd").cast(DoubleType()),
    col("marketCapUsd").cast(DoubleType()),
    col("volumeUsd24Hr").cast(DoubleType()),
    col("rank").cast(IntegerType())
)

# Write to BigQuery streaming table
query = df_transformed.writeStream \
    .format("bigquery") \
    .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}") \
    .option("checkpointLocation", f"gs://{os.getenv('GCS_BUCKET')}/coincap_checkpoints/") \
    .outputMode("append") \
    .start()

query.awaitTermination()

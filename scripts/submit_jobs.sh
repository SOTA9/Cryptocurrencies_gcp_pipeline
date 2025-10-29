#!/bin/bash

# ===================================
# CoinCap ETL Pipeline Automation
# ===================================

# Exit immediately if any command fails
set -e

# -------------------------------
# 1️⃣ Load environment variables safely
# -------------------------------
if [ -f ".env" ]; then
    echo "🔹 Loading environment variables from .env..."
    set -a           # automatically export all variables
    source .env
    set +a
else
    echo "❌ .env file not found! Please create one."
    exit 1
fi

# Verify critical variables
echo "🌐 Using GCP Project: $GCP_PROJECT_ID"
echo "🗂 Dataproc Cluster: $DATAPROC_CLUSTER in $DATAPROC_REGION"
echo "📦 GCS Bucket: $GCS_BUCKET"
echo "🔑 Pub/Sub Topic: $PUBSUB_TOPIC"
echo "🔑 Pub/Sub Subscription: $PUBSUB_SUBSCRIPTION"

# Set GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=$GCP_SERVICE_ACCOUNT_KEY

# -------------------------------
# 2️⃣ Batch Pipeline
# -------------------------------
echo "🚀 Starting batch pipeline: GCS -> BigQuery"

gcloud dataproc jobs submit pyspark batch/batch_assets_to_bq.py \
    --cluster "$DATAPROC_CLUSTER" \
    --region "$DATAPROC_REGION" \
    --project "$GCP_PROJECT_ID" \
    --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0

echo "✅ Batch pipeline completed"

# -------------------------------
# 3️⃣ Streaming Pipeline
# -------------------------------
echo "🚀 Starting streaming pipeline: Pub/Sub -> BigQuery (background)"

# Run streaming pipeline in background safely
(
gcloud dataproc jobs submit pyspark streaming/stream_assets_to_bq.py \
    --cluster "$DATAPROC_CLUSTER" \
    --region "$DATAPROC_REGION" \
    --project "$GCP_PROJECT_ID" \
    --properties spark.jars.packages=com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0
) &

STREAM_JOB_PID=$!
echo "ℹ️ Streaming job submitted in background (PID: $STREAM_JOB_PID)"

echo "✅ Automation script finished. Streaming is running in background."

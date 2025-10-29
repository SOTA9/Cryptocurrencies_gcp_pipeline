import os
import json
import requests
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

COINCAP_API_KEY = os.getenv("COINCAP_API_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET")

# Authenticate GCP
SERVICE_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_KEY

BASE_URL = "https://rest.coincap.io/v3/assets"


def fetch_assets():
    """Fetch all assets from CoinCap API."""
    headers = {"Authorization": f"Bearer {COINCAP_API_KEY}"}
    response = requests.get(BASE_URL, headers=headers)
    response.raise_for_status()
    return response.json().get("data", [])


def upload_to_gcs(data, bucket_name):
    """Upload raw JSON data to GCS with timestamped filename."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    blob = bucket.blob(f"coincap_assets/raw_assets_{timestamp}.json")
    blob.upload_from_string(json.dumps(data), content_type="application/json")
    print(f"✅ Uploaded {len(data)} assets to GCS: {blob.name}")


if __name__ == "__main__":
    assets = fetch_assets()
    upload_to_gcs(assets, GCS_BUCKET)

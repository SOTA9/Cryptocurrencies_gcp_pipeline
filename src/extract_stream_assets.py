import os
import json
import requests
from google.cloud import pubsub_v1
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")
COINCAP_API_KEY = os.getenv("COINCAP_API_KEY")
SERVICE_KEY = os.getenv("GCP_SERVICE_ACCOUNT_KEY")

# Authenticate GCP
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_KEY

BASE_URL = "https://rest.coincap.io/v3/assets"


def fetch_assets():
    """Fetch all assets from CoinCap API."""
    headers = {"Authorization": f"Bearer {COINCAP_API_KEY}"}
    response = requests.get(BASE_URL, headers=headers)
    response.raise_for_status()
    return response.json().get("data", [])


def publish_to_pubsub(project_id, topic_id, assets):
    """Publish each asset as a message to Pub/Sub."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    for item in assets:
        # Keep all raw data as JSON string for flexibility downstream
        message = json.dumps(item)
        publisher.publish(topic_path, message.encode("utf-8"))

    print(f"✅ Published {len(assets)} assets to Pub/Sub topic {topic_id}")


if __name__ == "__main__":
    assets = fetch_assets()
    publish_to_pubsub(PROJECT_ID, TOPIC_ID, assets)

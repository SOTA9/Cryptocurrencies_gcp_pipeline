import unittest
from unittest.mock import patch, MagicMock
import streaming.transformed_stream_bq as streaming

class TestStreamingETL(unittest.TestCase):

    @patch("streaming.transformed_stream_bq.read_from_pubsub")
    @patch("streaming.transformed_stream_bq.spark")
    def test_streaming_transformation(self, mock_spark, mock_pubsub):
        # Mock Pub/Sub streaming data
        mock_pubsub.return_value = [
            {"id": "bitcoin", "symbol": "BTC", "priceUsd": "30000"},
            {"id": "ethereum", "symbol": "ETH", "priceUsd": "2000"}
        ]

        # Mock Spark streaming DataFrame
        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.load.return_value = mock_df

        # Run streaming pipeline function
        streaming.run_streaming_etl()  # Assume main function is run_streaming_etl()

        # Assertions
        mock_pubsub.assert_called_once()
        mock_spark.readStream.format.assert_called_with("pubsub")
        mock_df.writeStream.format.assert_called_with("bigquery")

if __name__ == "__main__":
    unittest.main()

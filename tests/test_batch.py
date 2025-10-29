import unittest
from unittest.mock import patch, MagicMock
import batch.transformed_batch_bq as batch

class TestBatchETL(unittest.TestCase):

    @patch("batch.transformed_batch_bq.fetch_coincap_data")
    @patch("batch.transformed_batch_bq.spark")
    def test_batch_transformation(self, mock_spark, mock_fetch):
        # Mock API response
        mock_fetch.return_value = [
            {"id": "bitcoin", "symbol": "BTC", "name": "Bitcoin", "priceUsd": "30000", "rank": "1"},
            {"id": "ethereum", "symbol": "ETH", "name": "Ethereum", "priceUsd": "2000", "rank": "2"}
        ]

        # Mock Spark DataFrame
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        # Run batch pipeline function
        batch.run_batch_etl()  # Assume your main function in batch script is run_batch_etl()

        # Assertions
        mock_fetch.assert_called_once()
        mock_spark.createDataFrame.assert_called_once_with(mock_fetch.return_value)
        mock_df.write.format.assert_called_with("bigquery")

if __name__ == "__main__":
    unittest.main()

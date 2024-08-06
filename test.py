import unittest
from unittest.mock import patch, MagicMock
from dataingestion import main
import pyspark

class TestDataIngestion(unittest.TestCase):

    @patch('dataingestion.spark')
    def test_main(self, mock_spark):
        # Mock the spark object
        mock_spark.read.csv.return_value = MagicMock()
        mock_spark.read.parquet.return_value = MagicMock()
        mock_spark.sql.return_value = MagicMock()

        # Call the main function
        main()

        # Assert that the spark object was used correctly
        mock_spark.read.csv.assert_called()
        mock_spark.read.parquet.assert_called()
        mock_spark.sql.assert_called()

    @patch('dataingestion.write_parquet_with_mode')
    @patch('dataingestion.write_parquet_with_compression')
    @patch('dataingestion.write_partitioned_parquet')
    def test_write_parquet(self, mock_write_partitioned_parquet, mock_write_parquet_with_compression, mock_write_parquet_with_mode):
        # Mock the write_parquet functions
        mock_write_parquet_with_mode.return_value = None
        mock_write_parquet_with_compression.return_value = None
        mock_write_partitioned_parquet.return_value = None

        # Call the main function
        main()

        # Assert that the write_parquet functions were called correctly
        mock_write_parquet_with_mode.assert_called()
        mock_write_parquet_with_compression.assert_called()
        mock_write_partitioned_parquet.assert_called()

    @patch('dataingestion.get_dashboard_data')
    def test_get_dashboard_data(self, mock_get_dashboard_data):
        # Mock the get_dashboard_data function
        mock_get_dashboard_data.return_value = MagicMock()

        # Call the main function
        main()

        # Assert that the get_dashboard_data function was called correctly
        mock_get_dashboard_data.assert_called()

if __name__ == '__main__':
    unittest.main()
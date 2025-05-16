"""
Tests for Bronze Products ETL.

This module contains tests for the Bronze Products ETL process.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.bronze.products_etl import (
    read_products_data,
    transform_products_data,
    write_products_data,
    register_products_table,
    main,
)
from etl.common.schemas import RAW_PRODUCTS_SCHEMA


class TestBronzeProductsETL(unittest.TestCase):
    """Test cases for Bronze Products ETL."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Create a Spark session for testing
        cls.spark = (
            SparkSession.builder.appName("test_bronze_products_etl")
            .master("local[1]")
            .getOrCreate()
        )

        # Create a sample products DataFrame
        data = [
            (1, 1, "Produce", "Organic Bananas"),
            (2, 1, "Produce", "Organic Strawberries"),
            (3, 2, "Dairy", "Organic Whole Milk"),
            (4, 3, "Bakery", "Whole Wheat Bread"),
        ]
        cls.sample_df = cls.spark.createDataFrame(
            data, ["product_id", "department_id", "department", "product_name"]
        )

    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures."""
        # Stop the Spark session
        cls.spark.stop()

    @patch("etl.bronze.products_etl.spark")
    def test_read_products_data(self, mock_spark):
        """Test reading products data."""
        # Configure the mock
        mock_spark.read.format.return_value.option.return_value.option.return_value.schema.return_value.load.return_value = self.sample_df

        # Call the function
        result_df = read_products_data(mock_spark, "test-bucket", "2023-05-15")

        # Assert that the function returns the sample DataFrame
        self.assertEqual(result_df, self.sample_df)

        # Assert that the read method was called with the correct arguments
        mock_spark.read.format.assert_called_once_with("csv")
        mock_spark.read.format.return_value.option.assert_called_with("header", "true")
        mock_spark.read.format.return_value.option.return_value.option.assert_called_with(
            "inferSchema", "false"
        )
        mock_spark.read.format.return_value.option.return_value.option.return_value.schema.assert_called_with(
            RAW_PRODUCTS_SCHEMA
        )

    @patch("etl.bronze.products_etl.validate_schema")
    @patch("etl.bronze.products_etl.add_metadata_columns")
    def test_transform_products_data(self, mock_add_metadata, mock_validate_schema):
        """Test transforming products data."""
        # Configure the mocks
        mock_validate_schema.return_value = (True, None, self.sample_df)
        mock_add_metadata.return_value = self.sample_df

        # Call the function
        result_df = transform_products_data(self.sample_df)

        # Assert that the function returns the sample DataFrame
        self.assertEqual(result_df, self.sample_df)

        # Assert that validate_schema was called with the correct arguments
        mock_validate_schema.assert_called_once_with(
            self.sample_df, RAW_PRODUCTS_SCHEMA, strict=True
        )

        # Assert that add_metadata_columns was called with the correct arguments
        mock_add_metadata.assert_called_once_with(
            self.sample_df,
            layer="bronze",
            source_file_column=True,
            ingestion_timestamp_column=True,
            processing_timestamp_column=True,
            layer_column=True,
        )

    @patch("etl.bronze.products_etl.write_delta_table")
    def test_write_products_data(self, mock_write_delta_table):
        """Test writing products data."""
        # Call the function
        write_products_data(self.sample_df, "test-bucket", "2023-05-15")

        # Assert that write_delta_table was called with the correct arguments
        mock_write_delta_table.assert_called_once()
        args, kwargs = mock_write_delta_table.call_args
        self.assertEqual(kwargs["df"], self.sample_df)
        self.assertEqual(kwargs["mode"], "overwrite")
        self.assertIsNone(kwargs["partition_by"])
        self.assertEqual(kwargs["bucket_name"], "test-bucket")

    @patch("etl.bronze.products_etl.register_delta_table")
    def test_register_products_table(self, mock_register_delta_table):
        """Test registering products table."""
        # Configure the mock
        mock_register_delta_table.return_value = True

        # Call the function
        register_products_table(self.spark, "test-bucket")

        # Assert that register_delta_table was called with the correct arguments
        mock_register_delta_table.assert_called_once()
        args, kwargs = mock_register_delta_table.call_args
        self.assertEqual(kwargs["spark"], self.spark)
        self.assertEqual(kwargs["table_name"], "products")
        self.assertEqual(kwargs["layer"], "bronze")
        self.assertEqual(kwargs["bucket_name"], "test-bucket")

    @patch("etl.bronze.products_etl.create_spark_session")
    @patch("etl.bronze.products_etl.read_products_data")
    @patch("etl.bronze.products_etl.transform_products_data")
    @patch("etl.bronze.products_etl.write_products_data")
    @patch("etl.bronze.products_etl.register_products_table")
    def test_main_success(
        self, mock_register, mock_write, mock_transform, mock_read, mock_create_spark
    ):
        """Test main function success case."""
        # Configure the mocks
        mock_create_spark.return_value = self.spark
        mock_read.return_value = self.sample_df
        mock_transform.return_value = self.sample_df

        # Call the function
        result = main("2023-05-15", "test-bucket", "us-east-1")

        # Assert that the function returns 0 (success)
        self.assertEqual(result, 0)

        # Assert that all the functions were called
        mock_create_spark.assert_called_once()
        mock_read.assert_called_once_with(self.spark, "test-bucket", "2023-05-15")
        mock_transform.assert_called_once_with(self.sample_df)
        mock_write.assert_called_once_with(self.sample_df, "test-bucket", "2023-05-15")
        mock_register.assert_called_once_with(self.spark, "test-bucket")

    @patch("etl.bronze.products_etl.create_spark_session")
    @patch("etl.bronze.products_etl.read_products_data")
    def test_main_failure(self, mock_read, mock_create_spark):
        """Test main function failure case."""
        # Configure the mocks
        mock_create_spark.return_value = self.spark
        mock_read.side_effect = Exception("Test error")

        # Call the function
        result = main("2023-05-15", "test-bucket", "us-east-1")

        # Assert that the function returns 1 (failure)
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()

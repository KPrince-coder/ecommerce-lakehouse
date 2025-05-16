"""
Tests for Bronze Orders ETL.

This module contains tests for the Bronze Orders ETL process.
"""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.bronze.orders_etl import (
    read_orders_data,
    transform_orders_data,
    write_orders_data,
    register_orders_table,
    get_date_range,
    main
)
from etl.common.schemas import RAW_ORDERS_SCHEMA, BRONZE_ORDERS_SCHEMA


class TestBronzeOrdersETL(unittest.TestCase):
    """Test cases for Bronze Orders ETL."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        # Create a Spark session for testing
        cls.spark = SparkSession.builder \
            .appName("test_bronze_orders_etl") \
            .master("local[1]") \
            .getOrCreate()
        
        # Create a sample orders DataFrame
        data = [
            (1, "10001", 1001, datetime.strptime("2023-05-15 10:30:00", "%Y-%m-%d %H:%M:%S"), 100.50, datetime.strptime("2023-05-15", "%Y-%m-%d").date()),
            (2, "10002", 1002, datetime.strptime("2023-05-15 11:45:00", "%Y-%m-%d %H:%M:%S"), 200.75, datetime.strptime("2023-05-15", "%Y-%m-%d").date()),
            (3, "10003", 1003, datetime.strptime("2023-05-16 09:15:00", "%Y-%m-%d %H:%M:%S"), 150.25, datetime.strptime("2023-05-16", "%Y-%m-%d").date()),
            (4, "10004", 1004, datetime.strptime("2023-05-16 14:20:00", "%Y-%m-%d %H:%M:%S"), 300.00, datetime.strptime("2023-05-16", "%Y-%m-%d").date()),
        ]
        cls.sample_df = cls.spark.createDataFrame(
            data,
            ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
        )
    
    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures."""
        # Stop the Spark session
        cls.spark.stop()
    
    def test_get_date_range(self):
        """Test getting date range."""
        # Call the function
        dates = get_date_range("2023-05-15", 3)
        
        # Assert that the function returns the correct dates
        self.assertEqual(dates, ["2023-05-15", "2023-05-14", "2023-05-13"])
    
    @patch("etl.bronze.orders_etl.spark")
    def test_read_orders_data(self, mock_spark):
        """Test reading orders data."""
        # Configure the mock
        mock_spark.read.format.return_value.option.return_value.option.return_value.schema.return_value.load.return_value = self.sample_df
        
        # Call the function
        result_df = read_orders_data(mock_spark, "test-bucket", ["2023-05-15", "2023-05-16"])
        
        # Assert that the function returns the sample DataFrame
        self.assertEqual(result_df, self.sample_df)
        
        # Assert that the read method was called with the correct arguments
        mock_spark.read.format.assert_called_once_with("csv")
        mock_spark.read.format.return_value.option.assert_called_with("header", "true")
        mock_spark.read.format.return_value.option.return_value.option.assert_called_with("inferSchema", "false")
        mock_spark.read.format.return_value.option.return_value.option.return_value.schema.assert_called_with(RAW_ORDERS_SCHEMA)
    
    @patch("etl.bronze.orders_etl.validate_schema")
    @patch("etl.bronze.orders_etl.add_metadata_columns")
    def test_transform_orders_data(self, mock_add_metadata, mock_validate_schema):
        """Test transforming orders data."""
        # Configure the mocks
        mock_validate_schema.return_value = (True, None, self.sample_df)
        mock_add_metadata.return_value = self.sample_df
        
        # Call the function
        result_df = transform_orders_data(self.sample_df)
        
        # Assert that the function returns the sample DataFrame
        self.assertEqual(result_df, self.sample_df)
        
        # Assert that validate_schema was called with the correct arguments
        mock_validate_schema.assert_called_once_with(self.sample_df, RAW_ORDERS_SCHEMA, strict=True)
        
        # Assert that add_metadata_columns was called with the correct arguments
        mock_add_metadata.assert_called_once_with(
            self.sample_df,
            layer="bronze",
            source_file_column=True,
            ingestion_timestamp_column=True,
            processing_timestamp_column=True,
            layer_column=True
        )
    
    @patch("etl.bronze.orders_etl.write_delta_table")
    def test_write_orders_data(self, mock_write_delta_table):
        """Test writing orders data."""
        # Call the function
        write_orders_data(self.sample_df, "test-bucket")
        
        # Assert that write_delta_table was called with the correct arguments
        mock_write_delta_table.assert_called_once()
        args, kwargs = mock_write_delta_table.call_args
        self.assertEqual(kwargs["df"], self.sample_df)
        self.assertEqual(kwargs["mode"], "append")
        self.assertEqual(kwargs["partition_by"], ["date"])
        self.assertEqual(kwargs["bucket_name"], "test-bucket")
    
    @patch("etl.bronze.orders_etl.register_delta_table")
    def test_register_orders_table(self, mock_register_delta_table):
        """Test registering orders table."""
        # Configure the mock
        mock_register_delta_table.return_value = True
        
        # Call the function
        register_orders_table(self.spark, "test-bucket")
        
        # Assert that register_delta_table was called with the correct arguments
        mock_register_delta_table.assert_called_once()
        args, kwargs = mock_register_delta_table.call_args
        self.assertEqual(kwargs["spark"], self.spark)
        self.assertEqual(kwargs["table_name"], "orders")
        self.assertEqual(kwargs["layer"], "bronze")
        self.assertEqual(kwargs["bucket_name"], "test-bucket")
    
    @patch("etl.bronze.orders_etl.get_date_range")
    @patch("etl.bronze.orders_etl.create_spark_session")
    @patch("etl.bronze.orders_etl.read_orders_data")
    @patch("etl.bronze.orders_etl.transform_orders_data")
    @patch("etl.bronze.orders_etl.write_orders_data")
    @patch("etl.bronze.orders_etl.register_orders_table")
    def test_main_success(self, mock_register, mock_write, mock_transform, mock_read, mock_create_spark, mock_get_date_range):
        """Test main function success case."""
        # Configure the mocks
        mock_get_date_range.return_value = ["2023-05-15", "2023-05-14"]
        mock_create_spark.return_value = self.spark
        mock_read.return_value = self.sample_df
        mock_transform.return_value = self.sample_df
        
        # Call the function
        result = main("2023-05-15", "test-bucket", "us-east-1", 2)
        
        # Assert that the function returns 0 (success)
        self.assertEqual(result, 0)
        
        # Assert that all the functions were called
        mock_get_date_range.assert_called_once_with("2023-05-15", 2)
        mock_create_spark.assert_called_once()
        mock_read.assert_called_once_with(self.spark, "test-bucket", ["2023-05-15", "2023-05-14"])
        mock_transform.assert_called_once_with(self.sample_df)
        mock_write.assert_called_once_with(self.sample_df, "test-bucket")
        mock_register.assert_called_once_with(self.spark, "test-bucket")
    
    @patch("etl.bronze.orders_etl.get_date_range")
    @patch("etl.bronze.orders_etl.create_spark_session")
    @patch("etl.bronze.orders_etl.read_orders_data")
    def test_main_failure(self, mock_read, mock_create_spark, mock_get_date_range):
        """Test main function failure case."""
        # Configure the mocks
        mock_get_date_range.return_value = ["2023-05-15"]
        mock_create_spark.return_value = self.spark
        mock_read.side_effect = Exception("Test error")
        
        # Call the function
        result = main("2023-05-15", "test-bucket", "us-east-1")
        
        # Assert that the function returns 1 (failure)
        self.assertEqual(result, 1)


if __name__ == "__main__":
    unittest.main()

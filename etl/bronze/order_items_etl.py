#!/usr/bin/env python
"""
Bronze Order Items ETL

This script ingests order item data from CSV files in the raw zone and writes it to a Delta table
in the bronze layer. It performs the following operations:
1. Reads order items CSV files from S3 raw zone
2. Applies schema to raw CSV data
3. Adds metadata columns (source_file, ingestion_timestamp)
4. Partitions data by date
5. Writes to Bronze Delta table
6. Updates Glue Data Catalog

Usage:
    python -m etl.bronze.order_items_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.common.spark_session import create_spark_session, write_delta_table
from etl.common.glue_catalog import register_delta_table
from etl.common.etl_utils import (
    add_metadata_columns,
    validate_schema,
)
from etl.common.schemas import RAW_ORDER_ITEMS_SCHEMA, BRONZE_ORDER_ITEMS_SCHEMA
from config import S3_BUCKET_NAME, AWS_REGION, LOG_LEVEL, LOG_FORMAT, get_prefix

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Ingest order item data from raw zone to bronze layer"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Processing date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default=S3_BUCKET_NAME,
        help=f"S3 bucket name (default: {S3_BUCKET_NAME})"
    )
    parser.add_argument(
        "--region",
        type=str,
        default=AWS_REGION,
        help=f"AWS region (default: {AWS_REGION})"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Number of days to process (default: 1)"
    )
    
    return parser.parse_args()


def get_date_range(end_date: str, days_back: int) -> List[str]:
    """
    Get a list of dates to process.
    
    Args:
        end_date: End date (YYYY-MM-DD)
        days_back: Number of days to go back
    
    Returns:
        List[str]: List of dates to process
    """
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    dates = []
    
    for i in range(days_back):
        date_obj = end_date_obj - timedelta(days=i)
        dates.append(date_obj.strftime("%Y-%m-%d"))
    
    return dates


def read_order_items_data(
    spark: SparkSession,
    bucket_name: str,
    dates: List[str]
) -> DataFrame:
    """
    Read order items data from S3 raw zone.
    
    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        dates: List of dates to process (YYYY-MM-DD)
    
    Returns:
        DataFrame: Order items data
    """
    # Construct the S3 paths
    order_items_paths = [
        f"s3a://{bucket_name}/{get_prefix('raw', 'order_items')}{date}.csv"
        for date in dates
    ]
    
    logger.info(f"Reading order items data from {len(order_items_paths)} files")
    
    try:
        # Read the CSV files
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(RAW_ORDER_ITEMS_SCHEMA)
            .load(order_items_paths)
        )
        
        logger.info(f"Successfully read {df.count()} order items")
        return df
    except Exception as e:
        logger.error(f"Error reading order items data: {str(e)}")
        raise


def transform_order_items_data(df: DataFrame) -> DataFrame:
    """
    Transform order items data for the bronze layer.
    
    Args:
        df: Order items data DataFrame
    
    Returns:
        DataFrame: Transformed order items data
    """
    logger.info("Transforming order items data")
    
    try:
        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            df, RAW_ORDER_ITEMS_SCHEMA, strict=True
        )
        
        if not success:
            logger.error(f"Schema validation failed: {error_msg}")
            raise ValueError(f"Schema validation failed: {error_msg}")
        
        # Add metadata columns
        result_df = add_metadata_columns(
            validated_df,
            layer="bronze",
            source_file_column=True,
            ingestion_timestamp_column=True,
            processing_timestamp_column=True,
            layer_column=True
        )
        
        logger.info("Successfully transformed order items data")
        return result_df
    except Exception as e:
        logger.error(f"Error transforming order items data: {str(e)}")
        raise


def write_order_items_data(
    df: DataFrame,
    bucket_name: str
) -> None:
    """
    Write order items data to bronze Delta table.
    
    Args:
        df: Transformed order items data DataFrame
        bucket_name: S3 bucket name
    
    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'order_items')}"
    
    logger.info(f"Writing order items data to {table_path}")
    
    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="append",  # Use append mode for incremental loads
            partition_by=["date"],  # Partition by date
            bucket_name=bucket_name
        )
        
        logger.info(f"Successfully wrote order items data to {table_path}")
    except Exception as e:
        logger.error(f"Error writing order items data: {str(e)}")
        raise


def register_order_items_table(
    spark: SparkSession,
    bucket_name: str
) -> None:
    """
    Register order items table in Glue Data Catalog.
    
    Args:
        spark: Spark session
        bucket_name: S3 bucket name
    
    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'order_items')}"
    
    logger.info("Registering order items table in Glue Data Catalog")
    
    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="order_items",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Bronze layer order items table",
            layer="bronze",
            bucket_name=bucket_name,
            columns_description={
                "id": "Unique identifier for the order item",
                "order_id": "Identifier for the order",
                "user_id": "Identifier for the user",
                "days_since_prior_order": "Days since prior order",
                "product_id": "Identifier for the product",
                "add_to_cart_order": "Order in which the item was added to cart",
                "reordered": "Whether the item was reordered",
                "order_timestamp": "Timestamp when the order was placed",
                "date": "Date of the order",
                "source_file": "Source file path",
                "ingestion_timestamp": "Timestamp when the data was ingested",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (bronze)"
            }
        )
        
        if success:
            logger.info("Successfully registered order items table in Glue Data Catalog")
        else:
            logger.error("Failed to register order items table in Glue Data Catalog")
    except Exception as e:
        logger.error(f"Error registering order items table: {str(e)}")
        raise


def main(
    date: str,
    bucket_name: str,
    region: str,
    days_back: int = 1
) -> int:
    """
    Main function to run the ETL process.
    
    Args:
        date: Processing date (YYYY-MM-DD)
        bucket_name: S3 bucket name
        region: AWS region
        days_back: Number of days to process
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Starting Bronze Order Items ETL for date: {date}")
    
    try:
        # Get the date range to process
        dates = get_date_range(date, days_back)
        logger.info(f"Processing dates: {dates}")
        
        # Create Spark session
        spark = create_spark_session(
            app_name=f"bronze_order_items_etl_{date}",
            enable_hive_support=True
        )
        
        # Read order items data
        order_items_df = read_order_items_data(spark, bucket_name, dates)
        
        # Transform order items data
        transformed_df = transform_order_items_data(order_items_df)
        
        # Write order items data
        write_order_items_data(transformed_df, bucket_name)
        
        # Register order items table in Glue Data Catalog
        register_order_items_table(spark, bucket_name)
        
        # Stop Spark session
        spark.stop()
        
        logger.info(f"Successfully completed Bronze Order Items ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Bronze Order Items ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    sys.exit(exit_code)

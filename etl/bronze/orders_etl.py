#!/usr/bin/env python
"""
Bronze Orders ETL

This script ingests order data from CSV files in the raw zone and writes it to a Delta table
in the bronze layer. It performs the following operations:
1. Reads orders CSV files from S3 raw zone
2. Applies schema to raw CSV data
3. Adds metadata columns (source_file, ingestion_timestamp)
4. Partitions data by date
5. Writes to Bronze Delta table
6. Updates Glue Data Catalog

Usage:
    python -m etl.bronze.orders_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.common.spark_session import create_spark_session, write_delta_table
from etl.common.glue_catalog import register_delta_table
from etl.common.etl_utils import (
    add_metadata_columns,
    validate_schema,
)
from etl.common.schemas import RAW_ORDERS_SCHEMA, BRONZE_ORDERS_SCHEMA
from config import S3_BUCKET_NAME, AWS_REGION, LOG_LEVEL, LOG_FORMAT, get_prefix

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Ingest order data from raw zone to bronze layer"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Processing date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default=S3_BUCKET_NAME,
        help=f"S3 bucket name (default: {S3_BUCKET_NAME})",
    )
    parser.add_argument(
        "--region",
        type=str,
        default=AWS_REGION,
        help=f"AWS region (default: {AWS_REGION})",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Number of days to process (default: 1)",
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


def read_orders_data(
    spark: SparkSession, bucket_name: str, dates: List[str]
) -> DataFrame:
    """
    Read orders data from S3 raw zone.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        dates: List of dates to process (YYYY-MM-DD)

    Returns:
        DataFrame: Orders data
    """
    # Construct the S3 paths
    orders_paths = [
        f"s3a://{bucket_name}/{get_prefix('raw', 'orders')}{date}.csv" for date in dates
    ]

    logger.info(f"Reading orders data from {len(orders_paths)} files")

    try:
        # Read the CSV files
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(RAW_ORDERS_SCHEMA)
            .load(orders_paths)
        )

        logger.info(f"Successfully read {df.count()} orders")
        return df
    except Exception as e:
        logger.error(f"Error reading orders data: {str(e)}")
        raise


def transform_orders_data(df: DataFrame) -> DataFrame:
    """
    Transform orders data for the bronze layer.

    Args:
        df: Orders data DataFrame

    Returns:
        DataFrame: Transformed orders data
    """
    logger.info("Transforming orders data")

    try:
        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            df, RAW_ORDERS_SCHEMA, strict=True
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
            layer_column=True,
        )

        logger.info("Successfully transformed orders data")
        return result_df
    except Exception as e:
        logger.error(f"Error transforming orders data: {str(e)}")
        raise


def write_orders_data(df: DataFrame, bucket_name: str) -> None:
    """
    Write orders data to bronze Delta table.

    Args:
        df: Transformed orders data DataFrame
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'orders')}"

    logger.info(f"Writing orders data to {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="append",  # Use append mode for incremental loads
            partition_by=["date"],  # Partition by date
            bucket_name=bucket_name,
        )

        logger.info(f"Successfully wrote orders data to {table_path}")
    except Exception as e:
        logger.error(f"Error writing orders data: {str(e)}")
        raise


def register_orders_table(spark: SparkSession, bucket_name: str) -> None:
    """
    Register orders table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'orders')}"

    logger.info("Registering orders table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="orders",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Bronze layer orders table",
            layer="bronze",
            bucket_name=bucket_name,
            columns_description={
                "order_num": "Order number",
                "order_id": "Unique identifier for the order",
                "user_id": "Identifier for the user",
                "order_timestamp": "Timestamp when the order was placed",
                "total_amount": "Total amount of the order",
                "date": "Date of the order",
                "source_file": "Source file path",
                "ingestion_timestamp": "Timestamp when the data was ingested",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (bronze)",
            },
        )

        if success:
            logger.info("Successfully registered orders table in Glue Data Catalog")
        else:
            logger.error("Failed to register orders table in Glue Data Catalog")
    except Exception as e:
        logger.error(f"Error registering orders table: {str(e)}")
        raise


def main(date: str, bucket_name: str, region: str, days_back: int = 1) -> int:
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
    logger.info(f"Starting Bronze Orders ETL for date: {date}")

    try:
        # Get the date range to process
        dates = get_date_range(date, days_back)
        logger.info(f"Processing dates: {dates}")

        # Create Spark session
        spark = create_spark_session(
            app_name=f"bronze_orders_etl_{date}", enable_hive_support=True
        )

        # Read orders data
        orders_df = read_orders_data(spark, bucket_name, dates)

        # Transform orders data
        transformed_df = transform_orders_data(orders_df)

        # Write orders data
        write_orders_data(transformed_df, bucket_name)

        # Register orders table in Glue Data Catalog
        register_orders_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(f"Successfully completed Bronze Orders ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Bronze Orders ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    sys.exit(exit_code)

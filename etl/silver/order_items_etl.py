#!/usr/bin/env python
"""
Silver Order Items ETL

This script transforms order item data from the bronze layer to the silver layer.
It performs the following operations:
1. Reads order item data from the bronze Delta table
2. Cleanses and validates the data
3. Adds derived columns (days_since_prior_order_bucket)
4. Writes to Silver Delta table
5. Updates Glue Data Catalog

Usage:
    python -m etl.silver.order_items_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    when,
    dayofweek,
    hour,
    to_date,
    date_format,
    coalesce,
    expr,
)

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.common.spark_session import (
    create_spark_session,
    read_delta_table,
    write_delta_table,
)
from etl.common.glue_catalog import register_delta_table
from etl.common.etl_utils import (
    add_metadata_columns,
    validate_schema,
)
from etl.common.schemas import BRONZE_ORDER_ITEMS_SCHEMA, SILVER_ORDER_ITEMS_SCHEMA
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
        description="Transform order item data from bronze to silver layer"
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


def read_bronze_order_items(
    spark: SparkSession, bucket_name: str, dates: List[str]
) -> DataFrame:
    """
    Read order items data from bronze layer.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        dates: List of dates to process (YYYY-MM-DD)

    Returns:
        DataFrame: Order items data from bronze layer
    """
    # Construct the bronze table path
    bronze_table_path = f"{get_prefix('bronze', 'order_items')}"

    logger.info(f"Reading order items data from bronze layer: {bronze_table_path}")

    try:
        # Read the bronze Delta table
        df = read_delta_table(
            spark=spark, table_path=bronze_table_path, bucket_name=bucket_name
        )

        # Filter by date if dates are provided
        if dates:
            date_filter = df.date.isin(dates)
            df = df.filter(date_filter)
            logger.info(f"Filtered order items data for dates: {dates}")

        logger.info(f"Successfully read {df.count()} order items from bronze layer")
        return df
    except Exception as e:
        logger.error(f"Error reading order items data from bronze layer: {str(e)}")
        raise


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns to order items data.

    Args:
        df: Order items DataFrame

    Returns:
        DataFrame: DataFrame with derived columns
    """
    logger.info("Adding derived columns to order items data")

    try:
        # Add days_since_prior_order_bucket column
        # Buckets: 0-1 days, 2-7 days, 8-14 days, 15-30 days, 30+ days, null (first order)
        df_with_bucket = df.withColumn(
            "days_since_prior_order_bucket",
            when(col("days_since_prior_order").isNull(), "first_order")
            .when(col("days_since_prior_order") <= 1, "0-1_days")
            .when(col("days_since_prior_order") <= 7, "2-7_days")
            .when(col("days_since_prior_order") <= 14, "8-14_days")
            .when(col("days_since_prior_order") <= 30, "15-30_days")
            .otherwise("30+_days"),
        )

        return df_with_bucket
    except Exception as e:
        logger.error(f"Error adding derived columns: {str(e)}")
        raise


def transform_order_items_data(df: DataFrame) -> DataFrame:
    """
    Transform order items data for the silver layer.

    Args:
        df: Order items data DataFrame from bronze layer

    Returns:
        DataFrame: Transformed order items data
    """
    logger.info("Transforming order items data for silver layer")

    try:
        # Validate input schema
        success, error_msg, validated_df = validate_schema(
            df, BRONZE_ORDER_ITEMS_SCHEMA, strict=False
        )

        if not success:
            logger.error(f"Schema validation failed for input data: {error_msg}")
            raise ValueError(f"Schema validation failed for input data: {error_msg}")

        # Add derived columns
        df_with_derived = add_derived_columns(validated_df)

        # Add metadata columns
        result_df = add_metadata_columns(
            df_with_derived,
            layer="silver",
            source_file_column=False,
            ingestion_timestamp_column=False,
            processing_timestamp_column=True,
            layer_column=True,
        )

        # Add bronze_source_file column
        result_df = result_df.withColumn("bronze_source_file", col("source_file"))

        # Select only the columns in the silver schema
        result_df = result_df.select(
            "id",
            "order_id",
            "user_id",
            "days_since_prior_order",
            "days_since_prior_order_bucket",
            "product_id",
            "add_to_cart_order",
            "reordered",
            "order_timestamp",
            "date",
            "bronze_source_file",
            "processing_timestamp",
            "layer",
        )

        # Validate output schema
        success, error_msg, validated_result_df = validate_schema(
            result_df, SILVER_ORDER_ITEMS_SCHEMA, strict=True
        )

        if not success:
            logger.error(f"Schema validation failed for output data: {error_msg}")
            raise ValueError(f"Schema validation failed for output data: {error_msg}")

        logger.info("Successfully transformed order items data for silver layer")
        return validated_result_df
    except Exception as e:
        logger.error(f"Error transforming order items data: {str(e)}")
        raise


def write_silver_order_items(df: DataFrame, bucket_name: str) -> None:
    """
    Write order items data to silver Delta table.

    Args:
        df: Transformed order items data DataFrame
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'order_items')}"

    logger.info(f"Writing order items data to silver layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="append",  # Use append mode for incremental loads
            partition_by=["date"],  # Partition by date
            z_order_by=["order_id", "product_id"],  # Z-order by order_id and product_id
            bucket_name=bucket_name,
        )

        logger.info(
            f"Successfully wrote order items data to silver layer: {table_path}"
        )
    except Exception as e:
        logger.error(f"Error writing order items data to silver layer: {str(e)}")
        raise


def register_silver_order_items_table(spark: SparkSession, bucket_name: str) -> None:
    """
    Register silver order items table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'order_items')}"

    logger.info("Registering silver order items table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="order_items",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Silver layer order items table",
            layer="silver",
            bucket_name=bucket_name,
            columns_description={
                "id": "Unique identifier for the order item",
                "order_id": "Identifier for the order",
                "user_id": "Identifier for the user",
                "days_since_prior_order": "Days since prior order",
                "days_since_prior_order_bucket": "Bucketed days since prior order",
                "product_id": "Identifier for the product",
                "add_to_cart_order": "Order in which the item was added to cart",
                "reordered": "Whether the item was reordered",
                "order_timestamp": "Timestamp when the order was placed",
                "date": "Date of the order",
                "bronze_source_file": "Source file in the bronze layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (silver)",
            },
        )

        if success:
            logger.info(
                "Successfully registered silver order items table in Glue Data Catalog"
            )
        else:
            logger.error(
                "Failed to register silver order items table in Glue Data Catalog"
            )
    except Exception as e:
        logger.error(f"Error registering silver order items table: {str(e)}")
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
    logger.info(f"Starting Silver Order Items ETL for date: {date}")

    try:
        # Get the date range to process
        dates = get_date_range(date, days_back)
        logger.info(f"Processing dates: {dates}")

        # Create Spark session
        spark = create_spark_session(
            app_name=f"silver_order_items_etl_{date}", enable_hive_support=True
        )

        # Read order items data from bronze layer
        bronze_df = read_bronze_order_items(spark, bucket_name, dates)

        # Transform order items data
        silver_df = transform_order_items_data(bronze_df)

        # Write order items data to silver layer
        write_silver_order_items(silver_df, bucket_name)

        # Register silver order items table in Glue Data Catalog
        register_silver_order_items_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(f"Successfully completed Silver Order Items ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Silver Order Items ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    sys.exit(exit_code)

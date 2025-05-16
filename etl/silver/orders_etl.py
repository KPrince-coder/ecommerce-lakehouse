#!/usr/bin/env python
"""
Silver Orders ETL

This script transforms order data from the bronze layer to the silver layer.
It performs the following operations:
1. Reads order data from the bronze Delta table
2. Cleanses and validates the data
3. Adds derived columns (day_of_week, hour_of_day)
4. Writes to Silver Delta table
5. Updates Glue Data Catalog

Usage:
    python -m etl.silver.orders_etl [--date YYYY-MM-DD]

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
from etl.common.schemas import BRONZE_ORDERS_SCHEMA, SILVER_ORDERS_SCHEMA
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
        description="Transform order data from bronze to silver layer"
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


def read_bronze_orders(
    spark: SparkSession, bucket_name: str, dates: List[str]
) -> DataFrame:
    """
    Read orders data from bronze layer.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        dates: List of dates to process (YYYY-MM-DD)

    Returns:
        DataFrame: Orders data from bronze layer
    """
    # Construct the bronze table path
    bronze_table_path = f"{get_prefix('bronze', 'orders')}"

    logger.info(f"Reading orders data from bronze layer: {bronze_table_path}")

    try:
        # Read the bronze Delta table
        df = read_delta_table(
            spark=spark, table_path=bronze_table_path, bucket_name=bucket_name
        )

        # Filter by date if dates are provided
        if dates:
            date_filter = df.date.isin(dates)
            df = df.filter(date_filter)
            logger.info(f"Filtered orders data for dates: {dates}")

        logger.info(f"Successfully read {df.count()} orders from bronze layer")
        return df
    except Exception as e:
        logger.error(f"Error reading orders data from bronze layer: {str(e)}")
        raise


def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Add derived columns to orders data.

    Args:
        df: Orders DataFrame

    Returns:
        DataFrame: DataFrame with derived columns
    """
    logger.info("Adding derived columns to orders data")

    try:
        # Add day_of_week column (1 = Sunday, 2 = Monday, ..., 7 = Saturday)
        df_with_day = df.withColumn("day_of_week", dayofweek(col("order_timestamp")))

        # Add hour_of_day column (0-23)
        df_with_hour = df_with_day.withColumn(
            "hour_of_day", hour(col("order_timestamp"))
        )

        return df_with_hour
    except Exception as e:
        logger.error(f"Error adding derived columns: {str(e)}")
        raise


def transform_orders_data(df: DataFrame) -> DataFrame:
    """
    Transform orders data for the silver layer.

    Args:
        df: Orders data DataFrame from bronze layer

    Returns:
        DataFrame: Transformed orders data
    """
    logger.info("Transforming orders data for silver layer")

    try:
        # Validate input schema
        success, error_msg, validated_df = validate_schema(
            df, BRONZE_ORDERS_SCHEMA, strict=False
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
            "order_num",
            "order_id",
            "user_id",
            "order_timestamp",
            "total_amount",
            "date",
            "day_of_week",
            "hour_of_day",
            "bronze_source_file",
            "processing_timestamp",
            "layer",
        )

        # Validate output schema
        success, error_msg, validated_result_df = validate_schema(
            result_df, SILVER_ORDERS_SCHEMA, strict=True
        )

        if not success:
            logger.error(f"Schema validation failed for output data: {error_msg}")
            raise ValueError(f"Schema validation failed for output data: {error_msg}")

        logger.info("Successfully transformed orders data for silver layer")
        return validated_result_df
    except Exception as e:
        logger.error(f"Error transforming orders data: {str(e)}")
        raise


def write_silver_orders(df: DataFrame, bucket_name: str) -> None:
    """
    Write orders data to silver Delta table.

    Args:
        df: Transformed orders data DataFrame
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'orders')}"

    logger.info(f"Writing orders data to silver layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="append",  # Use append mode for incremental loads
            partition_by=["date"],  # Partition by date
            z_order_by="order_id",  # Z-order by order_id for better query performance
            bucket_name=bucket_name,
        )

        logger.info(f"Successfully wrote orders data to silver layer: {table_path}")
    except Exception as e:
        logger.error(f"Error writing orders data to silver layer: {str(e)}")
        raise


def register_silver_orders_table(spark: SparkSession, bucket_name: str) -> None:
    """
    Register silver orders table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'orders')}"

    logger.info("Registering silver orders table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="orders",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Silver layer orders table",
            layer="silver",
            bucket_name=bucket_name,
            columns_description={
                "order_num": "Order number",
                "order_id": "Unique identifier for the order",
                "user_id": "Identifier for the user",
                "order_timestamp": "Timestamp when the order was placed",
                "total_amount": "Total amount of the order",
                "date": "Date of the order",
                "day_of_week": "Day of the week (1 = Sunday, 2 = Monday, ..., 7 = Saturday)",
                "hour_of_day": "Hour of the day (0-23)",
                "bronze_source_file": "Source file in the bronze layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (silver)",
            },
        )

        if success:
            logger.info(
                "Successfully registered silver orders table in Glue Data Catalog"
            )
        else:
            logger.error("Failed to register silver orders table in Glue Data Catalog")
    except Exception as e:
        logger.error(f"Error registering silver orders table: {str(e)}")
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
    logger.info(f"Starting Silver Orders ETL for date: {date}")

    try:
        # Get the date range to process
        dates = get_date_range(date, days_back)
        logger.info(f"Processing dates: {dates}")

        # Create Spark session
        spark = create_spark_session(
            app_name=f"silver_orders_etl_{date}", enable_hive_support=True
        )

        # Read orders data from bronze layer
        bronze_df = read_bronze_orders(spark, bucket_name, dates)

        # Transform orders data
        silver_df = transform_orders_data(bronze_df)

        # Write orders data to silver layer
        write_silver_orders(silver_df, bucket_name)

        # Register silver orders table in Glue Data Catalog
        register_silver_orders_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(f"Successfully completed Silver Orders ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Silver Orders ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    sys.exit(exit_code)

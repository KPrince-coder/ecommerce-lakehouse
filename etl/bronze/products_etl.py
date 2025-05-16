#!/usr/bin/env python
"""
Bronze Products ETL

This script ingests product data from CSV files in the raw zone and writes it to a Delta table
in the bronze layer. It performs the following operations:
1. Reads products.csv from S3 raw zone
2. Applies schema to raw CSV data
3. Adds metadata columns (source_file, ingestion_timestamp)
4. Writes to Bronze Delta table
5. Updates Glue Data Catalog

Usage:
    python -m etl.bronze.products_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.common.spark_session import create_spark_session, write_delta_table
from etl.common.glue_catalog import register_delta_table
from etl.common.etl_utils import (
    add_metadata_columns,
    validate_schema,
)
from etl.common.schemas import RAW_PRODUCTS_SCHEMA, BRONZE_PRODUCTS_SCHEMA
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
        description="Ingest product data from raw zone to bronze layer"
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

    return parser.parse_args()


def read_products_data(spark: SparkSession, bucket_name: str, date: str) -> DataFrame:
    """
    Read products data from S3 raw zone.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        DataFrame: Products data
    """
    # Construct the S3 path
    products_path = f"s3a://{bucket_name}/{get_prefix('raw', 'products')}products.csv"

    logger.info(f"Reading products data from {products_path}")

    try:
        # Read the CSV file
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .schema(RAW_PRODUCTS_SCHEMA)
            .load(products_path)
        )

        logger.info(f"Successfully read {df.count()} products")
        return df
    except Exception as e:
        logger.error(f"Error reading products data: {str(e)}")
        raise


def transform_products_data(df: DataFrame) -> DataFrame:
    """
    Transform products data for the bronze layer.

    Args:
        df: Products data DataFrame

    Returns:
        DataFrame: Transformed products data
    """
    logger.info("Transforming products data")

    try:
        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            df, BRONZE_PRODUCTS_SCHEMA, strict=True
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

        logger.info("Successfully transformed products data")
        return result_df
    except Exception as e:
        logger.error(f"Error transforming products data: {str(e)}")
        raise


def write_products_data(df: DataFrame, bucket_name: str, date: str) -> None:
    """
    Write products data to bronze Delta table.

    Args:
        df: Transformed products data DataFrame
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'products')}"

    logger.info(f"Writing products data to {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="overwrite",
            partition_by=None,  # No partitioning for products
            bucket_name=bucket_name,
        )

        logger.info(f"Successfully wrote products data to {table_path}")
    except Exception as e:
        logger.error(f"Error writing products data: {str(e)}")
        raise


def register_products_table(spark: SparkSession, bucket_name: str) -> None:
    """
    Register products table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the Delta table path
    table_path = f"{get_prefix('bronze', 'products')}"

    logger.info("Registering products table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="products",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Bronze layer products table",
            layer="bronze",
            bucket_name=bucket_name,
            columns_description={
                "product_id": "Unique identifier for the product",
                "department_id": "Identifier for the department",
                "department": "Name of the department",
                "product_name": "Name of the product",
                "source_file": "Source file path",
                "ingestion_timestamp": "Timestamp when the data was ingested",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (bronze)",
            },
        )

        if success:
            logger.info("Successfully registered products table in Glue Data Catalog")
        else:
            logger.error("Failed to register products table in Glue Data Catalog")
    except Exception as e:
        logger.error(f"Error registering products table: {str(e)}")
        raise


def main(date: str, bucket_name: str, region: str) -> int:
    """
    Main function to run the ETL process.

    Args:
        date: Processing date (YYYY-MM-DD)
        bucket_name: S3 bucket name
        region: AWS region

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Starting Bronze Products ETL for date: {date}")

    try:
        # Create Spark session
        spark = create_spark_session(
            app_name=f"bronze_products_etl_{date}", enable_hive_support=True
        )

        # Read products data
        products_df = read_products_data(spark, bucket_name, date)

        # Transform products data
        transformed_df = transform_products_data(products_df)

        # Write products data
        write_products_data(transformed_df, bucket_name, date)

        # Register products table in Glue Data Catalog
        register_products_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(f"Successfully completed Bronze Products ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Bronze Products ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region)
    sys.exit(exit_code)

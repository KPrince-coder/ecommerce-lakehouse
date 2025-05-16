#!/usr/bin/env python
"""
Silver Products ETL

This script transforms product data from the bronze layer to the silver layer.
It performs the following operations:
1. Reads product data from the bronze Delta table
2. Cleanses and standardizes the data
3. Adds derived columns
4. Writes to Silver Delta table
5. Updates Glue Data Catalog

Usage:
    python -m etl.silver.products_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, when, lower, trim, regexp_replace

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
)
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
        description="Transform product data from bronze to silver layer"
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


def read_bronze_products(spark: SparkSession, bucket_name: str, date: str) -> DataFrame:
    """
    Read products data from bronze layer.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        DataFrame: Products data from bronze layer
    """
    # Construct the bronze table path
    bronze_table_path = f"{get_prefix('bronze', 'products')}"

    logger.info(f"Reading products data from bronze layer: {bronze_table_path}")

    try:
        # Read the bronze Delta table
        df = read_delta_table(
            spark=spark, table_path=bronze_table_path, bucket_name=bucket_name
        )

        logger.info(f"Successfully read {df.count()} products from bronze layer")
        return df
    except Exception as e:
        logger.error(f"Error reading products data from bronze layer: {str(e)}")
        raise


def standardize_department_names(df: DataFrame) -> DataFrame:
    """
    Standardize department names.

    Args:
        df: Products DataFrame

    Returns:
        DataFrame: DataFrame with standardized department names
    """
    logger.info("Standardizing department names")

    # Define department name mapping
    department_mapping = {
        "books": "Books",
        "book": "Books",
        "sports": "Sports",
        "sport": "Sports",
        "toys": "Toys",
        "toy": "Toys",
        "home": "Home",
        "household": "Home",
        "clothing": "Clothing",
        "clothes": "Clothing",
        "apparel": "Clothing",
        "electronics": "Electronics",
        "electronic": "Electronics",
        "tech": "Electronics",
        "food": "Food",
        "grocery": "Food",
        "groceries": "Food",
    }

    # Create a mapping expression for standardizing department names
    mapping_expr = None
    for key, value in department_mapping.items():
        if mapping_expr is None:
            mapping_expr = when(
                lower(trim(col("department"))).like(f"%{key}%"), lit(value)
            )
        else:
            mapping_expr = mapping_expr.when(
                lower(trim(col("department"))).like(f"%{key}%"), lit(value)
            )

    # Add the default case
    mapping_expr = mapping_expr.otherwise(col("department"))

    # Apply the standardization
    result_df = df.withColumn("department_standardized", mapping_expr)

    return result_df


def transform_products_data(df: DataFrame) -> DataFrame:
    """
    Transform products data for the silver layer.

    Args:
        df: Products data DataFrame from bronze layer

    Returns:
        DataFrame: Transformed products data
    """
    logger.info("Transforming products data for silver layer")

    try:
        # Standardize department names
        df_with_std_dept = standardize_department_names(df)

        # Clean product names (remove extra spaces, special characters)
        df_cleaned = df_with_std_dept.withColumn(
            "product_name", regexp_replace(trim(col("product_name")), r"[^\w\s]", "")
        )

        # Add is_active column (all products are active by default)
        df_with_active = df_cleaned.withColumn("is_active", lit(True))

        # Add metadata columns
        result_df = add_metadata_columns(
            df_with_active,
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
            "product_id",
            "department_id",
            "department",
            "department_standardized",
            "product_name",
            "is_active",
            "bronze_source_file",
            "processing_timestamp",
            "layer",
        )

        logger.info("Successfully transformed products data for silver layer")
        return result_df
    except Exception as e:
        logger.error(f"Error transforming products data: {str(e)}")
        raise


def write_silver_products(df: DataFrame, bucket_name: str, date: str) -> None:
    """
    Write products data to silver Delta table.

    Args:
        df: Transformed products data DataFrame
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'products')}"

    logger.info(f"Writing products data to silver layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="overwrite",  # Use overwrite mode for silver products
            partition_by=None,  # No partitioning for products
            z_order_by="product_id",  # Z-order by product_id for better query performance
            bucket_name=bucket_name,
        )

        logger.info(f"Successfully wrote products data to silver layer: {table_path}")
    except Exception as e:
        logger.error(f"Error writing products data to silver layer: {str(e)}")
        raise


def register_silver_products_table(spark: SparkSession, bucket_name: str) -> None:
    """
    Register silver products table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the silver table path
    table_path = f"{get_prefix('silver', 'products')}"

    logger.info("Registering silver products table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="products",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Silver layer products table",
            layer="silver",
            bucket_name=bucket_name,
            columns_description={
                "product_id": "Unique identifier for the product",
                "department_id": "Identifier for the department",
                "department": "Original name of the department",
                "department_standardized": "Standardized name of the department",
                "product_name": "Cleaned name of the product",
                "is_active": "Whether the product is active",
                "bronze_source_file": "Source file in the bronze layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (silver)",
            },
        )

        if success:
            logger.info(
                "Successfully registered silver products table in Glue Data Catalog"
            )
        else:
            logger.error(
                "Failed to register silver products table in Glue Data Catalog"
            )
    except Exception as e:
        logger.error(f"Error registering silver products table: {str(e)}")
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
    logger.info(f"Starting Silver Products ETL for date: {date}")

    try:
        # Create Spark session
        spark = create_spark_session(
            app_name=f"silver_products_etl_{date}", enable_hive_support=True
        )

        # Read products data from bronze layer
        bronze_df = read_bronze_products(spark, bucket_name, date)

        # Transform products data
        silver_df = transform_products_data(bronze_df)

        # Write products data to silver layer
        write_silver_products(silver_df, bucket_name, date)

        # Register silver products table in Glue Data Catalog
        register_silver_products_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(f"Successfully completed Silver Products ETL for date: {date}")
        return 0
    except Exception as e:
        logger.error(f"Error in Silver Products ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region)
    sys.exit(exit_code)

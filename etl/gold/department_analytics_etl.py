#!/usr/bin/env python
"""
Gold Department Analytics ETL

This script creates a department analytics table in the gold layer.
It performs the following operations:
1. Reads data from silver layer tables (products, order_items, orders)
2. Aggregates and transforms the data to calculate department-level metrics
3. Writes to Gold Delta table
4. Updates Glue Data Catalog

Usage:
    python -m etl.gold.department_analytics_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, sum, avg, round, countDistinct

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
from etl.common.schemas import GOLD_DEPARTMENT_ANALYTICS_SCHEMA
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
        description="Create department analytics in gold layer"
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
        "--lookback-days",
        type=int,
        default=30,
        help="Number of days to look back for analytics (default: 30)",
    )

    return parser.parse_args()


def read_silver_data(
    spark: SparkSession, bucket_name: str, date: str, lookback_days: int
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Read data from silver layer tables.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)
        lookback_days: Number of days to look back for analytics

    Returns:
        Tuple[DataFrame, DataFrame, DataFrame]: Products, order items, and orders DataFrames
    """
    logger.info("Reading data from silver layer tables")

    try:
        # Read silver products
        products_path = f"{get_prefix('silver', 'products')}"
        products_df = read_delta_table(
            spark=spark, table_path=products_path, bucket_name=bucket_name
        )
        logger.info(f"Read {products_df.count()} products from silver layer")

        # Calculate date range
        end_date = datetime.strptime(date, "%Y-%m-%d")
        start_date = end_date - timedelta(days=lookback_days)
        start_date_str = start_date.strftime("%Y-%m-%d")

        # Read silver order items with date filter
        order_items_path = f"{get_prefix('silver', 'order_items')}"
        order_items_df = read_delta_table(
            spark=spark, table_path=order_items_path, bucket_name=bucket_name
        ).filter((col("date") >= start_date_str) & (col("date") <= date))
        logger.info(f"Read {order_items_df.count()} order items from silver layer")

        # Read silver orders with date filter
        orders_path = f"{get_prefix('silver', 'orders')}"
        orders_df = read_delta_table(
            spark=spark, table_path=orders_path, bucket_name=bucket_name
        ).filter((col("date") >= start_date_str) & (col("date") <= date))
        logger.info(f"Read {orders_df.count()} orders from silver layer")

        return products_df, order_items_df, orders_df
    except Exception as e:
        logger.error(f"Error reading data from silver layer: {str(e)}")
        raise


def calculate_department_analytics(
    products_df: DataFrame, order_items_df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """
    Calculate department analytics metrics.

    Args:
        products_df: Products DataFrame
        order_items_df: Order items DataFrame
        orders_df: Orders DataFrame

    Returns:
        DataFrame: Department analytics metrics
    """
    logger.info("Calculating department analytics metrics")

    try:
        # Join products with order items
        products_with_orders = order_items_df.join(
            products_df.select("product_id", "department_id", "department"),
            on="product_id",
            how="inner",
        )

        # Join with orders to get total amount
        department_data = products_with_orders.join(
            orders_df.select("order_id", "total_amount", "user_id"),
            on="order_id",
            how="inner",
        )

        # Calculate metrics by department
        department_metrics = department_data.groupBy("department_id", "department").agg(
            # Total sales amount
            sum("total_amount").alias("total_sales"),
            # Product count (distinct products sold)
            countDistinct("product_id").alias("product_count"),
            # Customer count (distinct users)
            countDistinct("user_id").alias("customer_count"),
            # Average order value
            avg("total_amount").alias("avg_order_value"),
        )

        # Round numeric values
        department_metrics = department_metrics.withColumn(
            "total_sales", round(col("total_sales"), 2)
        ).withColumn("avg_order_value", round(col("avg_order_value"), 2))

        # Add metadata columns
        result_df = add_metadata_columns(
            department_metrics,
            layer="gold",
            source_file_column=False,
            ingestion_timestamp_column=False,
            processing_timestamp_column=True,
            layer_column=True,
        )

        # Add silver_source_tables column
        result_df = result_df.withColumn(
            "silver_source_tables", lit("products,orders,order_items")
        )

        # Select and order columns according to the schema
        result_df = result_df.select(
            "department_id",
            "department",
            "total_sales",
            "product_count",
            "customer_count",
            "avg_order_value",
            "silver_source_tables",
            "processing_timestamp",
            "layer",
        )

        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            result_df, GOLD_DEPARTMENT_ANALYTICS_SCHEMA, strict=True
        )

        if not success:
            logger.error(f"Schema validation failed: {error_msg}")
            raise ValueError(f"Schema validation failed: {error_msg}")

        result_df = validated_df

        logger.info("Successfully calculated department analytics metrics")
        return result_df
    except Exception as e:
        logger.error(f"Error calculating department analytics metrics: {str(e)}")
        raise


def write_gold_department_analytics(df: DataFrame, bucket_name: str, date: str) -> None:
    """
    Write department analytics data to gold Delta table.

    Args:
        df: Department analytics DataFrame
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'department_analytics')}"

    logger.info(f"Writing department analytics data to gold layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="overwrite",  # Use overwrite mode for gold department analytics
            partition_by=None,  # No partitioning needed as the table is small
            z_order_by="department_id",  # Z-order by department_id
            bucket_name=bucket_name,
        )

        logger.info(
            f"Successfully wrote department analytics data to gold layer: {table_path}"
        )
    except Exception as e:
        logger.error(f"Error writing department analytics data to gold layer: {str(e)}")
        raise


def register_gold_department_analytics_table(
    spark: SparkSession, bucket_name: str
) -> None:
    """
    Register gold department analytics table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'department_analytics')}"

    logger.info("Registering gold department analytics table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="department_analytics",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Gold layer department analytics table",
            layer="gold",
            bucket_name=bucket_name,
            columns_description={
                "department_id": "Identifier for the department",
                "department": "Name of the department",
                "total_sales": "Total sales amount",
                "product_count": "Number of distinct products sold",
                "customer_count": "Number of distinct customers",
                "avg_order_value": "Average order value",
                "silver_source_tables": "Source tables in the silver layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (gold)",
            },
        )

        if success:
            logger.info(
                "Successfully registered gold department analytics table in Glue Data Catalog"
            )
        else:
            logger.error(
                "Failed to register gold department analytics table in Glue Data Catalog"
            )
    except Exception as e:
        logger.error(f"Error registering gold department analytics table: {str(e)}")
        raise


def main(date: str, bucket_name: str, region: str, lookback_days: int = 30) -> int:
    """
    Main function to run the ETL process.

    Args:
        date: Processing date (YYYY-MM-DD)
        bucket_name: S3 bucket name
        region: AWS region
        lookback_days: Number of days to look back for analytics

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Starting Gold Department Analytics ETL for date: {date}")

    try:
        # Create Spark session
        spark = create_spark_session(
            app_name=f"gold_department_analytics_etl_{date}", enable_hive_support=True
        )

        # Read data from silver layer
        products_df, order_items_df, orders_df = read_silver_data(
            spark, bucket_name, date, lookback_days
        )

        # Calculate department analytics metrics
        department_analytics_df = calculate_department_analytics(
            products_df, order_items_df, orders_df
        )

        # Write department analytics data to gold layer
        write_gold_department_analytics(department_analytics_df, bucket_name, date)

        # Register gold department analytics table in Glue Data Catalog
        register_gold_department_analytics_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(
            f"Successfully completed Gold Department Analytics ETL for date: {date}"
        )
        return 0
    except Exception as e:
        logger.error(f"Error in Gold Department Analytics ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.lookback_days)
    sys.exit(exit_code)

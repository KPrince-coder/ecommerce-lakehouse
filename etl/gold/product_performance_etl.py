#!/usr/bin/env python
"""
Gold Product Performance ETL

This script creates a product performance analytics table in the gold layer.
It performs the following operations:
1. Reads data from silver layer tables (products, order_items, orders)
2. Aggregates and transforms the data to calculate product performance metrics
3. Writes to Gold Delta table
4. Updates Glue Data Catalog

Usage:
    python -m etl.gold.product_performance_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, count, sum, avg, round, when
from pyspark.sql.window import Window
import pyspark.sql.functions as F

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
from etl.common.schemas import GOLD_PRODUCT_PERFORMANCE_SCHEMA
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
        description="Create product performance analytics in gold layer"
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


def calculate_product_performance(
    products_df: DataFrame, order_items_df: DataFrame, orders_df: DataFrame
) -> DataFrame:
    """
    Calculate product performance metrics.

    Args:
        products_df: Products DataFrame
        order_items_df: Order items DataFrame
        orders_df: Orders DataFrame

    Returns:
        DataFrame: Product performance metrics
    """
    logger.info("Calculating product performance metrics")

    try:
        # Join order items with orders to get total amount
        order_items_with_amount = order_items_df.join(
            orders_df.select("order_id", "total_amount"), on="order_id", how="inner"
        )

        # Calculate metrics by product
        product_metrics = order_items_with_amount.groupBy("product_id").agg(
            # Total quantity sold
            count("id").alias("total_quantity"),
            # Total sales amount (estimated by dividing order total by number of items)
            sum(
                orders_df.select("total_amount")
                / F.size(F.collect_list("id").over(Window.partitionBy("order_id")))
            ).alias("total_sales"),
            # Reorder rate
            (sum(when(col("reordered") == True, 1).otherwise(0)) / count("id")).alias(
                "reorder_rate"
            ),
            # Average days between orders
            avg("days_since_prior_order").alias("avg_days_between_orders"),
        )

        # Round numeric values
        product_metrics = (
            product_metrics.withColumn("total_sales", round(col("total_sales"), 2))
            .withColumn("reorder_rate", round(col("reorder_rate"), 4))
            .withColumn(
                "avg_days_between_orders", round(col("avg_days_between_orders"), 1)
            )
        )

        # Join with products to get product details
        result_df = product_metrics.join(
            products_df.select(
                "product_id", "department_id", "department", "product_name"
            ),
            on="product_id",
            how="inner",
        )

        # Add metadata columns
        result_df = add_metadata_columns(
            result_df,
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
            "product_id",
            "department_id",
            "department",
            "product_name",
            "total_quantity",
            "total_sales",
            "reorder_rate",
            "avg_days_between_orders",
            "silver_source_tables",
            "processing_timestamp",
            "layer",
        )

        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            result_df, GOLD_PRODUCT_PERFORMANCE_SCHEMA, strict=True
        )

        if not success:
            logger.error(f"Schema validation failed: {error_msg}")
            raise ValueError(f"Schema validation failed: {error_msg}")

        result_df = validated_df

        logger.info("Successfully calculated product performance metrics")
        return result_df
    except Exception as e:
        logger.error(f"Error calculating product performance metrics: {str(e)}")
        raise


def write_gold_product_performance(df: DataFrame, bucket_name: str, date: str) -> None:
    """
    Write product performance data to gold Delta table.

    Args:
        df: Product performance DataFrame
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'product_performance')}"

    logger.info(f"Writing product performance data to gold layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="overwrite",  # Use overwrite mode for gold product performance
            partition_by="department_id",  # Partition by department_id
            z_order_by="product_id",  # Z-order by product_id
            bucket_name=bucket_name,
        )

        logger.info(
            f"Successfully wrote product performance data to gold layer: {table_path}"
        )
    except Exception as e:
        logger.error(f"Error writing product performance data to gold layer: {str(e)}")
        raise


def register_gold_product_performance_table(
    spark: SparkSession, bucket_name: str
) -> None:
    """
    Register gold product performance table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'product_performance')}"

    logger.info("Registering gold product performance table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="product_performance",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Gold layer product performance table",
            layer="gold",
            bucket_name=bucket_name,
            columns_description={
                "product_id": "Unique identifier for the product",
                "department_id": "Identifier for the department",
                "department": "Name of the department",
                "product_name": "Name of the product",
                "total_quantity": "Total quantity sold",
                "total_sales": "Total sales amount",
                "reorder_rate": "Rate at which the product is reordered",
                "avg_days_between_orders": "Average days between orders",
                "silver_source_tables": "Source tables in the silver layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (gold)",
            },
        )

        if success:
            logger.info(
                "Successfully registered gold product performance table in Glue Data Catalog"
            )
        else:
            logger.error(
                "Failed to register gold product performance table in Glue Data Catalog"
            )
    except Exception as e:
        logger.error(f"Error registering gold product performance table: {str(e)}")
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
    logger.info(f"Starting Gold Product Performance ETL for date: {date}")

    try:
        # Create Spark session
        spark = create_spark_session(
            app_name=f"gold_product_performance_etl_{date}", enable_hive_support=True
        )

        # Read data from silver layer
        products_df, order_items_df, orders_df = read_silver_data(
            spark, bucket_name, date, lookback_days
        )

        # Calculate product performance metrics
        product_performance_df = calculate_product_performance(
            products_df, order_items_df, orders_df
        )

        # Write product performance data to gold layer
        write_gold_product_performance(product_performance_df, bucket_name, date)

        # Register gold product performance table in Glue Data Catalog
        register_gold_product_performance_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(
            f"Successfully completed Gold Product Performance ETL for date: {date}"
        )
        return 0
    except Exception as e:
        logger.error(f"Error in Gold Product Performance ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.lookback_days)
    sys.exit(exit_code)

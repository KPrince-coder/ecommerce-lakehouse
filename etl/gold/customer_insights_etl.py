#!/usr/bin/env python
"""
Gold Customer Insights ETL

This script creates a customer insights analytics table in the gold layer.
It performs the following operations:
1. Reads data from silver layer tables (orders, order_items)
2. Aggregates and transforms the data to calculate customer behavior metrics
3. Segments customers based on RFM (Recency, Frequency, Monetary) analysis
4. Writes to Gold Delta table
5. Updates Glue Data Catalog

Usage:
    python -m etl.gold.customer_insights_etl [--date YYYY-MM-DD]

The date parameter is optional and defaults to the current date.
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    count,
    sum,
    round,
    when,
    datediff,
    max,
    ntile,
)
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
from etl.common.schemas import GOLD_CUSTOMER_INSIGHTS_SCHEMA
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
        description="Create customer insights analytics in gold layer"
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
        default=90,
        help="Number of days to look back for analytics (default: 90)",
    )

    return parser.parse_args()


def read_silver_data(
    spark: SparkSession, bucket_name: str, date: str, lookback_days: int
) -> Tuple[DataFrame, DataFrame]:
    """
    Read data from silver layer tables.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)
        lookback_days: Number of days to look back for analytics

    Returns:
        Tuple[DataFrame, DataFrame]: Orders and order items DataFrames
    """
    logger.info("Reading data from silver layer tables")

    try:
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

        return orders_df, order_items_df
    except Exception as e:
        logger.error(f"Error reading data from silver layer: {str(e)}")
        raise


def calculate_customer_insights(
    orders_df: DataFrame, order_items_df: DataFrame, analysis_date: str
) -> DataFrame:
    """
    Calculate customer insights metrics and segment customers.

    Args:
        orders_df: Orders DataFrame
        order_items_df: Order items DataFrame
        analysis_date: Date for the analysis (YYYY-MM-DD)

    Returns:
        DataFrame: Customer insights metrics
    """
    logger.info("Calculating customer insights metrics")

    try:
        # Convert analysis_date to date type
        analysis_date_obj = datetime.strptime(analysis_date, "%Y-%m-%d").date()

        # Calculate customer metrics
        customer_metrics = orders_df.groupBy("user_id").agg(
            # Total spend
            sum("total_amount").alias("total_spend"),
            # Order count
            count("order_id").alias("order_count"),
            # Average order value
            (sum("total_amount") / count("order_id")).alias("avg_order_value"),
            # Most recent order date
            max("date").alias("last_order_date"),
        )

        # Calculate days since last order
        customer_metrics = customer_metrics.withColumn(
            "days_since_last_order",
            datediff(lit(analysis_date), col("last_order_date")),
        )

        # Round numeric values
        customer_metrics = customer_metrics.withColumn(
            "total_spend", round(col("total_spend"), 2)
        ).withColumn("avg_order_value", round(col("avg_order_value"), 2))

        # Perform RFM segmentation
        # Create windows for ntile calculations
        window_spec = Window.orderBy(col("days_since_last_order"))
        recency_window = Window.orderBy(col("days_since_last_order"))
        frequency_window = Window.orderBy(col("order_count").desc())
        monetary_window = Window.orderBy(col("total_spend").desc())

        # Calculate RFM scores (1-5 scale, 5 being best)
        customer_rfm = (
            customer_metrics.withColumn("recency_score", ntile(5).over(recency_window))
            .withColumn("frequency_score", ntile(5).over(frequency_window))
            .withColumn("monetary_score", ntile(5).over(monetary_window))
        )

        # Calculate combined RFM score
        customer_rfm = customer_rfm.withColumn(
            "rfm_score",
            col("recency_score") + col("frequency_score") + col("monetary_score"),
        )

        # Assign customer segments based on RFM score
        customer_segments = customer_rfm.withColumn(
            "customer_segment",
            when(col("rfm_score") >= 13, "Champions")
            .when(col("rfm_score") >= 10, "Loyal Customers")
            .when(col("rfm_score") >= 7, "Potential Loyalists")
            .when(col("rfm_score") >= 5, "At Risk")
            .when(col("rfm_score") >= 3, "Needs Attention")
            .otherwise("Lost"),
        )

        # Add metadata columns
        result_df = add_metadata_columns(
            customer_segments,
            layer="gold",
            source_file_column=False,
            ingestion_timestamp_column=False,
            processing_timestamp_column=True,
            layer_column=True,
        )

        # Add silver_source_tables column
        result_df = result_df.withColumn(
            "silver_source_tables", lit("orders,order_items")
        )

        # Select and order columns according to the schema
        result_df = result_df.select(
            "user_id",
            "total_spend",
            "order_count",
            "avg_order_value",
            "days_since_last_order",
            "customer_segment",
            "silver_source_tables",
            "processing_timestamp",
            "layer",
        )

        # Validate the schema
        success, error_msg, validated_df = validate_schema(
            result_df, GOLD_CUSTOMER_INSIGHTS_SCHEMA, strict=True
        )

        if not success:
            logger.error(f"Schema validation failed: {error_msg}")
            raise ValueError(f"Schema validation failed: {error_msg}")

        result_df = validated_df

        logger.info("Successfully calculated customer insights metrics")
        return result_df
    except Exception as e:
        logger.error(f"Error calculating customer insights metrics: {str(e)}")
        raise


def write_gold_customer_insights(df: DataFrame, bucket_name: str, date: str) -> None:
    """
    Write customer insights data to gold Delta table.

    Args:
        df: Customer insights DataFrame
        bucket_name: S3 bucket name
        date: Processing date (YYYY-MM-DD)

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'customer_insights')}"

    logger.info(f"Writing customer insights data to gold layer: {table_path}")

    try:
        # Write to Delta table
        write_delta_table(
            df=df,
            table_path=table_path,
            mode="overwrite",  # Use overwrite mode for gold customer insights
            partition_by="customer_segment",  # Partition by customer_segment
            z_order_by="user_id",  # Z-order by user_id
            bucket_name=bucket_name,
        )

        logger.info(
            f"Successfully wrote customer insights data to gold layer: {table_path}"
        )
    except Exception as e:
        logger.error(f"Error writing customer insights data to gold layer: {str(e)}")
        raise


def register_gold_customer_insights_table(
    spark: SparkSession, bucket_name: str
) -> None:
    """
    Register gold customer insights table in Glue Data Catalog.

    Args:
        spark: Spark session
        bucket_name: S3 bucket name

    Returns:
        None
    """
    # Construct the gold table path
    table_path = f"{get_prefix('gold', 'customer_insights')}"

    logger.info("Registering gold customer insights table in Glue Data Catalog")

    try:
        # Register the table
        success = register_delta_table(
            spark=spark,
            table_name="customer_insights",
            table_path=table_path,
            database_name=None,  # Use default from config
            description="Gold layer customer insights table",
            layer="gold",
            bucket_name=bucket_name,
            columns_description={
                "user_id": "Unique identifier for the user",
                "total_spend": "Total amount spent by the customer",
                "order_count": "Number of orders placed by the customer",
                "avg_order_value": "Average order value for the customer",
                "days_since_last_order": "Days since the customer's last order",
                "customer_segment": "Customer segment based on RFM analysis",
                "silver_source_tables": "Source tables in the silver layer",
                "processing_timestamp": "Timestamp when the data was processed",
                "layer": "Data layer (gold)",
            },
        )

        if success:
            logger.info(
                "Successfully registered gold customer insights table in Glue Data Catalog"
            )
        else:
            logger.error(
                "Failed to register gold customer insights table in Glue Data Catalog"
            )
    except Exception as e:
        logger.error(f"Error registering gold customer insights table: {str(e)}")
        raise


def main(date: str, bucket_name: str, region: str, lookback_days: int = 90) -> int:
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
    logger.info(f"Starting Gold Customer Insights ETL for date: {date}")

    try:
        # Create Spark session
        spark = create_spark_session(
            app_name=f"gold_customer_insights_etl_{date}", enable_hive_support=True
        )

        # Read data from silver layer
        orders_df, order_items_df = read_silver_data(
            spark, bucket_name, date, lookback_days
        )

        # Calculate customer insights metrics
        customer_insights_df = calculate_customer_insights(
            orders_df, order_items_df, date
        )

        # Write customer insights data to gold layer
        write_gold_customer_insights(customer_insights_df, bucket_name, date)

        # Register gold customer insights table in Glue Data Catalog
        register_gold_customer_insights_table(spark, bucket_name)

        # Stop Spark session
        spark.stop()

        logger.info(
            f"Successfully completed Gold Customer Insights ETL for date: {date}"
        )
        return 0
    except Exception as e:
        logger.error(f"Error in Gold Customer Insights ETL: {str(e)}")
        return 1


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(args.date, args.bucket_name, args.region, args.lookback_days)
    sys.exit(exit_code)

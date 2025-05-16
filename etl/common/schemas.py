"""
Schema definitions for the e-commerce lakehouse architecture.

This module contains schema definitions for all data models used in the ETL processes.
It includes schemas for:
- Raw data (CSV files)
- Bronze layer Delta tables
- Silver layer Delta tables
- Gold layer Delta tables

Each schema is defined using PySpark's StructType and StructField classes.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
    BooleanType,
)

# Raw Layer Schemas

RAW_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True),
    ]
)

RAW_ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("date", DateType(), True),
    ]
)

RAW_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", BooleanType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("date", DateType(), True),
    ]
)

# Bronze Layer Schemas

BRONZE_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True),
        # Metadata columns
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

BRONZE_ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("date", DateType(), True),
        # Metadata columns
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

BRONZE_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", BooleanType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("date", DateType(), True),
        # Metadata columns
        StructField("source_file", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

# Silver Layer Schemas

SILVER_PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("department_standardized", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("is_active", BooleanType(), True),
        # Metadata columns
        StructField("bronze_source_file", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

SILVER_ORDERS_SCHEMA = StructType(
    [
        StructField("order_num", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("date", DateType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("hour_of_day", IntegerType(), True),
        # Metadata columns
        StructField("bronze_source_file", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

SILVER_ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("order_id", StringType(), False),
        StructField("user_id", IntegerType(), True),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("days_since_prior_order_bucket", StringType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("add_to_cart_order", IntegerType(), True),
        StructField("reordered", BooleanType(), True),
        StructField("order_timestamp", TimestampType(), True),
        StructField("date", DateType(), True),
        # Metadata columns
        StructField("bronze_source_file", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

# Gold Layer Schemas

GOLD_DAILY_SALES_SCHEMA = StructType(
    [
        StructField("date", DateType(), False),
        StructField("total_sales", DoubleType(), True),
        StructField("order_count", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True),
        StructField("items_per_order", DoubleType(), True),
        StructField("unique_customers", IntegerType(), True),
        # Metadata columns
        StructField("silver_source_tables", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

GOLD_PRODUCT_PERFORMANCE_SCHEMA = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), True),
        StructField("department", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("total_quantity", IntegerType(), True),
        StructField("total_sales", DoubleType(), True),
        StructField("reorder_rate", DoubleType(), True),
        StructField("avg_days_between_orders", DoubleType(), True),
        # Metadata columns
        StructField("silver_source_tables", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

GOLD_DEPARTMENT_ANALYTICS_SCHEMA = StructType(
    [
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), True),
        StructField("total_sales", DoubleType(), True),
        StructField("product_count", IntegerType(), True),
        StructField("customer_count", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True),
        # Metadata columns
        StructField("silver_source_tables", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

GOLD_CUSTOMER_INSIGHTS_SCHEMA = StructType(
    [
        StructField("user_id", IntegerType(), False),
        StructField("total_spend", DoubleType(), True),
        StructField("order_count", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True),
        StructField("days_since_last_order", IntegerType(), True),
        StructField("customer_segment", StringType(), True),
        # Metadata columns
        StructField("silver_source_tables", StringType(), True),
        StructField("processing_timestamp", TimestampType(), True),
        StructField("layer", StringType(), True),
    ]
)

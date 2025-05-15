"""
Configuration settings for the e-commerce lakehouse architecture.

This module contains all configuration settings for the e-commerce lakehouse architecture,
including:
- AWS settings (region, bucket name)
- Data layer settings (raw, bronze, silver, gold)
- Processing settings (batch size, timeout)
- Logging settings

All settings can be overridden by environment variables with the same name prefixed with 'ECOM_'.
For example, AWS_REGION can be overridden by setting the ECOM_AWS_REGION environment variable.
"""

import os
from pathlib import Path
from typing import Dict, Any

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Data directory
DATA_DIR = os.path.join(PROJECT_ROOT, "Data")

# AWS Settings
AWS_REGION = os.environ.get("ECOM_AWS_REGION", "eu-west-1")
S3_BUCKET_NAME = os.environ.get("ECOM_S3_BUCKET_NAME", "ecommerce-lakehouse-bucket-123")

# S3 Prefix Structure
S3_PREFIX_STRUCTURE = {
    # Raw layer
    "raw": {
        "base": "raw/",
        "products": "raw/products/",
        "orders": "raw/orders/",
        "order_items": "raw/order_items/",
        "processing": "raw/processing/",
        "processed": "raw/processed/",
        "failed": "raw/failed/",
        "archive": "raw/archive/",
    },
    # Bronze layer
    "bronze": {
        "base": "bronze/",
        "products": "bronze/products/",
        "orders": "bronze/orders/",
        "order_items": "bronze/order_items/",
        "processing": "bronze/processing/",
        "failed": "bronze/failed/",
        "archive": "bronze/archive/",
    },
    # Silver layer
    "silver": {
        "base": "silver/",
        "products": "silver/products/",
        "orders": "silver/orders/",
        "order_items": "silver/order_items/",
        "processing": "silver/processing/",
        "failed": "silver/failed/",
        "archive": "silver/archive/",
    },
    # Gold layer
    "gold": {
        "base": "gold/",
        "daily_sales": "gold/daily_sales/",
        "product_performance": "gold/product_performance/",
        "department_analytics": "gold/department_analytics/",
        "customer_insights": "gold/customer_insights/",
        "processing": "gold/processing/",
        "failed": "gold/failed/",
        "archive": "gold/archive/",
    },
    # Other directories
    "other": {
        "scripts": "scripts/",
        "logs": "logs/",
        "metadata": "metadata/",
        "temp": "temp/",
    },
}

# Flatten the prefix structure for easy access
S3_PREFIXES = []
for category in S3_PREFIX_STRUCTURE.values():
    S3_PREFIXES.extend(category.values())

# Data file mappings
DATA_FILES = {
    "products": {
        "local_path": os.path.join(DATA_DIR, "products.csv"),
        "s3_key": "raw/products/products.csv",
    },
    "orders_dir": {
        "local_path": os.path.join(DATA_DIR, "orders"),
        "s3_prefix": "raw/orders/",
    },
    "order_items_dir": {
        "local_path": os.path.join(DATA_DIR, "order_items"),
        "s3_prefix": "raw/order_items/",
    },
}

# Processing settings
BATCH_SIZE = int(os.environ.get("ECOM_BATCH_SIZE", "100"))
PROCESSING_TIMEOUT = int(
    os.environ.get("ECOM_PROCESSING_TIMEOUT", "3600")
)  # in seconds

# Logging settings
LOG_LEVEL = os.environ.get("ECOM_LOG_LEVEL", "INFO")
LOG_FORMAT = os.environ.get(
    "ECOM_LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

# Glue Data Catalog settings
GLUE_DATABASE_PREFIX = os.environ.get("ECOM_GLUE_DATABASE_PREFIX", "ecommerce")
GLUE_DATABASES = {
    "bronze": f"{GLUE_DATABASE_PREFIX}_bronze",
    "silver": f"{GLUE_DATABASE_PREFIX}_silver",
    "gold": f"{GLUE_DATABASE_PREFIX}_gold",
}

# Delta Lake settings
DELTA_TABLE_PROPERTIES = {
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

# Schema settings
SCHEMA_VALIDATION = os.environ.get("ECOM_SCHEMA_VALIDATION", "true").lower() == "true"


# Function to get a specific prefix
def get_prefix(layer: str, category: str) -> str:
    """
    Get a specific prefix from the S3 prefix structure.

    Args:
        layer: The data layer (raw, bronze, silver, gold, other)
        category: The category within the layer

    Returns:
        str: The prefix

    Raises:
        KeyError: If the layer or category does not exist
    """
    return S3_PREFIX_STRUCTURE[layer][category]


# Function to get all settings as a dictionary
def get_all_settings() -> Dict[str, Any]:
    """
    Get all settings as a dictionary.

    Returns:
        Dict[str, Any]: All settings
    """
    return {
        "AWS_REGION": AWS_REGION,
        "S3_BUCKET_NAME": S3_BUCKET_NAME,
        "S3_PREFIX_STRUCTURE": S3_PREFIX_STRUCTURE,
        "S3_PREFIXES": S3_PREFIXES,
        "DATA_FILES": DATA_FILES,
        "BATCH_SIZE": BATCH_SIZE,
        "PROCESSING_TIMEOUT": PROCESSING_TIMEOUT,
        "LOG_LEVEL": LOG_LEVEL,
        "LOG_FORMAT": LOG_FORMAT,
        "GLUE_DATABASE_PREFIX": GLUE_DATABASE_PREFIX,
        "GLUE_DATABASES": GLUE_DATABASES,
        "DELTA_TABLE_PROPERTIES": DELTA_TABLE_PROPERTIES,
        "SCHEMA_VALIDATION": SCHEMA_VALIDATION,
    }

#!/usr/bin/env python
"""
Test ETL Components

This script tests individual components of the ETL pipeline without requiring a Spark session.
It reads the data files directly using pandas and validates the schemas.

Usage:
    python scripts/test_etl_components.py
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import LOG_LEVEL, LOG_FORMAT

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)


def test_products_data():
    """Test reading and validating products data."""
    logger.info("Testing products data")
    
    # Read the products data
    products_path = "Data/products.csv"
    if not os.path.exists(products_path):
        logger.error(f"Products data file not found: {products_path}")
        return False
    
    try:
        # Read the CSV file
        df = pd.read_csv(products_path)
        
        # Check the schema
        expected_columns = ["product_id", "department_id", "department", "product_name"]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing columns in products data: {missing_columns}")
            return False
        
        # Check for null values in required fields
        null_product_ids = df["product_id"].isnull().sum()
        if null_product_ids > 0:
            logger.error(f"Found {null_product_ids} null product_ids")
            return False
        
        # Print some statistics
        logger.info(f"Products data: {len(df)} rows")
        logger.info(f"Departments: {df['department'].nunique()} unique values")
        logger.info(f"Sample departments: {df['department'].unique()[:5]}")
        
        return True
    except Exception as e:
        logger.error(f"Error reading products data: {str(e)}")
        return False


def test_orders_data():
    """Test reading and validating orders data."""
    logger.info("Testing orders data")
    
    # Find order files
    orders_dir = "Data/orders"
    if not os.path.exists(orders_dir):
        logger.error(f"Orders directory not found: {orders_dir}")
        return False
    
    order_files = [f for f in os.listdir(orders_dir) if f.endswith(".csv")]
    if not order_files:
        logger.error(f"No order files found in {orders_dir}")
        return False
    
    try:
        # Read the first order file
        order_file = order_files[0]
        order_path = os.path.join(orders_dir, order_file)
        df = pd.read_csv(order_path)
        
        # Check the schema
        expected_columns = ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing columns in orders data: {missing_columns}")
            return False
        
        # Check for null values in required fields
        null_order_ids = df["order_id"].isnull().sum()
        if null_order_ids > 0:
            logger.error(f"Found {null_order_ids} null order_ids")
            return False
        
        # Print some statistics
        logger.info(f"Orders data ({order_file}): {len(df)} rows")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Total orders: {len(df)}")
        
        return True
    except Exception as e:
        logger.error(f"Error reading orders data: {str(e)}")
        return False


def test_order_items_data():
    """Test reading and validating order items data."""
    logger.info("Testing order items data")
    
    # Find order items files
    order_items_dir = "Data/order_items"
    if not os.path.exists(order_items_dir):
        logger.error(f"Order items directory not found: {order_items_dir}")
        return False
    
    order_items_files = [f for f in os.listdir(order_items_dir) if f.endswith(".csv")]
    if not order_items_files:
        logger.error(f"No order items files found in {order_items_dir}")
        return False
    
    try:
        # Read the first order items file
        order_items_file = order_items_files[0]
        order_items_path = os.path.join(order_items_dir, order_items_file)
        df = pd.read_csv(order_items_path)
        
        # Check the schema
        expected_columns = ["id", "order_id", "user_id", "days_since_prior_order", "product_id", "add_to_cart_order", "reordered", "order_timestamp", "date"]
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"Missing columns in order items data: {missing_columns}")
            return False
        
        # Check for null values in required fields
        null_ids = df["id"].isnull().sum()
        if null_ids > 0:
            logger.error(f"Found {null_ids} null ids")
            return False
        
        # Print some statistics
        logger.info(f"Order items data ({order_items_file}): {len(df)} rows")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Total order items: {len(df)}")
        logger.info(f"Unique orders: {df['order_id'].nunique()}")
        logger.info(f"Unique products: {df['product_id'].nunique()}")
        
        return True
    except Exception as e:
        logger.error(f"Error reading order items data: {str(e)}")
        return False


def main():
    """Main function to run all tests."""
    logger.info("Starting ETL component tests")
    
    # Test products data
    products_success = test_products_data()
    logger.info(f"Products data test {'passed' if products_success else 'failed'}")
    
    # Test orders data
    orders_success = test_orders_data()
    logger.info(f"Orders data test {'passed' if orders_success else 'failed'}")
    
    # Test order items data
    order_items_success = test_order_items_data()
    logger.info(f"Order items data test {'passed' if order_items_success else 'failed'}")
    
    # Overall result
    overall_success = products_success and orders_success and order_items_success
    logger.info(f"Overall test result: {'passed' if overall_success else 'failed'}")
    
    return 0 if overall_success else 1


if __name__ == "__main__":
    sys.exit(main())

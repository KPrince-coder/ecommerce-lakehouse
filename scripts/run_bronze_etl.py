#!/usr/bin/env python
"""
Run All Bronze Layer ETL Processes

This script runs all Bronze layer ETL processes in sequence:
1. Products ETL
2. Orders ETL
3. Order Items ETL

Usage:
    python scripts/run_bronze_etl.py [--date YYYY-MM-DD] [--bucket-name BUCKET_NAME] [--region REGION] [--days-back DAYS]
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.bronze.products_etl import main as products_main
from etl.bronze.orders_etl import main as orders_main
from etl.bronze.order_items_etl import main as order_items_main
from config import S3_BUCKET_NAME, AWS_REGION, LOG_LEVEL, LOG_FORMAT

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT
)
logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Run All Bronze Layer ETL Processes"
    )
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Processing date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default=S3_BUCKET_NAME,
        help=f"S3 bucket name (default: {S3_BUCKET_NAME})"
    )
    parser.add_argument(
        "--region",
        type=str,
        default=AWS_REGION,
        help=f"AWS region (default: {AWS_REGION})"
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=1,
        help="Number of days to process (default: 1)"
    )
    
    return parser.parse_args()


def main(date: str, bucket_name: str, region: str, days_back: int) -> int:
    """
    Main function to run all Bronze layer ETL processes.
    
    Args:
        date: Processing date (YYYY-MM-DD)
        bucket_name: S3 bucket name
        region: AWS region
        days_back: Number of days to process
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    logger.info(f"Starting all Bronze layer ETL processes for date: {date}")
    
    # Run Products ETL
    logger.info("Running Products ETL")
    products_exit_code = products_main(date, bucket_name, region)
    if products_exit_code != 0:
        logger.error("Products ETL failed")
        return products_exit_code
    
    # Run Orders ETL
    logger.info("Running Orders ETL")
    orders_exit_code = orders_main(date, bucket_name, region, days_back)
    if orders_exit_code != 0:
        logger.error("Orders ETL failed")
        return orders_exit_code
    
    # Run Order Items ETL
    logger.info("Running Order Items ETL")
    order_items_exit_code = order_items_main(date, bucket_name, region, days_back)
    if order_items_exit_code != 0:
        logger.error("Order Items ETL failed")
        return order_items_exit_code
    
    logger.info("All Bronze layer ETL processes completed successfully")
    return 0


if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Running all Bronze layer ETL processes for date: {args.date}, days back: {args.days_back}")
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    logger.info(f"All Bronze layer ETL processes completed with exit code: {exit_code}")
    sys.exit(exit_code)

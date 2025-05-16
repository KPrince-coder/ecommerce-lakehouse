#!/usr/bin/env python
"""
Run All Gold Layer ETL Processes

This script runs all Gold layer ETL processes in sequence:
1. Product Performance ETL
2. Customer Insights ETL

Usage:
    python scripts/run_gold_etl.py [--date YYYY-MM-DD] [--bucket-name BUCKET_NAME] [--region REGION]
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.gold.product_performance_etl import main as product_performance_main
from etl.gold.customer_insights_etl import main as customer_insights_main
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
        description="Run All Gold Layer ETL Processes"
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
        "--product-lookback-days",
        type=int,
        default=30,
        help="Number of days to look back for product performance analytics (default: 30)"
    )
    parser.add_argument(
        "--customer-lookback-days",
        type=int,
        default=90,
        help="Number of days to look back for customer insights analytics (default: 90)"
    )
    
    return parser.parse_args()


def main(
    date: str, 
    bucket_name: str, 
    region: str, 
    product_lookback_days: int,
    customer_lookback_days: int
) -> int:
    """
    Main function to run all Gold layer ETL processes.
    
    Args:
        date: Processing date (YYYY-MM-DD)
        bucket_name: S3 bucket name
        region: AWS region
        product_lookback_days: Number of days to look back for product performance analytics
        customer_lookback_days: Number of days to look back for customer insights analytics
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    logger.info(f"Starting all Gold layer ETL processes for date: {date}")
    
    # Run Product Performance ETL
    logger.info("Running Product Performance ETL")
    product_performance_exit_code = product_performance_main(
        date, bucket_name, region, product_lookback_days
    )
    if product_performance_exit_code != 0:
        logger.error("Product Performance ETL failed")
        return product_performance_exit_code
    
    # Run Customer Insights ETL
    logger.info("Running Customer Insights ETL")
    customer_insights_exit_code = customer_insights_main(
        date, bucket_name, region, customer_lookback_days
    )
    if customer_insights_exit_code != 0:
        logger.error("Customer Insights ETL failed")
        return customer_insights_exit_code
    
    logger.info("All Gold layer ETL processes completed successfully")
    return 0


if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Running all Gold layer ETL processes for date: {args.date}")
    exit_code = main(
        args.date, 
        args.bucket_name, 
        args.region, 
        args.product_lookback_days,
        args.customer_lookback_days
    )
    logger.info(f"All Gold layer ETL processes completed with exit code: {exit_code}")
    sys.exit(exit_code)

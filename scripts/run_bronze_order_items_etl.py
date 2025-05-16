#!/usr/bin/env python
"""
Run Bronze Order Items ETL

This script runs the Bronze Order Items ETL process.

Usage:
    python scripts/run_bronze_order_items_etl.py [--date YYYY-MM-DD] [--bucket-name BUCKET_NAME] [--region REGION] [--days-back DAYS]
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.bronze.order_items_etl import main
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
        description="Run Bronze Order Items ETL"
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


if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Running Bronze Order Items ETL for date: {args.date}, days back: {args.days_back}")
    exit_code = main(args.date, args.bucket_name, args.region, args.days_back)
    logger.info(f"Bronze Order Items ETL completed with exit code: {exit_code}")
    sys.exit(exit_code)

#!/usr/bin/env python
"""
Run Silver Products ETL

This script runs the Silver Products ETL process.

Usage:
    python scripts/run_silver_products_etl.py [--date YYYY-MM-DD] [--bucket-name BUCKET_NAME] [--region REGION]
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.silver.products_etl import main
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
        description="Run Silver Products ETL"
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
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Running Silver Products ETL for date: {args.date}")
    exit_code = main(args.date, args.bucket_name, args.region)
    logger.info(f"Silver Products ETL completed with exit code: {exit_code}")
    sys.exit(exit_code)

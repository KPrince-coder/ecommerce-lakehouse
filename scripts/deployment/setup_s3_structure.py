#!/usr/bin/env python
"""
S3 Bucket Structure Setup Script

This script sets up the S3 bucket structure for the e-commerce lakehouse architecture.
It creates a single bucket with the appropriate prefix structure for raw, bronze, silver,
and gold layers as outlined in the roadmap.

The script also uploads the initial data files to the raw layer.

Usage:
    python setup_s3_structure.py [--bucket-name ecommerce-lakehouse] [--region us-east-1]

The bucket name and region can also be set using environment variables:
    - ECOM_S3_BUCKET_NAME: S3 bucket name
    - ECOM_AWS_REGION: AWS region

Requirements:
    - AWS credentials configured
    - boto3 package installed
    - Data files in the Data/ directory
"""

import argparse
import logging
from pathlib import Path
from typing import List, Optional

import sys

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from etl.common.s3_utils import (
    create_bucket,
    create_prefix_structure,
    upload_file,
    upload_directory,
)
from etl.common.s3_policies import (
    configure_bucket_security,
)
from config import (
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_PREFIXES,
    DATA_FILES,
    LOG_LEVEL,
    LOG_FORMAT,
)

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
        description="Set up S3 bucket structure for e-commerce lakehouse architecture"
    )
    parser.add_argument(
        "--bucket-name",
        default=S3_BUCKET_NAME,
        help=f"Name of the S3 bucket to create (default: {S3_BUCKET_NAME})",
    )
    parser.add_argument(
        "--region",
        default=AWS_REGION,
        help=f"AWS region where the bucket should be created (default: {AWS_REGION})",
    )
    parser.add_argument(
        "--data-dir",
        default="Data",
        help="Directory containing the data files to upload",
    )
    parser.add_argument(
        "--skip-upload", action="store_true", help="Skip uploading data files"
    )
    parser.add_argument(
        "--skip-security",
        action="store_true",
        help="Skip configuring bucket security settings",
    )

    return parser.parse_args()


def get_prefix_structure() -> List[str]:
    """
    Define the prefix structure for the S3 bucket.

    Returns:
        List[str]: List of prefixes to create
    """
    # Use the prefixes defined in the configuration
    return S3_PREFIXES


def upload_initial_data(bucket_name: str, data_dir: str) -> bool:
    """
    Upload initial data files to the raw layer.

    Args:
        bucket_name: Name of the S3 bucket
        data_dir: Directory containing the data files

    Returns:
        bool: True if all files were uploaded successfully, False otherwise
    """
    data_path = Path(data_dir)

    if not data_path.exists() or not data_path.is_dir():
        logger.error(f"Data directory {data_dir} does not exist or is not a directory")
        return False

    # Upload products.csv
    products_file = Path(DATA_FILES["products"]["local_path"])
    if products_file.exists():
        if not upload_file(
            products_file, bucket_name, DATA_FILES["products"]["s3_key"]
        ):
            return False
    else:
        logger.warning(f"Products file {products_file} does not exist")

    # Upload orders directory
    orders_dir = Path(DATA_FILES["orders_dir"]["local_path"])
    if orders_dir.exists() and orders_dir.is_dir():
        orders_results = upload_directory(
            orders_dir, bucket_name, DATA_FILES["orders_dir"]["s3_prefix"]
        )
        if not all(orders_results.values()):
            logger.warning("Some order files failed to upload")
    else:
        logger.warning(
            f"Orders directory {orders_dir} does not exist or is not a directory"
        )

    # Upload order_items directory
    order_items_dir = Path(DATA_FILES["order_items_dir"]["local_path"])
    if order_items_dir.exists() and order_items_dir.is_dir():
        order_items_results = upload_directory(
            order_items_dir, bucket_name, DATA_FILES["order_items_dir"]["s3_prefix"]
        )
        if not all(order_items_results.values()):
            logger.warning("Some order item files failed to upload")
    else:
        logger.warning(
            f"Order items directory {order_items_dir} does not exist or is not a directory"
        )

    return True


def main(
    bucket_name: str,
    region: Optional[str] = None,
    data_dir: str = "Data",
    skip_upload: bool = False,
    configure_security: bool = True,
) -> int:
    """
    Main function to set up the S3 bucket structure.

    Args:
        bucket_name: Name of the S3 bucket to create
        region: AWS region where the bucket should be created
        data_dir: Directory containing the data files to upload
        skip_upload: Whether to skip uploading data files
        configure_security: Whether to configure bucket security settings

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    logger.info(f"Setting up S3 bucket structure for {bucket_name}")

    # Step 1: Create the bucket
    try:
        if not create_bucket(bucket_name, region):
            logger.error(f"Failed to create bucket {bucket_name}")
            return 1
    except Exception as e:
        logger.error(f"Error creating bucket: {str(e)}")
        return 1

    # Step 2: Create the prefix structure
    prefix_list = get_prefix_structure()
    if not create_prefix_structure(bucket_name, prefix_list):
        logger.error(f"Failed to create prefix structure in bucket {bucket_name}")
        return 1

    # Step 3: Upload initial data files (if not skipped)
    if not skip_upload and not upload_initial_data(bucket_name, data_dir):
        logger.error(f"Failed to upload initial data to bucket {bucket_name}")
        return 1

    # Step 4: Configure bucket security settings (if not skipped)
    if configure_security:
        logger.info(f"Configuring security settings for bucket {bucket_name}")
        try:
            if not configure_bucket_security(bucket_name, region):
                logger.error(
                    f"Failed to configure security settings for bucket {bucket_name}"
                )
                return 1
        except Exception as e:
            logger.error(f"Error configuring security settings: {str(e)}")
            return 1

    logger.info(f"Successfully set up S3 bucket structure for {bucket_name}")
    return 0


if __name__ == "__main__":
    args = parse_arguments()
    exit_code = main(
        bucket_name=args.bucket_name,
        region=args.region,
        data_dir=args.data_dir,
        skip_upload=args.skip_upload,
        configure_security=not args.skip_security,
    )
    sys.exit(exit_code)

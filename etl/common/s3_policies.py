"""
S3 bucket policies and lifecycle rules for the e-commerce lakehouse architecture.

This module provides functions to configure S3 bucket policies and lifecycle rules
for the e-commerce lakehouse architecture, including:
- Setting up lifecycle policies to archive raw data after processing
- Configuring access controls for different layers
- Setting up bucket policies for security
"""

import json
import logging
from typing import Optional, Any

import boto3
from botocore.exceptions import ClientError

from config import (
    AWS_REGION,
    LOG_LEVEL,
    LOG_FORMAT,
)

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def create_s3_client(region_name: Optional[str] = None) -> Any:
    """
    Create and return an S3 client.

    Args:
        region_name: AWS region name. If None, uses the default region from AWS configuration.

    Returns:
        boto3.client: Configured S3 client
    """
    try:
        return boto3.client("s3", region_name=region_name)
    except Exception as e:
        logger.error(f"Failed to create S3 client: {str(e)}")
        raise


def set_lifecycle_policy(
    bucket_name: str,
    region: Optional[str] = None,
) -> bool:
    """
    Set lifecycle policy for a bucket to archive raw data after processing.

    This function configures lifecycle rules to:
    1. Transition objects in raw/archive/ to Standard-IA after 30 days
    2. Transition objects in raw/archive/ to Glacier after 90 days
    3. Expire objects in raw/archive/ after 365 days
    4. Expire objects in temp/ after 7 days

    Args:
        bucket_name: Name of the bucket
        region: AWS region. If None, uses the default from config.

    Returns:
        bool: True if lifecycle policy was set successfully, False otherwise
    """
    if region is None:
        region = AWS_REGION

    s3_client = create_s3_client(region)

    # Define lifecycle configuration
    lifecycle_config = {
        "Rules": [
            # Rule for archiving raw data
            {
                "ID": "Archive-Raw-Data",
                "Status": "Enabled",
                "Filter": {"Prefix": "raw/archive/"},
                "Transitions": [
                    {"Days": 30, "StorageClass": "STANDARD_IA"},
                    {"Days": 90, "StorageClass": "GLACIER"},
                ],
                "Expiration": {"Days": 365},
            },
            # Rule for cleaning up temporary files
            {
                "ID": "Cleanup-Temp-Files",
                "Status": "Enabled",
                "Filter": {"Prefix": "temp/"},
                "Expiration": {"Days": 7},
            },
            # Rule for cleaning up failed processing files after 30 days
            {
                "ID": "Cleanup-Failed-Files",
                "Status": "Enabled",
                "Filter": {"Prefix": "raw/failed/"},
                "Expiration": {"Days": 30},
            },
        ]
    }

    try:
        # Set lifecycle configuration
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name, LifecycleConfiguration=lifecycle_config
        )
        logger.info(f"Successfully set lifecycle policy for bucket {bucket_name}")
        return True
    except ClientError as e:
        logger.error(
            f"Failed to set lifecycle policy for bucket {bucket_name}: {str(e)}"
        )
        return False


def set_bucket_policy(
    bucket_name: str,
    region: Optional[str] = None,
) -> bool:
    """
    Set bucket policy for access control.

    This function sets a bucket policy that:
    1. Denies access to the bucket over HTTP (requires HTTPS)
    2. Restricts access to specific IAM roles or users

    Args:
        bucket_name: Name of the bucket
        region: AWS region. If None, uses the default from config.

    Returns:
        bool: True if bucket policy was set successfully, False otherwise
    """
    if region is None:
        region = AWS_REGION

    s3_client = create_s3_client(region)

    # Define bucket policy
    bucket_policy = {
        "Version": "2012-10-17",
        "Statement": [
            # Deny access over HTTP
            {
                "Sid": "DenyHTTP",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
                "Condition": {"Bool": {"aws:SecureTransport": "false"}},
            },
            # Allow access to specific IAM roles (example)
            # Note: Replace with actual IAM roles/users as needed
            {
                "Sid": "AllowSpecificRoles",
                "Effect": "Allow",
                "Principal": {
                    "AWS": [
                        "arn:aws:iam::123456789012:role/GlueETLRole",
                        "arn:aws:iam::123456789012:role/AthenaQueryRole",
                    ]
                },
                "Action": ["s3:GetObject", "s3:ListBucket"],
                "Resource": [
                    f"arn:aws:s3:::{bucket_name}",
                    f"arn:aws:s3:::{bucket_name}/*",
                ],
            },
        ],
    }

    try:
        # Convert policy to JSON string
        policy_json = json.dumps(bucket_policy)

        # Set bucket policy
        s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_json)
        logger.info(f"Successfully set bucket policy for bucket {bucket_name}")
        return True
    except ClientError as e:
        logger.error(f"Failed to set bucket policy for bucket {bucket_name}: {str(e)}")
        return False


def enable_bucket_encryption(
    bucket_name: str,
    region: Optional[str] = None,
) -> bool:
    """
    Enable server-side encryption for a bucket.

    Args:
        bucket_name: Name of the bucket
        region: AWS region. If None, uses the default from config.

    Returns:
        bool: True if encryption was enabled successfully, False otherwise
    """
    if region is None:
        region = AWS_REGION

    s3_client = create_s3_client(region)

    # Define encryption configuration (using AES-256)
    encryption_config = {
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                "BucketKeyEnabled": True,
            }
        ]
    }

    try:
        # Set encryption configuration
        s3_client.put_bucket_encryption(
            Bucket=bucket_name, ServerSideEncryptionConfiguration=encryption_config
        )
        logger.info(f"Successfully enabled encryption for bucket {bucket_name}")
        return True
    except ClientError as e:
        logger.error(f"Failed to enable encryption for bucket {bucket_name}: {str(e)}")
        return False


def configure_bucket_security(
    bucket_name: str,
    region: Optional[str] = None,
) -> bool:
    """
    Configure comprehensive security settings for a bucket.

    This function:
    1. Sets lifecycle policies
    2. Sets bucket policies
    3. Enables encryption
    4. Blocks public access

    Args:
        bucket_name: Name of the bucket
        region: AWS region. If None, uses the default from config.

    Returns:
        bool: True if all security settings were configured successfully, False otherwise
    """
    if region is None:
        region = AWS_REGION

    s3_client = create_s3_client(region)

    # Step 1: Set lifecycle policy
    if not set_lifecycle_policy(bucket_name, region):
        return False

    # Step 2: Set bucket policy
    if not set_bucket_policy(bucket_name, region):
        return False

    # Step 3: Enable encryption
    if not enable_bucket_encryption(bucket_name, region):
        return False

    # Step 4: Block public access
    try:
        s3_client.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        logger.info(f"Successfully blocked public access for bucket {bucket_name}")
    except ClientError as e:
        logger.error(
            f"Failed to block public access for bucket {bucket_name}: {str(e)}"
        )
        return False

    logger.info(
        f"Successfully configured all security settings for bucket {bucket_name}"
    )
    return True

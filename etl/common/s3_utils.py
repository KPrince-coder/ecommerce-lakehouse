"""
Utility functions for Amazon S3 operations.

This module provides helper functions for common S3 operations such as:
- Creating buckets
- Creating prefix structures
- Uploading files
- Checking if objects exist
- Listing objects
- Tracking file processing status

All functions include proper error handling and type hints.
"""

import logging
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

import boto3
from botocore.exceptions import ClientError


# Define file processing status enum
class FileStatus(Enum):
    """Enum representing the status of a file in the processing pipeline."""

    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    ARCHIVED = "archive"


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
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


def create_bucket(bucket_name: str, region: Optional[str] = None) -> bool:
    """
    Create an S3 bucket.

    Args:
        bucket_name: Name of the bucket to create
        region: AWS region where the bucket should be created
                If None, uses the default region from AWS configuration

    Returns:
        bool: True if bucket was created or already exists, False otherwise

    Raises:
        ClientError: If bucket creation fails for reasons other than the bucket already existing
    """
    s3_client = create_s3_client(region)

    try:
        # Check if bucket already exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket {bucket_name} already exists")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            # Bucket doesn't exist, create it
            try:
                if region is None:
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    location = {"LocationConstraint": region}
                    s3_client.create_bucket(
                        Bucket=bucket_name, CreateBucketConfiguration=location
                    )
                logger.info(f"Bucket {bucket_name} created successfully")
                return True
            except ClientError as e:
                logger.error(f"Failed to create bucket {bucket_name}: {str(e)}")
                raise
        else:
            # Other error occurred
            logger.error(f"Error checking bucket {bucket_name}: {str(e)}")
            raise

    return False


def create_prefix_structure(bucket_name: str, prefix_list: List[str]) -> bool:
    """
    Create a prefix structure in an S3 bucket.

    In S3, folders are actually just prefixes, so we create empty objects with the prefix paths.

    Args:
        bucket_name: Name of the bucket
        prefix_list: List of prefixes to create (e.g., ["raw/", "bronze/", "silver/"])

    Returns:
        bool: True if all prefixes were created successfully, False otherwise
    """
    s3_client = create_s3_client()

    try:
        for prefix in prefix_list:
            # Ensure prefix ends with a slash
            if not prefix.endswith("/"):
                prefix = f"{prefix}/"

            # Create empty object with the prefix path
            s3_client.put_object(Bucket=bucket_name, Key=prefix)
            logger.info(f"Created prefix {prefix} in bucket {bucket_name}")

        return True
    except ClientError as e:
        logger.error(
            f"Failed to create prefix structure in bucket {bucket_name}: {str(e)}"
        )
        return False


def upload_file(
    file_path: Union[str, Path], bucket_name: str, object_key: Optional[str] = None
) -> bool:
    """
    Upload a file to an S3 bucket.

    Args:
        file_path: Path to the file to upload
        bucket_name: Name of the bucket
        object_key: S3 object key (path in the bucket). If None, uses the file name.

    Returns:
        bool: True if file was uploaded successfully, False otherwise
    """
    # Convert to Path object if string
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # If object_key is not provided, use the file name
    if object_key is None:
        object_key = file_path.name

    # Ensure the file exists
    if not file_path.exists():
        logger.error(f"File {file_path} does not exist")
        return False

    s3_client = create_s3_client()

    try:
        s3_client.upload_file(str(file_path), bucket_name, object_key)
        logger.info(f"Uploaded {file_path} to s3://{bucket_name}/{object_key}")
        return True
    except ClientError as e:
        logger.error(
            f"Failed to upload {file_path} to s3://{bucket_name}/{object_key}: {str(e)}"
        )
        return False


def upload_directory(
    directory_path: Union[str, Path], bucket_name: str, prefix: str = ""
) -> Dict[str, bool]:
    """
    Upload all files in a directory to an S3 bucket.

    Args:
        directory_path: Path to the directory to upload
        bucket_name: Name of the bucket
        prefix: S3 prefix to prepend to the object keys

    Returns:
        Dict[str, bool]: Dictionary mapping file paths to upload success status
    """
    # Convert to Path object if string
    if isinstance(directory_path, str):
        directory_path = Path(directory_path)

    # Ensure the directory exists
    if not directory_path.exists() or not directory_path.is_dir():
        logger.error(f"Directory {directory_path} does not exist or is not a directory")
        return {}

    # Ensure prefix ends with a slash if it's not empty
    if prefix and not prefix.endswith("/"):
        prefix = f"{prefix}/"

    results = {}

    # Walk through the directory and upload all files
    for root, _, files in os.walk(directory_path):
        for file in files:
            file_path = Path(root) / file

            # Calculate the relative path from the directory_path
            relative_path = file_path.relative_to(directory_path)

            # Create the object key with the prefix
            object_key = f"{prefix}{relative_path}"

            # Upload the file
            success = upload_file(file_path, bucket_name, object_key)
            results[str(file_path)] = success

    return results


def update_file_status(
    bucket_name: str,
    object_key: str,
    status: FileStatus,
    layer: str = "raw",
    error_message: Optional[str] = None,
) -> bool:
    """
    Update the status of a file in the processing pipeline.

    This function moves the file to the appropriate status folder and optionally
    adds metadata about the processing status.

    Args:
        bucket_name: Name of the S3 bucket
        object_key: S3 object key (path in the bucket)
        status: New status of the file (from FileStatus enum)
        layer: Data layer (raw, bronze, silver, gold)
        error_message: Optional error message if status is FAILED

    Returns:
        bool: True if status was updated successfully, False otherwise
    """
    s3_client = create_s3_client()

    try:
        # Extract the filename from the object key
        filename = os.path.basename(object_key)

        # Determine the source and destination paths
        source_path = f"{bucket_name}/{object_key}"
        destination_key = f"{layer}/{status.value}/{filename}"

        # Create metadata about the status change
        metadata = {
            "original_path": object_key,
            "status": status.value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if error_message and status == FileStatus.FAILED:
            metadata["error_message"] = error_message

        # Copy the object to the new location with metadata
        s3_client.copy_object(
            CopySource=source_path,
            Bucket=bucket_name,
            Key=destination_key,
            Metadata=metadata,
            MetadataDirective="REPLACE",
        )

        # If status is ARCHIVED, keep the original file
        # Otherwise, delete the original file
        if status != FileStatus.ARCHIVED:
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)

        logger.info(f"Updated status of {object_key} to {status.value}")
        return True
    except ClientError as e:
        logger.error(f"Failed to update status of {object_key}: {str(e)}")
        return False


def get_file_status(
    bucket_name: str, object_key: str
) -> Tuple[Optional[FileStatus], Optional[Dict[str, Any]]]:
    """
    Get the current status of a file in the processing pipeline.

    Args:
        bucket_name: Name of the S3 bucket
        object_key: S3 object key (path in the bucket)

    Returns:
        Tuple[Optional[FileStatus], Optional[Dict[str, Any]]]:
            A tuple containing the file status and metadata, or (None, None) if not found
    """
    s3_client = create_s3_client()

    try:
        # Check if the file exists in any of the status folders
        for status in FileStatus:
            # Extract the filename from the object key
            filename = os.path.basename(object_key)

            # Determine the layer from the object key
            if "/raw/" in object_key:
                layer = "raw"
            elif "/bronze/" in object_key:
                layer = "bronze"
            elif "/silver/" in object_key:
                layer = "silver"
            elif "/gold/" in object_key:
                layer = "gold"
            else:
                layer = "raw"  # Default to raw if not found

            # Check if the file exists in this status folder
            status_key = f"{layer}/{status.value}/{filename}"

            try:
                response = s3_client.head_object(Bucket=bucket_name, Key=status_key)

                # File found in this status folder
                return status, response.get("Metadata", {})
            except ClientError as e:
                # File not found in this status folder, continue checking
                if e.response["Error"]["Code"] == "404":
                    continue
                else:
                    raise

        # File not found in any status folder
        return None, None
    except ClientError as e:
        logger.error(f"Failed to get status of {object_key}: {str(e)}")
        return None, None


def list_files_by_status(
    bucket_name: str, status: FileStatus, layer: str = "raw"
) -> List[Dict[str, Any]]:
    """
    List all files with a specific status in a layer.

    Args:
        bucket_name: Name of the S3 bucket
        status: Status to filter by (from FileStatus enum)
        layer: Data layer (raw, bronze, silver, gold)

    Returns:
        List[Dict[str, Any]]: List of files with their metadata
    """
    s3_client = create_s3_client()

    try:
        # List objects in the status folder
        prefix = f"{layer}/{status.value}/"
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        files = []

        # Process each object
        for obj in response.get("Contents", []):
            # Get the object metadata
            metadata_response = s3_client.head_object(
                Bucket=bucket_name, Key=obj["Key"]
            )

            # Add the file info to the list
            files.append(
                {
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "metadata": metadata_response.get("Metadata", {}),
                }
            )

        return files
    except ClientError as e:
        logger.error(f"Failed to list files with status {status.value}: {str(e)}")
        return []

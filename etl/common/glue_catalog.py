"""
AWS Glue Data Catalog utilities for the e-commerce lakehouse architecture.

This module provides functions to interact with the AWS Glue Data Catalog, including:
- Creating and updating Glue databases
- Creating and updating Glue tables
- Registering Delta tables in the Glue Data Catalog
- Updating table partitions
- Retrieving table metadata
"""

import logging
import time
from typing import Dict, Optional, Any

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession

from config import (
    AWS_REGION,
    S3_BUCKET_NAME,
    GLUE_DATABASES,
    LOG_LEVEL,
    LOG_FORMAT,
)

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def create_glue_client(region_name: Optional[str] = None) -> Any:
    """
    Create and return an AWS Glue client.

    Args:
        region_name: AWS region name. If None, uses the default region from config.

    Returns:
        boto3.client: Configured Glue client
    """
    if region_name is None:
        region_name = AWS_REGION

    try:
        return boto3.client("glue", region_name=region_name)
    except Exception as e:
        logger.error(f"Failed to create Glue client: {str(e)}")
        raise


def create_database(
    database_name: str,
    description: Optional[str] = None,
    region_name: Optional[str] = None,
) -> bool:
    """
    Create a Glue Data Catalog database if it doesn't exist.

    Args:
        database_name: Name of the database to create
        description: Description of the database
        region_name: AWS region name. If None, uses the default region from config.

    Returns:
        bool: True if database was created or already exists, False otherwise
    """
    glue_client = create_glue_client(region_name)

    try:
        # Check if database already exists
        glue_client.get_database(Name=database_name)
        logger.info(f"Database {database_name} already exists")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "EntityNotFoundException":
            # Database doesn't exist, create it
            try:
                database_input = {
                    "Name": database_name,
                }

                if description:
                    database_input["Description"] = description

                glue_client.create_database(DatabaseInput=database_input)
                logger.info(f"Database {database_name} created successfully")
                return True
            except ClientError as e:
                logger.error(f"Failed to create database {database_name}: {str(e)}")
                return False
        else:
            # Other error occurred
            logger.error(f"Error checking database {database_name}: {str(e)}")
            return False


def create_all_databases() -> bool:
    """
    Create all databases defined in the configuration.

    Returns:
        bool: True if all databases were created successfully, False otherwise
    """
    success = True

    for layer, database_name in GLUE_DATABASES.items():
        description = f"E-commerce lakehouse {layer} layer database"
        if not create_database(database_name, description):
            success = False

    return success


def register_delta_table(
    spark: SparkSession,
    table_name: str,
    table_path: str,
    database_name: Optional[str] = None,
    description: Optional[str] = None,
    layer: str = "bronze",
    bucket_name: Optional[str] = None,
    columns_description: Optional[Dict[str, str]] = None,
) -> bool:
    """
    Register a Delta table in the Glue Data Catalog.

    Args:
        spark: Spark session
        table_name: Name of the table to register
        table_path: Path to the Delta table (without s3:// prefix)
        database_name: Name of the database. If None, uses the default for the layer.
        description: Description of the table
        layer: Data layer (bronze, silver, gold)
        bucket_name: S3 bucket name. If None, uses the default from config.
        columns_description: Dictionary mapping column names to descriptions

    Returns:
        bool: True if table was registered successfully, False otherwise
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    if database_name is None:
        database_name = GLUE_DATABASES.get(layer)
        if not database_name:
            logger.error(f"No database defined for layer: {layer}")
            return False

    # Ensure the path doesn't start with a slash
    if table_path.startswith("/"):
        table_path = table_path[1:]

    # Construct the full S3 path
    full_path = f"s3a://{bucket_name}/{table_path}"

    try:
        # Create the database if it doesn't exist
        if not create_database(database_name):
            logger.error(f"Failed to create database {database_name}")
            return False

        # Register the table using Spark SQL
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
        USING DELTA
        LOCATION '{full_path}'
        """

        spark.sql(create_table_sql)

        # Add table description if provided
        if description:
            spark.sql(
                f"COMMENT ON TABLE {database_name}.{table_name} IS '{description}'"
            )

        # Add column descriptions if provided
        if columns_description:
            for column_name, column_description in columns_description.items():
                spark.sql(
                    f"COMMENT ON COLUMN {database_name}.{table_name}.{column_name} IS '{column_description}'"
                )

        logger.info(f"Successfully registered Delta table {database_name}.{table_name}")
        return True
    except Exception as e:
        logger.error(
            f"Failed to register Delta table {database_name}.{table_name}: {str(e)}"
        )
        return False


def update_table_partitions(
    table_name: str,
    database_name: Optional[str] = None,
    layer: str = "bronze",
    region_name: Optional[str] = None,
) -> bool:
    """
    Update the partitions of a table in the Glue Data Catalog.

    Args:
        table_name: Name of the table
        database_name: Name of the database. If None, uses the default for the layer.
        layer: Data layer (bronze, silver, gold)
        region_name: AWS region name. If None, uses the default region from config.

    Returns:
        bool: True if partitions were updated successfully, False otherwise
    """
    if database_name is None:
        database_name = GLUE_DATABASES.get(layer)
        if not database_name:
            logger.error(f"No database defined for layer: {layer}")
            return False

    glue_client = create_glue_client(region_name)

    try:
        # Start a partition update
        response = glue_client.start_partition_detection_run(
            DatabaseName=database_name,
            TableName=table_name,
        )

        # Get the job run ID
        job_run_id = response["JobRunId"]
        logger.info(
            f"Started partition update for {database_name}.{table_name} with job ID: {job_run_id}"
        )

        # Wait for the job to complete
        status = "STARTING"
        while status in ["STARTING", "RUNNING"]:
            time.sleep(5)
            response = glue_client.get_partition_detection_run(JobRunId=job_run_id)
            status = response["Status"]

        if status == "SUCCEEDED":
            logger.info(
                f"Successfully updated partitions for {database_name}.{table_name}"
            )
            return True
        else:
            logger.error(
                f"Failed to update partitions for {database_name}.{table_name}: {status}"
            )
            return False
    except ClientError as e:
        logger.error(
            f"Error updating partitions for {database_name}.{table_name}: {str(e)}"
        )
        return False


def get_table_metadata(
    table_name: str,
    database_name: Optional[str] = None,
    layer: str = "bronze",
    region_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Get metadata for a table from the Glue Data Catalog.

    Args:
        table_name: Name of the table
        database_name: Name of the database. If None, uses the default for the layer.
        layer: Data layer (bronze, silver, gold)
        region_name: AWS region name. If None, uses the default region from config.

    Returns:
        Optional[Dict[str, Any]]: Table metadata or None if table doesn't exist
    """
    if database_name is None:
        database_name = GLUE_DATABASES.get(layer)
        if not database_name:
            logger.error(f"No database defined for layer: {layer}")
            return None

    glue_client = create_glue_client(region_name)

    try:
        response = glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name,
        )
        return response["Table"]
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "EntityNotFoundException":
            logger.warning(f"Table {database_name}.{table_name} not found")
            return None
        else:
            logger.error(
                f"Error getting metadata for {database_name}.{table_name}: {str(e)}"
            )
            return None

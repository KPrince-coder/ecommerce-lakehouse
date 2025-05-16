"""
ETL utilities for the e-commerce lakehouse architecture.

This module provides common utilities for ETL operations, including:
- Data validation and quality checks
- Schema management and evolution
- File processing and tracking
- Error handling and logging
- Common transformations
"""

import logging
from typing import Dict, List, Optional, Tuple, Callable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, input_file_name
from pyspark.sql.types import StructType

from etl.common.s3_utils import FileStatus, update_file_status
from config import (
    S3_BUCKET_NAME,
    LOG_LEVEL,
    LOG_FORMAT,
    SCHEMA_VALIDATION,
)

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)


def add_metadata_columns(
    df: DataFrame,
    layer: str = "bronze",
    source_file_column: bool = True,
    ingestion_timestamp_column: bool = True,
    processing_timestamp_column: bool = True,
    layer_column: bool = True,
) -> DataFrame:
    """
    Add metadata columns to a DataFrame.

    Args:
        df: Spark DataFrame
        layer: Data layer (bronze, silver, gold)
        source_file_column: Whether to add source_file column
        ingestion_timestamp_column: Whether to add ingestion_timestamp column
        processing_timestamp_column: Whether to add processing_timestamp column
        layer_column: Whether to add layer column

    Returns:
        DataFrame: DataFrame with metadata columns added
    """
    result_df = df

    # Add source file column
    if source_file_column:
        result_df = result_df.withColumn("source_file", input_file_name())

    # Add timestamp columns
    now = current_timestamp()

    if ingestion_timestamp_column:
        result_df = result_df.withColumn("ingestion_timestamp", now)

    if processing_timestamp_column:
        result_df = result_df.withColumn("processing_timestamp", now)

    # Add layer column
    if layer_column:
        result_df = result_df.withColumn("layer", lit(layer))

    return result_df


def validate_schema(
    df: DataFrame,
    expected_schema: StructType,
    strict: bool = False,
) -> Tuple[bool, Optional[str], DataFrame]:
    """
    Validate the schema of a DataFrame against an expected schema.

    Args:
        df: Spark DataFrame to validate
        expected_schema: Expected schema
        strict: Whether to require exact schema match (True) or allow additional columns (False)

    Returns:
        Tuple[bool, Optional[str], DataFrame]:
            - Success flag
            - Error message (if any)
            - DataFrame with the expected schema (if successful) or original DataFrame (if failed)
    """
    if not SCHEMA_VALIDATION:
        # Schema validation is disabled in config
        return True, None, df

    actual_schema = df.schema
    actual_fields = {field.name: field for field in actual_schema.fields}
    expected_fields = {field.name: field for field in expected_schema.fields}

    # Check for missing fields
    missing_fields = [
        field_name
        for field_name in expected_fields.keys()
        if field_name not in actual_fields
    ]

    if missing_fields:
        error_msg = f"Missing fields in schema: {', '.join(missing_fields)}"
        logger.error(error_msg)
        return False, error_msg, df

    # Check for extra fields
    extra_fields = [
        field_name
        for field_name in actual_fields.keys()
        if field_name not in expected_fields
    ]

    if strict and extra_fields:
        error_msg = f"Extra fields in schema: {', '.join(extra_fields)}"
        logger.error(error_msg)
        return False, error_msg, df

    # Check field types
    type_mismatches = []
    for field_name, expected_field in expected_fields.items():
        actual_field = actual_fields[field_name]
        if str(actual_field.dataType) != str(expected_field.dataType):
            type_mismatches.append(
                f"{field_name}: expected {expected_field.dataType}, got {actual_field.dataType}"
            )

    if type_mismatches:
        error_msg = f"Schema type mismatches: {', '.join(type_mismatches)}"
        logger.error(error_msg)
        return False, error_msg, df

    # If we get here, the schema is valid
    # Select only the expected columns in the expected order if strict mode
    if strict:
        result_df = df.select(*[col(field.name) for field in expected_schema.fields])
    else:
        # Keep all columns but ensure expected columns are cast to the expected type
        for field_name, expected_field in expected_fields.items():
            df = df.withColumn(
                field_name, col(field_name).cast(expected_field.dataType)
            )
        result_df = df

    return True, None, result_df


def process_file_with_tracking(
    spark: SparkSession,
    file_path: str,
    process_func: Callable[[SparkSession, str], DataFrame],
    bucket_name: Optional[str] = None,
    layer: str = "raw",
) -> Tuple[bool, Optional[DataFrame], Optional[str]]:
    """
    Process a file with status tracking.

    Args:
        spark: Spark session
        file_path: Path to the file to process
        process_func: Function that processes the file and returns a DataFrame
        bucket_name: S3 bucket name. If None, uses the default from config.
        layer: Data layer (raw, bronze, silver, gold)

    Returns:
        Tuple[bool, Optional[DataFrame], Optional[str]]:
            - Success flag
            - Processed DataFrame (if successful) or None (if failed)
            - Error message (if failed) or None (if successful)
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    try:
        # Mark file as being processed
        update_file_status(bucket_name, file_path, FileStatus.PROCESSING, layer)

        # Process the file
        result_df = process_func(spark, file_path)

        # Mark file as processed
        update_file_status(bucket_name, file_path, FileStatus.PROCESSED, layer)

        return True, result_df, None
    except Exception as e:
        error_message = str(e)
        logger.error(f"Error processing file {file_path}: {error_message}")

        # Mark file as failed
        update_file_status(
            bucket_name, file_path, FileStatus.FAILED, layer, error_message
        )

        return False, None, error_message


def validate_data_quality(
    df: DataFrame,
    rules: Dict[str, Callable[[DataFrame], Tuple[bool, Optional[str]]]],
) -> Tuple[bool, Dict[str, str]]:
    """
    Validate data quality using a set of rules.

    Args:
        df: Spark DataFrame to validate
        rules: Dictionary mapping rule names to rule functions.
               Each rule function should return (success, error_message).

    Returns:
        Tuple[bool, Dict[str, str]]:
            - Overall success flag
            - Dictionary mapping rule names to error messages for failed rules
    """
    failed_rules = {}

    for rule_name, rule_func in rules.items():
        success, error_message = rule_func(df)
        if not success:
            failed_rules[rule_name] = error_message

    return len(failed_rules) == 0, failed_rules


def archive_processed_file(
    bucket_name: str,
    file_path: str,
    layer: str = "raw",
) -> bool:
    """
    Archive a processed file.

    Args:
        bucket_name: S3 bucket name
        file_path: Path to the file to archive
        layer: Data layer (raw, bronze, silver, gold)

    Returns:
        bool: True if file was archived successfully, False otherwise
    """
    try:
        # Mark file as archived
        return update_file_status(bucket_name, file_path, FileStatus.ARCHIVED, layer)
    except Exception as e:
        logger.error(f"Error archiving file {file_path}: {str(e)}")
        return False


def get_delta_table_path(
    table_name: str,
    layer: str,
    bucket_name: Optional[str] = None,
) -> str:
    """
    Get the S3 path for a Delta table.

    Args:
        table_name: Name of the table
        layer: Data layer (bronze, silver, gold)
        bucket_name: S3 bucket name. If None, uses the default from config.

    Returns:
        str: S3 path for the Delta table
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    return f"s3a://{bucket_name}/{layer}/{table_name}/"


def create_empty_delta_table(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
    layer: str,
    partition_by: Optional[List[str]] = None,
    bucket_name: Optional[str] = None,
) -> bool:
    """
    Create an empty Delta table with the specified schema.

    Args:
        spark: Spark session
        table_name: Name of the table
        schema: Schema for the table
        layer: Data layer (bronze, silver, gold)
        partition_by: Columns to partition by
        bucket_name: S3 bucket name. If None, uses the default from config.

    Returns:
        bool: True if table was created successfully, False otherwise
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    table_path = get_delta_table_path(table_name, layer, bucket_name)

    try:
        # Create an empty DataFrame with the specified schema
        empty_df = spark.createDataFrame([], schema)

        # Add metadata columns
        empty_df = add_metadata_columns(empty_df, layer)

        # Write the empty DataFrame to Delta
        writer = empty_df.write.format("delta").mode("overwrite")

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(table_path)

        logger.info(f"Created empty Delta table at {table_path}")
        return True
    except Exception as e:
        logger.error(f"Error creating empty Delta table at {table_path}: {str(e)}")
        return False

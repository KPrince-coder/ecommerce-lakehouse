"""
Spark session utility for the e-commerce lakehouse architecture.

This module provides functions to create and configure Spark sessions with Delta Lake
for the e-commerce lakehouse architecture. It includes:
- Creating a Spark session with appropriate configurations
- Setting up Delta Lake integration
- Configuring logging and other Spark properties
- Helper functions for common Spark operations
"""

import logging
from typing import Dict, List, Optional, Any, Union

from pyspark.sql import SparkSession

from config import (
    AWS_REGION,
    S3_BUCKET_NAME,
    DELTA_TABLE_PROPERTIES,
    LOG_LEVEL,
)

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def create_spark_session(
    app_name: str = "E-Commerce Lakehouse",
    master: str = "local[*]",
    config_props: Optional[Dict[str, str]] = None,
    enable_hive_support: bool = True,
    enable_delta: bool = True,
    log_level: str = "WARN",
    additional_packages: Optional[List[str]] = None,
) -> SparkSession:
    """
    Create and configure a Spark session with Delta Lake support.

    Args:
        app_name: Name of the Spark application
        master: Spark master URL (local[*] for local mode, yarn for YARN cluster)
        config_props: Additional configuration properties for Spark
        enable_hive_support: Whether to enable Hive support
        enable_delta: Whether to enable Delta Lake support
        log_level: Log level for Spark (WARN, INFO, DEBUG, etc.)
        additional_packages: Additional packages to include

    Returns:
        SparkSession: Configured Spark session
    """
    # Start building the Spark session
    builder = SparkSession.builder.appName(app_name).master(master)

    # Add default configurations
    default_configs = {
        # General Spark configs
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.adaptive.optimizeSkewedJoin": "true",
        # AWS configs
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "spark.hadoop.fs.s3a.endpoint": f"s3.{AWS_REGION}.amazonaws.com",
        "spark.hadoop.fs.s3a.path.style.access": "false",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "true",
        # Delta Lake configs
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.databricks.delta.optimizeWrite.enabled": "true",
        "spark.databricks.delta.autoCompact.enabled": "true",
        "spark.databricks.delta.merge.repartitionBeforeWrite.enabled": "true",
    }

    # Add Delta table properties from config
    for key, value in DELTA_TABLE_PROPERTIES.items():
        default_configs[key] = value

    # Add user-provided configs, overriding defaults if needed
    if config_props:
        default_configs.update(config_props)

    # Apply all configurations
    for key, value in default_configs.items():
        builder = builder.config(key, value)

    # Enable Hive support if requested
    if enable_hive_support:
        builder = builder.enableHiveSupport()

    # Configure Delta Lake if enabled
    if enable_delta:
        # Import Delta Lake packages
        try:
            # These imports are needed to ensure Delta Lake is properly initialized
            from delta import configure_spark_with_delta_pip

            # Configure Spark with Delta
            builder = configure_spark_with_delta_pip(builder)
            logger.info("Delta Lake support enabled")
        except ImportError as e:
            logger.warning(f"Failed to import Delta Lake packages: {str(e)}")
            logger.warning("Delta Lake support may not be fully enabled")

    # Create the Spark session
    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel(log_level)

    # Log Spark session creation
    logger.info(f"Created Spark session with app name: {app_name}")

    return spark


def read_delta_table(
    spark: SparkSession,
    table_path: str,
    bucket_name: Optional[str] = None,
) -> Any:
    """
    Read a Delta table from S3.

    Args:
        spark: Spark session
        table_path: Path to the Delta table (without s3:// prefix)
        bucket_name: S3 bucket name. If None, uses the default from config.

    Returns:
        DataFrame: Spark DataFrame containing the Delta table data
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    # Ensure the path doesn't start with a slash
    if table_path.startswith("/"):
        table_path = table_path[1:]

    # Construct the full S3 path
    full_path = f"s3a://{bucket_name}/{table_path}"

    try:
        # Read the Delta table
        df = spark.read.format("delta").load(full_path)
        logger.info(f"Successfully read Delta table from {full_path}")
        return df
    except Exception as e:
        logger.error(f"Failed to read Delta table from {full_path}: {str(e)}")
        raise


def write_delta_table(
    df: Any,
    table_path: str,
    mode: str = "overwrite",
    partition_by: Optional[Union[str, List[str]]] = None,
    z_order_by: Optional[Union[str, List[str]]] = None,
    bucket_name: Optional[str] = None,
    table_properties: Optional[Dict[str, str]] = None,
) -> None:
    """
    Write a DataFrame to a Delta table in S3.

    Args:
        df: Spark DataFrame to write
        table_path: Path to the Delta table (without s3:// prefix)
        mode: Write mode (overwrite, append, etc.)
        partition_by: Column(s) to partition by
        z_order_by: Column(s) to Z-order by (for optimization)
        bucket_name: S3 bucket name. If None, uses the default from config.
        table_properties: Additional Delta table properties

    Returns:
        None
    """
    if bucket_name is None:
        bucket_name = S3_BUCKET_NAME

    # Ensure the path doesn't start with a slash
    if table_path.startswith("/"):
        table_path = table_path[1:]

    # Construct the full S3 path
    full_path = f"s3a://{bucket_name}/{table_path}"

    try:
        # Start the write operation
        writer = df.write.format("delta").mode(mode)

        # Add partitioning if specified
        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)

        # Add table properties if specified
        if table_properties:
            for key, value in table_properties.items():
                writer = writer.option(key, value)

        # Write the Delta table
        writer.save(full_path)

        # Optimize with Z-ordering if specified
        if z_order_by:
            if isinstance(z_order_by, str):
                z_order_by = [z_order_by]

            # Get the Spark session from the DataFrame
            spark = df.sparkSession

            # Execute OPTIMIZE command
            z_order_cols = ", ".join(z_order_by)
            spark.sql(f"OPTIMIZE delta.`{full_path}` ZORDER BY ({z_order_cols})")

        logger.info(f"Successfully wrote Delta table to {full_path}")
    except Exception as e:
        logger.error(f"Failed to write Delta table to {full_path}: {str(e)}")
        raise

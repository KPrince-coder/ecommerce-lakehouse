# Common ETL Utilities

This directory contains common utilities used across the ETL processes for the e-commerce lakehouse architecture.

## S3 Utilities (`s3_utils.py`)

The `s3_utils.py` module provides helper functions for common S3 operations:

- Creating buckets
- Creating prefix structures
- Uploading files
- Checking if objects exist
- Listing objects
- Tracking file processing status

### Key Features

- Type hints for all functions and parameters
- Comprehensive error handling
- Detailed logging
- Clean, easy-to-understand syntax
- Single-responsibility principle

### Usage Examples

#### Creating an S3 Bucket

```python
from etl.common.s3_utils import create_bucket

# Create a bucket in the default region
success = create_bucket('my-bucket')

# Create a bucket in a specific region
success = create_bucket('my-bucket', 'us-west-2')
```

#### Creating a Prefix Structure

```python
from etl.common.s3_utils import create_prefix_structure

# Create prefixes in a bucket
prefixes = ['raw/', 'bronze/', 'silver/', 'gold/']
success = create_prefix_structure('my-bucket', prefixes)
```

#### Uploading Files

```python
from etl.common.s3_utils import upload_file, upload_directory

# Upload a single file
success = upload_file('data.csv', 'my-bucket', 'raw/data.csv')

# Upload an entire directory
results = upload_directory('data_dir', 'my-bucket', 'raw')
```

#### Tracking File Processing Status

```python
from etl.common.s3_utils import FileStatus, update_file_status, get_file_status, list_files_by_status

# Mark a file as being processed
update_file_status('my-bucket', 'raw/products/data.csv', FileStatus.PROCESSING)

# Mark a file as successfully processed
update_file_status('my-bucket', 'raw/products/data.csv', FileStatus.PROCESSED)

# Mark a file as failed with an error message
update_file_status('my-bucket', 'raw/products/data.csv', FileStatus.FAILED, error_message="Invalid data format")

# Archive a file after processing
update_file_status('my-bucket', 'raw/products/data.csv', FileStatus.ARCHIVED)

# Get the current status of a file
status, metadata = get_file_status('my-bucket', 'raw/products/data.csv')

# List all files with a specific status
failed_files = list_files_by_status('my-bucket', FileStatus.FAILED, layer='bronze')
```

## Spark Session Utilities (`spark_session.py`)

The `spark_session.py` module provides functions to create and configure Spark sessions with Delta Lake support:

- Creating a Spark session with appropriate configurations
- Setting up Delta Lake integration
- Reading and writing Delta tables
- Optimizing Delta tables

### Spark Session Examples

```python
from etl.common.spark_session import create_spark_session, read_delta_table, write_delta_table

# Create a Spark session
spark = create_spark_session(
    app_name="My ETL Job",
    enable_hive_support=True,
    enable_delta=True
)

# Read a Delta table
orders_df = read_delta_table(spark, "bronze/orders")

# Write a Delta table
write_delta_table(
    df=processed_df,
    table_path="silver/orders",
    mode="overwrite",
    partition_by="date"
)
```

## Glue Data Catalog Utilities (`glue_catalog.py`)

The `glue_catalog.py` module provides functions to interact with the AWS Glue Data Catalog:

- Creating and updating Glue databases
- Registering Delta tables in the Glue Data Catalog
- Updating table partitions
- Retrieving table metadata

### Glue Catalog Examples

```python
from etl.common.glue_catalog import (
    create_database,
    register_delta_table,
    update_table_partitions,
    get_table_metadata
)

# Create a database
create_database("ecommerce_bronze", "Bronze layer database")

# Register a Delta table
register_delta_table(
    spark=spark,
    table_name="orders",
    table_path="bronze/orders",
    database_name="ecommerce_bronze",
    description="Orders table in the bronze layer"
)

# Update table partitions
update_table_partitions("orders", database_name="ecommerce_bronze")

# Get table metadata
metadata = get_table_metadata("orders", database_name="ecommerce_bronze")
```

## ETL Utilities (`etl_utils.py`)

The `etl_utils.py` module provides common utilities for ETL operations:

- Adding metadata columns to DataFrames
- Validating schemas
- Processing files with status tracking
- Validating data quality
- Archiving processed files
- Creating empty Delta tables

### ETL Utilities Examples

```python
from etl.common.etl_utils import (
    add_metadata_columns,
    validate_schema,
    process_file_with_tracking,
    validate_data_quality,
    archive_processed_file,
    create_empty_delta_table
)

# Add metadata columns
df_with_metadata = add_metadata_columns(df, layer="bronze")

# Validate schema
success, error_msg, validated_df = validate_schema(df, expected_schema)

# Process a file with tracking
success, result_df, error_msg = process_file_with_tracking(
    spark=spark,
    file_path="raw/orders/2023-05-15.csv",
    process_func=process_orders_file
)

# Validate data quality
rules = {
    "no_null_order_ids": lambda df: (df.filter(col("order_id").isNull()).count() == 0,
                                    "Found null order_ids"),
    "positive_amounts": lambda df: (df.filter(col("amount") <= 0).count() == 0,
                                   "Found non-positive amounts")
}
success, failed_rules = validate_data_quality(df, rules)

# Archive a processed file
archive_processed_file(
    bucket_name="ecommerce-lakehouse",
    file_path="raw/orders/2023-05-15.csv"
)

# Create an empty Delta table
create_empty_delta_table(
    spark=spark,
    table_name="orders",
    schema=orders_schema,
    layer="bronze",
    partition_by=["date"]
)
```

## Integration with Configuration

All utility modules integrate with the configuration module (`config`) to ensure consistent settings across the ETL processes. This includes:

- S3 bucket names
- AWS regions
- Database names
- Logging settings
- Schema validation settings

## Error Handling and Logging

All utility functions include comprehensive error handling and logging to ensure that errors are properly captured and reported. This makes it easier to troubleshoot issues in the ETL processes.

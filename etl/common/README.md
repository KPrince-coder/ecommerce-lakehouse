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

## Future Utilities

The following utilities will be implemented in future phases:

- `spark_session.py`: Utility to create and configure Spark session with Delta Lake
- `glue_utils.py`: Utilities for Glue Data Catalog operations

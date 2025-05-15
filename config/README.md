# Configuration Package

This package contains configuration settings for the e-commerce lakehouse architecture.

## Overview

The configuration package provides a centralized location for all settings used throughout the project. This makes it easy to:

- Change settings in one place
- Override settings with environment variables
- Access settings consistently across the codebase

## Usage

### Importing Settings

You can import specific settings:

```python
from config import S3_BUCKET_NAME, AWS_REGION

print(f"Using bucket {S3_BUCKET_NAME} in region {AWS_REGION}")
```

Or import all settings:

```python
import config

print(f"Using bucket {config.S3_BUCKET_NAME} in region {config.AWS_REGION}")
```

### Getting S3 Prefixes

To get a specific S3 prefix:

```python
from config import get_prefix

# Get the prefix for raw products
raw_products_prefix = get_prefix("raw", "products")
print(f"Raw products prefix: {raw_products_prefix}")

# Get the prefix for silver orders
silver_orders_prefix = get_prefix("silver", "orders")
print(f"Silver orders prefix: {silver_orders_prefix}")
```

### Getting All Settings

To get all settings as a dictionary:

```python
from config import get_all_settings

settings = get_all_settings()
print(f"S3 bucket name: {settings['S3_BUCKET_NAME']}")
```

## Environment Variables

All settings can be overridden by environment variables with the same name prefixed with `ECOM_`. For example:

- `AWS_REGION` can be overridden by setting the `ECOM_AWS_REGION` environment variable
- `S3_BUCKET_NAME` can be overridden by setting the `ECOM_S3_BUCKET_NAME` environment variable

### Setting Environment Variables

In Bash:

```bash
export ECOM_S3_BUCKET_NAME="my-custom-bucket"
export ECOM_AWS_REGION="us-west-2"
```

In PowerShell:

```powershell
$env:ECOM_S3_BUCKET_NAME = "my-custom-bucket"
$env:ECOM_AWS_REGION = "us-west-2"
```

## Available Settings

### AWS Settings

- `AWS_REGION`: AWS region (default: "us-east-1")
- `S3_BUCKET_NAME`: S3 bucket name (default: "ecommerce-lakehouse")

### S3 Prefix Structure

- `S3_PREFIX_STRUCTURE`: Dictionary of S3 prefixes organized by layer and category
- `S3_PREFIXES`: Flattened list of all S3 prefixes

### Data Files

- `DATA_FILES`: Dictionary mapping data file names to local paths and S3 keys

### Processing Settings

- `BATCH_SIZE`: Number of records to process in a batch (default: 100)
- `PROCESSING_TIMEOUT`: Timeout for processing operations in seconds (default: 3600)

### Logging Settings

- `LOG_LEVEL`: Logging level (default: "INFO")
- `LOG_FORMAT`: Logging format (default: "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

### Glue Data Catalog Settings

- `GLUE_DATABASE_PREFIX`: Prefix for Glue databases (default: "ecommerce")
- `GLUE_DATABASES`: Dictionary mapping layer names to Glue database names

### Delta Lake Settings

- `DELTA_TABLE_PROPERTIES`: Dictionary of Delta table properties
- `SCHEMA_VALIDATION`: Whether to enable schema validation (default: true)

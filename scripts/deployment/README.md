# S3 Bucket Structure Setup

This directory contains scripts for setting up the S3 bucket structure for the e-commerce lakehouse architecture.

## Setup S3 Structure Script

The `setup_s3_structure.py` script creates the S3 bucket and sets up the appropriate prefix structure for raw, bronze, silver, and gold layers as outlined in the roadmap.

### Features

- Creates a single S3 bucket with the appropriate prefix structure
- Uploads initial data files to the raw layer
- Provides detailed logging
- Includes error handling
- Supports command-line arguments for customization

### Usage

```bash
python setup_s3_structure.py [--bucket-name ecommerce-lakehouse] [--region us-east-1] [--data-dir Data] [--skip-upload] [--skip-security]
```

#### Arguments

- `--bucket-name` (optional): Name of the S3 bucket to create (default: from config)
- `--region` (optional): AWS region where the bucket should be created (default: from config)
- `--data-dir` (optional): Directory containing the data files to upload (default: `Data`)
- `--skip-upload` (optional): Skip uploading data files
- `--skip-security` (optional): Skip configuring bucket security settings

#### Environment Variables

The bucket name and region can also be set using environment variables:

- `ECOM_S3_BUCKET_NAME`: S3 bucket name
- `ECOM_AWS_REGION`: AWS region

These environment variables are read by the configuration module (`config/settings.py`).

### Prerequisites

- AWS credentials configured
- boto3 package installed
- Data files in the specified directory

### Examples

```bash
# Create bucket and upload data using default settings from config
python setup_s3_structure.py

# Create bucket with custom name and upload data
python setup_s3_structure.py --bucket-name ecommerce-lakehouse-123456789012 --region us-east-1

# Create bucket without uploading data
python setup_s3_structure.py --skip-upload

# Create bucket without configuring security settings
python setup_s3_structure.py --skip-security

# Create bucket without uploading data and without configuring security
python setup_s3_structure.py --skip-upload --skip-security

# Specify a different data directory
python setup_s3_structure.py --data-dir /path/to/data

# Using environment variables
export ECOM_S3_BUCKET_NAME="my-custom-bucket"
export ECOM_AWS_REGION="us-west-2"
python setup_s3_structure.py
```

## S3 Bucket Structure

The script creates the following prefix structure in the S3 bucket:

```plaintext
s3://ecommerce-lakehouse-{account-id}/
├── raw/
│   ├── products/                # Raw product data
│   ├── orders/                  # Raw order data
│   ├── order_items/             # Raw order item data
│   ├── processing/              # Files currently being processed
│   ├── processed/               # Files successfully processed
│   ├── failed/                  # Files that failed processing
│   └── archive/                 # Original files after successful processing
├── bronze/
│   ├── products/                # Bronze product data
│   ├── orders/                  # Bronze order data
│   ├── order_items/             # Bronze order item data
│   ├── processing/              # Files currently being processed
│   ├── failed/                  # Files that failed processing
│   └── archive/                 # Previous versions of data
├── silver/
│   ├── products/                # Silver product data
│   ├── orders/                  # Silver order data
│   ├── order_items/             # Silver order item data
│   ├── processing/              # Files currently being processed
│   ├── failed/                  # Files that failed processing
│   └── archive/                 # Previous versions of data
├── gold/
│   ├── daily_sales/             # Gold daily sales data
│   ├── product_performance/     # Gold product performance data
│   ├── department_analytics/    # Gold department analytics data
│   ├── customer_insights/       # Gold customer insights data
│   ├── processing/              # Files currently being processed
│   ├── failed/                  # Files that failed processing
│   └── archive/                 # Previous versions of data
├── scripts/                     # ETL scripts
├── logs/                        # Execution logs
├── metadata/                    # Metadata about processing runs
└── temp/                        # Temporary files during processing
```

This structure aligns with the medallion architecture pattern described in the project roadmap and includes additional folders for tracking file processing status throughout the pipeline.

## Bucket Security Settings

The script configures the following security settings for the S3 bucket:

### Lifecycle Policies

- **Raw Archive Data**: Transitions to Standard-IA after 30 days, Glacier after 90 days, and expires after 365 days
- **Temporary Files**: Expires after 7 days
- **Failed Processing Files**: Expires after 30 days

### Access Controls

- **HTTPS Only**: Denies access over HTTP, requiring HTTPS for all requests
- **IAM Role Restrictions**: Restricts access to specific IAM roles
- **Public Access Blocking**: Blocks all public access to the bucket

### Encryption

- **Server-Side Encryption**: Enables AES-256 encryption for all objects in the bucket

These security settings ensure that the data in the bucket is properly protected and managed throughout its lifecycle.

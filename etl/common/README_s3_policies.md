# S3 Bucket Policies and Lifecycle Rules

This module provides functions to configure S3 bucket policies and lifecycle rules for the e-commerce lakehouse architecture.

## Overview

The S3 policies module provides functions to:

- Set up lifecycle policies to archive raw data after processing
- Configure access controls for different layers
- Set up bucket policies for security
- Enable server-side encryption

## Key Features

- Type hints for all functions and parameters
- Comprehensive error handling
- Detailed logging
- Clean, easy-to-understand syntax
- Single-responsibility principle

## Usage Examples

### Configure All Security Settings

To configure all security settings for a bucket:

```python
from etl.common.s3_policies import configure_bucket_security

# Configure all security settings
success = configure_bucket_security('my-bucket', 'us-east-1')
```

### Set Lifecycle Policy

To set only the lifecycle policy:

```python
from etl.common.s3_policies import set_lifecycle_policy

# Set lifecycle policy
success = set_lifecycle_policy('my-bucket', 'us-east-1')
```

### Set Bucket Policy

To set only the bucket policy:

```python
from etl.common.s3_policies import set_bucket_policy

# Set bucket policy
success = set_bucket_policy('my-bucket', 'us-east-1')
```

### Enable Encryption

To enable server-side encryption:

```python
from etl.common.s3_policies import enable_bucket_encryption

# Enable encryption
success = enable_bucket_encryption('my-bucket', 'us-east-1')
```

## Lifecycle Policies

The module configures the following lifecycle policies:

### Raw Archive Data

- Transitions to Standard-IA after 30 days
- Transitions to Glacier after 90 days
- Expires after 365 days

### Temporary Files

- Expires after 7 days

### Failed Processing Files

- Expires after 30 days

## Access Controls

The module configures the following access controls:

### HTTPS Only

- Denies access over HTTP, requiring HTTPS for all requests

### IAM Role Restrictions

- Restricts access to specific IAM roles

### Public Access Blocking

- Blocks all public access to the bucket

## Encryption

The module configures the following encryption settings:

### Server-Side Encryption

- Enables AES-256 encryption for all objects in the bucket

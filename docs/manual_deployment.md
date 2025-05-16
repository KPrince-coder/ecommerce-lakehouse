# Manual Deployment Guide

This guide explains how to manually deploy the E-commerce Lakehouse ETL pipeline to AWS using the provided deployment script.

## Prerequisites

Before you begin, make sure you have the following:

1. **AWS CLI**: Installed and configured with appropriate credentials
2. **Python 3.9+**: Installed on your system
3. **Required Python packages**: Install them using `pip install -r requirements.txt`
4. **AWS Account**: With permissions to create the following resources:
   - S3 buckets
   - IAM roles and policies
   - Glue jobs
   - Lambda functions
   - Step Functions state machines

## Deployment Steps

### 1. Configure AWS CLI

Ensure your AWS CLI is configured with the appropriate credentials:

```bash
aws configure
```

Or use a named profile:

```bash
aws configure --profile your-profile-name
```

### 2. Run the Deployment Script

The deployment script will create all the necessary AWS resources for the ETL pipeline:

```bash
python scripts/deploy_manual.py
```

If you're using a named AWS profile:

```bash
python scripts/deploy_manual.py --profile your-profile-name
```

### 3. Customizing the Deployment

You can customize the deployment by providing additional command-line arguments:

```bash
python scripts/deploy_manual.py \
  --bucket-name your-bucket-name \
  --region your-aws-region \
  --stack-name your-stack-name \
  --glue-job-prefix your-glue-job-prefix
```

Available options:

- `--profile`: AWS CLI profile to use
- `--region`: AWS region (default: from config)
- `--bucket-name`: S3 bucket name (default: from config)
- `--stack-name`: Name prefix for AWS resources (default: ecommerce-lakehouse-etl)
- `--glue-job-prefix`: Prefix for Glue job names (default: ecommerce-lakehouse)

## Deployment Process

The deployment script performs the following steps:

1. **S3 Setup**:
   - Creates an S3 bucket if it doesn't exist
   - Creates the directory structure in the bucket
   - Packages and uploads ETL scripts

2. **Glue Setup**:
   - Creates an IAM role for Glue
   - Creates Glue jobs for all ETL processes (Bronze, Silver, Gold)

3. **Lambda Setup**:
   - Creates an IAM role for Lambda
   - Creates a Lambda layer with the project configuration
   - Creates Lambda functions to trigger Glue jobs

4. **Step Functions Setup**:
   - Creates an IAM role for Step Functions
   - Creates a Step Functions state machine to orchestrate the ETL pipeline

## Running the ETL Pipeline

After deployment, you can run the ETL pipeline using the AWS CLI:

```bash
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:your-region:your-account-id:stateMachine:your-stack-name-pipeline \
  --input '{"date": "2023-05-15"}'
```

Or use the AWS Management Console:

1. Go to the Step Functions console
2. Find the state machine with the name `your-stack-name-pipeline`
3. Click "Start execution"
4. Enter the input JSON: `{"date": "2023-05-15"}`
5. Click "Start execution"

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure your AWS credentials have the necessary permissions
2. **Resource Already Exists**: The script will update existing resources when possible
3. **S3 Bucket Already Exists**: If the bucket exists but is owned by another account, use a different bucket name

### Logs and Monitoring

- **Deployment Logs**: The deployment script outputs detailed logs to the console
- **Glue Job Logs**: Available in CloudWatch Logs
- **Lambda Function Logs**: Available in CloudWatch Logs
- **Step Functions Execution Logs**: Available in the Step Functions console

## Cleanup

To clean up the resources created by the deployment script, you can use the AWS Management Console or AWS CLI to delete:

1. The Step Functions state machine
2. The Lambda functions and layers
3. The Glue jobs
4. The IAM roles
5. The S3 bucket (make sure to empty it first)

Example cleanup commands:

```bash
# Delete the Step Functions state machine
aws stepfunctions delete-state-machine --state-machine-arn arn:aws:states:your-region:your-account-id:stateMachine:your-stack-name-pipeline

# Delete Lambda functions
aws lambda delete-function --function-name your-stack-name-bronze-products
# Repeat for all Lambda functions

# Delete Lambda layer
aws lambda delete-layer-version --layer-name ecommerce-lakehouse-config --version-number 1

# Delete Glue jobs
aws glue delete-job --job-name your-glue-job-prefix-bronze-products-etl
# Repeat for all Glue jobs

# Delete IAM roles
aws iam delete-role --role-name your-stack-name-glue-role
aws iam delete-role --role-name your-stack-name-lambda-role
aws iam delete-role --role-name your-stack-name-step-functions-role

# Empty and delete S3 bucket
aws s3 rm s3://your-bucket-name --recursive
aws s3 rb s3://your-bucket-name
```

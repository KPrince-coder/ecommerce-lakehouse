# Infrastructure

This directory contains infrastructure-as-code templates for the e-commerce lakehouse architecture.

## Overview

The infrastructure for the e-commerce lakehouse architecture is defined using AWS CloudFormation templates. These templates define the AWS resources required for the lakehouse architecture, including:

- S3 buckets
- Glue Data Catalog databases and tables
- Step Functions state machines
- Lambda functions
- IAM roles and policies
- CloudWatch alarms and dashboards

## CloudFormation Templates

The `cloudformation` directory contains the following CloudFormation templates:

- `s3_buckets.yaml`: Defines the S3 bucket structure
- `glue_catalog.yaml`: Defines the Glue Data Catalog databases and tables
- `step_functions.yaml`: Defines the Step Functions state machine for ETL orchestration
- `monitoring.yaml`: Defines CloudWatch alarms and dashboards

## Deployment

The CloudFormation templates can be deployed using the AWS CLI or the AWS Management Console.

### Using the AWS CLI

```bash
# Deploy the S3 buckets template
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/s3_buckets.yaml \
  --stack-name ecommerce-lakehouse-s3 \
  --parameter-overrides BucketName=ecommerce-lakehouse-{account-id}

# Deploy the Glue Data Catalog template
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/glue_catalog.yaml \
  --stack-name ecommerce-lakehouse-glue \
  --capabilities CAPABILITY_IAM

# Deploy the Step Functions template
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/step_functions.yaml \
  --stack-name ecommerce-lakehouse-step-functions \
  --capabilities CAPABILITY_IAM

# Deploy the monitoring template
aws cloudformation deploy \
  --template-file infrastructure/cloudformation/monitoring.yaml \
  --stack-name ecommerce-lakehouse-monitoring
```

### Using the AWS Management Console

1. Open the AWS CloudFormation console
2. Click "Create stack"
3. Select "Upload a template file"
4. Choose the CloudFormation template file
5. Enter a stack name
6. Configure stack options
7. Review and create the stack

## Parameters

Each CloudFormation template includes parameters that can be customized during deployment. For example:

- `BucketName`: Name of the S3 bucket
- `Region`: AWS region
- `Environment`: Deployment environment (dev, staging, prod)
- `GlueDatabasePrefix`: Prefix for Glue databases

## Outputs

Each CloudFormation template includes outputs that provide information about the deployed resources. For example:

- `BucketName`: Name of the created S3 bucket
- `BucketArn`: ARN of the created S3 bucket
- `GlueDatabaseNames`: Names of the created Glue databases
- `StepFunctionArn`: ARN of the created Step Functions state machine

# CloudFormation Templates

This directory contains CloudFormation templates for the e-commerce lakehouse architecture.

## Templates

### S3 Buckets (`s3_buckets.yaml`)

This template defines the S3 bucket structure for the e-commerce lakehouse architecture. It creates a single S3 bucket with the appropriate prefix structure for raw, bronze, silver, and gold layers.

#### Parameters

- `BucketName`: Name of the S3 bucket
- `Region`: AWS region
- `Environment`: Deployment environment (dev, staging, prod)

#### Resources

- S3 bucket
- Bucket policy
- Lifecycle configuration

#### Outputs

- `BucketName`: Name of the created S3 bucket
- `BucketArn`: ARN of the created S3 bucket

### Glue Data Catalog (`glue_catalog.yaml`)

This template defines the Glue Data Catalog databases and tables for the e-commerce lakehouse architecture. It creates separate databases for bronze, silver, and gold layers.

#### Parameters

- `GlueDatabasePrefix`: Prefix for Glue databases
- `Environment`: Deployment environment (dev, staging, prod)

#### Resources

- Glue databases
- IAM role for Glue

#### Outputs

- `GlueDatabaseNames`: Names of the created Glue databases

### Step Functions (`step_functions.yaml`)

This template defines the Step Functions state machine for ETL orchestration. It creates a state machine that orchestrates the ETL processes for bronze, silver, and gold layers.

#### Parameters

- `Environment`: Deployment environment (dev, staging, prod)
- `BucketName`: Name of the S3 bucket
- `GlueDatabasePrefix`: Prefix for Glue databases

#### Resources

- Step Functions state machine
- Lambda functions for ETL execution
- IAM roles and policies
- CloudWatch Events rules

#### Outputs

- `StepFunctionArn`: ARN of the created Step Functions state machine
- `StepFunctionName`: Name of the created Step Functions state machine

### Monitoring (`monitoring.yaml`)

This template defines CloudWatch alarms and dashboards for monitoring the e-commerce lakehouse architecture. It creates alarms for ETL job failures and dashboards for visualizing ETL metrics.

#### Parameters

- `Environment`: Deployment environment (dev, staging, prod)
- `AlarmNotificationEmail`: Email address for alarm notifications

#### Resources

- CloudWatch alarms
- CloudWatch dashboards
- SNS topic for notifications

#### Outputs

- `DashboardURL`: URL of the created CloudWatch dashboard
- `SNSTopicArn`: ARN of the created SNS topic

## Deployment Order

The CloudFormation templates should be deployed in the following order:

1. `s3_buckets.yaml`
2. `glue_catalog.yaml`
3. `step_functions.yaml`
4. `monitoring.yaml`

This ensures that dependencies between resources are properly resolved during deployment.

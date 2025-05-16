# E-Commerce Lakehouse Deployment Guide

## CI/CD Pipeline Overview

The e-commerce lakehouse architecture uses GitHub Actions for continuous integration and continuous deployment (CI/CD). This automated pipeline ensures consistent, reliable, and repeatable deployments of the entire data infrastructure.

```mermaid
flowchart TD
    subgraph "GitHub Repository"
        A[Code Changes]
        B[Pull Request]
        C[Code Review]
        D[Merge to Main]
    end

    subgraph "CI/CD Pipeline"
        E[GitHub Actions Workflow]
        F[Code Validation]
        G[Unit Tests]
        H[Integration Tests]
        I[Build Artifacts]
        J[Deploy Infrastructure]
        K[Deploy ETL Jobs]
        L[Update Step Functions]
        M[Validate Deployment]
    end

    subgraph "AWS Environment"
        N[S3 Buckets]
        O[Glue Jobs]
        P[Step Functions]
        Q[Glue Data Catalog]
        R[IAM Roles]
        S[CloudWatch Alarms]
    end

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> H
    H --> I
    I --> J
    J --> K
    K --> L
    L --> M

    J --> N
    J --> R
    J --> S
    K --> O
    L --> P
    M --> Q
```

## GitHub Actions Workflow

The CI/CD pipeline is implemented using GitHub Actions workflows defined in YAML files in the `.github/workflows` directory.

### Main Workflow

```yaml
name: ETL Pipeline CI/CD

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:
    inputs:
      environment:
        description: 'Environment to deploy to'
        required: true
        default: 'dev'
        type: choice
        options:
          - dev
          - test
          - prod

jobs:
  lint:
    name: Lint Code
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install flake8 black mypy
        pip install -r requirements.txt

    - name: Lint with flake8
      run: |
        flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
        flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

    - name: Check formatting with black
      run: |
        black --check .

    - name: Type check with mypy
      run: |
        mypy --ignore-missing-imports etl/

  test:
    name: Run Tests
    runs-on: ubuntu-latest
    needs: lint

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install pytest pytest-cov
        pip install -r requirements.txt

    - name: Run tests
      run: |
        pytest tests/ --cov=etl --cov-report=xml

    - name: Upload coverage report
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
        fail_ci_if_error: false

  build:
    name: Build Artifacts
    runs-on: ubuntu-latest
    needs: test
    if: github.event_name == 'push' || github.event_name == 'workflow_dispatch'

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt

    - name: Create Lambda layers
      run: |
        mkdir -p build/lambda/layers
        python infrastructure/lambda/create_config_layer.py

    - name: Generate Lambda functions
      run: |
        python infrastructure/lambda/generate_lambda_functions.py

    - name: Package Lambda functions
      run: |
        mkdir -p build/lambda
        for file in infrastructure/lambda/*.py; do
          if [[ $(basename $file) != "create_config_layer.py" && $(basename $file) != "generate_lambda_functions.py" && $(basename $file) != "lambda_template.py" ]]; then
            zip -j build/lambda/$(basename ${file%.py}).zip $file
          fi
        done

    - name: Upload artifacts
      uses: actions/upload-artifact@v3
      with:
        name: etl-artifacts
        path: |
          build/
          infrastructure/step_functions/
          infrastructure/cloudformation/

  deploy-dev:
    name: Deploy to Dev
    runs-on: ubuntu-latest
    needs: build
    if: (github.event_name == 'push' && github.ref == 'refs/heads/main') || (github.event_name == 'workflow_dispatch' && github.event.inputs.environment == 'dev')
    environment: dev

    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Download artifacts
      uses: actions/download-artifact@v3
      with:
        name: etl-artifacts
        path: .

    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v2
      with:
        aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
        aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
        aws-region: ${{ secrets.AWS_REGION }}

    - name: Create S3 bucket if not exists
      run: |
        aws s3api head-bucket --bucket ${{ secrets.S3_BUCKET_NAME }} 2>/dev/null || aws s3 mb s3://${{ secrets.S3_BUCKET_NAME }} --region ${{ secrets.AWS_REGION }}

    - name: Upload artifacts to S3
      run: |
        aws s3 cp build/lambda/layers/config_layer.zip s3://${{ secrets.S3_BUCKET_NAME }}/lambda/layers/config_layer.zip
        aws s3 cp infrastructure/step_functions/etl_state_machine.json s3://${{ secrets.S3_BUCKET_NAME }}/step_functions/etl_state_machine.json
        for file in build/lambda/*.zip; do
          aws s3 cp $file s3://${{ secrets.S3_BUCKET_NAME }}/lambda/$(basename $file)
        done

    - name: Deploy CloudFormation stack
      run: |
        aws cloudformation deploy \
          --template-file infrastructure/cloudformation/etl_pipeline.yaml \
          --stack-name ${{ secrets.STACK_NAME }}-dev \
          --capabilities CAPABILITY_IAM \
          --parameter-overrides \
            S3BucketName=${{ secrets.S3_BUCKET_NAME }} \
            GlueJobPrefix=${{ secrets.GLUE_JOB_PREFIX }}-dev \
            AwsRegion=${{ secrets.AWS_REGION }} \
            ConfigLayerS3Key=lambda/layers/config_layer.zip
```

## Infrastructure as Code

The infrastructure is defined using AWS CloudFormation templates, which are deployed as part of the CI/CD pipeline.

### CloudFormation Template Structure

```plaintext
infrastructure/
├── cloudformation.yaml       # Main template
├── s3-buckets.yaml           # S3 bucket definitions
├── glue-resources.yaml       # Glue jobs and crawlers
├── step-functions.yaml       # Step Functions workflow
├── iam-roles.yaml            # IAM roles and policies
└── monitoring.yaml           # CloudWatch alarms and dashboards
```

### Main CloudFormation Template

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Description: 'E-Commerce Lakehouse Architecture'

Parameters:
  Environment:
    Type: String
    Default: dev
    AllowedValues:
      - dev
      - test
      - prod
    Description: Deployment environment

  DeploymentBucket:
    Type: String
    Description: S3 bucket containing deployment artifacts

Resources:
  # Import nested stacks
  S3Resources:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: !Sub https://s3.amazonaws.com/${DeploymentBucket}/infrastructure/s3-buckets.yaml
      Parameters:
        Environment: !Ref Environment

  IAMResources:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: !Sub https://s3.amazonaws.com/${DeploymentBucket}/infrastructure/iam-roles.yaml
      Parameters:
        Environment: !Ref Environment

  GlueResources:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: !Sub https://s3.amazonaws.com/${DeploymentBucket}/infrastructure/glue-resources.yaml
      Parameters:
        Environment: !Ref Environment
        DeploymentBucket: !Ref DeploymentBucket
      DependsOn:
        - S3Resources
        - IAMResources

  StepFunctionsResources:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: !Sub https://s3.amazonaws.com/${DeploymentBucket}/infrastructure/step-functions.yaml
      Parameters:
        Environment: !Ref Environment
      DependsOn:
        - GlueResources

  MonitoringResources:
    Type: AWS::CloudFormation::Stack
    Properties:
      TemplateURL: !Sub https://s3.amazonaws.com/${DeploymentBucket}/infrastructure/monitoring.yaml
      Parameters:
        Environment: !Ref Environment
      DependsOn:
        - StepFunctionsResources

Outputs:
  RawBucket:
    Description: Raw data bucket
    Value: !GetAtt S3Resources.Outputs.RawBucket

  ProcessedBucket:
    Description: Processed data bucket
    Value: !GetAtt S3Resources.Outputs.ProcessedBucket

  StepFunctionsArn:
    Description: Step Functions workflow ARN
    Value: !GetAtt StepFunctionsResources.Outputs.StepFunctionsArn
```

## Deployment Process

### 1. Code Changes and Pull Requests

1. Developers create feature branches from `main`
2. Code changes are made and committed
3. Pull requests are created for code review
4. Automated validation and tests run on pull requests
5. Code is reviewed and approved
6. Pull request is merged to `main`

### 2. Continuous Integration

When code is pushed to the `main` branch, the GitHub Actions workflow automatically:

1. Runs linting and code style checks
2. Executes unit tests for ETL scripts
3. Validates infrastructure templates
4. Builds deployment artifacts

### 3. Continuous Deployment

After successful validation and testing, the workflow:

1. Deploys or updates CloudFormation stacks
2. Uploads ETL scripts to S3
3. Updates Glue job definitions
4. Updates Step Functions workflow
5. Runs validation tests to ensure successful deployment

## Environment Management

The deployment pipeline supports multiple environments (development, testing, production) through parameterization.

```mermaid
flowchart LR
    A[Feature Branch] --> B[Development Environment]
    B --> C[Testing Environment]
    C --> D[Production Environment]

    subgraph "Development"
        B1[dev-raw-bucket]
        B2[dev-processed-bucket]
        B3[dev-glue-jobs]
        B4[dev-step-functions]
    end

    subgraph "Testing"
        C1[test-raw-bucket]
        C2[test-processed-bucket]
        C3[test-glue-jobs]
        C4[test-step-functions]
    end

    subgraph "Production"
        D1[prod-raw-bucket]
        D2[prod-processed-bucket]
        D3[prod-glue-jobs]
        D4[prod-step-functions]
    end

    B --> B1
    B --> B2
    B --> B3
    B --> B4

    C --> C1
    C --> C2
    C --> C3
    C --> C4

    D --> D1
    D --> D2
    D --> D3
    D --> D4
```

### Environment Configuration

Environment-specific configurations are managed through parameter files:

```plaintext
config/
├── dev/
│   ├── parameters.json
│   └── secrets.json
├── test/
│   ├── parameters.json
│   └── secrets.json
└── prod/
    ├── parameters.json
    └── secrets.json
```

## Deployment Validation

After deployment, automated validation tests ensure the infrastructure and ETL processes are functioning correctly.

### Validation Tests

1. **Infrastructure Validation**
   - Verify all resources were created successfully
   - Check IAM permissions
   - Validate bucket policies

2. **ETL Job Validation**
   - Verify Glue jobs are properly configured
   - Test job execution with sample data
   - Validate output data structure

3. **End-to-End Validation**
   - Run a complete data pipeline with test data
   - Verify data flows through all layers
   - Validate final output data

## Rollback Procedures

In case of deployment failures, the pipeline includes automated rollback procedures:

1. **CloudFormation Rollback**
   - CloudFormation automatically rolls back failed stack updates
   - Previous resource state is restored

2. **ETL Script Rollback**
   - Previous versions of ETL scripts are maintained in S3
   - Rollback script restores previous version

3. **Manual Intervention**
   - Critical failures trigger notifications
   - Runbook provides manual recovery steps

## Security Considerations

### Secrets Management

Sensitive information is managed securely:

1. **GitHub Secrets**
   - AWS credentials stored as GitHub repository secrets
   - Accessed only during workflow execution

2. **AWS Secrets Manager**
   - Database credentials and API keys stored in AWS Secrets Manager
   - Accessed by Glue jobs using IAM roles

### IAM Roles and Policies

The deployment follows the principle of least privilege:

1. **Service Roles**
   - Specific roles for Glue, Step Functions, and Lambda
   - Permissions limited to required actions

2. **Cross-Account Access**
   - If using multiple AWS accounts, cross-account roles with limited permissions

## Monitoring and Alerting

The deployment includes monitoring and alerting resources:

1. **CloudWatch Dashboards**
   - ETL job performance metrics
   - Step Functions execution metrics
   - Data quality metrics

2. **CloudWatch Alarms**
   - Job failure alerts
   - Processing time thresholds
   - Data quality thresholds

3. **SNS Topics**
   - Notification channels for different alert severities
   - Integration with email and chat systems

## Deployment Schedule

The deployment pipeline supports different deployment schedules:

1. **Continuous Deployment**
   - Every merge to `main` triggers deployment to development
   - Automated promotion to testing after validation

2. **Scheduled Deployments**
   - Production deployments scheduled during maintenance windows
   - Controlled by GitHub Actions schedule triggers

## Documentation Updates

The documentation is automatically updated as part of the deployment process:

1. **Generated Documentation**
   - Data catalog documentation generated from metadata
   - API documentation generated from code

2. **Deployment History**
   - Changelog maintained for each deployment
   - Version history tracked in documentation

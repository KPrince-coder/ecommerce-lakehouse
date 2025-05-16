# GitHub Actions Workflows

This directory contains GitHub Actions workflows for the e-commerce lakehouse architecture.

## Workflows

### CI/CD Pipeline (`ci-cd.yaml`)

This workflow implements the CI/CD pipeline for the e-commerce lakehouse architecture. It runs on push to the main branch and pull requests to the main branch.

#### Stages

1. **Lint**: Runs linting checks on the Python code
2. **Test**: Runs unit tests and integration tests
3. **Build**: Builds the Python package
4. **Deploy**: Deploys the infrastructure and ETL scripts to AWS

#### Environment Variables

The workflow uses the following environment variables:

- `AWS_REGION`: AWS region
- `S3_BUCKET_NAME`: Name of the S3 bucket
- `ENVIRONMENT`: Deployment environment (dev, staging, prod)

#### Secrets

The workflow uses the following secrets:

- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key

### Data Quality Checks (`data-quality.yaml`)

This workflow runs data quality checks on the Delta tables in the lakehouse architecture. It runs on a schedule (daily) and can also be triggered manually.

#### Stages

1. **Bronze Layer**: Runs data quality checks on bronze layer tables
2. **Silver Layer**: Runs data quality checks on silver layer tables
3. **Gold Layer**: Runs data quality checks on gold layer tables

#### Environment Variables

The workflow uses the following environment variables:

- `AWS_REGION`: AWS region
- `S3_BUCKET_NAME`: Name of the S3 bucket
- `ENVIRONMENT`: Deployment environment (dev, staging, prod)

#### Secrets

The workflow uses the following secrets:

- `AWS_ACCESS_KEY_ID`: AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: AWS secret access key

## Usage

### Setting Up Secrets

To use these workflows, you need to set up the following secrets in your GitHub repository:

1. Go to your repository on GitHub
2. Click on "Settings"
3. Click on "Secrets and variables" > "Actions"
4. Click on "New repository secret"
5. Add the following secrets:
   - `AWS_ACCESS_KEY_ID`: Your AWS access key ID
   - `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key

### Customizing Workflows

You can customize the workflows by editing the YAML files. For example, you can:

- Change the trigger events
- Add or remove stages
- Modify environment variables
- Add additional steps

### Running Workflows Manually

You can run the workflows manually by:

1. Go to your repository on GitHub
2. Click on "Actions"
3. Select the workflow you want to run
4. Click on "Run workflow"
5. Select the branch and enter any input parameters
6. Click on "Run workflow"

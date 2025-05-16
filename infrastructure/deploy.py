#!/usr/bin/env python
"""
Deploy ETL Pipeline

This script packages and deploys the ETL pipeline to AWS.
"""

import os
import sys
import shutil
import tempfile
import zipfile
import argparse
import subprocess
from pathlib import Path

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from config import S3_BUCKET_NAME, AWS_REGION


def parse_arguments() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Deploy ETL Pipeline"
    )
    parser.add_argument(
        "--bucket-name",
        type=str,
        default=S3_BUCKET_NAME,
        help=f"S3 bucket name (default: {S3_BUCKET_NAME})"
    )
    parser.add_argument(
        "--region",
        type=str,
        default=AWS_REGION,
        help=f"AWS region (default: {AWS_REGION})"
    )
    parser.add_argument(
        "--stack-name",
        type=str,
        default="ecommerce-lakehouse-etl",
        help="CloudFormation stack name (default: ecommerce-lakehouse-etl)"
    )
    parser.add_argument(
        "--glue-job-prefix",
        type=str,
        default="ecommerce-lakehouse",
        help="Prefix for Glue job names (default: ecommerce-lakehouse)"
    )
    
    return parser.parse_args()


def package_lambda_function(function_file: str, output_dir: str) -> str:
    """
    Package a Lambda function.
    
    Args:
        function_file: Path to the Lambda function file
        output_dir: Directory to save the zip file
    
    Returns:
        str: Path to the zip file
    """
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy the Lambda function file
        shutil.copy(function_file, temp_dir)
        
        # Create the zip file
        function_name = os.path.basename(function_file)
        output_path = os.path.join(output_dir, f"{os.path.splitext(function_name)[0]}.zip")
        with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(os.path.join(temp_dir, function_name), function_name)
        
        print(f"Packaged Lambda function: {output_path}")
        return output_path


def create_s3_bucket(bucket_name: str, region: str) -> None:
    """
    Create an S3 bucket if it doesn't exist.
    
    Args:
        bucket_name: S3 bucket name
        region: AWS region
    """
    try:
        # Check if the bucket exists
        subprocess.run(
            ["aws", "s3api", "head-bucket", "--bucket", bucket_name],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        print(f"S3 bucket {bucket_name} already exists")
    except subprocess.CalledProcessError:
        # Create the bucket
        subprocess.run(
            ["aws", "s3", "mb", f"s3://{bucket_name}", "--region", region],
            check=True
        )
        print(f"Created S3 bucket: {bucket_name}")


def upload_to_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    """
    Upload a file to S3.
    
    Args:
        file_path: Path to the file
        bucket_name: S3 bucket name
        s3_key: S3 key
    """
    subprocess.run(
        ["aws", "s3", "cp", file_path, f"s3://{bucket_name}/{s3_key}"],
        check=True
    )
    print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")


def deploy_cloudformation_stack(
    stack_name: str,
    template_file: str,
    bucket_name: str,
    region: str,
    glue_job_prefix: str
) -> None:
    """
    Deploy a CloudFormation stack.
    
    Args:
        stack_name: CloudFormation stack name
        template_file: Path to the CloudFormation template file
        bucket_name: S3 bucket name
        region: AWS region
        glue_job_prefix: Prefix for Glue job names
    """
    subprocess.run(
        [
            "aws", "cloudformation", "deploy",
            "--template-file", template_file,
            "--stack-name", stack_name,
            "--capabilities", "CAPABILITY_IAM",
            "--parameter-overrides",
            f"S3BucketName={bucket_name}",
            f"GlueJobPrefix={glue_job_prefix}",
            f"AwsRegion={region}",
            f"ConfigLayerS3Key=lambda/layers/config_layer.zip",
            "--region", region
        ],
        check=True
    )
    print(f"Deployed CloudFormation stack: {stack_name}")


def main():
    """Main function."""
    # Parse arguments
    args = parse_arguments()
    
    # Get the project root directory
    project_root = Path(__file__).resolve().parents[1]
    
    # Create the output directory
    output_dir = project_root / "infrastructure" / "build"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the Lambda output directory
    lambda_output_dir = output_dir / "lambda"
    os.makedirs(lambda_output_dir, exist_ok=True)
    
    # Create the Lambda layers output directory
    layers_output_dir = lambda_output_dir / "layers"
    os.makedirs(layers_output_dir, exist_ok=True)
    
    # Create the S3 bucket
    create_s3_bucket(args.bucket_name, args.region)
    
    # Create the config layer
    sys.path.append(str(project_root / "infrastructure" / "lambda"))
    from create_config_layer import create_config_layer
    config_layer_path = create_config_layer(str(layers_output_dir))
    
    # Upload the config layer to S3
    upload_to_s3(
        config_layer_path,
        args.bucket_name,
        "lambda/layers/config_layer.zip"
    )
    
    # Generate Lambda functions
    lambda_dir = project_root / "infrastructure" / "lambda"
    subprocess.run(
        ["python", str(lambda_dir / "generate_lambda_functions.py")],
        check=True
    )
    
    # Package and upload Lambda functions
    lambda_functions = [
        "bronze_products_lambda.py",
        "bronze_orders_lambda.py",
        "bronze_order_items_lambda.py",
        "silver_products_lambda.py",
        "silver_orders_lambda.py",
        "silver_order_items_lambda.py",
        "gold_product_performance_lambda.py",
        "gold_customer_insights_lambda.py"
    ]
    
    for function_name in lambda_functions:
        function_path = lambda_dir / function_name
        zip_path = package_lambda_function(str(function_path), str(lambda_output_dir))
        upload_to_s3(
            zip_path,
            args.bucket_name,
            f"lambda/{os.path.basename(zip_path)}"
        )
    
    # Upload the Step Functions state machine definition
    state_machine_path = project_root / "infrastructure" / "step_functions" / "etl_state_machine.json"
    upload_to_s3(
        str(state_machine_path),
        args.bucket_name,
        "step_functions/etl_state_machine.json"
    )
    
    # Deploy the CloudFormation stack
    template_path = project_root / "infrastructure" / "cloudformation" / "etl_pipeline.yaml"
    deploy_cloudformation_stack(
        args.stack_name,
        str(template_path),
        args.bucket_name,
        args.region,
        args.glue_job_prefix
    )
    
    print("Deployment complete!")


if __name__ == "__main__":
    main()

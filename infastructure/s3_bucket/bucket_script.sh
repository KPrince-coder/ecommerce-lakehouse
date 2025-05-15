#!/bin/bash

# Create S3 buckets
echo "Creating S3 bucket..."
aws s3 mb s3://ecommerce-lakehouse-123 --region eu-west-1

# Create folders in the S3 bucket
echo "Creating folders in the S3 bucket..."
aws s3api put-object --bucket ecommerce-lakehouse-123 --key raw/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key bronze/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key silver/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key gold/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key archive/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key rejected/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key scripts/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key libs/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key rejected/order_items/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key rejected/orders/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key rejected/products/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key gold/sales_by_department/
aws s3api put-object --bucket ecommerce-lakehouse-123 --key gold/customer_order_metrics/


echo "S3 bucket and folders created successfully."


echo "\nCreated bucket and folders in the S3 bucket:\n"

aws s3 ls s3://ecommerce-lakehouse-123 --recursive 
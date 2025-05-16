#!/usr/bin/env python
"""
Generate Lambda Functions

This script generates Lambda function files from a template for all ETL processes.
"""

from pathlib import Path

# ETL processes configuration
ETL_PROCESSES = [
    {
        "name": "Bronze Products",
        "glue_job_name": "bronze-products-etl",
        "file_name": "bronze_products_lambda.py",
        "additional_params": "",
        "additional_args": "",
    },
    {
        "name": "Bronze Orders",
        "glue_job_name": "bronze-orders-etl",
        "file_name": "bronze_orders_lambda.py",
        "additional_params": "",
        "additional_args": "",
    },
    {
        "name": "Bronze Order Items",
        "glue_job_name": "bronze-order-items-etl",
        "file_name": "bronze_order_items_lambda.py",
        "additional_params": "",
        "additional_args": "",
    },
    {
        "name": "Silver Products",
        "glue_job_name": "silver-products-etl",
        "file_name": "silver_products_lambda.py",
        "additional_params": "",
        "additional_args": "",
    },
    {
        "name": "Silver Orders",
        "glue_job_name": "silver-orders-etl",
        "file_name": "silver_orders_lambda.py",
        "additional_params": '    days_back = event.get("days_back", 1)',
        "additional_args": ",\n                '--days-back': str(days_back)",
    },
    {
        "name": "Silver Order Items",
        "glue_job_name": "silver-order-items-etl",
        "file_name": "silver_order_items_lambda.py",
        "additional_params": '    days_back = event.get("days_back", 1)',
        "additional_args": ",\n                '--days-back': str(days_back)",
    },
    {
        "name": "Gold Product Performance",
        "glue_job_name": "gold-product-performance-etl",
        "file_name": "gold_product_performance_lambda.py",
        "additional_params": '    lookback_days = event.get("product_lookback_days", 30)',
        "additional_args": ",\n                '--lookback-days': str(lookback_days)",
    },
    {
        "name": "Gold Customer Insights",
        "glue_job_name": "gold-customer-insights-etl",
        "file_name": "gold_customer_insights_lambda.py",
        "additional_params": '    lookback_days = event.get("customer_lookback_days", 90)',
        "additional_args": ",\n                '--lookback-days': str(lookback_days)",
    },
]


def generate_lambda_function(template_path, output_path, etl_process):
    """
    Generate a Lambda function file from the template.

    Args:
        template_path: Path to the template file
        output_path: Path to the output file
        etl_process: ETL process configuration
    """
    # Skip if the file already exists and is not the template
    if output_path.exists() and output_path.name != "lambda_template.py":
        print(f"Skipping existing file: {output_path}")
        return

    # Read the template
    with open(template_path, "r") as f:
        template = f.read()

    # Replace placeholders
    content = template.replace("{ETL_NAME}", etl_process["name"])
    content = content.replace("{GLUE_JOB_NAME}", etl_process["glue_job_name"])
    content = content.replace("{ADDITIONAL_PARAMS}", etl_process["additional_params"])
    content = content.replace("{ADDITIONAL_ARGS}", etl_process["additional_args"])

    # Write the output file
    with open(output_path, "w") as f:
        f.write(content)

    print(f"Generated Lambda function: {output_path}")


def main():
    """Main function."""
    # Get the directory of this script
    script_dir = Path(__file__).resolve().parent

    # Path to the template file
    template_path = script_dir / "lambda_template.py"

    # Generate Lambda functions
    for etl_process in ETL_PROCESSES:
        output_path = script_dir / etl_process["file_name"]
        generate_lambda_function(template_path, output_path, etl_process)

    print("Lambda function generation complete.")


if __name__ == "__main__":
    main()

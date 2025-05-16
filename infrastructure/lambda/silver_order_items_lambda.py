"""
Lambda function to trigger the Silver Order Items ETL Glue job.

This function is triggered by the Step Functions state machine and starts
the Glue job that runs the Silver Order Items ETL process.
"""

import json
import logging
import os
import sys
import boto3
from datetime import datetime

# Add the Lambda layer path to the Python path to import config
sys.path.append("/opt/python")

# Import project configuration
from config import (
    S3_BUCKET_NAME,
    AWS_REGION,
    LOG_LEVEL,
    LOG_FORMAT,
    GLUE_DATABASE_PREFIX,
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(getattr(logging, LOG_LEVEL))
formatter = logging.Formatter(LOG_FORMAT)
for handler in logger.handlers:
    handler.setFormatter(formatter)

# Initialize Glue client
glue_client = boto3.client("glue")

# Get environment variables or use config defaults
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "silver-order-items-etl")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", S3_BUCKET_NAME)
REGION = os.environ.get("AWS_REGION", AWS_REGION)


def lambda_handler(event, context):
    """
    Lambda function handler.

    Args:
        event: Event data from Step Functions
        context: Lambda context

    Returns:
        dict: Response with job details
    """
    logger.info(f"Received event: {json.dumps(event)}")

    # Get the date from the event or use current date
    date = event.get("date", datetime.now().strftime("%Y-%m-%d"))

    # Get additional parameters if needed

    try:
        # Start the Glue job
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                "--date": date,
                "--bucket-name": S3_BUCKET,
                "--region": REGION,
                "--glue-database-prefix": GLUE_DATABASE_PREFIX,
            },
        )

        job_run_id = response["JobRunId"]
        logger.info(f"Started Glue job silver-order-items-etl with run ID {job_run_id}")

        # Wait for the job to complete
        job_status = wait_for_job_completion(job_run_id)

        if job_status == "SUCCEEDED":
            logger.info(f"Glue job silver-order-items-etl completed successfully")
            return {
                "statusCode": 200,
                "jobRunId": job_run_id,
                "status": "SUCCEEDED",
                "message": f"Glue job silver-order-items-etl completed successfully",
            }
        else:
            logger.error(f"Glue job silver-order-items-etl failed with status {job_status}")
            return {
                "statusCode": 500,
                "jobRunId": job_run_id,
                "status": "FAILED",
                "message": f"Glue job silver-order-items-etl failed with status {job_status}",
            }
    except Exception as e:
        logger.error(f"Error starting Glue job: {str(e)}")
        return {
            "statusCode": 500,
            "status": "FAILED",
            "message": f"Error starting Glue job: {str(e)}",
        }


def wait_for_job_completion(job_run_id):
    """
    Wait for the Glue job to complete.

    Args:
        job_run_id: Glue job run ID

    Returns:
        str: Job status
    """
    import time

    while True:
        response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=job_run_id)

        status = response["JobRun"]["JobRunState"]

        if status in ["SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"]:
            return status

        logger.info(f"Job status: {status}, waiting...")
        time.sleep(30)  # Wait for 30 seconds before checking again

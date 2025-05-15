"""
Tests for S3 utility functions.

This module contains tests for the S3 utility functions in etl/common/s3_utils.py.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.common.s3_utils import (
    create_s3_client,
    create_bucket,
    create_prefix_structure,
    upload_file,
    FileStatus,
    update_file_status,
    get_file_status,
    list_files_by_status,
)


class TestS3Utils(unittest.TestCase):
    """Test cases for S3 utility functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock S3 client
        self.mock_s3_client = MagicMock()

        # Create a patch for boto3.client
        self.boto3_client_patch = patch(
            "boto3.client", return_value=self.mock_s3_client
        )
        self.mock_boto3_client = self.boto3_client_patch.start()

    def tearDown(self):
        """Tear down test fixtures."""
        # Stop the patch
        self.boto3_client_patch.stop()

    def test_create_s3_client(self):
        """Test creating an S3 client."""
        # Call the function
        client = create_s3_client()

        # Assert that boto3.client was called with the correct arguments
        self.mock_boto3_client.assert_called_once_with("s3", region_name=None)

        # Assert that the function returns the mock client
        self.assertEqual(client, self.mock_s3_client)

    def test_create_bucket_existing(self):
        """Test creating a bucket that already exists."""
        # Configure the mock to indicate that the bucket exists
        self.mock_s3_client.head_bucket.return_value = {}

        # Call the function
        result = create_bucket("test-bucket")

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that head_bucket was called with the correct arguments
        self.mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

        # Assert that create_bucket was not called
        self.mock_s3_client.create_bucket.assert_not_called()

    @patch("etl.common.s3_utils.logger")
    def test_create_bucket_new(self, mock_logger):
        """Test creating a new bucket."""
        # Configure the mock to indicate that the bucket doesn't exist
        from botocore.exceptions import ClientError

        self.mock_s3_client.head_bucket.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadBucket"
        )

        # Call the function
        result = create_bucket("test-bucket")

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that head_bucket was called with the correct arguments
        self.mock_s3_client.head_bucket.assert_called_once_with(Bucket="test-bucket")

        # Assert that create_bucket was called with the correct arguments
        self.mock_s3_client.create_bucket.assert_called_once_with(Bucket="test-bucket")

        # Assert that the logger was called with the correct message
        mock_logger.info.assert_called_once_with(
            "Bucket test-bucket created successfully"
        )

    def test_create_prefix_structure(self):
        """Test creating a prefix structure."""
        # Call the function
        result = create_prefix_structure("test-bucket", ["raw/", "bronze/", "silver/"])

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that put_object was called for each prefix
        self.assertEqual(self.mock_s3_client.put_object.call_count, 3)

        # Assert that put_object was called with the correct arguments
        self.mock_s3_client.put_object.assert_any_call(Bucket="test-bucket", Key="raw/")
        self.mock_s3_client.put_object.assert_any_call(
            Bucket="test-bucket", Key="bronze/"
        )
        self.mock_s3_client.put_object.assert_any_call(
            Bucket="test-bucket", Key="silver/"
        )

    @patch("pathlib.Path.exists", return_value=True)
    def test_upload_file(self, mock_exists):
        """Test uploading a file."""
        # Call the function
        result = upload_file("test.csv", "test-bucket", "raw/test.csv")

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that upload_file was called with the correct arguments
        self.mock_s3_client.upload_file.assert_called_once_with(
            "test.csv", "test-bucket", "raw/test.csv"
        )

    @patch("pathlib.Path.exists", return_value=False)
    @patch("etl.common.s3_utils.logger")
    def test_upload_file_not_exists(self, mock_logger, mock_exists):
        """Test uploading a file that doesn't exist."""
        # Call the function
        result = upload_file("test.csv", "test-bucket", "raw/test.csv")

        # Assert that the function returns False
        self.assertFalse(result)

        # Assert that upload_file was not called
        self.mock_s3_client.upload_file.assert_not_called()

        # Assert that the logger was called with the correct message
        mock_logger.error.assert_called_once_with("File test.csv does not exist")

    @patch("etl.common.s3_utils.datetime")
    def test_update_file_status_processing(self, mock_datetime):
        """Test updating a file status to PROCESSING."""
        # Configure the mock datetime
        mock_datetime.utcnow.return_value.isoformat.return_value = "2023-05-15T12:34:56"

        # Call the function
        result = update_file_status(
            "test-bucket", "raw/products/test.csv", FileStatus.PROCESSING
        )

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that copy_object was called with the correct arguments
        self.mock_s3_client.copy_object.assert_called_once()
        call_args = self.mock_s3_client.copy_object.call_args[1]
        self.assertEqual(call_args["CopySource"], "test-bucket/raw/products/test.csv")
        self.assertEqual(call_args["Bucket"], "test-bucket")
        self.assertEqual(call_args["Key"], "raw/processing/test.csv")
        self.assertEqual(call_args["MetadataDirective"], "REPLACE")

        # Assert that delete_object was called with the correct arguments
        self.mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket", Key="raw/products/test.csv"
        )

    @patch("etl.common.s3_utils.datetime")
    def test_update_file_status_archived(self, mock_datetime):
        """Test updating a file status to ARCHIVED."""
        # Configure the mock datetime
        mock_datetime.utcnow.return_value.isoformat.return_value = "2023-05-15T12:34:56"

        # Call the function
        result = update_file_status(
            "test-bucket", "raw/products/test.csv", FileStatus.ARCHIVED
        )

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that copy_object was called with the correct arguments
        self.mock_s3_client.copy_object.assert_called_once()

        # Assert that delete_object was NOT called (archived files should be kept)
        self.mock_s3_client.delete_object.assert_not_called()

    @patch("etl.common.s3_utils.datetime")
    def test_update_file_status_failed_with_error(self, mock_datetime):
        """Test updating a file status to FAILED with an error message."""
        # Configure the mock datetime
        mock_datetime.utcnow.return_value.isoformat.return_value = "2023-05-15T12:34:56"

        # Call the function
        result = update_file_status(
            "test-bucket",
            "raw/products/test.csv",
            FileStatus.FAILED,
            error_message="Invalid data format",
        )

        # Assert that the function returns True
        self.assertTrue(result)

        # Assert that copy_object was called with the correct arguments
        self.mock_s3_client.copy_object.assert_called_once()
        call_args = self.mock_s3_client.copy_object.call_args[1]

        # Assert that the metadata includes the error message
        self.assertIn("error_message", call_args["Metadata"])
        self.assertEqual(call_args["Metadata"]["error_message"], "Invalid data format")

    def test_get_file_status_found(self):
        """Test getting a file status when the file is found."""
        # Configure the mock to indicate that the file exists in the PROCESSED folder
        from botocore.exceptions import ClientError

        self.mock_s3_client.head_object.side_effect = [
            ClientError(
                {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
            ),  # PROCESSING
            {
                "Metadata": {
                    "original_path": "raw/products/test.csv",
                    "status": "processed",
                    "timestamp": "2023-05-15T12:34:56",
                }
            },  # PROCESSED
        ]

        # Call the function
        status, metadata = get_file_status("test-bucket", "raw/products/test.csv")

        # Assert that the function returns the correct status and metadata
        self.assertEqual(status, FileStatus.PROCESSED)
        self.assertEqual(metadata["status"], "processed")
        self.assertEqual(metadata["original_path"], "raw/products/test.csv")

    def test_get_file_status_not_found(self):
        """Test getting a file status when the file is not found."""
        # Configure the mock to indicate that the file doesn't exist in any status folder
        from botocore.exceptions import ClientError

        self.mock_s3_client.head_object.side_effect = ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject"
        )

        # Call the function
        status, metadata = get_file_status("test-bucket", "raw/products/test.csv")

        # Assert that the function returns None for both status and metadata
        self.assertIsNone(status)
        self.assertIsNone(metadata)

    @patch("etl.common.s3_utils.datetime")
    def test_list_files_by_status(self, mock_datetime):
        """Test listing files by status."""
        # Configure the mock datetime
        mock_now = MagicMock()
        mock_now.isoformat.return_value = "2023-05-15T12:34:56"

        # Configure the mock to return a list of objects
        self.mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "raw/failed/test1.csv", "Size": 1024, "LastModified": mock_now},
                {"Key": "raw/failed/test2.csv", "Size": 2048, "LastModified": mock_now},
            ]
        }

        # Configure the mock to return metadata for each object
        self.mock_s3_client.head_object.side_effect = [
            {
                "Metadata": {
                    "original_path": "raw/products/test1.csv",
                    "status": "failed",
                    "error_message": "Invalid data format",
                }
            },
            {
                "Metadata": {
                    "original_path": "raw/products/test2.csv",
                    "status": "failed",
                    "error_message": "Missing required fields",
                }
            },
        ]

        # Call the function
        files = list_files_by_status("test-bucket", FileStatus.FAILED)

        # Assert that the function returns the correct list of files
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0]["key"], "raw/failed/test1.csv")
        self.assertEqual(files[0]["metadata"]["error_message"], "Invalid data format")
        self.assertEqual(files[1]["key"], "raw/failed/test2.csv")
        self.assertEqual(
            files[1]["metadata"]["error_message"], "Missing required fields"
        )


if __name__ == "__main__":
    unittest.main()

"""
Tests for S3 bucket policies and lifecycle rules.

This module contains tests for the S3 bucket policies and lifecycle rules in etl/common/s3_policies.py.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Add the project root to the Python path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from etl.common.s3_policies import (
    create_s3_client,
    set_lifecycle_policy,
    set_bucket_policy,
    enable_bucket_encryption,
    configure_bucket_security,
)


class TestS3Policies(unittest.TestCase):
    """Test cases for S3 bucket policies and lifecycle rules."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create a mock S3 client
        self.mock_s3_client = MagicMock()
        
        # Create a patch for boto3.client
        self.boto3_client_patch = patch('boto3.client', return_value=self.mock_s3_client)
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
        self.mock_boto3_client.assert_called_once_with('s3', region_name=None)
        
        # Assert that the function returns the mock client
        self.assertEqual(client, self.mock_s3_client)
    
    def test_set_lifecycle_policy(self):
        """Test setting lifecycle policy."""
        # Call the function
        result = set_lifecycle_policy('test-bucket', 'us-east-1')
        
        # Assert that the function returns True
        self.assertTrue(result)
        
        # Assert that put_bucket_lifecycle_configuration was called
        self.mock_s3_client.put_bucket_lifecycle_configuration.assert_called_once()
        
        # Get the arguments passed to put_bucket_lifecycle_configuration
        call_args = self.mock_s3_client.put_bucket_lifecycle_configuration.call_args[1]
        
        # Assert that the bucket name is correct
        self.assertEqual(call_args['Bucket'], 'test-bucket')
        
        # Assert that the lifecycle configuration contains the expected rules
        lifecycle_config = call_args['LifecycleConfiguration']
        self.assertIn('Rules', lifecycle_config)
        self.assertEqual(len(lifecycle_config['Rules']), 3)
        
        # Check that the rules have the expected IDs
        rule_ids = [rule['ID'] for rule in lifecycle_config['Rules']]
        self.assertIn('Archive-Raw-Data', rule_ids)
        self.assertIn('Cleanup-Temp-Files', rule_ids)
        self.assertIn('Cleanup-Failed-Files', rule_ids)
    
    def test_set_bucket_policy(self):
        """Test setting bucket policy."""
        # Call the function
        result = set_bucket_policy('test-bucket', 'us-east-1')
        
        # Assert that the function returns True
        self.assertTrue(result)
        
        # Assert that put_bucket_policy was called
        self.mock_s3_client.put_bucket_policy.assert_called_once()
        
        # Get the arguments passed to put_bucket_policy
        call_args = self.mock_s3_client.put_bucket_policy.call_args[1]
        
        # Assert that the bucket name is correct
        self.assertEqual(call_args['Bucket'], 'test-bucket')
        
        # Assert that the policy is a string
        self.assertIsInstance(call_args['Policy'], str)
    
    def test_enable_bucket_encryption(self):
        """Test enabling bucket encryption."""
        # Call the function
        result = enable_bucket_encryption('test-bucket', 'us-east-1')
        
        # Assert that the function returns True
        self.assertTrue(result)
        
        # Assert that put_bucket_encryption was called
        self.mock_s3_client.put_bucket_encryption.assert_called_once()
        
        # Get the arguments passed to put_bucket_encryption
        call_args = self.mock_s3_client.put_bucket_encryption.call_args[1]
        
        # Assert that the bucket name is correct
        self.assertEqual(call_args['Bucket'], 'test-bucket')
        
        # Assert that the encryption configuration contains the expected rules
        encryption_config = call_args['ServerSideEncryptionConfiguration']
        self.assertIn('Rules', encryption_config)
        self.assertEqual(len(encryption_config['Rules']), 1)
        
        # Check that the rule has the expected SSE algorithm
        rule = encryption_config['Rules'][0]
        self.assertIn('ApplyServerSideEncryptionByDefault', rule)
        self.assertEqual(rule['ApplyServerSideEncryptionByDefault']['SSEAlgorithm'], 'AES256')
    
    @patch('etl.common.s3_policies.set_lifecycle_policy', return_value=True)
    @patch('etl.common.s3_policies.set_bucket_policy', return_value=True)
    @patch('etl.common.s3_policies.enable_bucket_encryption', return_value=True)
    def test_configure_bucket_security(self, mock_encryption, mock_policy, mock_lifecycle):
        """Test configuring all bucket security settings."""
        # Call the function
        result = configure_bucket_security('test-bucket', 'us-east-1')
        
        # Assert that the function returns True
        self.assertTrue(result)
        
        # Assert that all the component functions were called with the correct arguments
        mock_lifecycle.assert_called_once_with('test-bucket', 'us-east-1')
        mock_policy.assert_called_once_with('test-bucket', 'us-east-1')
        mock_encryption.assert_called_once_with('test-bucket', 'us-east-1')
        
        # Assert that put_public_access_block was called
        self.mock_s3_client.put_public_access_block.assert_called_once()
        
        # Get the arguments passed to put_public_access_block
        call_args = self.mock_s3_client.put_public_access_block.call_args[1]
        
        # Assert that the bucket name is correct
        self.assertEqual(call_args['Bucket'], 'test-bucket')
        
        # Assert that all public access block settings are True
        public_access_config = call_args['PublicAccessBlockConfiguration']
        self.assertTrue(public_access_config['BlockPublicAcls'])
        self.assertTrue(public_access_config['IgnorePublicAcls'])
        self.assertTrue(public_access_config['BlockPublicPolicy'])
        self.assertTrue(public_access_config['RestrictPublicBuckets'])


if __name__ == '__main__':
    unittest.main()

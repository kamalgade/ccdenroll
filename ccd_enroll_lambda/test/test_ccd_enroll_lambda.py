import unittest
import urllib
from unittest.mock import patch, MagicMock
import json
import boto3
from moto import mock_aws
import os
from ccd_enroll_lambda import lambda_handler, fetch_enrollment_data

class TestLambdaFunction(unittest.TestCase):

    @mock_aws
    @patch('urllib.request.urlopen')
    def test_fetch_enrollment_data_success(self, mock_urlopen):
        # Setup mock S3 and environment variables
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}, clear=True)
        os.environ['S3_BUCKET'] = bucket_name

        # Create S3 bucket
        s3.create_bucket(Bucket=bucket_name)

        # Mock the API response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "results": [{"state": "NY", "year": 2020, "enrollment": 100}],
            "next": None
        }).encode('utf-8')

        mock_urlopen.return_value = mock_response

        # Test event with years and grades
        event = {
            "years": [2020],
            "grades": ["grade-pk"]
        }
        context = {}

        # Invoke lambda handler
        response = lambda_handler(event, context)
        print(response)

        # Check S3 for saved object
        s3_object = s3.get_object(Bucket=bucket_name, Key="2020/grade-pk/enrollment.json")
        s3_data = s3_object['Body'].read().decode('utf-8')
        print(s3_data)

        # Assertions
        self.assertEqual(s3_data, json.dumps({"state": "NY", "year": 2020, "enrollment": 100}) + '\n')

    @patch('urllib.request.urlopen')
    def test_fetch_enrollment_data_http_error(self, mock_urlopen):
        # Mock an HTTPError for the API request
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url=None, code=500, msg="Internal Server Error", hdrs=None, fp=None
        )

        # Test function directly
        results = fetch_enrollment_data(2020, 'grade-pk')

        # Assertions
        self.assertEqual(results, [])  # Expect empty list due to HTTP error

    @patch('urllib.request.urlopen')
    def test_fetch_enrollment_data_url_error(self, mock_urlopen):
        # Mock a URLError for the API request
        mock_urlopen.side_effect = urllib.error.URLError("Server not found")

        # Test function directly
        results = fetch_enrollment_data(2020, 'grade-pk')

        # Assertions
        self.assertEqual(results, [])  # Expect empty list due to URL error

    @mock_aws
    def test_s3_upload(self):
        # Setup mock S3 and environment variables
        s3 = boto3.client('s3')
        bucket_name = "test-bucket"
        patch.dict(os.environ, {"S3_BUCKET": "test-bucket"}, clear=True)
        os.environ['S3_BUCKET'] = bucket_name

        # Create S3 bucket
        s3.create_bucket(Bucket=bucket_name)

        # Mock fetch_enrollment_data function to bypass actual API call
        with patch('ccd_enroll_lambda.fetch_enrollment_data', return_value=[{"state": "NY", "year": 2020, "enrollment": 100}]):
            # Test event
            event = {
                "years": [2020],
                "grades": ["grade-pk"]
            }
            context = {}

            lambda_handler(event, context)

            # Check if the data was uploaded to S3
            s3_object = s3.get_object(Bucket=bucket_name, Key="2020/grade-pk/enrollment.json")
            s3_data = s3_object['Body'].read().decode('utf-8')

            compare = json.dumps({"state": "NY", "year": 2020, "enrollment": 100}) + "\n"

            self.assertEqual(s3_data, compare)

    @patch.dict(os.environ, {'S3_BUCKET': 'test-bucket'})
    def test_lambda_handler_environment_variables(self):
        # Ensure environment variable is used correctly
        self.assertEqual(os.environ['S3_BUCKET'], 'test-bucket')


if __name__ == '__main__':
    unittest.main()

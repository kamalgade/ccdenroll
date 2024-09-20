import urllib.request
import urllib.error
import json
import boto3
import os

s3 = boto3.client('s3')
BUCKET_NAME = os.environ['S3_BUCKET']  # Store bucket name as environment variable

def fetch_enrollment_data(year, grade):
    url = f"https://educationdata.urban.org/api/v1/schools/ccd/enrollment/{year}/{grade}/"
    results = []
    while url: #loop through all the pages of the results
        try:
            response = urllib.request.urlopen(url)
            data = json.loads(response.read())
            results = results + data.get('results')
            header = data.pop('results')
            url = data.get('next')
        except urllib.error.HTTPError as e:
            print("HTTP Error:", e.code, e.reason)

        except urllib.error.URLError as e:
            print("URL Error:", e.reason)

    return results


def lambda_handler(event, context):
    # the event is used to dynamically pass years and grades
    # the code supports including additional years and grades
    years = event.get('years')
    grades = event.get('grades')
    enrollments = []
    for year in years:
        for grade in grades:
            enrollments = fetch_enrollment_data(year, grade)
            file_key = f"{year}/{grade}/enrollment.json"
            json_str = None
            for obj in enrollments:
                # serialize each object and write it to the file in one line for athena to query
                json_str += json.dumps(obj)+'\n'
            
            if json_str:
                s3.put_object(Bucket=BUCKET_NAME, Key=file_key, Body=json_str)

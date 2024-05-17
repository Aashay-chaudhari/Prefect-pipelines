"""
This script is to test and understand the functionality of prefect on a whole.
"""

import boto3
from prefect import task, Flow
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Task to download an object from S3
@task
def download_from_s3(bucket_name, object_key, download_path):
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, object_key, download_path)
    print(f"Downloaded {object_key} from {bucket_name} to {download_path}")

# Define the Prefect flow
with Flow("download-s3-object") as flow:
    bucket_name = os.getenv("S3_BUCKET_NAME")
    object_key = os.getenv("S3_OBJECT_KEY")
    download_path = os.getenv("DOWNLOAD_PATH")
    download_from_s3(bucket_name, object_key, download_path)

# Run the flow
if __name__ == "__main__":
    flow.run()

import boto3
import io
from config import BUCKET_NAME

client = boto3.client('s3')

def list_s3_files(prefix):
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix)

    keys = []

    for page in pages:
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])

    return keys



def read_s3_file(key):
    response = client.get_object(Bucket=BUCKET_NAME, Key=key)
    file_stream = io.BytesIO(response['Body'].read())
    return file_stream
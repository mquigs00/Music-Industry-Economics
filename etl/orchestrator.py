import boto3
import io
from etl.schemas.billboard_magazine_3.curation.curate import curate_events, curate_event_ticket_prices
import pandas as pd
from config.config import BUCKET_NAME, STORAGE_FORMAT, STORAGE_MODE
from config.paths import LOCAL_PROCESSED_DATA_PATH, LOCAL_CURATED_DATA_PATH
import glob
import os
from etl.utils.utils import load_dimension_tables

s3 = boto3.client("s3")
prefix = "processed/billboard/magazines/"

def run_pipeline(s3_client=None):
    dimension_tables = load_dimension_tables(STORAGE_MODE)
    print("Starting pipeline ...")
    if STORAGE_MODE == "s3":
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

        if "Contents" not in response:
            print("No files found")
            return

        csv_keys = [
            obj["Key"]
            for obj in response["Contents"]
            if obj["Key"].endswith(".csv")
        ]

        csv_keys = csv_keys[:1]

        print("Found csv files:")
        for key in csv_keys:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            body = obj["Body"].read()
            csv_stream = io.BytesIO(body)
            processed_events_df = pd.read_csv(csv_stream)
            curate_events(processed_events_df, BUCKET_NAME, key,dimension_tables, s3)
    else:
        processed_events_files = glob.glob(os.path.join(LOCAL_PROCESSED_DATA_PATH, "**", "*.csv"), recursive=True)      # get all local processed files
        print("Found csv files:")

        for processed_events_file in processed_events_files:
            print("Processing events file ", processed_events_file)
            processed_events_df = pd.read_csv(processed_events_file)
            curate_events(processed_events_df, processed_events_file, dimension_tables)


if __name__ == "__main__":
    run_pipeline()
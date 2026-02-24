from config import BUCKET_NAME
from utils.s3_utils import write_s3_to_parquet
from utils.utils import read_local_csv
import boto3

from config.paths import (
    DIM_ARTISTS_PATH,
    DIM_VENUES_PATH,
    DIM_CITIES_PATH,
    DIM_STATES_PATH,
    DIM_PROMOTERS_PATH,
    DIM_SPECIAL_EVENTS_PATH,
    S3_BUCKET,
    S3_DIM_ARTISTS_PATH,
    S3_DIM_CITIES_PATH,
    S3_DIM_VENUES_PATH,
    S3_DIM_PROMOTERS_PATH,
    S3_DIM_STATES_PATH,
    S3_DIM_SPECIAL_EVENTS_PATH,
    EVENT_CORRECTIONS_PATH,
    S3_EVENT_CORRECTIONS_PATH
)

dimension_map = {
    "artists": (DIM_ARTISTS_PATH, S3_DIM_ARTISTS_PATH),
    "venues": (DIM_VENUES_PATH, S3_DIM_VENUES_PATH),
    "promoters": (DIM_PROMOTERS_PATH, S3_DIM_PROMOTERS_PATH),
    "cities": (DIM_CITIES_PATH, S3_DIM_CITIES_PATH),
    "states": (DIM_STATES_PATH, S3_DIM_STATES_PATH),
    "special_events": (DIM_SPECIAL_EVENTS_PATH, S3_DIM_SPECIAL_EVENTS_PATH)
}

corrections_map = {
    "bb_3": (EVENT_CORRECTIONS_PATH, S3_EVENT_CORRECTIONS_PATH)
}

s3_client = boto3.client("s3")

def preload_dimension_tables():
    """

    :return:
    """
    for name, (local_path, s3_key) in dimension_map.items():
        df = read_local_csv(local_path)
        write_s3_to_parquet(df, s3_client, S3_BUCKET, s3_key)
        print(f"Wrote {name} to parquet in s3")

def preload_corrections_table():
    for name, (local_path, s3_key) in corrections_map.items():
        df = read_local_csv(local_path)
        write_s3_to_parquet(df, s3_client, S3_BUCKET, s3_key)
        print(f"Wrote {name} to parquet in s3")
from config.config import BUCKET_NAME
from utils.s3_utils import write_s3_to_parquet
from utils.utils import read_local_csv
import boto3

from config.paths import (
    LOCAL_DIM_ARTISTS_PATH,
    LOCAL_DIM_VENUES_PATH,
    LOCAL_DIM_CITIES_PATH,
    LOCAL_DIM_STATES_PATH,
    LOCAL_DIM_PROMOTERS_PATH,
    LOCAL_DIM_SPECIAL_EVENTS_PATH,
    S3_DIM_ARTISTS_PATH,
    S3_DIM_CITIES_PATH,
    S3_DIM_VENUES_PATH,
    S3_DIM_PROMOTERS_PATH,
    S3_DIM_STATES_PATH,
    S3_DIM_SPECIAL_EVENTS_PATH,
    LOCAL_CORRECTION_TABLES_DIR,
    S3_EVENT_CORRECTIONS_PATH
)

dimension_map = {
    "artists": (LOCAL_DIM_ARTISTS_PATH, S3_DIM_ARTISTS_PATH),
    "venues": (LOCAL_DIM_VENUES_PATH, S3_DIM_VENUES_PATH),
    "promoters": (LOCAL_DIM_PROMOTERS_PATH, S3_DIM_PROMOTERS_PATH),
    "cities": (LOCAL_DIM_CITIES_PATH, S3_DIM_CITIES_PATH),
    "states": (LOCAL_DIM_STATES_PATH, S3_DIM_STATES_PATH),
    "special_events": (LOCAL_DIM_SPECIAL_EVENTS_PATH, S3_DIM_SPECIAL_EVENTS_PATH)
}

corrections_map = {
    "bb_3": (LOCAL_CORRECTION_TABLES_DIR, S3_EVENT_CORRECTIONS_PATH)
}

s3_client = boto3.client("s3")

def preload_dimension_tables():
    """

    :return:
    """
    for name, (local_path, s3_key) in dimension_map.items():
        df = read_local_csv(local_path)
        write_s3_to_parquet(df, s3_client, BUCKET_NAME, s3_key)
        print(f"Wrote {name} to parquet in s3")

def preload_corrections_table():
    for name, (local_path, s3_key) in corrections_map.items():
        df = read_local_csv(local_path)
        write_s3_to_parquet(df, s3_client, BUCKET_NAME, s3_key)
        print(f"Wrote {name} to parquet in s3")
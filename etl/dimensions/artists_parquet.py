from etl.utils.utils import load_dimension_tables
from config.config import STORAGE_MODE, BUCKET_NAME
from config.paths import LOCAL_DIM_ARTISTS_PATH, S3_DIM_ARTISTS_PATH
import slugify
import pandas as pd
import pyarrow.parquet as pq
import io

def append_artists_dim(s3_client, key, artist_id, name, slug):
    """
    Adds the new artist to the dimension table csv file
    :param s3_client:
    :param bucket:
    :param key:
    :param artist_id (int)
    :param name (str) the name of the artist
    :param slug (str)
    """
    if STORAGE_MODE == 's3':
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(obj['Body'], engine='pyarrow')

        new_row = pd.DataFrame([[artist_id, name, slug]], columns=df.columns)
        df = pd.concat([df, new_row], ignore_index=True)

        buffer = io.BytesIO()
        df.to_parquet(buffer, engine='pyarrow', index=False)
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, BUCKET_NAME, key)
    else:
        artists_df = pd.read_parquet(LOCAL_DIM_ARTISTS_PATH)

        new_row = pd.DataFrame([[artist_id, name, slug]], columns=artists_df.columns)
        artists_df = pd.concat([artists_df, new_row], ignore_index=True)

        artists_df.to_parquet(LOCAL_DIM_ARTISTS_PATH, index=False)

def update_artists_dim(all_artists, dim_artists):
    """
    Add any new artists to the artists dimension table

    :param all_artists: a set of all artists in the current issue
    :param dim_artists: the dictionary of existing artists
    :return: max_artist_id
    """
    max_artists_id = dim_artists["max_id"]
    existing_artists = dim_artists["by_slug"]
    for artist in all_artists:
        if not artist:
            continue
        artist_name_proper = artist.title()
        key = slugify.slugify(artist)
        if key not in existing_artists:
            max_artists_id += 1
            existing_artists[key] = {
                "id": max_artists_id,
                "name": artist,
                "slug": key,
            }
            append_artists_dim(LOCAL_DIM_ARTISTS_PATH, max_artists_id, artist_name_proper, key)

    return max_artists_id
from etl.utils.utils import load_dimension_tables
import csv
from config.config import STORAGE_MODE, STORAGE_FORMAT
from config.paths import LOCAL_DIM_ARTISTS_PATH, S3_DIM_ARTISTS_PATH
import slugify

def append_artists_dim_csv(path, artist_id, name, slug):
    """
    Adds the new artist to the dimension table csv file
    :param path (str) the path to the local file or s3 object
    :param artist_id (int)
    :param name (str) the name of the artist
    :param slug (str)
    """
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([artist_id, name, slug])

def update_artists_dim_csv(all_artists, dim_artists):
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
            artist_record = {"id": max_artists_id, "name": artist,"slug": key}
            existing_artists[key] = artist_record
            dim_artists["by_slug"][key] = [artist_record]
            dim_artists["by_id"][max_artists_id] = artist_record
            append_artists_dim_csv(LOCAL_DIM_ARTISTS_PATH, max_artists_id, artist_name_proper, key)
            dim_artists["max_id"] = max_artists_id

    return max_artists_id
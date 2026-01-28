from utils.utils import load_dimension_tables
import re
import csv
from config.paths import DIM_ARTISTS_PATH
import slugify
import ast

def get_first_artist_name_by_id(artist_ids):
    if not artist_ids:
        return None
    return get_artist_name(artist_ids[0])

def get_artist_name(artist_id):
    if isinstance(artist_id, str):
        if artist_id.isdigit():
            artist_id = int(artist_id)

    dimension_tables = load_dimension_tables()
    dim_artists_by_id = dimension_tables["artists"]["by_id"]
    artist_name = dim_artists_by_id[artist_id]["name"]
    return artist_name

def append_artists_dim(path, artist_id, name, slug):
    """
    Adds the new artist to the dimension table csv file
    :param path (str)
    :param artist_id (int)
    :param name (str) the name of the artist
    :param slug (str)
    """
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([artist_id, name, slug])

def get_artist_ids(artist_names, dim_artists):
    """
    Convert a list of artist names to their corresponding id numbers

    :param artist_names (list)
    :param dim_artists (dict)
    :return: artist_ids (list)
    """
    existing_artists = dim_artists["by_slug"]
    artist_ids = []

    for artist in artist_names:
        if artist is None:
            continue
        key = slugify.slugify(artist)
        artist_id = existing_artists[key][0]["id"]
        artist_ids.append(artist_id)

    return artist_ids

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
            append_artists_dim(DIM_ARTISTS_PATH, max_artists_id, artist_name_proper, key)

    return max_artists_id
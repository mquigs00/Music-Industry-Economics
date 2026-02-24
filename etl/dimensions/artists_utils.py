from etl.utils.utils import load_dimension_tables
from config.paths import LOCAL_DIM_ARTISTS_PATH
import slugify

def get_artist_name(artist_id, dim_artists):
    """

    :param artist_id: (int)
    :param dim_artists: (dict)
    :return:
    """
    print(f"Artist ID is type: {type(artist_id)}")
    if isinstance(artist_id, str):
        if artist_id.isdigit():
            artist_id = int(artist_id)

    dim_artists_by_id = dim_artists["by_id"]
    print("Dim Artists by ID")
    print(dim_artists_by_id)
    artist_record = dim_artists_by_id.get(artist_id)
    if artist_record is None:
        raise KeyError(f"Artist ID {artist_id} not found in artists dimension table")
    return artist_record["name"]

def get_first_artist_name_by_id(artist_ids):
    if not artist_ids:
        return None
    return get_artist_name(artist_ids[0])

def get_artist_ids(artist_names, dim_artists):
    """
    Convert a list of artist names to their corresponding id numbers

    :param artist_names (list)
    :param dim_artists (dict)
    :return: artist_ids (list)
    """
    existing_artists = dim_artists["by_slug"]
    print(existing_artists.keys())
    artist_ids = []

    for artist in artist_names:
        if artist is None:
            continue
        key = slugify.slugify(artist)

        #artist_id = existing_artists[key][0]["id"]
        artist_record = existing_artists.get(key)
        if artist_record is None:
            raise KeyError(f"Artist {artist} not found in artists dimension table")
        artist_ids.append(artist_record[0]["id"])

    return artist_ids
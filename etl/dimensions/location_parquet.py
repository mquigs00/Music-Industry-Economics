import slugify
from config.paths import LOCAL_DIM_VENUES_PATH, LOCAL_DIM_CITIES_PATH
import pandas as pd
import io

def append_venue(s3_client, bucket, key, venue_name, dim_venues, city_id, state_id):
    """
    Adds the new venue to dim_venues.csv and dim_venues dictionary

    :param s3_client:
    :param bucket:
    :param key:
    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param state_id (int)
    :return: venue_id (int)
    """
    if venue_name is None:
        print(f"Venue name {venue_name} not specified")
        return None

    venue_id = dim_venues["max_id"] + 1
    venue_slug = slugify.slugify(venue_name)
    if city_id is None:
        city_id = -1

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(obj["Body"], engine="pyarrow")

    new_row = pd.DataFrame([{
        "id": venue_id,
        "name": venue_name,
        "slug": venue_slug,
        "city_id": city_id,
        "state_id": state_id
    }])

    df = pd.concat([df, new_row], ignore_index=True)

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket, key)

    if venue_slug not in dim_venues["by_slug"]:
        dim_venues["by_slug"][venue_slug] = [{
            'id': venue_id,
            'name': venue_name,
            'slug': venue_slug,
            'city_id': city_id,
            'state_id': state_id
        }]
    else:
        dim_venues["by_slug"][venue_slug].append({
            'id': venue_id,
            'name': venue_name,
            'slug': venue_slug,
            'city_id': city_id,
            'state_id': state_id})

    dim_venues["max_id"] += 1
    return venue_id

def append_city(s3_client, bucket, key, city_candidate, dim_cities, state_id):
    '''
    Add the new city candidate
    :param city_candidate:
    :param dim_cities:
    :param state_id:
    :return:
    '''
    city_id = dim_cities["max_id"] + 1
    city_slug = slugify.slugify(city_candidate)

    obj = s3_client.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(obj["Body"], engine="pyarrow")

    new_row = pd.DataFrame([{
        "id": city_id,
        "name": city_candidate,
        "slug": city_slug,
        "state_id": state_id
    }])

    df = pd.concat([df, new_row], ignore_index=True)
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket, key)

    dim_cities["max_id"] += 1

    return city_id
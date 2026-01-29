import slugify
from config.paths import DIM_VENUES_PATH, DIM_CITIES_PATH, LOCATION_ALIASES_PATH
from data_cleaning.normalization import build_reverse_map
import csv

def append_venue(venue_name, dim_venues, city_id, state_id):
    '''
    Adds the new venue to dim_venues.csv and dim_venues dictionary

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param state_id (int)
    :return: venue_id (int)
    '''
    if venue_name is None:
        print(f"Venue name {venue_name} not specified")
        return None
    venue_id = dim_venues["max_id"] + 1
    venue_slug = slugify.slugify(venue_name)

    with open(DIM_VENUES_PATH, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([venue_id, venue_name, venue_slug, city_id, state_id])

    if venue_slug not in dim_venues["by_slug"]:
        dim_venues["by_slug"][venue_slug] = [{'id': venue_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id}]
    else:
        dim_venues["by_slug"][venue_slug].append({'id': venue_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id})

    dim_venues["max_id"] += 1
    return venue_id

def append_city(city_candidate, dim_cities, state_id):
    '''
    Add the new city candidate
    :param city_candidate:
    :param dim_cities:
    :param state_id:
    :return:
    '''
    city_id = dim_cities["max_id"] + 1
    city_slug = slugify.slugify(city_candidate)
    with open(DIM_CITIES_PATH, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([city_id, city_candidate, city_slug, None, state_id])

    dim_cities[city_slug] = {'id': city_id, 'name': city_candidate, 'slug': city_slug, 'aliases': None, 'state_id': state_id}
    dim_cities["max_id"] += 1

    return city_id
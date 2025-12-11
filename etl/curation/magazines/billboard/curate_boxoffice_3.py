import io
import re
import ast
from botocore.exceptions import ClientError
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import DIM_ARTISTS_PATH, STATE_ALIASES_PATH, VENUE_PATTERNS_PATH, DIM_CITIES_PATH, DIM_VENUES_PATH
from utils.utils import load_dimension_tables
from utils.utils import *
import pandas as pd
import os
import csv
import json
from Levenshtein import distance as levenshtein_distance
import logging
import slugify
from data_cleaning.normalization import build_reverse_map
logger = logging.getLogger()

'''
This curation script is for the Billboard Boxscore schema that ran from 1984-10-20 to 2001-07-21
'''

object_key = "processed/billboard/magazines/1984/10/BB-1984-10-20.csv"

def append_artists_dim(path, artist_id, name):
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([artist_id, name, slugify.slugify(name)])

def append_venues_dim(path, venue_id, name, city_id):
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([venue_id, name, slugify.slugify(name), city_id])

def add_weekly_ranks(events_df):
    print("Hi")

def curate_event_name(events_df):
    first_artist_line = ast.literal_eval(events_df["artists"])[0]

    if ":" in first_artist_line:
        events_df["event_name"] = first_artist_line[:first_artist_line.index(":")]
    else:
        events_df["event_name"] = None

def replace_artist_names(artist_names, existing_artists, max_artists_id):
    artist_ids = []

    for artist in artist_names:
        key = slugify.slugify(artist)
        artist_id = existing_artists.get(key)["id"]
        artist_ids.append(artist_id)

    return artist_ids

def update_artists_dim(all_artists, existing_artists, max_artists_id):
    for artist in all_artists:
        key = slugify.slugify(artist)
        if key not in existing_artists:
            max_artists_id += 1
            existing_artists[key] = {
                "id": max_artists_id,
                "name": artist,
                "slug": key,
            }
            append_artists_dim(DIM_ARTISTS_PATH, max_artists_id, artist, key)

    return max_artists_id

def curate_artists(events_df, existing_artists, max_artists_id):
    events_df["artists"] = events_df["artists"].apply(ast.literal_eval)

    # create a set of all unique artist names in the current issue
    all_artists = {
        artist
        for lst in events_df["artists"]
        for artist in lst
    }

    update_artists_dim(all_artists, existing_artists, max_artists_id)

    events_df["artists"] = events_df["artists"].apply(
        lambda artist_names: replace_artist_names(artist_names, existing_artists, max_artists_id)
    )

def match_city_after_venue(location_tokens, dim_cities):
    max_city_tokens = 3  # set maximum length of city name to 3 words/tokens
    city_id = city_index = None
    city_len = 0

    # start by checking if the first 3 words in the location are a city, then the first 2, then the first word
    for n in range(-1, -3, -1):
        candidate = " ".join(location_tokens[n:])
        candidate_slug = slugify.slugify(candidate)  # make a slug of the next n words
        #print(f"Candidate = {candidate}")

        # if a city already exists in the dimension table with the given slug
        if candidate_slug in dim_cities["data"]:
            city_id = dim_cities["data"][candidate_slug]["id"]
            city_index = location_tokens.index(candidate.split()[0])
            break  # break once a match is found

    return city_id, city_index

def find_city_candidate(location_tokens, dim_cities):
    city_candidate = city_id = pattern_idx = None
    reverse_venue_map = build_reverse_map(VENUE_PATTERNS_PATH)
    #print(reverse_venue_map)

    for index, word in reversed(list(enumerate(location_tokens))):
        clean_word = word.lower()
        if clean_word in reverse_venue_map:
            pattern_idx = index
            city_candidate = " ".join(location_tokens[index+1:]).replace(",", "")
            break

    #print(f"City candidate: {city_candidate}")
    return city_candidate, pattern_idx

def append_city(city_candidate, dim_cities, state_id):
    city_id = dim_cities["max_id"] + 1
    city_slug = slugify.slugify(city_candidate)
    with open(DIM_CITIES_PATH, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([city_id, city_candidate, city_slug, None, state_id, "no"])

    dim_cities["data"][city_slug] = {'id': city_id, 'name': city_candidate, 'slug': city_slug, 'aliases': None, state_id: state_id, 'verified': "no"}
    dim_cities["max_id"] += 1

def match_state_after_venue(location_tokens):
    '''
    Searches for a state from the end of the location tokens until it finds a venue type like 'hall' or 'auditorium'
    :param location_tokens:
    :return:
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    venue_patterns = build_reverse_map(VENUE_PATTERNS_PATH)
    state = state_idx = None
    #REMOVE_CHARS = "(){}|"
    #translator = str.maketrans("", "", REMOVE_CHARS)

    # loop through each in the rest of the location strings
    for index, token in reversed(list(enumerate(location_tokens))):
        token_clean = token.lower()
        if token_clean in venue_patterns:
            break
        if token_clean in state_aliases:                                                      # if the word is one of the possible aliases for a state
            state = state_aliases[token_clean]                                                # get the state id
            state_idx = index
            break

    return state, state_idx

def match_state_in_venue(location_tokens):
    '''
    Checks if any of the location tokens contain a state alias. Only records state if it is in bracket, parentheses, etc like
    'Charlotte (N.C.) Coliseum'
    Will not extract state name like 'Ohio Center' because many venues have states in their name but are not actually located there

    :param location_tokens: the remaining location tokens
    :return:
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    state_id = state_idx = None
    state_chars = set("(){}|")
    remove_chars = "(){}|"
    translator = str.maketrans("", "", remove_chars)

    for index, token in enumerate(location_tokens):
        if any(char in state_chars for char in token):
            state = token.translate(translator).lower()
            state_idx = index
            state_id = state_aliases[state]

    return state_id, state_idx

def match_city_in_venue(location_tokens, dim_cities, state_id):
    '''
    Searches for a city in the venue name.

    :param location_tokens:
    :param dim_cities:
    :return:
    '''
    city_id = None

    for i in range(len(location_tokens)):
        # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
        for window_size in range(1, 4):
            candidate = slugify.slugify(" ".join(location_tokens[i:i+window_size]).lower())
            if candidate in dim_cities["data"] and state_id == int(dim_cities["data"][candidate]["state_id"]):
                city_id = dim_cities["data"][candidate]["id"]
                print("Found matching city in venue name")

    return city_id

def match_existing_venues(venue_name, dim_venues, city_id, state_id):
    venue_slug = slugify.slugify(venue_name)
    venue_id = None
    if venue_slug in dim_venues:
        venue_id = dim_venues[venue_slug]["id"]

    return venue_id

def append_venue(venue_name, dim_venues, city_id, state_id):
    venue_id = dim_venues["max_id"] + 1
    venue_slug = slugify.slugify(venue_name)
    with open(DIM_VENUES_PATH, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([venue_id, venue_name, venue_slug, city_id, state_id, "no"])

    dim_venues["data"][venue_slug] = {'id': city_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id, 'verified': "no"}
    dim_venues["max_id"] += 1

    return venue_id

def update_locations_dim(event_locations, dimension_tables):
    '''
    Checks each location in the issue to see if it already exists in dim_venues and dim_cities. If not, adds it to the dimension table(s)
    :param event_locations:
    :param dimension_tables:
    :return:
    '''
    dim_venues = dimension_tables["venues"]
    dim_cities = dimension_tables["cities"]
    dim_states = dimension_tables["states"]
    reverse_venue_map = build_reverse_map(VENUE_PATTERNS_PATH)
    city_candidate = None

    # loop through every location in the next issue
    for location in event_locations:
        total_location = " ".join(location)
        location_tokens = total_location.split(" ")
        state_id, state_idx = match_state_after_venue(location_tokens)

        if state_idx is not None:
            del location_tokens[state_idx]

        city_id, city_index = match_city_after_venue(location_tokens, dim_cities)

        # if existing city was found, everything before the city should be the venue name
        if city_id is not None:
            location_tokens = location_tokens[:city_index]
            #print(f"Ready to search for curate venue, city index = {city_index}, remaining tokens are {' '.join(location_tokens)}")
        # if no existing city was found, check for a possible city to be recorded
        else:
            city_candidate, venue_pattern_idx = find_city_candidate(location_tokens, dim_cities)
            if venue_pattern_idx is not None:
                location_tokens = location_tokens[:venue_pattern_idx+1]
            #print(f"Ready to search for curate venue, pattern index = {venue_pattern_idx}, remaining tokens are {' '.join(location_tokens)}")

        if state_id is None:
            state_id, state_idx = match_state_in_venue(location_tokens)

            if state_idx is not None:
                del location_tokens[state_idx]

        if city_id is None:
            city_id = match_city_in_venue(location_tokens, dim_cities, state_id)
        venue_name = " ".join(location_tokens)
        venue_id = match_existing_venues(venue_name, dim_venues, city_id, state_id)

        if venue_id is None:
            venue_id = append_venue(venue_name, dim_venues, city_id, state_id)

def curate_location(events_df, dimension_tables):
    '''

    :param events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    events_df["location"] = events_df["location"].apply(ast.literal_eval)                       # convert location values from a string to an array
    dim_cities = dimension_tables["cities"]

    update_locations_dim(events_df["location"], dimension_tables)                               # update the location dimension tables

def curate_events():
    #processed_data = pd.read_csv(f"s3://{BUCKET_NAME}/{object_key}")
    events_df = pd.read_csv("test_files/BB-1984-10-20.csv")

    dimension_tables = load_dimension_tables()

    #events_df["weekly_rank"] = range(1, len(events_df) + 1)
    #curate_event_name(events_df)
    #curate_artists(events_df, dimension_tables["artists"])
    curate_location(events_df, dimension_tables)
    #print(dimension_tables["venues"])

    #events_df.to_csv("test_files/BB-1984-10-20_cur.csv", index=False)

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
    '''
    See if an existing city can be found after the venue name

    :param location_tokens (list)
    :param dim_cities (dict)
    :return:
    '''
    city_id = city_index = None

    # start by checking if the last 3 words in the location are a city, then the last 2, then the last word
    for n in range(-1, -3, -1):
        candidate = " ".join(location_tokens[n:])
        candidate_slug = slugify.slugify(candidate)                                                                     # make a slug of the next n words

        # if a city already exists in the dimension table with the given slug
        if candidate_slug in dim_cities["data"]:
            city_id = dim_cities["data"][candidate_slug]["id"]                                                          # get the existing city id
            city_index = location_tokens.index(candidate.split()[0])                                                    # get the index of the first word in the city
            break  # break once a match is found

    return city_id, city_index

def find_city_candidate(location_tokens):
    '''
    Find a potential new city that is not already stored in dim_cities

    :param location_tokens (list)
    :return: city_candidate (str), venue_idx (int)
    '''
    city_candidate = venue_idx = None
    reverse_venue_map = build_reverse_map(VENUE_PATTERNS_PATH)

    for index, word in reversed(list(enumerate(location_tokens))):
        clean_word = word.lower()
        # once a venue pattern like "hall", "auditorium", etc... is found
        if clean_word in reverse_venue_map:
            venue_idx = index
            # extract all tokens that come after the venue as the city candidate
            city_candidate = " ".join(location_tokens[index+1:]).replace(",", "")
            break

    return city_candidate, venue_idx

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
        writer.writerow([city_id, city_candidate, city_slug, None, state_id, "no"])

    dim_cities["data"][city_slug] = {'id': city_id, 'name': city_candidate, 'slug': city_slug, 'aliases': None, 'state_id': state_id, 'verified': "no"}
    dim_cities["max_id"] += 1

    return city_id

def match_state_after_venue(location_tokens):
    '''
    Searches for a state from the end of the location tokens until it finds a venue type like 'hall' or 'auditorium'
    :param location_tokens:
    :return:
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    venue_patterns = build_reverse_map(VENUE_PATTERNS_PATH)
    state_id = None

    # loop through each in the rest of the location strings
    for index, token in reversed(list(enumerate(location_tokens))):
        token_clean = token.lower()
        # this function is only meant to find states that come after the venue
        # if a venue pattern like "hall", "auditorium", etc... is found then there is no state after the venue, just break
        if token_clean in venue_patterns:
            break
        if token_clean in state_aliases:                                                      # if the word is one of the possible aliases for a state
            state_id = state_aliases[token_clean]                                                # get the state id
            del location_tokens[index]
            break

    return state_id

def match_state_in_venue(location_tokens):
    '''
    Checks if any of the location tokens contain a state alias. Only records state if it is in bracket, parentheses, etc like
    'Charlotte (N.C.) Coliseum'
    Will not extract state name like 'Ohio Center' because many venues have states in their name but are not actually located in the given state

    :param location_tokens (list)
    :return: state_id, state_idx
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    state_id = None
    state_chars = set("(){}|")                                              # state should be explicitly surrounded by (), could be {, }, or | due to OCR error
    remove_chars = "(){}|"
    translator = str.maketrans("", "", remove_chars)

    for index, token in enumerate(location_tokens):
        # if any of the bracket chars are in the next token
        if any(char in state_chars for char in token):
            state = token.translate(translator).lower()                     # remove the brackets
            state_id = state_aliases[state]                                 # get the states id number
            del location_tokens[index]

    return state_id

def match_city_in_venue(location_tokens, dim_cities, state_id):
    '''
    Searches for a city in the venue name.

    :param location_tokens (list)
    :param dim_cities (dict)
    :return: city_id (int)
    '''
    city_id = None

    for i in range(len(location_tokens)):
        # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
        for window_size in range(1, 4):
            candidate = slugify.slugify(" ".join(location_tokens[i:i+window_size]).lower())

            # if a city is found and it has the same state_id as the existing city in dim_cities, it is a match
            if candidate in dim_cities["data"] and state_id == int(dim_cities["data"][candidate]["state_id"]):
                city_id = dim_cities["data"][candidate]["id"]

    return city_id

def match_existing_venues(venue_name, dim_venues, city_id, state_id):
    '''
    Checks if the given venue is already in dim_venues

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param state_id (int)
    :return: venue_id
    '''
    venue_slug = slugify.slugify(venue_name)
    venue_id = None
    if venue_slug in dim_venues:
        venue_id = dim_venues[venue_slug]["id"]

    return venue_id

def append_venue(venue_name, dim_venues, city_id, state_id):
    '''
    Adds the new venue to dim_venues.csv and dim_venues dictionary

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param state_id (int)
    :return: venue_id (int)
    '''
    venue_id = dim_venues["max_id"] + 1
    venue_slug = slugify.slugify(venue_name)
    with open(DIM_VENUES_PATH, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([venue_id, venue_name, venue_slug, city_id, state_id, "no"])

    dim_venues["data"][venue_slug] = {'id': city_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id, 'verified': "no"}
    dim_venues["max_id"] += 1

    return venue_id

def detect_venue_typo(location_tokens):
    '''
    Checks for a type on the venue type. Last word of venue is usually Hall, Auditorium, Center, etc... so it uses venue_patterns to see if there is the venue type
    matches any of the common typos and corrects it

    :param location_tokens: the remaining location tokens, only the name of the venue should be left
    :return: the updated location tokens
    '''

    venue_patterns = build_reverse_map(VENUE_PATTERNS_PATH)             # import the map of common venue typos to their corrected version

    # if the last word/venue type is in venue_patterns, swap it out for it's corrected version
    if location_tokens[-1].lower() in venue_patterns:
        location_tokens[-1] = venue_patterns[location_tokens[-1].lower()].capitalize()

    return location_tokens

def curate_location(events_df, dimension_tables):
    '''

    :param events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]
    event_locations = events_df["location"].apply(ast.literal_eval)                                                     # convert location values from a string to an array
    venue_ids = []
    venue_id = None

    for location in event_locations:
        location_tokens = [token for part in location for token in part.split()]
        state_id = match_state_after_venue(location_tokens)
        city_id, city_index = match_city_after_venue(location_tokens, dim_cities)

        # if existing city was found, everything before the city should be the venue name
        if city_id is not None:
            location_tokens = location_tokens[:city_index]
        # if no existing city was found, check for a possible city to be recorded
        else:
            city_candidate, venue_type_idx = find_city_candidate(location_tokens)

            if city_candidate:                                                                                          # if city candidate found
                city_id = append_city(city_candidate, dim_cities, state_id)                                             # add city to dim_cities

            if venue_type_idx:                                                                                          # remove city candidate from location tokens
                location_tokens = location_tokens[:venue_type_idx+1]

        if state_id is None:
            state_id = match_state_in_venue(location_tokens)

        if city_id is None:
            city_id = match_city_in_venue(location_tokens, dim_cities, state_id)

        location_tokens = detect_venue_typo(location_tokens)
        venue_name = " ".join(location_tokens)
        venue_id = match_existing_venues(venue_name, dim_venues, city_id, state_id)

        if venue_id is None:
            venue_id = append_venue(venue_name, dim_venues, city_id, state_id)
        venue_ids.append(venue_id)

    return venue_ids

def curate_events():
    #processed_data = pd.read_csv(f"s3://{BUCKET_NAME}/{object_key}")
    events_df = pd.read_csv("test_files/BB-1984-10-20.csv")

    dimension_tables = load_dimension_tables()

    #events_df["weekly_rank"] = range(1, len(events_df) + 1)
    #curate_event_name(events_df)
    #curate_artists(events_df, dimension_tables["artists"])
    venue_id = curate_location(events_df, dimension_tables)
    #print(dimension_tables["venues"])

    #events_df.to_csv("test_files/BB-1984-10-20_cur.csv", index=False)

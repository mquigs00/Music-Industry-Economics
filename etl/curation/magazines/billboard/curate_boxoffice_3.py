import io
import re
import ast
from botocore.exceptions import ClientError
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import *
from utils.utils import *
import pandas as pd
import os
import csv
import json
from Levenshtein import distance as levenshtein_distance
import logging
import slugify
from data_cleaning.normalization import build_reverse_map
from datetime import date
logger = logging.getLogger()

'''
This curation script is for the Billboard Boxscore schema that ran from 1984-10-20 to 2001-07-21
'''

object_key = "processed/billboard/magazines/1984/11/BB-1984-11-24.csv"

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5,"Jun": 6, "Jul": 7, "Aug": 8,
    "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

ORDINAL_FIX = re.compile(r"\b(\d+)(St|Nd|Rd|Th)\b")
APOSTROPHE_FIX = re.compile(r"(['’])S\b")

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

def append_venues_dim(path, venue_id, name, city_id):
    """
    Adds the new venue to the dimension table csv file
    :param path (str)
    :param venue_id (int)
    :param name (str)
    :param city_id (int)
    """
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([venue_id, name, slugify.slugify(name), city_id])

def normalize_event_name(event_name):
    """

    :param event_name:
    :return:
    """
    event_name = event_name.title()
    event_name = ORDINAL_FIX.sub(lambda m: m.group(1) + m.group(2).lower(), event_name)
    event_name = APOSTROPHE_FIX.sub(r"\1s", event_name)
    return event_name

def parse_event_name(artist_lines):
    """
    Detects a
    :param artist_lines:
    :return:
    """
    event_name_parts = []
    updated_artists = []
    event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)

    for i, line in enumerate(artist_lines):
        contains_event_keyword = any(keyword in line for keyword in event_keywords)
        if contains_event_keyword:
            if ':' in line:
                before, after = line.split(":", 1)                                                                      # split string on colon
                event_name_parts.append(before.strip())                                                                 # add text before colon to event name

                if after.strip():                                                                                       # if there is any text after the colon
                    updated_artists.append(after.strip())                                                               # add it to the updated artists list
            else:
                event_name_parts.append(line)                                                                           # assume event keyword always is at the end of the line

            updated_artists.extend(artist_lines[i + 1:])                                                                # put all remaining lines in the updated artists list
            event_name = normalize_event_name(" ".join(event_name_parts))                                               # join event name and fix casing
            return event_name, updated_artists
        else:
            event_name_parts.append(line)

    return None, artist_lines

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
        key = slugify.slugify(artist)
        artist_id = existing_artists[key][0]["id"]
        artist_ids.append(artist_id)

    return artist_ids

def update_artists_dim(all_artists, dim_artists):
    '''
    Add any new artists to the artists dimension table

    :param all_artists: a set of all artists in the current issue
    :param dim_artists: the dictionary of existing artists
    :return: max_artist_id
    '''

    max_artists_id = dim_artists["max_id"]
    existing_artists = dim_artists["by_slug"]
    for artist in all_artists:
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

def identify_first_artist(processed_events_df):
    """
    Adds a new column to the dataframe with the name of the first artist for the event
    :param processed_events_df (dataframe)
    """
    processed_events_df["artists"] = processed_events_df["artists"].apply(ast.literal_eval)                             # convert artist list from string to list
    processed_events_df["first_artist"] = processed_events_df["artists"].apply(
        lambda artists: artists[0] if artists else None
    )

def parse_artist_names(artists, has_event_name):
    '''
    Processed artist list can contain multiple artists in one token. This function separates every artist into a separate token
    :param artists:
    :param has_event_name:
    :return:
    '''
    separated_artists = []
    final_artists = []

    # if there is event name that signifies a festival separate artists that are on the same line and have a comma between them
    # if there is no event name, Billboard does not use commas to separate ar
    if has_event_name:
        for artist_line in artists:
            if ',' in artist_line:
                print(f'Comma in {artist_line}')
                artist_set = artist_line.split(',')
                for artist in artist_set:
                    if artist:
                        artist = clean_artist_name(artist)
                        separated_artists.append(artist)
            else:
                artist_line = clean_artist_name(artist_line)
                separated_artists.append(artist_line)
    else:
        for artist_line in artists:
            separated_artists.append(clean_artist_name(artist_line))

    i = 0

    while i < len(separated_artists):
        token = separated_artists[i]

        if token.endswith('&'):
            merged_artist = f"{token} {separated_artists[i+1]}".strip()
            final_artists.append(merged_artist)
            i += 2
        else:
            final_artists.append(token)
            i += 1

    return final_artists

def curate_artists(processed_events_df, curated_events_df, dim_artists):
    """
    Transforms the list of artist strings into a series of artist id numbers

    :param processed_events_df:
    :param curated_events_df:
    :param dim_artists:
    :return:
    """
    processed_events_df["artists_clean"] = processed_events_df.apply(
        lambda row: parse_artist_names(
            artists=row["artists"],
            has_event_name=row["event_name"] is not None
        ),
        axis=1
    )

    # create a set of all unique artist names in the current issue
    all_artists = {
        artist
        for artist_list in processed_events_df["artists_clean"]
        for artist in artist_list
    }

    update_artists_dim(all_artists, dim_artists)

    curated_events_df["artist_ids"] = processed_events_df["artists_clean"].apply(
        lambda artist_names: get_artist_ids(artist_names, dim_artists)
    )

def find_venue_type_idx(location_tokens):
    venue_types = build_reverse_map(VENUE_TYPES_PATH)

    for i, token in enumerate(location_tokens):
        if token.lower() in venue_types:
            return i

    return None

def match_city_after_venue(location_tokens, state_id, dim_cities):
    '''
    See if an existing city can be found after the venue name

    :param location_tokens (list)
    :param dim_cities (dict)
    :return:
    '''
    city_id = city_index = city_name = post_venue_tokens = None
    dim_cities_by_key = dim_cities["by_key"]
    dim_cities_by_slug = dim_cities["by_slug"]
    venue_type_idx = find_venue_type_idx(location_tokens)

    # find the venue name by identifying a venue type like "Hall", "Auditorium", "Stadium", and select all words after it
    if venue_type_idx:
        post_venue_tokens = location_tokens[venue_type_idx+1:]
        num_tokens = len(post_venue_tokens)
        difference = len(location_tokens) - num_tokens
    else:
        post_venue_tokens = location_tokens
        num_tokens = len(post_venue_tokens)
        difference = 0

    # start by checking for exact matches by (venue-slug, state_id) key
    for start in range(num_tokens):
        for end in range(num_tokens, start, -1):
            candidate_city_name = " ".join(post_venue_tokens[start:end]).lower()
            candidate_slug = slugify.slugify(candidate_city_name)
            if state_id:
                candidate_key = (candidate_slug, state_id)
                if candidate_key in dim_cities_by_key:
                    city_id = dim_cities_by_key[candidate_key]["id"]                                                                     # get the existing city id
                    city_name = dim_cities_by_key[candidate_key]["name"]
                    city_index = start+difference                                                                                        # get the index of the first word in the city
                    return city_id, city_name, city_index

    # if no exact match, filter down to venues in the same state, and check against venue names in case of typos in venue name
    for start in range(num_tokens):
        for end in range(num_tokens, start, -1):
            if state_id:
                candidate_city_name = " ".join(post_venue_tokens[start:end]).lower()
                cities_with_matching_state = [
                    city
                    for cities in dim_cities_by_slug.values()
                    for city in cities
                    if int(city["state_id"]) == state_id
                ]

                if cities_with_matching_state:
                    for city in cities_with_matching_state:
                        if len(city["name"]) >= 5 and levenshtein_distance(city["name"].lower(), candidate_city_name) <= 2:
                            city_id = city["id"]
                            city_name = city["name"]
                            city_index = start+difference
                            return city_id, city_name, city_index

    for start in range(num_tokens):
        for end in range(num_tokens, start, -1):
            candidate_city_name = " ".join(post_venue_tokens[start:end]).lower()
            candidate_slug = slugify.slugify(candidate_city_name)

            # if no state id, check for a unique city with the same name ("Springfield", "Arlington", etc... are common, get ignored)
            city_name_matches = dim_cities_by_slug.get(candidate_slug, [])
            if len(city_name_matches) == 1:
                city_id = city_name_matches[0]["id"]
                city_name = city_name_matches[0]["name"]
                city_index = start+difference
                return city_id, city_name, city_index

    return None, None, None

def find_city_candidate(location_tokens):
    '''
    Find a potential new city that is not already stored in dim_cities

    :param location_tokens (list)
    :return: city_candidate (str), venue_idx (int)
    '''
    city_candidate = venue_idx = None
    reverse_venue_map = build_reverse_map(VENUE_TYPES_PATH)

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

    dim_cities[city_slug] = {'id': city_id, 'name': city_candidate, 'slug': city_slug, 'aliases': None, 'state_id': state_id, 'verified': 0}
    dim_cities["max_id"] += 1

    return city_id

def match_state_after_venue(location_tokens):
    '''
    Searches for a state from the end of the location tokens until it finds a venue type like 'hall' or 'auditorium'

    :param location_tokens: each remaining word in the location data broken into separate strings
    :return: state_id
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    venue_patterns = build_reverse_map(VENUE_TYPES_PATH)
    state_id = None

    # loop through each in the rest of the location strings
    for index, token in reversed(list(enumerate(location_tokens))):
        token_clean = token.lower().strip('.')
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
    :return: state_id
    '''
    state_aliases = build_reverse_map(STATE_ALIASES_PATH)
    state_id = None
    state_chars = set("(){}")                                              # state should be explicitly surrounded by (), could be {, }, or | due to OCR error
    remove_chars = "(){}."
    translator = str.maketrans("", "", remove_chars)

    for index, token in enumerate(location_tokens):
        # if any of the bracket chars are in the next token
        if any(char in state_chars for char in token):
            state = token.translate(translator).lower()                     # remove the brackets
            try:
                state_id = state_aliases[state]                                 # get the states id number
                del location_tokens[index]
            except KeyError:
                state_id = -1                                               # if there is a state present, but it doesn't match any existing states, return -1 for unknown state id
                del location_tokens[index]

    return state_id

def match_city_in_venue(location_tokens, dim_cities, state_id):
    '''
    Searches for a city in the venue name, must have state_id to guarantee the city belongs to the same state

    :param location_tokens (list)
    :param dim_cities (dict)
    :return: city_id (int)
    '''
    city_id = None
    dim_cities_by_key = dim_cities["by_key"]

    for i in range(len(location_tokens)):
        # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
        for window_size in range(1, 4):
            candidate_slug = slugify.slugify(" ".join(location_tokens[i:i+window_size]).lower())
            candidate_key = (candidate_slug, state_id)

            if candidate_key in dim_cities_by_key:
                city_id = dim_cities_by_key[candidate_key]["id"]
                break

    return city_id

def potential_city_match_in_venue(location_tokens, dim_cities):
    '''
    Finds a matching city name in the venue name. Used when state_id is not known, does not guarantee the city is a true match
    :param location_tokens: the remaining tokens, should just be composed of the venue name
    :param dim_cities: the dictionary of current cities
    :return: the name of the matching city
    '''
    city_name = None
    dim_cities_by_slug = dim_cities["by_slug"]

    for i in range(len(location_tokens)):
        # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
        for window_size in range(1, 4):
            next_candidate = " ".join(location_tokens[i:i + window_size])
            candidate_slug = slugify.slugify(next_candidate.lower())

            if candidate_slug in dim_cities_by_slug:
                city_name = next_candidate
                break

    return city_name

def match_existing_venues(venue_name, dim_venues, city_id, city_name, dim_cities):
    '''
    Checks if the given venue is already in dim_venues

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param city_name (int)
    :param dim_cities (dict)
    :return: venue_id
    '''
    venue_slug = slugify.slugify(venue_name)
    existing_venues_by_slug = dim_venues["by_slug"]
    existing_cities_by_id = dim_cities["by_id"]

    if venue_slug not in existing_venues_by_slug:
        # if there are no venues with the same name, check for typos
        if city_id:
            venues_with_matching_city = [
                venue
                for venues in existing_venues_by_slug.values()
                for venue in venues
                if venue["city_id"] == city_id
            ]
            if not venues_with_matching_city:
                return None
            else:
                for existing_venue in venues_with_matching_city:
                    if len(venue_name) > 7 and levenshtein_distance(existing_venue["name"], venue_name) <= 2:
                        venue_slug = existing_venue["slug"]
                        continue
                if venue_slug not in existing_venues_by_slug:
                    return None
        else:
            return None

    venue_id = candidate_city_id = None
    candidates = existing_venues_by_slug[venue_slug]                                                                    # get all venues that have the given name

    for candidate in candidates:
        #print(f"Found potential match for {city_name}")
        candidate_city_id = candidate["city_id"]
        candidate_city_name = existing_cities_by_id[int(candidate_city_id)]["name"]

        # if the city of the current venue has already been verified, make sure it is the same as the city id of the potential match
        if city_id == candidate_city_id:
            venue_id = candidate["id"]
            break
        elif city_name == candidate_city_name:                                                                          # check if potential city name matches
            venue_id = candidate["id"]
            break
        else:
            print(f"Candidate {candidate} does not have matching city id or city name")

    return venue_id

def validate_venue(venue_name):
    if venue_name is None:
        return False
    return True

def append_venue(venue_name, dim_venues, city_id, state_id):
    '''
    Adds the new venue to dim_venues.csv and dim_venues dictionary

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_id (int)
    :param state_id (int)
    :return: venue_id (int)
    '''
    if validate_venue(venue_name):
        venue_id = dim_venues["max_id"] + 1
        venue_slug = slugify.slugify(venue_name)

        with open(DIM_VENUES_PATH, "a", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([venue_id, venue_name, venue_slug, city_id, state_id, "no"])

        dim_venues[venue_slug] = {'id': city_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id, 'verified': 0}
        dim_venues["max_id"] += 1
        return venue_id
    else:
        return False

def clean_venue_name(location_tokens):
    '''
    Checks for a type on the venue type. Last word of venue is usually Hall, Auditorium, Center, etc... so it uses venue_patterns to see if there is the venue type
    matches any of the common typos and corrects it

    :param location_tokens: the remaining location tokens, only the name of the venue should be left
    :return: the updated location tokens
    '''

    venue_types = build_reverse_map(VENUE_TYPES_PATH)             # import the map of common venue typos to their corrected version
    clean_venue_tokens = []

    for token in location_tokens:
        if token.lower() in venue_types:
            clean_venue_tokens.append(venue_types[token.lower()].title())
            break
        else:
            clean_venue_tokens.append(token)

    return clean_venue_tokens

def identify_venue(processed_events_df, dimension_tables):
    '''
    Identifies the state, city, and venue name without writing to any dimension tables

    :param processed_events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]
    processed_events_df["location"] = processed_events_df["location"].apply(ast.literal_eval)                           # convert location values from a string to an array
    venue_names = []

    for location in processed_events_df["location"]:
        city_id = city_index = city_candidate = None
        location_tokens = [token for part in location for token in part.split()]                                        # split every word/item into a token
        state_id = match_state_after_venue(location_tokens)

        # only check for an existing city if a state was provided, can't compare cities without knowing state
        if state_id is not None:
            city_id, city_name, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)

        # if existing city was found, everything before the city should be the venue name
        if city_id is not None:
            location_tokens = location_tokens[:city_index]
        # if no existing city was found, check for a possible city to be recorded
        else:
            city_candidate, venue_type_idx = find_city_candidate(location_tokens)                                       # find what looks like a city name

            if venue_type_idx:                                                                                          # remove city candidate from location tokens
                location_tokens = location_tokens[:venue_type_idx+1]

        venue_name = " ".join(location_tokens)

        venue_names.append(venue_name)

    processed_events_df["venue_name"] = venue_names

def curate_location(processed_events_df, dimension_tables):
    '''

    :param processed_events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]
    venue_ids = []
    venue_names = []
    venue_id = None

    for location in processed_events_df["location"]:
        city_id = city_index = city_candidate = None
        location_tokens = [token for part in location for token in part.split()]                                        # split every word/item into a token
        location_tokens = clean_location(location_tokens)
        state_id = match_state_after_venue(location_tokens)

        city_id, city_name, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)

        # if existing city was found, everything before the city should be the venue name
        if city_id is not None:
            location_tokens = location_tokens[:city_index]
        # if no existing city was found, check for a possible city to be recorded
        else:
            city_candidate, venue_type_idx = find_city_candidate(location_tokens)                                       # find what looks like a city name
            if city_candidate and state_id is not None:                                                                 # if city candidate found and a state was found
                city_id = append_city(city_candidate, dim_cities, state_id)                                             # add city to dim_cities
            if venue_type_idx:                                                                                          # remove city candidate from location tokens
                location_tokens = location_tokens[:venue_type_idx+1]

        if state_id is None:
            state_id = match_state_in_venue(location_tokens)                                                            # check if there is a state in the venue name

        if city_id is None:
            if state_id:
                city_id = match_city_in_venue(location_tokens, dim_cities, state_id)                                    # check if there is a city in the venue name
            else:
                city_candidate = potential_city_match_in_venue(location_tokens, dim_cities)

        location_tokens = clean_venue_name(location_tokens)
        venue_name = " ".join(location_tokens)
        venue_id = match_existing_venues(venue_name, dim_venues, city_id, city_candidate, dim_cities)                   # check if the venue already exists in dim_venues

        # if it is a new venue
        if venue_id is None:
            venue_id = append_venue(venue_name, dim_venues, city_id, state_id)                                          # add it to the dim_venues table

        venue_ids.append(venue_id)
        venue_names.append(venue_name)

    return venue_ids, venue_names

def identify_start_date(processed_events_df):
    '''

    :param processed_events_df:
    :return:
    '''
    event_dates = processed_events_df["dates"].apply(ast.literal_eval)                                                  # convert dates string from string to array
    issue_year = object_key.split('/')[-3]                                                                              # get issue year from S3 uri

    processed_events_df["start_date"] = [
        curate_date(dates, issue_year)[0]
        for dates in event_dates
    ]

def curate_dates(processed_events_df, curated_events_df):
    '''

    :param events_df:
    :return:
    '''
    event_dates = processed_events_df["dates"].apply(ast.literal_eval)                                                  # convert dates string from string to array
    issue_year = object_key.split('/')[-3]                                                                              # get issue year from S3 uri

    curated_dates = [curate_date(dates, issue_year) for dates in event_dates]
    curated_events_df["start_date"], curated_events_df["end_date"], curated_events_df["dates"] = zip(*curated_dates)

def clean_stray_numbers(dates):
    '''
    Remove any numbers that don't make sense in the dates
    Ex. ['Oct. 13', '13']. The second 13 actually comes from the name of the concert and has nothing to do with the dates

    :param dates: (list): a list of unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :return: clean_date_items
    '''
    clean_date_items = []                                                                                               # only store verified items in clean_date_items
    last_month_seen = None
    last_day_seen = None

    # loop through each date string, check if it is a month or a valid day
    for date_str in dates:
        date_items = date_str.split()
        for date_item in date_items:
            if date_item in MONTH_MAP:                                                                                  # if next item is a month
                clean_date_items.append(date_item)                                                                      # add the month to clean_items
                last_month_seen = date_item                                                                             # record that the month was seen
            elif last_month_seen:                                                                                       # if a month has been found
                # if the next item is a number and it is less or equal to than the previous date seen
                if date_item.isdigit() and last_day_seen and date_item <= last_day_seen:
                    continue
                elif date_item.isdigit() and 1 <= int(date_item) <= 31:
                    clean_date_items.append(date_item)
                    last_day_seen = date_item
                elif '-' in date_item:
                    clean_date_items.append(date_item)

    return clean_date_items

def clean_dates(dates):
    '''

    :param dates (list): a list of unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :return: total_dates (str): a clean string of dates ex. 'Oct 27-28/30-31/Nov 2-3'
    '''
    for i, date in enumerate(dates):
        dates[i] = re.sub(r"\d{3,}", "", dates[i])
        dates[i] = re.sub(r"\b(?!Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-zA-Z]{2,}\b", "", dates[i])
        dates[i] = re.sub(r"[.,;]", " ", dates[i])
        dates[i] = re.sub(r"\s+", " ", dates[i]).strip()

    cleaned_dates = clean_stray_numbers(dates)                                                                          # remove any garbage numbers that may have gotten mixed in
    total_dates = "".join(cleaned_dates)                                                                                # join into string with no spaces
    total_dates = re.sub(r"([a-z])([0-9])", r"\1 \2", total_dates)                                          # add spaces between a number and letter, not /

    return total_dates

def curate_date(dates, issue_year):
    '''

    :param dates (list) a list of unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :param issue_year: the year the current magazine issue was released
    :return:
    '''
    total_dates = clean_dates(dates)

    # Case 1: 'Oct 7'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)", total_dates)
    if m:
        m, d1 = m.groups()
        start_date = end_date = date(int(issue_year), MONTH_MAP[m], int(d1))
        return start_date, end_date, total_dates

    # Case 2: 'Sept 20-27'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-(\d+)", total_dates)
    if m:
        m, d1, d2 = m.groups()
        start_date = date(int(issue_year), MONTH_MAP[m], int(d1))
        end_date = date(int(issue_year), MONTH_MAP[m], int(d2))
        return start_date, end_date, total_dates

    # Case 3: 'Oct 30-Nov 8'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-([A-Za-z]+)[.,]? (\d+)", total_dates)
    if m:
        m1, d1, m2, d2 = m.groups()
        start_date = date(int(issue_year), MONTH_MAP[m1], int(d1))
        end_date = date(int(issue_year), MONTH_MAP[m2], int(d2))
        return start_date, end_date, total_dates

    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-(\d+)/(\d+)-(\d+)/([A-Za-z]+)[.,]? (\d+)-(\d+)", total_dates)
    if m:
        m1, start_day, e1, e2, e3, m2, e4, end_day = m.groups()
        start_date = date(int(issue_year), MONTH_MAP[m1], int(start_day))
        end_date = date(int(issue_year), MONTH_MAP[m2], int(end_day))
        return start_date, end_date, total_dates

    return None, None, None

def curate_num_sellouts(processed_events_df, curated_events_df):
    mask = processed_events_df[["num_shows", "num_sellouts"]].dropna()                                                  # drop rows that are empty before validating
    all_numeric = mask["num_sellouts"].ge(0).all()                                                                      # verify all values are numeric and greater than 0
    sellouts_less_than_shows = (mask["num_sellouts"] <= mask["num_shows"]).all()                                        # num_sellouts can never be greater than num_shows

    # if both constraints met, copy the data into curated as int
    if all_numeric and sellouts_less_than_shows:
        curated_events_df["num_sellouts"] = processed_events_df["num_sellouts"].apply(lambda x: int(x) if pd.notnull(x) else pd.NA)

def validate_numeric_column(df, col_name):
    '''

    :param df:
    :param col_name:
    :return:
    '''
    all_valid = df[col_name].dropna().ge(0).all()                                                                       # verify all financial values are all greater than 0
    return all_valid

def curate_ticket_prices(processed_events_df, curated_events_df):
    clean_prices = []

    for row in processed_events_df["ticket_prices"]:
        event_prices = []
        for prices in row:
            if "/" in prices:
                for price in prices.split("/"):
                    price = price.replace(',', '.')
                    event_prices.append(float(price))
            elif "-" in prices:
                for price in prices.split("-"):
                    price = price.replace(',', '.')
                    event_prices.append(float(price))
            else:
                prices = prices.replace(',', '.')
                event_prices.append(prices)

        clean_prices.append(event_prices)

    curated_events_df["ticket_prices"] = clean_prices

def validate_promoter(token):
    is_valid = not token.isnumeric() and not '$' in token

    return is_valid

def append_dim_promoters(name, slug, next_id):
    with open(DIM_PROMOTERS_PATH, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([next_id, name, slug, None])

def update_dim_promoters(promoter_names, dim_promoters):
    existing_promoters = dim_promoters["by_slug"]
    max_promoter_id = dim_promoters["max_id"]

    for name in promoter_names:
        slug = slugify.slugify(name)
        if slug not in existing_promoters:
            max_promoter_id += 1
            append_dim_promoters(name, slug, max_promoter_id)
            dim_promoters[slug] = {
                "id": max_promoter_id,
                "name": name,
                "slug": slug
            }
            dim_promoters["max_id"] = max_promoter_id

def parse_promoters(promoters_list, venue_names):
    promoters_per_event = []
    unique_promoters = set()

    # loop through the list of promoter strings for each event
    for event_idx, event_promoters in enumerate(promoters_list):
        event_promoters_str = "".join(event_promoters)  # join all promoter lines to one string
        individual_promoters = event_promoters_str.split('/')  # split by '/' to clean each promoter separately
        cleaned_event_promoters = []

        for promoter in individual_promoters:
            next_promoter = []
            promoter_tokens = promoter.split()  # the next promoter by whitespaces
            for token in promoter_tokens:
                if validate_promoter(token):
                    if levenshtein_distance("in-house", token.lower()) < 2:
                        next_promoter.append(venue_names[event_idx])
                    else:
                        next_promoter.append(token)
            next_promoter = " ".join(next_promoter)  # combine validated tokens to get promoter name
            if next_promoter:
                unique_promoters.add(next_promoter)
                cleaned_event_promoters.append(next_promoter)

        promoters_per_event.append(cleaned_event_promoters)

    return promoters_per_event, unique_promoters

def curate_promoters(processed_events_df, curated_events_df, dim_promoters, venue_names):
    promoters_list = processed_events_df["promoter"].apply(ast.literal_eval)

    promoters_names_per_event, unique_promoters = parse_promoters(promoters_list, venue_names)

    update_dim_promoters(unique_promoters, dim_promoters)                                                                 # add any new promoters to dim_promoters
    existing_promoters = dim_promoters["by_slug"]

    promoter_ids = []

    # loop through each set of promoter names
    for promoters in promoters_names_per_event:
        promoter_ids_per_event = []
        for promoter_name in promoters:
            promoter_slug = slugify.slugify(promoter_name)
            promoter_ids_per_event.append(existing_promoters[promoter_slug][0]["id"])                                      # get the id for that promoter
        promoter_ids.append(promoter_ids_per_event)

    curated_events_df["promoters"] = promoter_ids

def get_first_artist_name_by_id(artist_ids):
    if not artist_ids:
        return None
    return get_artist_name(artist_ids[0])

def add_raw_event_signature(processed_events_df):
    processed_events_df["signature"] = processed_events_df.apply(
        lambda row:
            f"{row['first_artist'].lower().replace(' ', '-')}-"
            f"{row['venue_name'].lower().replace(' ', '-')}-"
            f"{row['start_date']}",
            axis=1
    )

def normalize_event_or_artists(row):
    '''
    Returns either the slugified version of the event name or the name of the first artist
    :param row:
    :return:
    '''
    if row["event_name"]:
        return slugify.slugify(row["event_name"])

    artist_ids = row["artist_ids"]
    return slugify.slugify(get_artist_name(artist_ids[0]))

def curate_event_signature(curated_events_df):
    curated_events_df["signature"] = curated_events_df.apply(
        lambda row:
            f"{normalize_event_or_artists(row)}-"
            f"{get_venue_name(row['venue_id']).lower().replace(' ', '-')}-"
            f"{row['start_date']}",
            axis=1
        )

def implement_corrections(processed_events_df):
    '''
    Update any fields that were misread by OCR and do not have the context to autocorrect

    :param processed_events_df:
    :return:
    '''
    correction_dict = load_corrections_table(EVENT_CORRECTIONS_PATH)
    current_event_signatures = processed_events_df["signature"].to_list()

    # one event can need corrections for multiple fields
    # {'journey-civic-center-1984-10-20': [{'event_signature': 'journey-civic-center-1984-10-20', 'field': 'tickets_sold', 'true_value': 10000},
    #                                      {'event_signature': 'journey-civic-center-1984-10-20', 'field': 'capacity', 'true_value': 12200}]
    corrections_per_event = {
        signature: correction_dict[signature]
        for signature in current_event_signatures
        if signature in correction_dict
    }

    for event_signature, corrections in corrections_per_event.items():
        event = processed_events_df.index[
            processed_events_df["signature"] == event_signature
        ]

        row_idx = event[0]

        for correction in corrections:
            field = correction["field"]
            raw_value = correction["true_value"]

            if isinstance(raw_value, str):
                try:
                    true_value = ast.literal_eval(raw_value)
                    print(f"{true_value} converted to {type(true_value)}")
                except (ValueError, SyntaxError):
                    true_value = raw_value
            else:
                true_value = raw_value

            processed_events_df.at[row_idx, field] = true_value

def curate_events():
    #processed_data = pd.read_csv(f"s3://{BUCKET_NAME}/{object_key}")
    processed_events_df = pd.read_csv("test_files/BB-1984-11-24.csv")
    curated_events_df = pd.DataFrame()

    dimension_tables = load_dimension_tables()
    identify_venue(processed_events_df, dimension_tables)
    identify_first_artist(processed_events_df)
    identify_start_date(processed_events_df)
    processed_events_df["ticket_prices"] = processed_events_df["ticket_prices"].apply(ast.literal_eval)
    add_raw_event_signature(processed_events_df)
    implement_corrections(processed_events_df)

    curated_events_df["weekly_rank"] = range(1, len(processed_events_df) + 1)
    event_name_results = processed_events_df["artists"].apply(parse_event_name)
    #print(event_name_results.apply(lambda x: x[0]))
    processed_events_df["event_name"] = event_name_results.apply(lambda x: x[0])
    curated_events_df["event_name"] = event_name_results.apply(lambda x: x[0])
    processed_events_df["artists"] = event_name_results.apply(lambda x: x[1])
    curate_artists(processed_events_df, curated_events_df, dimension_tables["artists"])
    curated_events_df["venue_id"], venue_names = curate_location(processed_events_df, dimension_tables)
    curate_promoters(processed_events_df, curated_events_df, dimension_tables["promoters"], venue_names)
    curate_dates(processed_events_df, curated_events_df)
    curate_event_signature(curated_events_df)
    
    for col in ["gross_receipts_us", "gross_receipts_canadian", "attendance", "capacity", "num_shows"]:
        if validate_numeric_column(processed_events_df, col):
            curated_events_df[col] = processed_events_df[col].apply(parse_ocr_int)       # copy all original values as integers
    curate_num_sellouts(processed_events_df, curated_events_df)
    curate_ticket_prices(processed_events_df, curated_events_df)

    curated_events_df.to_csv("test_files/BB-1984-11-24_cur.csv", index=False)
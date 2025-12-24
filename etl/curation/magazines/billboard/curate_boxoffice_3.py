import io
import re
import ast
from botocore.exceptions import ClientError
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import DIM_ARTISTS_PATH, STATE_ALIASES_PATH, VENUE_PATTERNS_PATH, DIM_CITIES_PATH, DIM_VENUES_PATH, DIM_PROMOTERS_PATH, EVENT_CORRECTIONS_PATH
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

object_key = "processed/billboard/magazines/1984/10/BB-1984-10-20.csv"

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5,"Jun": 6, "Jul": 7, "Aug": 8,
    "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

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

def get_artist_ids(artist_names, dim_artists):
    existing_artists = dim_artists["by_slug"]
    max_artist_id = dim_artists["max_id"]
    artist_ids = []

    for artist in artist_names:
        key = slugify.slugify(artist)
        artist_id = existing_artists[key][0]["id"]
        artist_ids.append(artist_id)

    return artist_ids

def update_artists_dim(all_artists, dim_artists):
    max_artists_id = dim_artists["max_id"]
    existing_artists = dim_artists["by_slug"]
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

def identify_first_artist(processed_events_df):
    processed_events_df["artists"] = processed_events_df["artists"].apply(ast.literal_eval)
    processed_events_df["first_artist"] = processed_events_df["artists"].apply(
        lambda artists: artists[0] if artists else None
    )

def curate_artists(processed_events_df, curated_events_df, dim_artists):
    processed_events_df["artists"] = processed_events_df["artists"].apply(ast.literal_eval)

    # create a set of all unique artist names in the current issue
    all_artists = {
        artist
        for lst in processed_events_df["artists"]
        for artist in lst
    }

    update_artists_dim(all_artists, dim_artists)

    curated_events_df["artist_ids"] = processed_events_df["artists"].apply(
        lambda artist_names: get_artist_ids(artist_names, dim_artists)
    )

def match_city_after_venue(location_tokens, state_id, dim_cities):
    '''
    See if an existing city can be found after the venue name

    :param location_tokens (list)
    :param dim_cities (dict)
    :return:
    '''
    city_id = city_index = None
    dim_cities_by_key = dim_cities["by_key"]

    # start by checking if the last 3 words in the location are a city, then the last 2, then the last word
    for n in range(-1, -3, -1):
        candidate = " ".join(location_tokens[n:])
        candidate_slug = slugify.slugify(candidate)                                                                     # make a slug of the next n words
        candidate_key = (candidate_slug, state_id)

        # if a city already exists in the dimension table with the given slug and same state id
        if candidate_key in dim_cities_by_key:
            city_id = dim_cities_by_key[candidate_key]["id"]                                                            # get the existing city id
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

    dim_cities[city_slug] = {'id': city_id, 'name': city_candidate, 'slug': city_slug, 'aliases': None, 'state_id': state_id, 'verified': "no"}
    dim_cities["max_id"] += 1

    return city_id

def match_state_after_venue(location_tokens):
    '''
    Searches for a state from the end of the location tokens until it finds a venue type like 'hall' or 'auditorium'

    :param location_tokens:
    :return: state_id
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
    :return: state_id
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
            candidate_slug = slugify.slugify(" ".join(location_tokens[i:i+window_size]).lower())
            candidate_key = (candidate_slug, state_id)

            if candidate_key in dim_cities:
                city_id = dim_cities[candidate_key]["id"]

    return city_id

def match_existing_venues(venue_name, dim_venues, city_name, dim_cities):
    '''
    Checks if the given venue is already in dim_venues

    :param venue_name (str)
    :param dim_venues (dict)
    :param city_name (int)
    :param dim_cities (dict)
    :return: venue_id
    '''
    venue_slug = slugify.slugify(venue_name)
    existing_venues_by_slug = dim_venues["by_slug"]
    existing_cities_by_id = dim_cities["by_id"]

    if venue_slug not in existing_venues_by_slug:
        print(f"Venue slug {venue_slug} not in dim_venues")
        return None, None

    venue_id = candidate_city_id = None
    candidates = existing_venues_by_slug[venue_slug]                                                                    # get all venues that have the given name
    print(candidates)

    for candidate in candidates:
        #print(f"Next candidate: {candidate}")
        candidate_city_id = candidate["city_id"]
        #print(f"Candidate city id: {candidate_city_id}")

        candidate_city_name = existing_cities_by_id[int(candidate_city_id)]["name"]
        #print(f"Incoming city name = {city_name}, Candidate city name: {candidate_city_name}")
        if city_name == candidate_city_name:
            venue_id = candidate["id"]
            #print(f"Found existing venue id = {venue_id}")
            break

    return venue_id, candidate_city_id

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

    dim_venues[venue_slug] = {'id': city_id, 'name': venue_name, 'slug': venue_slug, 'city_id': city_id, state_id: state_id, 'verified': "no"}
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

def identify_venue(processed_events_df, dimension_tables):
    '''
    Identifies the state, city, and venue name without writing to any dimension tables

    :param processed_events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]
    event_locations = processed_events_df["location"].apply(ast.literal_eval)                                                     # convert location values from a string to an array
    venue_names = []

    for location in event_locations:
        city_id = city_index = city_candidate = None
        location_tokens = [token for part in location for token in part.split()]                                        # split every word/item into a token
        state_id = match_state_after_venue(location_tokens)

        # only check for an existing city if a state was provided, can't compare cities without knowing state
        if state_id is not None:
            city_id, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)

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

def curate_location(events_df, dimension_tables, curated_events_df):
    '''

    :param events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]
    event_locations = events_df["location"].apply(ast.literal_eval)                                                     # convert location values from a string to an array
    venue_ids = []
    venue_names = []
    venue_id = None

    for location in event_locations:
        city_id = city_index = city_candidate = None
        location_tokens = [token for part in location for token in part.split()]                                        # split every word/item into a token
        state_id = match_state_after_venue(location_tokens)

        # only check for an existing city if a state was provided, can't compare cities without knowing state
        if state_id is not None:
            city_id, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)

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
            city_id = match_city_in_venue(location_tokens, dim_cities, state_id)                                        # check if there is a city in the venue name

        location_tokens = detect_venue_typo(location_tokens)
        venue_name = " ".join(location_tokens)
        venue_id, city_id = match_existing_venues(venue_name, dim_venues, city_candidate, dim_cities)                   # check if the venue already exists in dim_venues

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
    all_valid = df[col_name].dropna().ge(0).all()
    return all_valid

def curate_ticket_prices(processed_events_df, curated_events_df):
    ticket_prices_org = processed_events_df["ticket_prices"].apply(ast.literal_eval)
    clean_prices = []

    for row in ticket_prices_org:
        event_prices = []
        for prices in row:
            if "/" in prices:
                for price in prices.split("/"):
                    event_prices.append(float(price))
            elif "-" in prices:
                for price in prices.split("-"):
                    event_prices.append(float(price))
            else:
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
            dim_promoters["data"][slug] = {
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
    print(existing_promoters)

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

def curate_event_signature(curated_events_df):
    curated_events_df["signature"] = curated_events_df.apply(
        lambda row:
            f"{row['artists'][0].lower().replace(' ', '-')}-"
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

    # corrections_per_event = {'journey-civic-center-1984-10-20': [{'event_signature': 'journey-civic-center-1984-10-20', 'field': 'tickets_sold', 'true_value': 10000]...}
    corrections_per_event = {
        sig: correction_dict[sig]
        for sig in current_event_signatures
        if sig in correction_dict
    }

    for event_signature in corrections_per_event:
        mask = processed_events_df["signature"] == event_signature                                                      # get row with the current event signature

        for correction in corrections_per_event[event_signature]:                                                       # make each correction for the current event
            field_to_correct = correction["field"]
            true_value = correction["true_value"]
            if true_value.isdigit():
                true_value = int(true_value)
            processed_events_df.loc[mask, field_to_correct] = true_value                                                # update field with its correct value

def curate_events():
    #processed_data = pd.read_csv(f"s3://{BUCKET_NAME}/{object_key}")
    processed_events_df = pd.read_csv("test_files/BB-1984-10-20.csv")
    curated_events_df = pd.DataFrame()

    dimension_tables = load_dimension_tables()
    identify_venue(processed_events_df, dimension_tables)
    identify_first_artist(processed_events_df)
    identify_start_date(processed_events_df)
    add_raw_event_signature(processed_events_df)
    implement_corrections(processed_events_df)

    curated_events_df["weekly_rank"] = range(1, len(processed_events_df) + 1)
    curate_artists(processed_events_df, curated_events_df, dimension_tables["artists"])
    curated_events_df["venue_id"], venue_names = curate_location(processed_events_df, dimension_tables, curated_events_df)
    curate_promoters(processed_events_df, curated_events_df, dimension_tables["promoters"], venue_names)
    curate_dates(processed_events_df, curated_events_df)
    curated_events_df["artists"] = processed_events_df["artists"].apply(ast.literal_eval)
    curate_event_signature(curated_events_df)
    
    for col in ["gross_receipts_us", "gross_receipts_canadian", "tickets_sold", "capacity", "num_shows"]:
        if validate_numeric_column(processed_events_df, col):
            curated_events_df[col] = processed_events_df[col].apply(lambda x: int(x) if pd.notnull(x) else pd.NA)       # copy all original values as integers
    curate_num_sellouts(processed_events_df, curated_events_df)
    curate_ticket_prices(processed_events_df, curated_events_df)

    curated_events_df.to_csv("test_files/BB-1984-10-20_cur.csv", index=False)
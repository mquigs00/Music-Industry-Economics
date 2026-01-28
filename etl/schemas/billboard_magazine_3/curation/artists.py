from etl.dimensions.artists import update_artists_dim, get_artist_ids
from utils.utils import load_artist_corrections
import slugify
import ast
import re


def identify_first_artist_line(processed_events_df):
    """
    Adds a new column to the dataframe with the first string from the raw artists list

    :param processed_events_df (dataframe)
    """
    processed_events_df["artists"] = processed_events_df["artists"].apply(ast.literal_eval)                             # convert artist list from string to list
    processed_events_df["first_artist_line"] = processed_events_df["artists"].apply(                                    # add first artist string to its own column
        lambda artists: artists[0] if artists else None
    )

def clean_artist_name(artist, in_special_event):
    """

    :param artist:
    :param in_special_event:
    :return:
    """
    artist = artist.strip()
    artist = re.sub(r"[._:|{}\[\]]", "", artist)
    if not in_special_event:
        artist = artist.replace(',', '')
    return artist

def separate_event_artists(artists):
    """
    Divide artists that are grouped on the same line but have commas dividing them

    :param artists:
    :return:
    """
    separated_artists = []

    if any(',' in artist for artist in artists):
        combined_artists = " ".join(artists)                                                                            # combine all tokens
        separated_artists = combined_artists.split(",")                                                                 # split by comma
        separated_artists = [artist.strip() for artist in separated_artists]                                            # separate
        return separated_artists
    else:
        return artists

def merge_artists(raw_artist_strings, has_event_name):
    """
    Merge any artist names that overflow across multiple tokens into one token

    :param raw_artist_strings: list
    :return:
    """
    merged_artists = []
    signal = join_behavior =  None

    overflow_signals = {
        '&': 'space',
        'AND': 'space',
        'THE': 'space',
        '-': 'none'
    }

    i = 0

    while i < len(raw_artist_strings):
        signal = None
        token = raw_artist_strings[i]

        for overflow_signal, join_type in overflow_signals.items():
            if token.endswith(overflow_signal):
                signal = overflow_signal
                join_behavior = join_type
                continue                                                                                                # if a signal is found, move to next iteration
        if signal:                                                                                                      # if an overflow signal was found
            if len(raw_artist_strings) > i+1:                                                                           # if there is at least one more token left
                if join_behavior == 'space':
                    merged_artist = f"{token} {raw_artist_strings[i + 1]}".strip()
                else:
                    merged_artist = f"{token}{raw_artist_strings[i + 1]}".strip()
                merged_artists.append(clean_artist_name(merged_artist, has_event_name))                                 # add the merged token to the result list
                i += 2                                                                                                  # skip next token
            else:
                print(f"Overflow signal detected but no token after {raw_artist_strings[i]}")
                i += 1
        else:
            merged_artists.append(clean_artist_name(token, has_event_name))                                             # if no signal, just add token to list
            i += 1

    return merged_artists

def separate_artists(raw_artists_strings, has_event_name):
    """
    Separate artists that appeared on the same line but are not part of the same group

    :param raw_artists_strings:
    :param has_event_name: bool, does the raw artists list contain a special event name
    :return:
    """
    separated_artists = []
    i = 0

    # if there is event name then that signifies a festival with separate artists that are on the same line and have a comma between them
    # if there is no event name, Billboard does not use commas to separate are
    if has_event_name:
        separated_artists = separate_event_artists(raw_artists_strings)
    else:
        while i < len(raw_artists_strings):
            token = raw_artists_strings[i]

            if '/' in token:
                left, right = token.rsplit('/', 1)
                separated_artists.append(clean_artist_name(left, has_event_name))

                if i + 1 < len(raw_artists_strings):                                                                    # if there is still another token in the list
                    separated_artists.append(clean_artist_name(f"{right}/{raw_artists_strings[i+1]}", has_event_name))
                    i += 1
                else:
                    separated_artists.append(clean_artist_name(right, has_event_name))
            else:
                separated_artists.append(clean_artist_name(token, has_event_name))
            i += 1

    return separated_artists

def generate_artist_candidates(artist_string):
    """
    Generate possible
    :param artist_string:
    :return:
    """
    candidates = set()
    delimiters = [',', '&', 'AND', 'THE', '/']

    candidates.add(artist_string)

    for delimiter in delimiters:
        if delimiter in artist_string:
            left, right = artist_string.split(delimiter, 1)                                                             # split string at the first instance of delimiter
            candidates.add(left.strip())                                                                                # add each part of the string to candidates
            candidates.add(right.strip())

            if delimiter == '/':                                                                                        # "/" signifies separate artists
                candidate_minus_delimiter = artist_string.replace('/', ' ')
                candidate_minus_delimiter = " ".join(candidate_minus_delimiter.split())
            else:                                                                                                       #",", "&", "AND", and "THE" signify same artist
                candidate_minus_delimiter = artist_string.replace(delimiter, '')
                candidate_minus_delimiter = " ".join(candidate_minus_delimiter.split())
            candidates.add(candidate_minus_delimiter.strip())

    return candidates

def validate_artist(artist, dim_artists):
    """
    Checks if an artist candidate matches an existing artist in the dimension table

    :param artist: string
    :param dim_artists: dict, the existing artists
    :return:
    """
    candidates = generate_artist_candidates(artist)                                                                     # generate potential candidates from the string
    artist_corrections_dict = load_artist_corrections()
    best_candidate = None
    best_score = 0

    for candidate in candidates:
        candidate_slug = slugify.slugify(candidate)
        if candidate_slug in artist_corrections_dict:
            return artist_corrections_dict[candidate_slug]

    for candidate in candidates:
        if slugify.slugify(candidate) in dim_artists['by_slug']:
            return candidate

    return artist

def parse_artist_names(raw_artists_strings, has_event_name, dim_artists):
    """
    Processed artist list can contain multiple artists in one token. This function separates every artist into a separate token

    :param raw_artists_strings: list of raw artists strings. Ex: ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS']
    :param has_event_name: bool, if the event's raw artist started with an event name like "OZZFEST 1997:"
    :param dim_artists: dict, the existing artists dimension table
    :return: final_artists: the artists list with each artist in one token
    """

    merged_artists = merge_artists(raw_artists_strings, has_event_name)
    separated_artists = separate_artists(merged_artists, has_event_name)

    curated_artists = []

    for artist_string in separated_artists:
        validated_artist = validate_artist(artist_string, dim_artists)
        curated_artists.append(validated_artist)

    return curated_artists

def curate_artists(processed_events_df, curated_events_df, dim_artists):
    """
    Transforms the artist lists from a list of artist name strings into a list of artist id numbers

    :param processed_events_df: the processed events dataframe with uncleaned artist data
    :param curated_events_df: the curated events dataframe
    :param dim_artists: the dimension table of existing artists
    """
    # make sure all artists are separated into separate tokens before
    processed_events_df["artists_clean"] = processed_events_df.apply(
        lambda row: parse_artist_names(
            raw_artists_strings=row["artists"],
            has_event_name=row["event_name"] is not None,
            dim_artists=dim_artists
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
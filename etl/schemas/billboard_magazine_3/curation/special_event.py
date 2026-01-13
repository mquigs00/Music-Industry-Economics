from utils.utils import load_event_keywords
from config.paths import EVENT_KEYWORDS_PATH, DIM_SPECIAL_EVENTS_PATH
import re

ORDINAL_FIX = re.compile(r"\b(\d+)(St|Nd|Rd|Th)\b")
APOSTROPHE_FIX = re.compile(r"(['’])S\b")

def special_event_score(artist_lines):
    score = 0
    event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)

    total_artists_string = "".join(artist_lines)

    if any(keyword in total_artists_string for keyword in event_keywords):
        score += 3

    if total_artists_string.count(":") > 1:
        score += 2

    if total_artists_string.count(",") > 1:
        score += 2

def parse_event_name(artist_lines, existing_special_events):
    """
    Detects a
    :param artist_lines: the raw lines from the artists column
    :param existing_special_events: the special_events dimension table
    :return: event_name (str), updated_artists
    """
    event_name_parts = []
    updated_artists = []
    event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)

    for i, line in enumerate(artist_lines):
        contains_event_keyword = any(keyword in line for keyword in event_keywords)                                     # check for a token like "Festival", "Fest", "Show"
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

def normalize_event_name(event_name):
    """

    :param event_name:
    :return:
    """
    event_name = event_name.title()
    event_name = ORDINAL_FIX.sub(lambda m: m.group(1) + m.group(2).lower(), event_name)                                 # lowercase first letter in Th/Rd of "6Th" or "3Rd"
    event_name = APOSTROPHE_FIX.sub(r"\1s", event_name)
    return event_name

def curate_event_name(processed_events_df, curated_events_df):
    '''
    Finds an event_name in artists list. Removes it into its own field, and updates the dim_special_events table to get an id number for the event
    :param processed_events_df:
    :param curated_events_df:
    :return:
    '''
    event_name_results = processed_events_df["artists"].apply(
        parse_event_name,
        args=(DIM_SPECIAL_EVENTS_PATH,)
    )
    processed_events_df["event_name"] = event_name_results.apply(lambda x: x[0])                                        # add the event name to the processed_events_df
    curated_events_df["event_name"] = event_name_results.apply(lambda x: x[0])                                          # add the event name to the curated_events_df
    processed_events_df["artists"] = event_name_results.apply(lambda x: x[1])                                           # update the artists list with the event name removed
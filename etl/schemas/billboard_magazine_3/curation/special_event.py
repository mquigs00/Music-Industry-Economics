from etl.utils.utils import load_event_keywords
from config.paths import EVENT_KEYWORDS_PATH, LOCAL_DIM_SPECIAL_EVENTS_PATH
import re

ORDINAL_FIX = re.compile(r"\b(\d+)(St|Nd|Rd|Th)\b")
APOSTROPHE_FIX = re.compile(r"(['’])S\b")

def find_event_end_index(artist_lines, event_keywords):
    """

    :param artist_lines: list
    :param event_keywords: list
    :return:
    """
    for i, line in enumerate(artist_lines):
        lowered = line.lower()
        for keyword in event_keywords:
            if keyword in lowered:
                return i

    return -1

def find_tag_index(artist_lines, tag_keywords):
    for i, line in enumerate(artist_lines):
        lowered = line.lower()
        for keyword in tag_keywords:
            if keyword in lowered:
                return i

    return -1

def calc_special_event_score(artist_lines):
    """
    Generates a score to estimate if the artist lines contain a special event name
    :param artist_lines: list
    :return:
    """
    score = 0
    event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)
    strong_event_keywords = event_keywords['strong']
    weak_event_keywords = event_keywords['weak']
    tag_keywords = event_keywords['tags']
    total_artists_string = "".join(artist_lines).lower()
    event_candidate = None

    if any(keyword in total_artists_string for keyword in strong_event_keywords):
        score += 7

    if any(keyword in total_artists_string for keyword in tag_keywords):
        tag_idx = find_tag_index(artist_lines, tag_keywords)
        post_tag = " ".join(artist_lines[tag_idx:])
        print(f"Post tag: {post_tag}")
        if any(char.isdigit() for char in post_tag):
            score += 7

    if any(keyword in total_artists_string for keyword in weak_event_keywords):
        score += 5

    if ":" in total_artists_string:
        score += 5
        pre_colon, post_colon = total_artists_string.split(":", 1)
        event_candidate = pre_colon

    else:
        event_end_idx = find_event_end_index(artist_lines, strong_event_keywords+weak_event_keywords)
        print(event_end_idx)
        if event_end_idx is not None:
            event_candidate = " ".join(artist_lines[: event_end_idx + 1])
            print(event_candidate)
    if "'" in event_candidate:
        score += 2

    if "," in total_artists_string:
        score += 2

    return score

def extract_event_name(artist_lines):
    artist_lines_lowered = [artist_line.lower() for artist_line in artist_lines]
    event_name_parts = []
    updated_artists = []
    event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)["strong"] + load_event_keywords(EVENT_KEYWORDS_PATH)["weak"]
    combined_artists = " ".join(artist_lines).lower()
    event_name = None
    found_colon = False
    found_keyword = False
    contains_colon = ":" in combined_artists
    contains_keyword = any(keyword in combined_artists for keyword in event_keywords)
    print(f"Contains colon: {contains_colon}, contains keyword: {contains_keyword}")

    for i, line in enumerate(artist_lines_lowered):
        print(line)
        keyword_in_line = any(keyword in line for keyword in event_keywords)                                            # check for a token like "Festival", "Fest", "Show"
        if keyword_in_line:
            found_keyword = True
            print(f"Found keyword {keyword_in_line} in {line}")
            if ':' in line:
                found_colon = True
                before, after = line.split(":", 1)                                                                      # split string on colon
                event_name_parts.append(before.strip())                                                                 # add text before colon to event name

                if after.strip():                                                                                       # if there is any text after the colon
                    updated_artists.append(after.strip())                                                               # add it to the updated artists list
            else:
                event_name_parts.append(line)                                                                           # assume event keyword always is at the end of the line

            if not contains_colon or (contains_colon and found_colon):                                                  # if no colon or colon already found
                updated_artists.extend(artist_lines[i + 1:])                                                            # put all remaining lines in the updated artists list
                event_name = normalize_event_name(" ".join(event_name_parts))                                           # join event name and fix casing
                print("Ready to return in keyword path")
                return event_name, updated_artists
        elif ":" in line:
            print(f"Found colon in {line}")
            found_colon = True
            if not contains_keyword or (contains_keyword and found_keyword):                                            # if no event name or already found
                before, after = line.split(":", 1)                                                                      # split string on colon
                event_name_parts.append(before.strip())                                                                 # add text before colon to event name

                if after.strip():                                                                                       # if there is any text after the colon
                    updated_artists.append(after.strip())                                                               # add it to the updated artists list

                updated_artists.extend(artist_lines[i + 1:])                                                            # put all remaining lines in the updated artists list
                event_name = normalize_event_name(" ".join(event_name_parts))                                           # join event name and fix casing
                print("Ready to return in colon path")
                return event_name, updated_artists
            else:
                event_name_parts.append(line)                                                                           # if still an event name to find, just append
        else:
            event_name_parts.append(line)

    event_name = normalize_event_name(" ".join(event_name_parts))
    print("Ready to return at end")
    return event_name, updated_artists

def parse_event_name(artist_lines, existing_special_events):
    """
    :param artist_lines: list, the raw lines from the artists column
    :param existing_special_events: dict, the special_events dimension table
    :return: event_name str, updated_artists
    """
    special_event_score = calc_special_event_score(artist_lines)

    if special_event_score >= 7:
        event_name, updated_artists = extract_event_name(artist_lines)
        return event_name, updated_artists
    else:
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
    Searches for an event_name in artists list. If found, separates event name into its own field and removes it from the artist names

    :param processed_events_df:
    :param curated_events_df:
    :return:
    '''
    event_name_results = processed_events_df["artists"].apply(
        parse_event_name,
        args=(LOCAL_DIM_SPECIAL_EVENTS_PATH,)
    )
    processed_events_df["event_name"] = event_name_results.apply(lambda x: x[0])                                        # add the event name to the processed_events_df
    curated_events_df["event_name"] = event_name_results.apply(lambda x: x[0])                                          # add the event name to the curated_events_df
    processed_events_df["artists"] = event_name_results.apply(lambda x: x[1])                                           # update the artists list with the event name removed
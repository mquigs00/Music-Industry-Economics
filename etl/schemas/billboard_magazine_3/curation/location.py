from etl.data_cleaning.normalization import build_reverse_map
import slugify
from config.paths import LOCATION_ALIASES_PATH
from etl.utils.utils import load_json
from Levenshtein import distance as levenshtein_distance
from etl.dimensions.location import append_venue, append_city
import ast
import re

EDUCATIONAL_TOKENS = {"univ", "unwv", "unv"}

LOCATION_ALIASES = load_json(LOCATION_ALIASES_PATH)

VENUE_TYPES_MAP = build_reverse_map(LOCATION_ALIASES["venue_types"])
CITY_ALIAS_MAP = build_reverse_map(LOCATION_ALIASES["cities"])
STATE_ALIAS_MAP = LOCATION_ALIASES["states"]

def find_venue_type_idx(location_tokens):
    venue_types = build_reverse_map(VENUE_TYPES_MAP)

    for i, token in enumerate(location_tokens):
        if token.lower() in venue_types:
            return i

    return None

def clean_location(location_tokens):
    venue_types = build_reverse_map(VENUE_TYPES_MAP)
    NOISE = {"productions", "promotions", "presents", "presentations", "prods", "concerts", "inc", "jam"
             "sellout", "associates", "attractions"}
    clean_tokens = []
    for i, token in enumerate(location_tokens):
        token = token.replace(',', '')
        token = token.replace('.', '')
        lowered = token.lower()

        if lowered in NOISE:
            continue

        if token.lower() in venue_types:
            clean_tokens.append(venue_types[lowered].title())
            print(f"Changes token to {venue_types[lowered]}")
        else:
            clean_tokens.append(token.title())
    return clean_tokens

def normalize_location_tokens(location_tokens):
    """
    Split tokens up that have hyphens, that way each part can be compared to venue keywords, city aliases, and state aliases independently
    :param location_tokens:
    :return:
    """
    normalized = []

    for token in location_tokens:
        parts = token.split('-')
        normalized.extend(parts)

    return normalized

def match_city_after_venue(location_tokens, state_id, dim_cities):
    """
    See if an existing city can be found after the venue name

    :param location_tokens (list)
    :param state_id (int)
    :param dim_cities (dict)
    :return: city_id (int), city_name (str), city_index (int)
    """
    venue_type_idx = find_venue_type_idx(location_tokens)

    if venue_type_idx is None:
        return None, None, None

    city_id = city_index = city_name = post_venue_tokens = None
    dim_cities_by_key = dim_cities["by_key"]
    dim_cities_by_slug = dim_cities["by_slug"]

    # find the venue name by identifying a venue type like "Hall", "Auditorium", "Stadium", and select all words after it
    post_venue_tokens = location_tokens[venue_type_idx+1:]
    num_tokens = len(post_venue_tokens)
    difference = len(location_tokens) - num_tokens

    if state_id:
        # start by checking for exact matches by (venue-slug, state_id) key
        for start in range(num_tokens):
            for end in range(num_tokens, start, -1):
                candidate_city_name = " ".join(post_venue_tokens[start:end]).lower()
                candidate_slug = slugify.slugify(candidate_city_name)
                candidate_key = (candidate_slug, state_id)
                if candidate_key in dim_cities_by_key:
                    city_id = dim_cities_by_key[candidate_key]["id"]                                                                     # get the existing city id
                    city_name = dim_cities_by_key[candidate_key]["name"]
                    city_index = start+difference                                                                                        # get the index of the first word in the city
                    return city_id, city_name, city_index

        # if no exact match, filter down to venues in the same state, and check against venue names in case of typos in venue name
        for start in range(num_tokens):
            for end in range(num_tokens, start, -1):
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
    else:
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
    reverse_venue_map = build_reverse_map(VENUE_TYPES_MAP)

    for index, word in reversed(list(enumerate(location_tokens))):
        clean_word = word.lower()
        if clean_word in reverse_venue_map:                                                                             # once a venue pattern like "hall", "auditorium", etc... is found
            venue_idx = index
            # extract all tokens that come after the venue as the city candidate
            city_candidate = " ".join(location_tokens[index+1:]).replace(",", "")
            break

    return city_candidate, venue_idx

def match_state_after_venue(location_tokens):
    '''
    Searches for a state from the end of the location tokens until it finds a venue type like 'hall' or 'auditorium'

    :param location_tokens: each remaining word in the location data broken into separate strings
    :return: state_id, location_tokens
    '''
    state_aliases = build_reverse_map(STATE_ALIAS_MAP)
    venue_patterns = build_reverse_map(VENUE_TYPES_MAP)
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

    return state_id, location_tokens

def match_state_in_venue(location_tokens):
    '''
    Checks if any of the location tokens contain a state alias. Only records state if it is in bracket, parentheses, etc like
    'Charlotte (N.C.) Coliseum'
    Will not extract state name like 'Ohio Center' because many venues have states in their name but are not actually located in the given state

    :param location_tokens (list)
    :return: state_id, location
    '''
    state_aliases = build_reverse_map(STATE_ALIAS_MAP)
    state_id = None
    state_chars = set("(){}")                                                                                           # state will usually be surrounded by parentheses
    remove_chars = "(){}."
    translator = str.maketrans("", "", remove_chars)
    found_match = False

    # loop through all words in venue name, checking if any of them are a state
    for index, token in enumerate(location_tokens):
        token_clean = token.translate(translator).lower()
        if token_clean in state_aliases:
            found_match = True
            try:
                state_id = state_aliases[token_clean]                                                                   # get the states id number
                del location_tokens[index]
            except KeyError:
                state_id = -1                                               # if there is a state present, but it doesn't match any existing states, return -1 for unknown state id
                del location_tokens[index]
        if not found_match and any(state_char in token for state_char in state_chars):
            state_id = -1
            del location_tokens[index]

    return state_id, location_tokens

def match_city_in_venue(location_tokens, dim_cities, state_id):
    '''
    Searches for a city in the venue name, must have state_id to guarantee the city belongs to the same state

    :param location_tokens (list)
    :param dim_cities (dict)
    :param state_id: int
    :return: city_id (int)
    '''
    city_id = None
    dim_cities_by_key = dim_cities["by_key"]
    dim_cities_by_slug = dim_cities["by_slug"]

    if state_id:
        cities_with_matching_state = [
            city
            for cities in dim_cities_by_slug.values()
            for city in cities
            if int(city["state_id"]) == state_id
        ]
        #print(cities_with_matching_state)

        for i in range(len(location_tokens)):
            # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
            for window_size in range(1, 4):
                candidate_slug = slugify.slugify(" ".join(location_tokens[i:i + window_size]).lower())
                candidate_key = (candidate_slug, state_id)

                if candidate_key in dim_cities_by_key:
                    city_id = dim_cities_by_key[candidate_key]["id"]
                    break

        if not city_id:
            #print(f"City id is none, checking for cities in same state")
            for city in cities_with_matching_state:
                print(city)
                for i in range(len(location_tokens)):
                    for window_size in range(1, 4):
                        candidate_slug = slugify.slugify(" ".join(location_tokens[i:i + window_size]).lower())
                        #print(f"{city['slug']} vs candidate: {levenshtein_distance(city['slug'], candidate_slug)}")
                        if len(candidate_slug) > 7 and levenshtein_distance(city["slug"], candidate_slug) <= 2:
                            city_id = city["id"]
                            return city_id

    # if state_id is not present, only match if there is only one instance of the given slug in dim cities (ex. "Las Vegas", "Los Angeles", "Honolulu")
    for i in range(len(location_tokens)):
        # check all combinations of words from left to right length 1 to 3 to see if any of them are in the existing city slugs
        for window_size in range(1, 4):
            candidate_slug = slugify.slugify(" ".join(location_tokens[i:i+window_size]).lower())

            if candidate_slug in dim_cities_by_slug and len(dim_cities_by_slug[candidate_slug]) == 1:
                print(dim_cities_by_slug[candidate_slug])
                city_id = dim_cities_by_slug[candidate_slug][0]["id"]
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
    :param city_name (str)
    :param dim_cities (dict)
    :return: venue_id
    '''
    venue_slug = slugify.slugify(venue_name)
    existing_venues_by_slug = dim_venues["by_slug"]
    existing_cities_by_id = dim_cities["by_id"]

    if venue_slug not in existing_venues_by_slug:
        print(f"Venue {venue_slug} not in existing venues")
        # if there are no venues with the same name, check for typos
        if city_id:
            venues_with_matching_city = [
                venue
                for venues in existing_venues_by_slug.values()
                for venue in venues
                if venue["city_id"] == city_id
            ]
            if not venues_with_matching_city:
                print(f"No existing venues with city id {city_id}")
                return None, None
            else:
                for existing_venue in venues_with_matching_city:
                    if len(venue_name) > 7 and levenshtein_distance(existing_venue["name"], venue_name) <= 2:
                        venue_slug = existing_venue["slug"]
                        venue_name = existing_venue["name"]
                        continue
                if venue_slug not in existing_venues_by_slug:
                    return None, None
        else:
            return None, None

    venue_id = candidate_city_id = None
    candidates = existing_venues_by_slug[venue_slug]                                                                    # get all venues that have the given name

    for candidate in candidates:
        candidate_city_id = candidate["city_id"]
        candidate_city_name = existing_cities_by_id[int(candidate_city_id)]["name"]

        if any(educational_token in venue_slug for educational_token in EDUCATIONAL_TOKENS):
            venue_id = candidate["id"]
            venue_name = candidate["name"]
            break
        if city_id == candidate_city_id:
            venue_id = candidate["id"]
            venue_name = candidate["name"]
            break
        elif city_name == candidate_city_name:                                                                          # check if potential city name matches
            venue_id = candidate["id"]
            venue_name = candidate["name"]
            break
        else:
            print(f"Candidate {candidate} does not have matching city id or city name")

    return venue_id, venue_name

def looks_like_educational_institution(location_tokens):
    return any(educational_tokens in location_tokens for educational_tokens in EDUCATIONAL_TOKENS)

def isolate_venue_name(location_tokens):
    """
    Checks for a type on the venue type. Last word of venue is usually Hall, Auditorium, Center, etc... so it uses
    venue_patterns to see if there is the venue type matches any of the common typos and corrects it

    :param location_tokens: the remaining location tokens, only the name of the venue should be left
    :return: the updated location tokens
    """

    venue_types = build_reverse_map(VENUE_TYPES_MAP)             # import the map of common venue typos to their corrected version
    venue_tokens = None
    reverse_furthest_keyword_idx = None
    found_venue_type = False

    if not any(token.lower() in venue_types for token in location_tokens):
        if looks_like_educational_institution(location_tokens):
            return location_tokens

    # loop backwards through the remaining tokens. Once a venue keyword is found, extract every token from the front of the list to the keyword
    for i, token in enumerate(reversed(location_tokens)):
        if token.lower() in venue_types:
            found_venue_type = True
            reverse_furthest_keyword_idx = i
            break

    if not found_venue_type:
        venue_tokens = location_tokens
    else:
        furthest_keyword_idx = len(location_tokens) - reverse_furthest_keyword_idx
        venue_tokens = location_tokens[:furthest_keyword_idx]

    return venue_tokens

def correct_location_typos(location_tokens):
    corrected_tokens = []

    for token in location_tokens:
        lowered = token.lower()
        corrected = token

        for misspelled_city, correct_city in CITY_ALIAS_MAP.items():
            pattern = rf"\b{re.escape(misspelled_city)}\b"
            if re.search(pattern, lowered):
                corrected = re.sub(pattern, correct_city.title(), corrected, flags=re.IGNORECASE)

        corrected_tokens.append(corrected)

    return corrected_tokens

def identify_venue_name(processed_events_df, dimension_tables):
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
        state_id, location_tokens = match_state_after_venue(location_tokens)

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

def curate_location(location, dimension_tables):
    """

    :param location: list of strings
    :param dimension_tables:
    :return:
    """
    dim_cities = dimension_tables["cities"]
    dim_venues = dimension_tables["venues"]

    venue_id = venue_name = city_id = city_index = city_candidate = None
    location_tokens = [token for part in location for token in part.split()]  # split every word/item into a token
    location_tokens = normalize_location_tokens(location_tokens)
    location_tokens = clean_location(location_tokens)
    state_id, location_tokens = match_state_after_venue(location_tokens)

    city_id, city_name, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)

    # if existing city was found, everything before the city should be the venue name
    if city_id:
        location_tokens = location_tokens[:city_index]
        print(f"Location tokens after cutting: {location_tokens}")
    # if no existing city was found, check for a possible city to be recorded
    else:
        city_candidate, venue_type_idx = find_city_candidate(location_tokens)  # find what looks like a city name
        if city_candidate and state_id is not None:  # if city candidate found and a state was found
            city_id = append_city(city_candidate, dim_cities, state_id)  # add city to dim_cities
        if venue_type_idx:  # remove city candidate from location tokens
            location_tokens = location_tokens[:venue_type_idx + 1]

    if state_id is None:
        state_id, location_tokens = match_state_in_venue(location_tokens)  # check if there is a state in the venue name

    if city_id is None:
        city_id = match_city_in_venue(location_tokens, dim_cities, state_id)  # check if there is a city in the venue name

    print(f"Location tokens before isolate venue name: {location_tokens}")
    location_tokens = isolate_venue_name(location_tokens)
    print(f"Location tokens after isolate venue name: {location_tokens}")
    location_tokens = correct_location_typos(location_tokens)
    print(f"Location tokens after correct location typos: {location_tokens}")

    if city_id is None:
        city_id = match_city_in_venue(location_tokens, dim_cities, state_id)

    venue_name = " ".join(location_tokens)
    print(f"Venue name: {venue_name}")

    if venue_name is not None:
        venue_id, existing_venue_name = match_existing_venues(venue_name, dim_venues, city_id, city_candidate, dim_cities)  # check if the venue already exists in dim_venues

        if venue_id is None:  # if it is a new venue
            venue_id = append_venue(venue_name, dim_venues, city_id, state_id)  # add it to the dim_venues table
        else:
            venue_name = existing_venue_name

    return venue_id, venue_name

def curate_locations(processed_events_df, dimension_tables):
    '''

    :param processed_events_df: a dataframe of all the events in the current Billboard issue
    :param dimension_tables:
    :return:
    '''
    venue_ids = []
    venue_names = []

    for location in processed_events_df["location"]:
        print(f"Location = {location}")
        venue_id, venue_name = curate_location(location, dimension_tables)
        venue_ids.append(venue_id)
        venue_names.append(venue_name)

    return venue_ids, venue_names
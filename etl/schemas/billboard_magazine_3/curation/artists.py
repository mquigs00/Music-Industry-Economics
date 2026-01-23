from etl.dimensions.artists import update_artists_dim, clean_artist_name, get_artist_ids
import slugify

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

def merge_artists(raw_artist_strings):
    """
    Merge any artist names that overflow across multiple tokens into one token

    :param raw_artist_strings: list
    :return:
    """
    merged_artists = []
    signal = None

    overflow_signals = ['&', 'AND', 'THE']

    i = 0

    while i < len(raw_artist_strings):
        signal = None
        token = raw_artist_strings[i]

        # if the last token signifies overflow
        for overflow_signal in overflow_signals:
            if token.endswith(overflow_signal):
                signal = overflow_signal
                break
        if signal:                                                                                                      # if an overflow signal was found
            if len(raw_artist_strings) > i+1:
                merged_artist = f"{token} {raw_artist_strings[i + 1]}".strip()                                          # merge current token with next token
                merged_artists.append(clean_artist_name(merged_artist))
                i += 2                                                                                                  # skip next token
            else:
                print(f"Overflow signal detected but no token after {raw_artist_strings[i]}")
                i += 1
        else:
            merged_artists.append(clean_artist_name(token))
            i += 1

    return merged_artists

def separate_artists(raw_artists_strings, has_event_name):
    """
    Separate artists that appeared on the same line but are not part of the same group

    :param raw_artists_strings:
    :param has_event_name:
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
                separated_artists.append(clean_artist_name(left))

                if i + 1 < len(raw_artists_strings):                                                                    # if there is still another token in the list
                    separated_artists.append(clean_artist_name(f"{right}/{raw_artists_strings[i+1]}"))                  # combine right side with next token
                    i += 1
                else:
                    separated_artists.append(clean_artist_name(right))
            else:
                separated_artists.append(clean_artist_name(token))
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

def score_candidate(candidate, dim_artists):
    dim_artists_by_slug = dim_artists['by_slug']
    score = 0

    if slugify.slugify(candidate) in dim_artists_by_slug:
        score += 10

    word_count = len(candidate.split())

    if word_count > 1:
        score += 2
    else:
        score -= 1

    return score

def validate_artist(artist, dim_artists):
    """
    Checks if an artist candidate matches an existing artist in the dimension table

    :param artist: string
    :param dim_artists: dict, the existing artists
    :return:
    """
    candidates = generate_artist_candidates(artist)                                                                     # generate potential candidates from the string
    candidate_match = None
    max_score = 0

    for candidate in candidates:
        candidate_score = score_candidate(candidate, dim_artists)                                                       # generate a score for the candidate
        if candidate_score > max_score:
            candidate_match = candidate
            max_score = candidate_score

    return candidate_match

def parse_artist_names(raw_artists_strings, has_event_name, dim_artists):
    """
    Processed artist list can contain multiple artists in one token. This function separates every artist into a separate token

    :param raw_artists_strings: list of raw artists strings. Ex: ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS']
    :param has_event_name: bool, if the event's raw artist started with an event name like "OZZFEST 1997:"
    :param dim_artists: dict, the existing artists dimension table
    :return: final_artists: the artists list with each artist in one token
    """

    merged_artists = merge_artists(raw_artists_strings)
    separated_artists = separate_artists(merged_artists, has_event_name)
    print("Separated Artists:")
    print(separated_artists)

    curated_artists = []

    for artist_string in separated_artists:
        validated_artist = validate_artist(artist_string, dim_artists)
        print(f"Validated Artist: {validated_artist}")
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

    for artist in all_artists:
        print(artist)

    update_artists_dim(all_artists, dim_artists)

    curated_events_df["artist_ids"] = processed_events_df["artists_clean"].apply(
        lambda artist_names: get_artist_ids(artist_names, dim_artists)
    )
from etl.dimensions.artists import update_artists_dim, clean_artist_name, get_artist_ids

def parse_event_artists(artists):
    separated_artists = []

    if any(',' in artist for artist in artists):
        combined_artists = " ".join(artists)
        separated_artists = combined_artists.split(",")
        separated_artists = [artist.strip() for artist in separated_artists]
        return separated_artists
    else:
        return artists

def parse_artist_names(artists, has_event_name):
    """
    Processed artist list can contain multiple artists in one token. This function separates every artist into a separate token

    :param artists:
    :param has_event_name:
    :return:
    """
    separated_artists = []
    final_artists = []

    # if there is event name that signifies a festival separate artists that are on the same line and have a comma between them
    # if there is no event name, Billboard does not use commas to separate are
    if has_event_name:
        separated_artists = parse_event_artists(artists)
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
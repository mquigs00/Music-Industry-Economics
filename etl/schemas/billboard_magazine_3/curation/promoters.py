from Levenshtein import distance as levenshtein_distance
import slugify
from etl.dimensions.promoters import update_dim_promoters
import ast

def parse_promoters(promoters_list, venue_names):
    promoters_per_event = []
    unique_promoters = set()

    # loop through the list of promoter strings for each event
    for event_idx, event_promoters in enumerate(promoters_list):
        event_promoters_str = "".join(event_promoters)                                                                  # join all promoter lines to one string
        individual_promoters = event_promoters_str.split('/')                                                           # split by '/' to clean each promoter separately
        cleaned_event_promoters = []

        for promoter in individual_promoters:
            next_promoter = []
            promoter_tokens = promoter.split()  # the next promoter by whitespaces
            for token in promoter_tokens:
                if validate_promoter(token):
                    if levenshtein_distance("in-house", token.lower()) < 2:                                         # check if promoter looks like "In-House
                        if venue_names[event_idx]:
                            next_promoter.append(venue_names[event_idx])                                                    # if so, use the venue name as the promoter
                        else:
                            next_promoter = None
                    else:
                        next_promoter.append(token)                                                                     # otherwise use the literal text
            if next_promoter:
                next_promoter = " ".join(next_promoter)                                                                     # combine validated tokens to get promoter name
                unique_promoters.add(next_promoter)
                cleaned_event_promoters.append(next_promoter)

        promoters_per_event.append(cleaned_event_promoters)

    return promoters_per_event, unique_promoters

def curate_promoters(processed_events_df, curated_events_df, dim_promoters, venue_names):
    promoters_list = processed_events_df["promoter"]

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

def validate_promoter(token):
    if any(char.isdigit() for char in token):
        return False
    if '$' in token:
        return False

    return True
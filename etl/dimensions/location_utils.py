import math

def get_venue_name(venue_id, dim_venues):
    """

    :param venue_id: int
    :param dim_venues
    :return:
    """
    if math.isnan(venue_id):
        return ""

    print(venue_id)

    if isinstance(venue_id, str):
        if venue_id.isdigit():
            venue_id = int(venue_id)
    elif isinstance(venue_id, bool):
        raise TypeError("venue_id must be int, not bool")
    elif isinstance(venue_id, float):
        venue_id = int(venue_id)
    if not isinstance(venue_id, int):
        print(f"venue_id must be int, not {type(venue_id)}")

    dim_venues_by_id = dim_venues["by_id"]
    venue_record = dim_venues_by_id.get(venue_id)
    print(f"Venue Record for id {venue_id}")
    print(venue_record)
    venue_name = venue_record["name"]
    return venue_name
from utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pandas as pd

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_curate_location_clean():
    test_data = [["['Oakland Coliseum', 'Calif.']"]]
    test_df = pd.DataFrame(test_data, columns=['location'])
    test_df["venue_id"], venue_names = curate_location(test_df, dimension_tables)

    assert venue_names == ['Oakland Coliseum']
    assert test_df["venue_id"] is not None

def test_curate_location_promoter():
    test_data = [["['Reunion Arena', 'Dallas', 'Productions']"]]
    test_df = pd.DataFrame(test_data, columns=['location'])
    test_df["venue_id"], venue_names = curate_location(test_df, dimension_tables)

    assert venue_names == ['Reunion Arena']

def test_match_state_after_venue():
    location_data = ['Byrne', 'Meadowlands', 'Arena', 'East', 'Ruthertord,', 'NJ.']
    state_id = match_state_after_venue(location_data)

    assert state_id == 30

def test_match_state_in_venue_ambiguous():
    location_data = ['Casper', '(Wya.)', 'Events', 'Center']
    state_id = match_state_in_venue(location_data)
    assert state_id == -1

def test_city_at_end_of_venue_name():
    location_tokens = ['Univ.', 'of', 'Colorado', 'at', 'Boulder']
    city_id, city_name, city_index = match_city_after_venue(location_tokens, None, dim_cities)
    if city_id is not None:
        location_tokens = location_tokens[:city_index]
    location_data = clean_location(location_tokens)
    venue_name = " ".join(location_data)
    assert city_name == 'Boulder'
    assert venue_name == 'Univ. of Colorado at Boulder'

def test_match_city_in_venue_typo():
    location_tokens = ['Harttord', '(Conn.)', 'Civic', 'Center', '&', 'Associates', 'for', 'Pertorming', 'Arts']
    state_id, location_tokens = match_state_in_venue(location_tokens)
    city_candidate, venue_type_idx = find_city_candidate(location_tokens)  # find what looks like a city name
    if venue_type_idx:                                                                                                  # remove city candidate from location tokens
        location_tokens = location_tokens[:venue_type_idx + 1]
    print(location_tokens)
    city_id = match_city_in_venue(location_tokens, dim_cities, state_id)
    location_tokens = clean_venue_name(location_tokens)
    venue_name = " ".join(location_tokens)
    venue_id, venue_name = match_existing_venues(venue_name, dim_venues, city_id, city_candidate, dim_cities)
    assert state_id == 7
    assert city_id == 132
    assert venue_name == 'Hartford Civic Center'

def test_venue_keyword_before_end_of_name():
    # venue name should end at the last event keyword, not at the first event keyword
    location_data = ['Memorial', 'Coliseum']
    location_data = clean_venue_name(location_data)
    venue_name = " ".join(location_data)
    assert venue_name == "Memorial Coliseum"

def test_clean_venue_name():
    location_date = ['Univ,', 'of', 'Tennessee', 'at', 'Chattanooga', 'Arena']
    clean_location_data = clean_venue_name(location_date)
    venue_name = " ".join(clean_location_data)
    assert venue_name == "Univ. of"

def test_multiple_venue_keywords():
    location_data = ['Stabler', 'Arena,', 'Lehigh', 'Unv.', 'Bethlehem,', 'Pa.']
    state_id, location_tokens = match_state_after_venue(location_data)
    city_id, city_name, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)
    location_tokens = location_tokens[:city_index]
    location_tokens = isolate_venue_name(location_tokens)
    location_tokens = clean_venue_name(location_tokens)
    venue_name = " ".join(location_tokens)
    assert state_id == 38
    assert city_name == "Bethlehem"
    assert venue_name == "Stabler Arena, Lehigh Univ"

def test_isolate_venue_name_unrelated_text():
    remaining_tokens = ['Harttord', '(Conn.)', 'Civic', 'Center', '&', 'Associates', 'for', 'Pertorming', 'Arts']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Harttord (Conn.) Civic Center"

def test_isolate_venue_name_multiple_keywords():
    remaining_tokens = ['Memorial', 'Auditorium']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Memorial Auditorium"

def test_clean_venue_name():
    location_tokens = ['Stabler', 'Arena,', 'Lehigh', 'Unv.']
    clean_tokens= clean_venue_name(location_tokens)
    venue_name = " ".join(clean_tokens)
    assert venue_name == "Stabler Arena Lehigh Univ"

def test_isolate_venue_name_empty():
    remaining_tokens = []
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == ""

def test_match_city_after_venue():
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
    test_data = clean_location(test_data)
    city_id, city_name, city_index = match_city_after_venue(test_data, None, dim_cities)

    assert city_name == "Dallas"

def test_match_city_after_venue_2():
    test_data = ['Thomas & Mack Center', 'Las Vegas']
    test_data = clean_location(test_data)
    city_id, city_name, city_index = match_city_after_venue(test_data, None, dim_cities)

    assert city_name == "Las Vegas"

def test_match_city_in_venue():
    test_data = ['San', 'Francisco', 'Civic', 'Auditorium']
    state_id = match_state_after_venue(test_data)
    city_id, city_name, city_index = match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == 'San Francisco'

def test_match_city_in_venue_noise():
    test_data = ['Frank', 'Cainer', 'Arena', 'Behan', 'Park', 'Nanaima', 'B.C.']
    test_data = clean_location(test_data)
    state_id = match_state_after_venue(test_data)
    city_id, city_name, city_index = match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == 'San Francisco'

def test_find_city_state_after_venue():
    test_data = ['Hilton', 'Coliseum', 'Ames', 'Iowa']
    state_id = match_state_after_venue(test_data)
    city_id, city_name, city_index = match_city_after_venue(test_data, state_id, dim_cities)
    assert state_id == 15
    assert city_name == 'Ames'

def test_match_city_after_venue_state_in_venue_name():
    test_data = ['Middle', 'Tennessee', 'State', 'Univ.', 'Murphy Center', 'Murfreesboro']
    state_id = match_state_after_venue(test_data)
    city_id, city_name, city_index = match_city_after_venue(test_data, state_id, dim_cities)
    assert state_id is None
    assert city_id is None

def test_find_state_in_venue_no_parentheses():
    test_data = ['Rochester' 'N.Y,' 'War', 'Memorial']
    state_id = match_state_in_venue(test_data)
    assert state_id == 32

def test_find_venue_type_idx():
    test_data = ['Frank', 'Cainer', 'Arena', 'Behan', 'Park', 'Nanaima']
    venue_type_idx = find_venue_type_idx(test_data)
    assert venue_type_idx == 2

def test_new_venue_existing_city():
    test_data = ['Oakland', 'Coliseum', 'Calif.']
    state_id = match_state_after_venue(test_data)
    city_id, city_name, city_idx = match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == "Oakland"
    assert state_id == 5

def test_find_city_candidate():
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
    test_data = clean_location(test_data)
    city_candidate, venue_type_idx = find_city_candidate(test_data)
    assert city_candidate == "Dallas"
    assert venue_type_idx == 1
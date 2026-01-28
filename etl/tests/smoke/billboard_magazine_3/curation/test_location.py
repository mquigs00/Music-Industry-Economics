from utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pandas as pd
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

### CLEAN LOCATION
@pytest.mark.xfail(reason="Ambiguous promoter/location text; requires manual correction record")
def test_clean_location_ambiguous_promoter_text():
    raw_location = ['Harttord (Conn.) Civic Center', '& Associates', 'for Pertorming Arts']                             # BB-1984-11-24
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Harttord (Conn) Civic Center']

def test_clean_location_num_sellouts():
    raw_location = ['Riverside Theater', 'Milwaukee', 'sellouts']                                                       # BB-1984-11-24
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Riverside Theater', 'Milwaukee']

def test_clean_location_attractions():
    raw_location = ['The Forum', 'Inglewood, Cali', 'Attractions']                                                      # BB-1984-11-24
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['The Forum', 'Inglewood Cali']

@pytest.mark.xfail(reason="Ambigous artist/location text; requires manual correction record for now")
def test_clean_location_artist():
    raw_location = ['Providence (R.I.) Civic Center', '& THE E STREET BAND']
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Providence (R.I.) Civic Center']

@pytest.mark.xfail(reason="Currently 'sellout' is a signal of unrelated data so the second token is removed")
def test_clean_location_sellout_mixed_in():
    raw_location = ['Long Beach Arena, Long Beach', 'Convention & Entertainment sellout',                               # Billboard-1987-09-26
                    'Center', 'Long Beach, Calif.']

    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Long Beach Arena Long Beach', 'Convention & Entertainment', 'Center', 'Long Beach Calif']

### TEST MATCH STATE AFTER VENUE
def test_match_state_after_venue():
    location_data = ['Byrne', 'Meadowlands', 'Arena', 'East', 'Ruthertord,', 'NJ.']
    state_id = match_state_after_venue(location_data)

    assert state_id == 30

### TEST MATCH STATE IN VENUE
def test_match_state_in_venue_ambiguous():
    location_data = ['Casper', '(Wya.)', 'Events', 'Center']
    state_id = match_state_in_venue(location_data)
    assert state_id == -1

@pytest.mark.xfail(reason="Unclear that city is part of venue name and should not be removed")
def test_city_after_venue_at_end_of_venue_name():
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
    location_tokens = clean_location(location_tokens)
    venue_name = " ".join(location_tokens)
    venue_id, venue_name = match_existing_venues(venue_name, dim_venues, city_id, city_candidate, dim_cities)
    assert state_id == 7
    assert city_id == 132
    assert venue_name == 'Hartford Civic Center'

def test_venue_keyword_before_end_of_name():
    location_data = ['Memorial', 'Coliseum']
    location_data = clean_location(location_data)
    venue_name = " ".join(location_data)
    assert venue_name == "Memorial Coliseum"

def test_multiple_venue_keywords():
    location_data = ['Stabler', 'Arena,', 'Lehigh', 'Unv.', 'Bethlehem,', 'Pa.']
    state_id, location_tokens = match_state_after_venue(location_data)
    city_id, city_name, city_index = match_city_after_venue(location_tokens, state_id, dim_cities)
    location_tokens = location_tokens[:city_index]
    location_tokens = isolate_venue_name(location_tokens)
    location_tokens = clean_location(location_tokens)
    venue_name = " ".join(location_tokens)
    assert state_id == 38
    assert city_name == "Bethlehem"
    assert venue_name == "Stabler Arena, Lehigh Univ"

### ISOLATE VENUE NAME
def test_isolate_location_center():
    remaining_tokens = ['Harttord', '(Conn.)', 'Civic', 'Center', '&', 'Associates', 'for', 'Pertorming', 'Arts']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Harttord (Conn.) Civic Center"

def test_isolate_venue_name_multiple_keywords():
    remaining_tokens = ['Memorial', 'Auditorium']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Memorial Auditorium"

@pytest.mark.xfail(reason="Isolate venue name assumes that venue keyword is the end of the venue name")
def test_isolate_venue_name_starts_with_keyword():
    remaining_tokens = ['Memorial', 'Union']                                                                            # BB-1984-11-24
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Memorial Union"

def test_isolate_venue_name_empty():
    remaining_tokens = []
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == ""

def test_isolate_venue_name_university():
    remaining_tokens = ['Univ. of Colorado at Boulder']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Univ. of Colorado at Boulder"

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

### CURATE LOCATION
def test_curate_location_clean():
    raw_location = ['Oakland Coliseum', 'Calif.']
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == 'Oakland Coliseum'

def test_curate_location_promoter():
    raw_location = ['Reunion Arena', 'Dallas', 'Productions']
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == 'Reunion Arena'

@pytest.mark.xfail(reason="Unclear that Boulder is part of the venue name, so it gets extracted")
def test_curate_location_city_at_end_of_venue_name():
    raw_location = ['Univ. of Colorado at Boulder']                                                                     # BB-1984-11-24
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == 'Univ. of Colorado at Boulder'

def test_curate_location_state_at_beginning_of_venue_name():
    raw_location = ['Utah State Univ. Spectrum']                                                                        # BB-1984-11-24
    venue_id, venue_name = curate_location(raw_location, dimension_tables)

    assert venue_name == 'Utah State Univ. Spectrum'

@pytest.mark.xfail(reasos="Unclear that Cal is part of the venue name so it is extracted")
def test_curate_location_existing_no_city_or_state():
    raw_location = ['Cal Expo Amphitheatre', 'Sacramento']                                                              # BB-1984-11-24
    venue_id, venue_name = curate_location(raw_location, dimension_tables)

    assert venue_name == ['Cal Expo Amphitheatre', 'Sacramento']
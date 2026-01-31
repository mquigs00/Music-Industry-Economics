from etl.utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_match_city_after_venue_at_end_of_venue_name():
    location_tokens = ['Univ.', 'of', 'Colorado', 'at', 'Boulder']
    city_id, city_name, city_index = match_city_in_venue(location_tokens, dim_cities, dim_venues)
    assert city_name == 'Boulder'

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

def test_match_existing_venue_typo():
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

@pytest.mark.xfail(reasos="Unclear that 'Cal' is part of the venue name so it is extracted")
def test_curate_location_existing_no_city_or_state():
    raw_location = ['Cal Expo Amphitheatre', 'Sacramento']                                                              # BB-1984-11-24
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == ['Cal Expo Amphitheatre', 'Sacramento']

def test_curate_location_initials():
    raw_location = ['James L. Knight', 'International Center Miami']
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == "James L Knight International Center"

def test_curate_location_no_venue_type():
    raw_location = ['Houston Summit']                                                                                   # BB-1985-01-19
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == "Houston Summit"

def test_curate_location_venue_starts_with_city():
    raw_location = ['Cincinnati Gardens']                                                                               # BB-1985-01-19
    venue_id, venue_name = curate_location(raw_location, dimension_tables)
    assert venue_name == ['Cincinnati Gardens']
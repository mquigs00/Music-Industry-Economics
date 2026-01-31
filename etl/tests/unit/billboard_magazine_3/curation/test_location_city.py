from etl.utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_find_city_candidate():
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
    test_data = clean_location(test_data)
    city_candidate, venue_type_idx = find_city_candidate(test_data)
    assert city_candidate == "Dallas"
    assert venue_type_idx == 1

@pytest.mark.xfail(reason="When no state is provided the function only matches unique city names that perfectly match")
def test_match_city_after_venue_city_typo_no_state():
    location_tokens = ['Fox Theater', 'St. Lous']
    city_id, city_name, city_index = match_city_after_venue(location_tokens, None, dim_cities)
    assert city_name == 'St. Louis'

def test_match_city_after_venue_city_no_state():
    location_tokens = ['Freedom', 'Mall', 'Louisville']
    city_id, city_name, city_index = match_city_after_venue(location_tokens, None, dim_cities)
    assert city_id == 150

def test_match_city_in_venue_one_word():
    clean_location_tokens = ['Tallahassee', 'Leon', 'County', 'Civic', 'Center']                                                   # BB-1984-12-01
    city_id = match_city_in_venue(clean_location_tokens, dim_cities, 9)
    assert city_id == 147

def test_match_city_in_venue_two_words():
    test_data = ['San', 'Francisco', 'Civic', 'Auditorium']
    city_id = match_city_in_venue(test_data, dim_cities, 5)
    assert city_id == 17

def test_match_city_after_venue_after_venue():
    test_data = ['Hilton', 'Coliseum', 'Ames']
    city_id, city_name, city_index = match_city_after_venue(test_data, 15, dim_cities)
    assert city_id == 133

def test_match_city_after_venue_state_in_venue_name():
    test_data = ['Middle', 'Tennessee', 'State', 'Univ.', 'Murphy', 'Center', 'Murfreesboro']
    city_id, city_name, city_index = match_city_after_venue(test_data, 42, dim_cities)
    assert city_id == 134
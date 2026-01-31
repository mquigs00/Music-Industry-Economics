from etl.utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_match_state_after_venue():
    location_tokens = ['Byrne', 'Meadowlands', 'Arena', 'East', 'Ruthertord,', 'NJ.']
    state_id, location_tokens = match_state_after_venue(location_tokens)
    assert state_id == 30

def test_match_state_in_venue_ambiguous():
    location_data = ['Casper', '(Wya.)', 'Events', 'Center']
    state_id, location_tokens = match_state_in_venue(location_data)
    assert state_id == -1

def test_match_state_in_venue():
    location_tokens = ['New', 'Haven', 'Conn.)', 'Coliseum']
    state_id, location_tokens = match_state_in_venue(location_tokens)
    assert state_id == 7

def test_find_state_in_venue_no_parentheses():
    test_data = ['Rochester' 'N.Y,' 'War', 'Memorial']
    state_id = match_state_in_venue(test_data)
    assert state_id == 32
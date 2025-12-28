import pandas as pd
from etl.curation.magazines.billboard import curate_boxoffice_3 as curator
from datetime import date
from utils.utils import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_curate_noise_in_dates():
    dates = ['Sept. 30 9144.zia 10,894']
    issue_year = 1984

    start_date, end_date, dates = curator.curate_date(dates, issue_year)
    assert start_date == date(1984, 9, 30)
    assert end_date == date(1984, 9, 30)

def test_curate_location_clean():
    test_data = [["['Oakland Coliseum', 'Calif.']"]]
    test_df = pd.DataFrame(test_data, columns=['location'])
    test_df["venue_id"], venue_names = curator.curate_location(test_df, dimension_tables, test_df)

    assert venue_names == ['Oakland Coliseum']
    assert test_df["venue_id"] is not None

def test_curate_location_promoter():
    test_data = [["['Reunion Arena', 'Dallas', 'Productions']"]]
    test_df = pd.DataFrame(test_data, columns=['location'])
    test_df["venue_id"], venue_names = curator.curate_location(test_df, dimension_tables, test_df)

    assert venue_names == ['Reunion Arena']

def test_match_city_after_venue():
    # although Dallas is in the location, it should not match it because there is no state provided
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
    test_data = clean_location(test_data)
    city_id, city_name, city_index = curator.match_city_after_venue(test_data, None, dim_cities)

    assert city_name is None

def test_match_city_in_venue():
    test_data = ['San', 'Francisco', 'Civic', 'Auditorium']
    state_id = curator.match_state_after_venue(test_data)
    city_id, city_name, city_index = curator.match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == 'San Francisco'

def test_match_city_in_venue_noise():
    test_data = ['Frank', 'Cainer', 'Arena', 'Behan', 'Park', 'Nanaima', 'B.C.']
    test_data = clean_location(test_data)
    state_id = curator.match_state_after_venue(test_data)
    city_id, city_name, city_index = curator.match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == 'San Francisco'

@pytest.mark.only
def test_find_venue_type_idx():
    test_data = ['Frank', 'Cainer', 'Arena', 'Behan', 'Park', 'Nanaima']
    venue_type_idx = curator.find_venue_type_idx(test_data)
    assert venue_type_idx == 2

def test_new_venue_existing_city():
    test_data = ['Oakland', 'Coliseum', 'Calif.']
    state_id = curator.match_state_after_venue(test_data)
    city_id, city_name, city_idx = curator.match_city_in_venue(test_data, dim_cities, state_id)
    assert city_name == "Oakland"
    assert state_id == 5

def test_find_city_candidate():
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
    test_data = clean_location(test_data)
    city_candidate, venue_type_idx = curator.find_city_candidate(test_data)
    assert city_candidate == "Dallas"
    assert venue_type_idx == 1

def test_match_existing_venues():
    test_data = ['Reunion', 'Arena', 'Dallas', 'Productions']
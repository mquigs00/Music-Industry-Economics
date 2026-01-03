import pandas as pd
from etl.curation.magazines.billboard import curate_boxoffice_3 as curator
from datetime import date

from etl.curation.magazines.billboard.curate_boxoffice_3 import parse_artist_names
from utils.utils import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

# DATES
def test_curate_noise_in_dates():
    dates = ['Sept. 30 9144.zia 10,894']
    issue_year = 1984

    start_date, end_date, dates = curator.curate_date(dates, issue_year)
    assert start_date == date(1984, 9, 30)
    assert end_date == date(1984, 9, 30)

# ARTISTS
@pytest.mark.only
def test_curate_artists_festival():
    # if there is an event name, artists should be split by commas, and ampersands at the end of lines should signal to join adjacent artists
    orig_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, artists = curator.parse_event_name(orig_artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name == 'Budweiser Superfest'
    assert artists == ['PEABO BRYSON', 'KOOL & THE GANG', 'WHISPERS', 'MTUME', 'PATTI LABELLE']

def test_curate_artists_with_comma():
    # if there is no event name, commas should not be used to split artist names
    artists = ['CROSBY, STILLS & NASH']
    event_name, artists = curator.parse_event_name(artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name is None
    assert artists == ['CROSBY, STILLS & NASH']

#LOCATION
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

def test_match_state_after_venue():
    location_data = ['Byrne', 'Meadowlands', 'Arena', 'East', 'Ruthertord,', 'NJ.']
    state_id = curator.match_state_after_venue(location_data)

    assert state_id == 30

def test_match_state_in_venue_ambiguous():
    location_data = ['Casper', '(Wya.)', 'Events', 'Center']
    state_id = curator.match_state_in_venue(location_data)
    assert state_id == -1

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

# EVENT NAME
def test_parse_event_name_no_event_name():
    artist_lines = ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO D']
    event_name, updated_artists = curator.parse_event_name(artist_lines)
    assert event_name is None
    assert updated_artists == [
        'FIXX',
        'RONNIE HAYES & THE WILD',
        'COMBO D'
    ]

def test_parse_event_name_colon_first_line():
    artist_lines = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, updated_artists = curator.parse_event_name(artist_lines)
    assert event_name == "Budweiser Superfest"
    assert updated_artists == [
        'PEABO BRYSON, KOOL &',
        'THE GANG, WHISPERS,',
        'MTUME, PATTI LABELLE'
    ]

def test_parse_event_name_colon_second_line():
    artist_lines = ['10TH ANNUAL TEXXAS WORLD', 'MUSIC FESTIVAL:', 'BOSTON, AEROSMITH', 'WHITESNAKE, POISON, TESLA', 'FARRENHEIT']
    event_name, updated_artists = curator.parse_event_name(artist_lines)
    assert event_name == "10th Annual Texxas World Music Festival"
    assert updated_artists == [
        "BOSTON, AEROSMITH",
        "WHITESNAKE, POISON, TESLA",
        "FARRENHEIT"
    ]

def test_parse_event_name_false_colon():
    # colon usually means preceding text is an event name, but this colon was falsely read by OCR. Function should simply ignore colon and leave artists as is
    artist_lines = ['TOM JONES:', 'GEORGE WALLACE']
    event_name, updated_artists = curator.parse_event_name(artist_lines)
    assert event_name is None
    assert updated_artists == [
        'TOM JONES:',
        'GEORGE WALLACE'
    ]

def test_parse_event_name_keyword_no_colon():
    # event name usually is signaled by the presence of a colon, but this event name is still signaled by the keyword 'SHOW'
    artist_lines = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    event_name, updated_artists = curator.parse_event_name(artist_lines)
    assert event_name == "Richard Nader's Valentine's Doo Wopp Show"
    assert updated_artists == [
        "LITTLE ANTHONY",
        "FRED PARIS & THE LITTLE",
        "SATINS",
        "THE BELMONTS & MARVELETTES"
    ]
from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names
from etl.schemas.billboard_magazine_3.curation.special_event import parse_event_name
from etl.schemas.billboard_magazine_3.curation.dates import curate_date
from etl.utils.utils import load_dimension_tables
from datetime import date
import pytest

dim_special_events = load_dimension_tables()["special_events"]
dim_artists = load_dimension_tables()["artists"]

def test_event_year_before_issue_year():
    object_key = "processed/billboard/magazines/1985/01/BB-1985-01-19.csv"
    issue_year = int(object_key.split('/')[-3])
    issue_month = int(object_key.split('/')[-2])
    raw_dates = ['Dec. 31']
    start_date, end_date, total_dates = curate_date(raw_dates, issue_year, issue_month)
    print(start_date)
    assert start_date == date(1984, 12, 31)

def test_curate_artists_with_comma():
    """
    Despite having commas, this is just a single artist/group and should not be treated as a special event with multiple artists
    Example from
    """
    artists = ['CROSBY, STILLS & NASH']
    event_name, artists = parse_event_name(artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name is None
    assert artists == ['CROSBY, STILLS & NASH']

def test_curate_artists_festival():
    """

    Example from BB-1984-11-24
    """
    orig_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, artists = parse_event_name(orig_artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name == 'Budweiser Superfest'
    assert artists == ['PEABO BRYSON', 'KOOL & THE GANG', 'WHISPERS', 'MTUME', 'PATTI LABELLE']

def test_curate_artists_no_keyword_has_colon_comma():
    """

    Example from BB-1984-12-01
    """
    orig_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    event_name, artists = parse_event_name(orig_artists, dim_special_events)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name, dim_artists)
    assert event_name == 'Guitar Greats'
    assert artists == ['DAVID GILMOUR', 'DAVE EDMUNDS', 'JOHNNY WINTER', 'BRIAN SETZER', 'NEAL SCHON', 'DICKIE BETTS', 'TONY IOMMI', 'STEVE CROPPER', 'LINK WRAY']

def test_curate_artists_festival_period():
    orig_artists = ['WORLD SERIES OF ROCK', 'WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
    event_name, artists = parse_event_name(orig_artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name == 'World Series of Rock'
    assert artists == ['WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
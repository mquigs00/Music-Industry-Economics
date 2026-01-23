from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names
from etl.schemas.billboard_magazine_3.curation.special_event import parse_event_name
from utils.utils import load_dimension_tables
import pytest

dim_special_events = load_dimension_tables()["special_events"]
dim_artists = load_dimension_tables()["artists"]

def test_curate_artists_with_comma():
    # if there is no event name, commas should not be used to split artist names
    artists = ['CROSBY, STILLS & NASH']
    event_name, artists = parse_event_name(artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name is None
    assert artists == ['CROSBY, STILLS & NASH']

def test_curate_artists_festival():
    orig_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']      # source: BB-1984-11-24
    event_name, artists = parse_event_name(orig_artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name == 'Budweiser Superfest'
    assert artists == ['PEABO BRYSON', 'KOOL & THE GANG', 'WHISPERS', 'MTUME', 'PATTI LABELLE']

@pytest.mark.only
def test_curate_artists_festival_2():
    orig_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    event_name, artists = parse_event_name(orig_artists, dim_special_events)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name, dim_artists)
    assert event_name == 'GUITAR GREATS'
    assert artists == ['DAVID GILMOUR', 'DAVE EDMUNDS', 'JOHNNY WINTER', 'BRIAN SETZER', 'NEAL SCHON', 'DICKIE BETTS', 'TONY IOMMI', 'STEVE CROPPER', 'LINK WRAY']

def test_curate_artists_festival_period():
    orig_artists = ['WORLD SERIES OF ROCK', 'WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
    event_name, artists = parse_event_name(orig_artists)
    has_event_name = event_name is not None
    artists = parse_artist_names(artists, has_event_name)
    assert event_name == 'WORLD SERIES OF ROCK'
    assert artists == ['WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
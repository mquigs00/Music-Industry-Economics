from etl.schemas.billboard_magazine_3.curation.special_event import parse_event_name, calc_special_event_score, extract_event_name
from utils.utils import load_dimension_tables
import pytest

dimension_tables = load_dimension_tables()
dim_special_events = dimension_tables['special_events']

# CALCULATE EVENT SCORE
def test_calc_special_event_score_has_all_signals():
    """
    Test the calculated event score when there is an event keyword, colon, and comma in the raw artists
    Example from BB-1984-11-24
    """
    raw_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 14

def test_calc_special_event_score_keyword_colon():
    """
    Just a special event keyword present should return a score of 7
    Example from
    """
    raw_artists = ['SWATCH WATCH: NEW YORK', 'CITY FRESH FESTIVAL']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 12

def test_calc_special_event_score_colon_comma():
    raw_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_only_keyword():
    """
    Just a special event keyword present should return a score of 7
    Example from
    """
    raw_artists = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_only_comma():
    raw_artists = ['CROSBY, STILLS & NASH']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 2

def test_calc_special_event_score_no_signals():
    raw_artists = ['HUEY LEWIS & THE NEWS', 'TOWER OF POWER', 'LOS LOBOS', 'EDDIE & THE TIDE']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 0

def test_calc_special_event_score_only_colon():
    """
    Just a colon present should return a score of 5
    Example from
    """
    raw_artists = ['CHICAGO/THE BEACH BOYS:']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 5

# EXTRACT EVENT NAME
def test_extract_event_name_keyword_then_colon():
    raw_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == 'Budweiser Superfest'
    assert updated_artists == ['PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']

def test_extract_event_name_colon_then_keyword():
    raw_artists = ['SWATCH WATCH: NEW YORK', 'CITY FRESH FESTIVAL']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == 'Swatch Watch: New York City Fresh Festival'
    assert updated_artists == []

def test_extract_event_name_keyword_no_colon():
    raw_artists = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == "Richard Nader's Valentine's Doo Wopp Show"
    assert updated_artists == ["LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]

def test_extract_event_name_colon_no_keyword():
    raw_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == "Guitar Greats"
    assert updated_artists == ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']

@pytest.mark.only
def test_extract_event_name_keyword_not_end_of_event_name():
    raw_artists = ['WORLD SERIES OF ROCK', 'WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == "World Series of Rock"
    assert updated_artists == ['WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']

# PARSE EVENT NAME
def test_parse_event_name_no_event_name():
    artist_lines = ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO D']
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name is None
    assert updated_artists == [
        'FIXX',
        'RONNIE HAYES & THE WILD',
        'COMBO D'
    ]

def test_parse_event_name_colon_first_line():
    artist_lines = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name == "Budweiser Superfest"
    assert updated_artists == [
        'PEABO BRYSON, KOOL &',
        'THE GANG, WHISPERS,',
        'MTUME, PATTI LABELLE'
    ]

def test_parse_event_name_only_colon_comma():
    """

    Example from BB-1984-12-01
    """
    raw_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    event_name, artists = parse_event_name(raw_artists, dim_special_events)
    assert event_name == 'Guitar Greats'
    assert artists == ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']

def test_parse_event_name_colon_second_line():
    artist_lines = ['10TH ANNUAL TEXXAS WORLD', 'MUSIC FESTIVAL:', 'BOSTON, AEROSMITH', 'WHITESNAKE, POISON, TESLA', 'FARRENHEIT']
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name == "10th Annual Texxas World Music Festival"
    assert updated_artists == [
        "BOSTON, AEROSMITH",
        "WHITESNAKE, POISON, TESLA",
        "FARRENHEIT"
    ]

def test_parse_event_name_false_colon():
    # colon usually means preceding text is an event name, but this colon was falsely read by OCR. Function should simply ignore colon and leave artists as is
    artist_lines = ['TOM JONES:', 'GEORGE WALLACE']
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name is None
    assert updated_artists == [
        'TOM JONES:',
        'GEORGE WALLACE'
    ]

def test_parse_event_name_keyword_no_colon():
    # event name usually is signaled by the presence of a colon, but this event name is still signaled by the keyword 'SHOW'
    artist_lines = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name == "Richard Nader's Valentine's Doo Wopp Show"
    assert updated_artists == [
        "LITTLE ANTHONY",
        "FRED PARIS & THE LITTLE",
        "SATINS",
        "THE BELMONTS & MARVELETTES"
    ]
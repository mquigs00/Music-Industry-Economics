from etl.schemas.billboard_magazine_3.curation.special_event import parse_event_name, calc_special_event_score, extract_event_name, find_event_end_index
from utils.utils import load_dimension_tables, load_event_keywords
import pytest
from config.paths import EVENT_KEYWORDS_PATH

dimension_tables = load_dimension_tables()
dim_special_events = dimension_tables['special_events']
event_keywords = load_event_keywords(EVENT_KEYWORDS_PATH)
strong_keywords = event_keywords["strong"]
weak_keywords = event_keywords["weak"]

# FIND IDX OF EVENT KEYWORD
def test_find_idx_of_event_keyword_idx_zero():
    raw_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']       # BB-1984-11-24
    end_idx_event_keyword = find_event_end_index(raw_artists, strong_keywords + weak_keywords)
    assert end_idx_event_keyword == 0

def test_find_idx_of_event_keyword_idx_one():
    raw_artists = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY",
                   "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    end_idx_event_keyword = find_event_end_index(raw_artists, strong_keywords+weak_keywords)
    assert end_idx_event_keyword == 1

def test_find_idx_of_event_keyword_idx_keyword_after_colon():
    raw_artists = ['SWATCH WATCH: NEW YORK', 'CITY FRESH FESTIVAL']
    end_idx_event_keyword = find_event_end_index(raw_artists, strong_keywords+weak_keywords)
    assert end_idx_event_keyword == 1

# CALCULATE EVENT SCORE
def test_calc_special_event_score_strong_keyword_colon_comma():
    raw_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']       # BB-1984-11-24
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 14

def test_calc_special_event_score_strong_keyword_colon():
    raw_artists = ['SWATCH WATCH: NEW YORK', 'CITY FRESH FESTIVAL']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 12

def test_cal_special_event_weak_keyword_colon():
    raw_artists = ['WYNF BIRTHDAY PARTY:', 'JOE WALSH']                                                                 # Billboard-1987-04-11
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 10

def test_calc_special_event_score_weak_keyword_apostrophe():
    raw_artists = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY",
                   "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_apostrophe_after_event_name():
    # in this case, the apostrophe comes after the event name, so it does not count toward the special event score
    raw_artists = ['AGREAT NIGHT FOR THE IRISH: |', 'FRANK PATTERSON', "GERALDINE O'GRADY",                             # BB-1988-04-02
                   'DES KEOGH', 'NA CASAIEIGH', "EILY O'GRADY, & OTHERS"]
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_colon_comma():
    raw_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,',
                   'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_colon_apostrophe():
    raw_artists = ["VAN HALEN'S MONSTERS OF", 'ROCK:', 'VAN HALEN', 'SCORPIONS', 'DOKKEN', 'METALLICA', 'KINGDOM COME'] # BB-1988-08-13
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_colon_only_is_special():
    # This is a special event, but the only signal is a colon. I am moving on for now, will update later
    raw_artists = ['CHRISTMAS IN AMERICA:', 'KENNY ROGERS', 'MARK CHESNUTT', 'THE MCCARTERS']                           # Billboard-1992-01-11
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_apostrophe_misread_colon():
    raw_artists = ["O'JAYS:", 'PHYLLIS HYMAN:']                                                                         # BB-1988-02-20
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 7

def test_calc_special_event_score_only_colon():
    raw_artists = ['CHICAGO/THE BEACH BOYS:']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 5

def test_calc_special_event_score_only_weak_keyword():
    raw_artists = ['WORLD PARTY', 'WIRE TRAIN']                                                                         # Billboard-1987-06-13
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 5

def test_calc_special_event_score_only_colon_super_group():
    # A super group is structured the same way as a special event, except it possibly never uses commas to separate artists
    raw_artists = ['THE HIGHWAYWEN:', 'JOHNNY CASH', 'WILLIE NELSON', 'WAYLON JENNINGS', 'KRIS KRISTOFFERSON']          # BB-1991-12-14
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 5

def test_calc_special_event_score_only_comma():
    raw_artists = ['CROSBY, STILLS & NASH']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 2

def test_calc_special_event_score_only_apostrophe():
    raw_artists = ["O'JAYS/LEVERT"]                                                                                     # BB-1988-02-20
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 2

def test_calc_special_event_score_no_signals():
    raw_artists = ['HUEY LEWIS & THE NEWS', 'TOWER OF POWER', 'LOS LOBOS', 'EDDIE & THE TIDE']
    special_event_score = calc_special_event_score(raw_artists)
    assert special_event_score == 0

# EXTRACT EVENT NAME
def test_extract_event_name_keyword_then_colon():
    raw_artists = ['BUDWEISER SUPERFEST:', 'PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == 'Budweiser Superfest'
    assert updated_artists == ['PEABO BRYSON, KOOL &', 'THE GANG, WHISPERS,', 'MTUME, PATTI LABELLE']

@pytest.mark.only
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

def test_extract_event_name_keyword_not_end_of_event_name():
    raw_artists = ['WORLD SERIES OF ROCK', 'WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == "World Series of Rock"
    assert updated_artists == ['WHITESNAKE', 'SKID ROW', 'GREAT WHITE', 'BAD ENGLISH', 'HERICANE ALICE']

def test_extract_event_name_ends_with_number():
    raw_artists = ['ROYAL NEW YORK DOO WOPP', 'VOL. 13']
    event_name, updated_artists = extract_event_name(raw_artists)
    assert event_name == "Royal New York Doo Wopp Vol. 13"
    assert updated_artists == []

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
    raw_artists = ['GUITAR GREATS: DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,',                    # BB-1984-12-01
                   'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    event_name, artists = parse_event_name(raw_artists, dim_special_events)
    assert event_name == 'Guitar Greats'
    assert artists == ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,',
                       'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']

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
    artist_lines = ["RICHARD NADER'S VALENTINE'S", "DOO WOPP SHOW", "LITTLE ANTHONY", "FRED PARIS & THE LITTLE", "SATINS", "THE BELMONTS & MARVELETTES"]
    event_name, updated_artists = parse_event_name(artist_lines, dim_special_events)
    assert event_name == "Richard Nader's Valentine's Doo Wopp Show"
    assert updated_artists == [
        "LITTLE ANTHONY",
        "FRED PARIS & THE LITTLE",
        "SATINS",
        "THE BELMONTS & MARVELETTES"
    ]
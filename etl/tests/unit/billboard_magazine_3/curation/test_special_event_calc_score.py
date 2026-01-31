from etl.schemas.billboard_magazine_3.curation.special_event import *
from etl.utils.utils import load_dimension_tables, load_event_keywords
import pytest
from config.paths import EVENT_KEYWORDS_PATH

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

def test_calc_special_event_score_tag_and_number():
    raw_artists = ['ROYAL NEW YORK DOO WOPP', 'VOL. 13']
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

@pytest.mark.xfail(reason="only special event signal is a colon, could be random OCR noise")
def test_calc_special_event_score_colon_only_is_special():
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

def test_calc_special_event_score_multiple_colons():
    raw_artists = ['RONNIE LAWS:', 'ROYCE LYTEL', 'JOHN AYD :']
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
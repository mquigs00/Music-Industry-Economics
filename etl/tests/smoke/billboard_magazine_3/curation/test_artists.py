from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names, merge_artists, generate_artist_candidates, validate_artist
from utils.utils import load_dimension_tables
import pytest
dim_artists = load_dimension_tables()["artists"]

# MERGE ARTISTS
def test_merge_artists_hyphen():
    raw_artists = ['RINGO STARR & HIS ALL-', 'STARR BAND', 'MASON RUFFNER']                                             # BB-1989-08-26
    merged_artists = merge_artists(raw_artists, False)
    assert merged_artists == ["RINGO STARR & HIS ALL-STARR BAND", "MASON RUFFNER"]

def test_merge_artists_the():
    raw_artists = ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS,']
    merged_artists = merge_artists(raw_artists, False)
    assert merged_artists == ['AEROSMITH', 'JOAN JETT & THE BLACKHEARTS']

def test_merge_event_artists():
    artists = ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    merged_artists = merge_artists(artists, True)
    assert merged_artists == ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,', 'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']

#PARSE ARTIST NAMES
def test_parse_event_artists():
    artists = ['DAVID', 'GILMOUR, DAVE EDMUNDS,', 'JOHNNY WINTER, BRIAN SETZER,',
               'NEAL SCHON, DICKIE BETTS,', 'TONY IOMMI, STEVE CROPPER,', 'LINK WRAY']
    final_artists = parse_artist_names(artists, True, dim_artists)
    assert final_artists == ['DAVID GILMOUR', 'DAVE EDMUNDS', 'JOHNNY WINTER', 'BRIAN SETZER', 'NEAL SCHON',
                             'DICKIE BETTS', 'TONY IOMMI', 'STEVE CROPPER', 'LINK WRAY']

def test_parse_artists_ampersand():
    artists = ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO']
    final_artists = parse_artist_names(artists, False, dim_artists)
    assert final_artists == ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO']

def test_generate_candidates_one():
    artists = 'JEFF/BECK'
    candidates = generate_artist_candidates(artists)
    assert candidates == {'JEFF', 'BECK', 'JEFF BECK', 'JEFF/BECK'}

def test_validate_artist_slash():
    artist = validate_artist('JEFF/BECK', dim_artists)
    assert artist == "JEFF BECK"

def test_validate_artist_ampersand():
    artist = validate_artist('STEVIE RAY VAUGHN & DOUBLE TROUBLE', dim_artists)
    assert artist == "STEVIE RAY VAUGHN & DOUBLE TROUBLE"

def test_separate_artists():
    raw_artists = ['CHICAGO/THE BEACH BOYS:']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['CHICAGO', 'THE BEACH BOYS']

def test_curate_artists_overflow_the():
    raw_artists = ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS,']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['AEROSMITH', 'JOAN JETT & THE BLACKHEARTS']

def test_curate_artists_overflow_ampersand_slash_one():
    """
    Artist names mixed across lines with ampersand and "/" symbols
    Example from Billboard issue: BB-1990-01-06
    """
    raw_artists = ['STEVIE RAY VAUGHAN &', 'DOUBLE TROUBLE/JEFF', 'BECK']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['STEVIE RAY VAUGHAN & DOUBLE TROUBLE', 'JEFF/BECK']

def test_curate_artists_overflow_no_delimiter_one():
    raw_artists = ['THE ALLMAN BROTHERS', 'BAND']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['THE ALLMAN BROTHERS BAND']

def test_curate_artists_overflow_no_delimiter_two():
    raw_artists = ['BRUCE SPRINGSTEEN & THE E', 'STREET BAND']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['BRUCE SPRINGSTEEN & THE E STREET BAND']

def test_curate_artists_overflow_slash():
    raw_artists = ['JERRY LEE LEWIS/CHUCK', 'BERRY']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['JERRY LEE LEWIS', 'CHUCK BERRY']

def test_curate_artists_overflow_ampersand():
    raw_artists = ['MICKEY ROONEY & DONALD', "O'CONNOR", 'JENIFER GREEN']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ["MICKEY ROONEY & DONALD O'CONNOR", 'JENIFER GREEN']

def test_curate_artists_ampersand_no_overflow():
    raw_artists = ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO']                                                          # BB-1984-11-03
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['FIXX', 'RONNIE HAYES & THE WILD', 'COMBO']

def test_curate_artists_extra_words():
    raw_artists = ['THE MUSIC OF ANDREW', 'LLOYD WEBBER', 'FEATURING MICHAEL', 'CRAWFORD']                              # BB-1991-12-14
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ["ANDREW LLOYD WEBBER", 'MICHAEL CRAWFORD']
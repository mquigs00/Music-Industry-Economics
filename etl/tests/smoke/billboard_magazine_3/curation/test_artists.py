from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names, generate_artist_candidates
from utils.utils import load_dimension_tables
import pytest
dim_artists = load_dimension_tables()["artists"]

@pytest.mark.only
def test_generate_candidates_one():
    artists = ['JEFF/BECK']
    candidates = generate_artist_candidates(artists)
    assert candidates == {'JEFF', 'BECK', 'JEFF BECK', 'JEFF/BECK'}

def test_merge_artists_one():
    raw_artists = ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS,']
    final_artists = parse_artist_names(raw_artists, False, dim_artists )
    assert final_artists == ['AEROSMITH', 'JOAN JETT & THE BLACKHEARTS']

def test_separate_artists():
    raw_artists = ['CHICAGO/THE BEACH BOYS:']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['CHICAGO', 'THE BEACH BOYS']

def test_curate_artists_overflow_the():
    raw_artists = ['AEROSMITH', 'JOAN JETT & THE', 'BLACKHEARTS,']
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['AEROSMITH', 'JOAN JETT & THE BLACKHEARTS']

def test_curate_artists_overflow_ampersand_slash():
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


from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names, merge_artists, generate_artist_candidates, validate_artist
from etl.utils.utils import load_dimension_tables
import pytest

dim_artists = load_dimension_tables()["artists"]

def test_curate_artists_weird_merge():
    raw_artists = ['HANK WILLIAMS JR.', 'BAMA BAND', 'MERLE KILGORE']                                                   # BB-1985-03-16
    final_artists = parse_artist_names(raw_artists, False, dim_artists)
    assert final_artists == ['HANK WILLIAMS JR', 'BAMA BAND', 'MERLE KILGORE']
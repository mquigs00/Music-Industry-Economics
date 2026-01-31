from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

@pytest.mark.xfail(reason="Ambiguous promoter/location text; requires manual correction record")
def test_clean_location_ambiguous_promoter_text():
    raw_location = ['Harttord', '(Conn.)', 'Civic', 'Center', '&', 'Associates', 'for', 'Pertorming', 'Arts']           # BB-1984-11-24
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Harttord', '(Conn)', 'Civic', 'Center']

def test_clean_location_num_sellouts():
    raw_location = ['Riverside', 'Theater', 'Milwaukee', 'sellouts']                                                    # BB-1984-11-24
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Riverside', 'Theater', 'Milwaukee']

def test_clean_location_venue_starts_with_city():
    raw_location = ['Cincinnati', 'Gardens']
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Cincinnati', 'Gardens']

@pytest.mark.xfail(reason="Ambiguous artist/location text; requires manual correction record for now")
def test_clean_location_artist():
    raw_location = ['Providence (R.I.) Civic Center', '& THE E STREET BAND']
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Providence (R.I.) Civic Center']

@pytest.mark.xfail(reason="Currently 'sellout' is a signal of unrelated data so the second token is removed")
def test_clean_location_sellout_mixed_in():
    raw_location = ['Long Beach Arena, Long Beach', 'Convention & Entertainment sellout','Center', 'Long Beach, Calif.'] # Billboard-1987-09-26
    location_cleaned = clean_location(raw_location)
    assert location_cleaned == ['Long Beach Arena Long Beach', 'Convention & Entertainment', 'Center', 'Long Beach Calif']
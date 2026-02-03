from etl.utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

dimension_tables = load_dimension_tables()
dim_venues = load_dimension_tables()["venues"]
dim_cities = load_dimension_tables()["cities"]

def test_correct_city_typo_in_venue_with_hyphen():
    venue_tokens = ['Tatlahassee', 'Leon', 'County', 'Civic', 'Center']
    corrected_venue_tokens = correct_location_typos(venue_tokens)
    assert corrected_venue_tokens == ['Tallahassee', 'Leon', 'County', 'Civic', 'Center']

def test_clean_location_url():
    location_tokens = ['The', 'Copa', 'Toronto,', 'Ont.', 'Www.americanradiohistory.com']
    clean_location_tokens = clean_location(location_tokens)
    assert clean_location_tokens == ['The', 'Copa', 'Toronto', 'Ont']

def test_correct_city_typo_in_venue():
    venue_tokens = ['Winmpeg', '(Manitoba)', 'Arena', 'Donald/International']
    corrected_venue_tokens = correct_location_typos(venue_tokens)
    assert corrected_venue_tokens == ['Winnipeg', '(Manitoba)', 'Arena', 'Donald/International']

def test_find_venue_type_idx():
    test_data = ['Frank', 'Cainer', 'Arena', 'Behan', 'Park', 'Nanaima']
    venue_type_idx = find_venue_type_idx(test_data)
    assert venue_type_idx == 2

def test_normalize_location_tokens():
    location_tokens = ['Tatlahassee-Leon', 'County', '(Fla.)', 'Civic', 'Center']                                       # split every word/item into a token
    location_tokens = normalize_location_tokens(location_tokens)
    assert location_tokens == ['Tatlahassee', 'Leon', 'County', '(Fla.)', 'Civic', 'Center']

def test_match_existing_venue_no_state_unique_city():
    venue_id, existing_venue_name = match_existing_venues("Reunion Arena", dim_venues, 9, None, dim_cities)
    assert venue_id == 3

def test_match_existing_venues_typo():
    venue_id, existing_venue_name, = match_existing_venues("Radio City MusiÂ¢ Hall", dim_venues, 1, None, dim_cities) #BB-1984-11-24
    assert venue_id == 1

@pytest.mark.xfail(reason="No keywords to delete promoter data")
def test_find_city_candidate_promoter_noise():
    location_tokens = ['Mid-Hudson', 'Civic', 'Center', 'Poughkeepsie,', 'Donald/', 'Harvey', '&', 'Corky']             # BB-1985-02-09
    city_candidate, venue_type_idx = find_city_candidate(location_tokens)
    assert city_candidate == "Poughskeepsie"
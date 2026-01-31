from etl.utils.utils import load_dimension_tables
from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

def test_correct_city_typo_in_venue_with_hyphen():
    venue_tokens = ['Tatlahassee', 'Leon', 'County', 'Civic', 'Center']
    corrected_venue_tokens = correct_location_typos(venue_tokens)
    assert corrected_venue_tokens == ['Tallahassee', 'Leon', 'County', 'Civic', 'Center']

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
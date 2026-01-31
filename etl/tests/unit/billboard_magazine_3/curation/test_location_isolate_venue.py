from etl.schemas.billboard_magazine_3.curation.location import *
import pytest

def test_isolate_location_center():
    remaining_tokens = ['Harttord', '(Conn.)', 'Civic', 'Center', '&', 'Associates', 'for', 'Pertorming', 'Arts']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Harttord (Conn.) Civic Center"

def test_isolate_venue_name_multiple_event_types():
    remaining_tokens = ['Memorial', 'Auditorium']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Memorial Auditorium"

def test_isolate_venue_name_no_venue_type():
    remaining_tokens = ['Houston Summit']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Houston Summit"

@pytest.mark.xfail(reason="Isolate venue name assumes that venue keyword is the end of the venue name")
def test_isolate_venue_name_starts_with_event_type():
    remaining_tokens = ['Memorial', 'Union']                                                                            # BB-1984-11-24
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Memorial Union"

def test_isolate_venue_name_university():
    remaining_tokens = ['Univ. of Colorado at Boulder']
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == "Univ. of Colorado at Boulder"

def test_isolate_venue_name_empty():
    remaining_tokens = []
    venue_tokens = isolate_venue_name(remaining_tokens)
    venue_name = " ".join(venue_tokens)
    assert venue_name == ""
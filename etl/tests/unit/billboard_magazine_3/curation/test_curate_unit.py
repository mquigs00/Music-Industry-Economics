from etl.schemas.billboard_magazine_3.curation.artists import parse_artist_names
from etl.schemas.billboard_magazine_3.curation.special_event import parse_event_name
from etl.schemas.billboard_magazine_3.curation.dates import curate_date
from etl.schemas.billboard_magazine_3.curation.curate import *
from etl.utils.utils import load_dimension_tables
from datetime import date
import pytest

def test_curate_event_ticket_prices_comma_in_price():
    raw_ticket_prices = ['17,50/15']                                                                                    # BB-1985-06-15
    curated_ticket_prices = curate_event_ticket_prices(raw_ticket_prices)
    assert curated_ticket_prices == [17.5, 15.0]

def test_curate_event_ticket_prices_ends_in_comma():
    raw_ticket_prices = ['17.75/15.25,']                                                                                # BB-1985-03-16
    curated_ticket_prices = curate_event_ticket_prices(raw_ticket_prices)
    assert curated_ticket_prices == [17.75, 15.25]

def test_curate_event_ticket_prices_underscore():
    raw_ticket_prices = ['17.50/15.50_']                                                                                # Billboard-1987-04-11
    curated_ticket_prices = curate_event_ticket_prices(raw_ticket_prices)
    assert curated_ticket_prices == [17.50, 15.50]

def test_curate_event_ticket_prices_promoter_capacity():
    raw_ticket_prices = ['17.50/15.50_']                                                                                # Billboard-1987-04-11
    curated_ticket_prices = curate_event_ticket_prices(raw_ticket_prices)
    assert curated_ticket_prices == [17.50, 15.50]

def test_curate_event_dollar_sign_s():
    raw_ticket_prices = ['15/S14']                                                                                      # Billboard-1987-01-31
    curated_ticket_prices = curate_event_ticket_prices(raw_ticket_prices)
    assert curated_ticket_prices == [15.0, 14.0]
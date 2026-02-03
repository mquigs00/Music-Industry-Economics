from etl.schemas.billboard_magazine_3.curation.dates import *
from datetime import date
import pytest

def test_clean_dates_non_month_letters():
    dates = ['Sept. 28', 'Sept. 30 9144.zia 10,894']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sept. 28 Sept. 30'

def test_clean_dates_one_integer_comma():
    # even 5 should be removed because it is followed by a comma, implying that it comes before a large number and is not part of a date
    dates = ['Nov. 3 989,704 5,867']                                                                                    # BB-1984-12-01
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Nov 3'

def test_clean_dates_two_integers_comma():
    # even 5 should be removed because it is followed by a comma, implying that it comes before a large number and is not part of a date
    dates = ['Jan. 20-21 8141 22,130']                                                                                  # BB-1985-02-09
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Jan 20-21'

def test_clean_dates_text():
    dates = ['Multt-Purpose']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == ''

def test_clean_date_comma_after_month():
    dates = ['Nov, 2']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Nov 2'

def test_curate_date_schema_one_comma_after_month():
    dates = ['Nov, 2']
    start_date, end_date, total_dates = curate_date(dates, 1984)
    assert start_date == date(1984, 11, 2)
    assert end_date == date(1984, 11, 2)
    assert total_dates == 'Nov 2'

def test_curate_date_schema_five():
    raw_dates = ['Dec. 28-29, 31']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 1)
    assert start_date == date(1984, 12, 28)
    assert end_date == date(1984, 12, 31)

def test_clean_date_schema_six():
    date_strings = ['Nov. 4-5,7-9', '11-12']
    total_dates = clean_dates(date_strings)
    assert total_dates == "Nov 4-5,7-9,11-12"

def test_curate_date_schema_six():
    date_strings = ['Nov. 4-5,7-9', '11-12']
    start_date, end_date, total_dates = curate_date(date_strings, 1984)
    assert start_date == date(1984, 11, 4)
    assert end_date == date(1984, 11, 12)
    assert total_dates == 'Nov 4-5,7-9,11-12'

def test_curate_noise_in_dates():
    dates = ['Sept. 30 9144.zia 10,894']
    issue_year = 1984

    start_date, end_date, dates = curate_date(dates, issue_year)
    assert start_date == date(1984, 9, 30)
    assert end_date == date(1984, 9, 30)

def test_determine_event_year():
    raw_dates = ['Dec. 31']
    event_year = determine_event_year(1985, 1, 12)
    assert event_year == 1984

def test_curate_date_schema_one_event_year_before_issue_year():
    raw_dates = ['Dec. 31']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 1)
    assert start_date == date(1984, 12, 31)

def test_curate_date_schema_two():
    raw_dates = ['Dec. 20-21']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 1)
    assert start_date == date(1984, 12, 20)
    assert end_date == date(1984, 12, 21)

def test_curate_date_schema_three_start_year_before_end_year():
    raw_dates = ['Dec. 30-Jan.1']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 1)
    assert start_date == date(1984, 12, 30)
    assert end_date == date(1985, 1, 1)

def test_curate_date_schema_three_same_year():
    raw_dates = ['Feb.25-March 3.']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 3)
    assert start_date == date(1985, 2, 25)
    assert end_date == date(1985, 3, 3)

def test_curate_date_schema_one_comma():
    raw_dates = ['Feb. 23,']
    start_date, end_date, total_dates = curate_date(raw_dates, 1985, 3)
    assert start_date == date(1985, 2, 23)
    assert end_date == date(1985, 2, 23)
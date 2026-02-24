from etl.schemas.billboard_magazine_3.curation.dates import *
from datetime import date
import pytest

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

def test_curate_date_separate_tokens():
    raw_dates = ['Dec. 31-', 'Jan. 1']                                                                                  # BB-1989-02-04
    start_date, end_date, total_dates = curate_date(raw_dates, 1989, 2)
    assert start_date == date(1988, 12, 31)
    assert end_date == date(1989, 1, 1)

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

def test_curate_date_ampersand():
    raw_dates = ['Sept. 2&8']                                                                                           # BB-1986-09-27
    start_date, end_date, total_dates = curate_date(raw_dates, 1986, 9)
    assert start_date == date(1986, 9, 27)
    assert end_date == date(1986, 9, 27)

def test_curate_date_missing_hyphen_no_whitespace():
    raw_dates = ['Oct. 1415', '8']                                                                                      # BB-1986-11-01
    start_date, end_date, total_dates = curate_date(raw_dates, 1986, 11)
    assert start_date == date(1986, 10, 14)
    assert end_date == date(1986, 10, 15)

def test_curate_date_missing_whitespace():
    raw_dates = ['Jan5-6, 8']                                                                                           # Billboard-1987-01-24
    start_date, end_date, total_dates = curate_date(raw_dates, 1987, 1)
    assert start_date == date(1987, 1, 5)
    assert end_date == date(1987, 1, 8)

def test_curate_date_letters():
    raw_dates = ['Jan. 1b', '5.D.']                                                                                     # Billboard-1987-01-24
    start_date, end_date, total_dates = curate_date(raw_dates, 1987, 1)
    assert start_date == date(1987, 1, 11)
    assert end_date == date(1987, 1, 11)

def test_curate_date_missing_hyphen_new_line():
    raw_dates = ['Sept. 14', '15']                                                                                      # BB-1988-10-01
    start_date, end_date, total_dates = curate_date(raw_dates, 1988, 10)
    assert start_date == date(1988, 9, 14)
    assert end_date == date(1988, 9, 15)

def test_curate_date_three_spans():
    raw_dates = ['Sept. 14-', '16, 18-20,', '22-23']                                                                    # BB-1988-10-08
    start_date, end_date, total_dates = curate_date(raw_dates, 1988, 10)
    assert start_date == date(1988, 9, 14)
    assert end_date == date(1988, 9, 23)

def test_curate_dates_three_spans_ampersand():
    raw_dates = ['June 28-', '30, July 2', '5-10']                                                                      # BB-1989-07-22
    start_date, end_date, total_dates = curate_date(raw_dates, 1989, 7)
    assert start_date == date(1989, 6, 28)
    assert end_date == date(1989, 7, 10)
from etl.schemas.billboard_magazine_3.curation.dates import clean_dates, curate_date
from datetime import date
import pytest

def test_clean_dates_text():
    dates = ['Multt-Purpose']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == ''

def test_clean_dates_numbers():
    dates = ['Sept. 28', 'Sept. 30 9144.zia 10,894']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sept. 28 Sept. 30'

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
    date_strings = ['Nov. 4-5,7-9']
    start_date, end_date, total_dates = curate_date(date_strings, 1984)
    assert start_date == date(1984, 11, 4)
    assert end_date == date(1984, 11, 9)
    assert total_dates == ('Nov 4-5,7-9')

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

def test_clean_dates_numbers_2():
    # even 5 should be removed because it is followed by a comma, implying that it comes before a large number and is not part of a date
    dates = ['Nov. 3 989,704 5,867']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Nov 3'

def test_curate_noise_in_dates():
    dates = ['Sept. 30 9144.zia 10,894']
    issue_year = 1984

    start_date, end_date, dates = curate_date(dates, issue_year)
    assert start_date == date(1984, 9, 30)
    assert end_date == date(1984, 9, 30)
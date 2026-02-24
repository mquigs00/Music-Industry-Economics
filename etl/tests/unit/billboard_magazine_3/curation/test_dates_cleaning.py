from etl.schemas.billboard_magazine_3.curation.dates import *
from datetime import date
import pytest

def test_clean_dates_non_month_letters():
    dates = ['Sept. 28', 'Sept. 30 9144.zia 10,894']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sept. 28 Sept. 30'

def test_clean_dates_word_at_start():
    dates = ['Mayer Nov. 30,', 'Dec. 1-4']                                                                              # BB-1990-01-06
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Nov 30 Dec 1-4'

def test_clean_dates_curly_brace():
    dates = ['June 14-17}']                                                                                             # BB-1989-07-01
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'June 14-17'

def test_clean_dates_day_out_of_month_range():
    dates = ['Oct. 3-48', '67']                                                                                         # BB-1989-10-28
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Oct 3'

def test_clean_dates_noise_numbers_in_range():
    dates = ['March 23- 3435,694 21,174', '26']                                                                         # BB-1989-04-08
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'March 23-26'

def test_clean_date_missing_whitespace_between_month_day():
    dates = ['Jan.6']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Jan 6'

def test_clean_date_missing_whitespace_between_dates():
    dates = ['Sept.89']                                                                                                 # BB-1989-09-30
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sept 8-9'

def test_clean_dates_two_ranges_second_month():                                                                         # Billboard-1987-03-14
    dates = ['Feb. 10-15, 17-', '22, March 1']
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Feb 10-15 17-22 March 1'

def test_clean_dates_promoter():
    dates = ['Auditorium/Charlotte Sept. 12', '101']                                                                    # Billboard-1987-09-26
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Feb 10-15 17-22 March 1'

def test_clean_dates_missing_hyphen():
    dates = ['Sept. 14', '15']                                                                                          # BB-1988-10-01
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sep 14 15'

def test_clean_dates_colon():
    dates = ['Sept: 21']                                                                                                # BB-1988-10-08
    cleaned_dates = clean_dates(dates)
    assert cleaned_dates == 'Sept 21'

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
from datetime import date
import re
import ast

MONTH_MAP = {
    "Jan": 1, "Feb": 2, "March": 3, "Apr": 4,
    "May": 5, "Jun": 6, "June": 6, "July": 7, "Aug": 8,
    "Sept": 9, "Oct": 10, "Nov": 11, "Dec": 12
}

def get_issue_year(object_key):
    normalized = object_key.replace("\\", "/")
    issue_year = int(normalized.split('/')[-3])
    return issue_year

def get_issue_month(object_key):
    normalized = object_key.replace("\\", "/")
    issue_month = int(normalized.split('/')[-2])
    return issue_month

def identify_start_date(processed_events_df, object_key):
    '''
    Adds a start_date field to the processed_events_df in yyyy-mm-dd format
    :param processed_events_df:
    :param object_key: the key of the processed csv file (partitioned by year/month)
    '''
    processed_events_df["dates"] = processed_events_df["dates"].apply(ast.literal_eval)                                                  # convert dates string to array of strings                                                                         # get issue year from S3 uri
    issue_year = get_issue_year(object_key)
    issue_month = get_issue_month(object_key)

    # curate date returns start date, end, date, and full string of dates. Just get the start date
    processed_events_df["start_date"] = [
        curate_date(dates, issue_year, issue_month)[0]
        for dates in processed_events_df["dates"]
    ]

def clean_stray_numbers(dates):
    '''
    Remove any numbers that don't make sense in the dates
    Ex. ['Oct. 13', '13']. The second 13 actually comes from the name of the concert and has nothing to do with the dates

    :param dates: (list): a list of unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :return: clean_date_items
    '''
    clean_date_items = []                                                                                               # only store verified items in clean_date_items
    last_month_seen = None
    last_day_seen = None

    # loop through each date string, check if it is a month or a valid day
    for date_str in dates:
        date_items = date_str.split()
        for date_item in date_items:
            date_item = date_item.strip(',')
            if date_item in MONTH_MAP:                                                                                  # if next item is a month
                clean_date_items.append(date_item)                                                                      # add the month to clean_items
                last_month_seen = date_item                                                                             # record that the month was seen
                last_day_seen = None                                                                                    # reset the day since a new month has started
            elif last_month_seen:                                                                                       # if a month has been found
                # if the next item is a number and it is less or equal to than the previous date seen
                if date_item.isdigit() and last_day_seen and date_item <= last_day_seen:
                    continue
                elif date_item.isdigit() and 1 <= int(date_item) <= 31:
                    print(f"{date_item} in valid days")
                    clean_date_items.append(date_item)
                    last_day_seen = date_item
                elif '-' in date_item:
                    clean_date_items.append(date_item)

    return clean_date_items

def clean_dates(raw_dates):
    """
    :param raw_dates: a list of unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :return: total_dates (str): a clean string of dates ex. 'Oct 27-28/30-31/Nov 2-3'
    """
    cleaned_dates = []
    for i, date in enumerate(raw_dates):
        if i > 0 and date[0] != ',':
            date = "," + date

        if re.search(r'[a-z]:\s?\d', date):
            date = re.sub(r'([a-z]):(\s?\d)', r'\1.\2', date)                                               # replace colon with period
        date = re.sub(r"(?<=[A-Za-z]{3}),", ".", date)
        date = re.sub(r"\d{2,},\d{3,}", "", date)                                                           # remove values like '24,000'
        date = re.sub(r"\d,\d{3,}", "", date)                                                               # replace thousands
        date = re.sub(r"\d{3,}", "", date)                                                                  # remove any group of 3 or more digits
        date = re.sub(r"\b(?!Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-zA-Z]{2,}\b", "", date)# remove text that is not a month
        date = re.sub(r"[.;$\[\](){}]", "", date)                                                          # remove any punctuation
        date = re.sub(r"\s+", " ", date).strip()                                                            # remove any extra whitespace
        date = re.sub(r"([a-z])(\d)", r"\1 \2", date)

        cleaned_dates.append(date)

    cleaned_dates = clean_stray_numbers(cleaned_dates)                                                         # remove any garbage numbers that may have gotten mixed in
    total_dates = " ".join(cleaned_dates)                                                                               # join into string with no spaces
    total_dates = re.sub(r"([a-z])([0-9])", r"\1 \2", total_dates)                                          # add spaces between a number and letter, not /
    total_dates = re.sub(r"(-)\s([0-9A-Z])", r"\1\2", total_dates)                                          # remove space between int/letter and hyphen

    return total_dates

def determine_event_year(issue_year, issue_month, event_month):
    """
    Billboard does not provide event year, must be inferred based on the month the event took place in
    :param issue_year: int, the year that the magazine issue was released
    :param issue_month: int,  the month that the magazine issue was released
    :param event_month: int,  the month the event took place
    :return: event_year: int
    """
    # if the event month is later in year than the issue month, it must be from the previous year (impossible to report on future event metrics)
    if event_month > issue_month:
        event_year = issue_year-1
    else:
        event_year = issue_year

    return event_year

def curate_date(dates, issue_year, issue_month):
    '''
    Takes a list of date strings and returns the start_date, end_date, and string of total dates.
    Currently uses regex to parse different date schemas, will be transitioning to a more intelligent function doesn't require a different solution for every schema

    :param dates (list), the unstructured date strings, ex. ['Oct. 27-28/', '30-31/Nov. 2-3']
    :param issue_year: int, the year the current magazine issue was released
    :param issue_month: int
    :return: start_date (date), end_date (date), total_dates (string)
    '''
    total_dates = clean_dates(dates)
    print(total_dates)

    # Schema 1: 'Oct 7'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)", total_dates)
    if m:
        m, d1 = m.groups()
        event_month = MONTH_MAP[m]
        event_year = determine_event_year(issue_year, issue_month, event_month)
        start_date = end_date = date(event_year, event_month, int(d1))
        return start_date, end_date, total_dates

    # Schema 2: 'Sept 20-27'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-?\s?(\d+)", total_dates)
    if m:
        print("MATCHED SCHEMA 2")
        m, d1, d2 = m.groups()
        event_month = MONTH_MAP[m]
        event_year = determine_event_year(issue_year, issue_month, event_month)
        start_date = date(event_year, event_month, int(d1))
        end_date = date(event_year, event_month, int(d2))
        return start_date, end_date, total_dates

    # Schema 3: 'Oct 30-Nov 8'
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-([A-Za-z]+)[.,]? (\d+)", total_dates)
    if m:
        m1, d1, m2, d2 = m.groups()
        event_start_month = MONTH_MAP[m1]
        event_end_month = MONTH_MAP[m2]
        event_start_year = determine_event_year(issue_year, issue_month, event_start_month)
        event_end_year = determine_event_year(issue_year, issue_month, event_end_month)
        start_date = date(event_start_year, event_start_month, int(d1))
        end_date = date(event_end_year, event_end_month, int(d2))
        return start_date, end_date, total_dates

    # Schema 4:
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)-(\d+)/?(\d+)-(\d+)/?([A-Za-z]+)[.,]? (\d+)-(\d+)", total_dates)
    if m:
        print(f"{total_dates} matched Schema 4!")
        m1, start_day, e1, e2, e3, m2, e4, end_day = m.groups()
        event_month = MONTH_MAP[m1]
        event_year = determine_event_year(issue_year, issue_month, event_month)
        start_date = date(event_year, event_month, int(start_day))
        end_date = date(event_year, event_month, int(end_day))
        return start_date, end_date, total_dates

    # Schema 5: Nov. 4-5,7-9
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)(-\d+)?,\s?(\d+)(-\d+)?", total_dates)
    if m:
        m, d1, d2, d3, d4 = m.groups()
        event_month = MONTH_MAP[m]
        event_year = determine_event_year(issue_year, issue_month, event_month)
        start_date = date(event_year, event_month, int(d1))
        if d4 is not None:
            end_date = date(event_year, event_month, int(d4.replace('-', '')))
        else:
            end_date = date(event_year, event_month, int(d3))
        return start_date, end_date, total_dates

    # Case 6: ['Nov. 4-5,7-9', '11-12']
    m = re.fullmatch(r"([A-Za-z]+)[.,]? (\d+)(-\d+)?,?\s?(\d+)(-\d+)?,?\s?(\d+)(-\d+)?", total_dates)
    if m:
        print("MATCHED SCHEMA 6")
        m, d1, d2, d3, d4, d5, d6 = m.groups()
        event_month = MONTH_MAP[m]
        event_year = determine_event_year(issue_year, issue_month, event_month)
        start_date = date(event_year, event_month, int(d1))
        if d6 is not None:
            end_date = date(event_year, event_month, int(d6.replace('-', '')))
        else:
            end_date = date(event_year, event_month, int(d5))
        return start_date, end_date, total_dates

    return None, None, None

def curate_dates(processed_events_df, curated_events_df, object_key):
    """

    :param processed_events_df:
    :param curated_events_df
    :param issue_year: the year the magazine copy came out
    """
    issue_year = get_issue_year(object_key)
    issue_month = get_issue_month(object_key)
    curated_dates = [curate_date(dates, issue_year, issue_month) for dates in processed_events_df["dates"]]
    curated_events_df["start_date"], curated_events_df["end_date"], curated_events_df["dates"] = zip(*curated_dates)
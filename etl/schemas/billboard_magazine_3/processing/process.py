import pdfplumber
import io
import re
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import NON_MUSICIANS_PATH, NOISY_SYMBOLS_PATH
from utils.utils import *
import pandas as pd
import csv
import json
from Levenshtein import distance as levenshtein_distance
import logging
from word2number import w2n
logger = logging.getLogger()

'''
This parser is for the Billboard Boxscore schema that ran from 1984-10-20 to 2001-07-21
'''

class ParsingError(Exception):
    """Base class for parsing errors"""

class FileParsingError(ParsingError):
    """Raised when the entire file should be skipped"""

class EventParsingError(ParsingError):
    """Raised when just the current tour should be skipped"""

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

directory_prefix = "raw/billboard/pdf/magazines/"

object_key = 'raw/billboard/pdf/magazines/1995/02/BB-1995-02-11.pdf'
page_num = 18

'''
    Every tour has:
    Artist(s):
    Venue: John Bauer,
    City: Seattle,
    Date(s): March 25,
    Gross Revenue,
    Ticket Price,
    Attendance,
    Capacity,
    Promoter
'''

months = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."]

months_pattern = r'\b(?:Jan|Feb|March|April|May|June|July|Aug|Sept|Oct|Nov|Dec)[\.,\b]?'

new_event_pattern = re.compile(r"^[^a-z]*\s([^0-9]*\s)*((?:j|J(?:an|qn)|F(?:eb|eh)|Ma(?:r|rch|rc|rn|vch|tch)|A(?:pr|pril|prl|or|ar)|May|(?:Ju|du|tu|Su)(?:n|ne|u|l|ly)|Au(?:g|gg|uq)|S(?:ep|ept|eph)|O(?:ct|oet|oct)|N(?:ov|ow|no)|D(?:e[ceo]|ec|ee))[.,]?\s?.{1,2}-?){1,2}\s?[^A-Za-z]+\s(in-house|[A-Z])")

def extract_raw_event_lines(page_lines, should_save):
    """
    Extracts the top boxoffice table lines
    :param page_lines:
    :return:
    """
    found_boxscore = False
    event_lines = []

    for line in page_lines:
        # if the line starts with a number and is at least 15 digits long, it is the first line of a new boxoffice record
        if line == "ARTIST(S) Venue Date(s) Ticket Price(s) Capacity Promoter":
            found_boxscore = True
        elif line[0:11] == "Copyrighted":
            break
        elif found_boxscore:  # if inside box office section and the next line does not start with the next rank number, it must be overflow of the current tour data
            event_lines.append(line)

    if should_save:
        with open("raw_event_lines.json", "w") as f:
            json.dump(event_lines, f)

    return event_lines

def consolidate_events(event_lines):
    """
    Groups each event into one String
    :param event_lines:
    :return:
    """
    try:
        events = []
        next_event = []

        for line in event_lines:
            line = clean_event(line)
            if new_event_pattern.search(line):           # only the first line of a tour contains the date of the tour:
                if len(next_event) > 0:
                    events.append(" | ".join(next_event))
                next_event = []
                next_event.append(line)
            else:
                next_event.append(line)

        events.append(" | ".join(next_event))

        return events
    except Exception as e:
        print(f"Exception: {e}")

def parse_location(event_data, next_item, it):
    """
    Pieces together the venue name, stops once it reaches a month name
    :param event_data: the tour data dictionary of the current tour being processed
    :param next_item: the next item in the tour iterator
    :param it: the iterator of tour data
    :return: the updated next_item in the iterator
    """
    if not next_item:
        return
    next_location_line = []                                                                                 # group the next set of location data
    next_location_line.append(next_item)

    # while no month is in the next item
    while True:
        next_item = next(it, None)                                                                         # if next item is none, reached the end of the tour data
        if not next_item or next_item == "|":
            break
        if not re.fullmatch("[^0-9-/]+$", next_item) or re.search(months_pattern, next_item):       # if next item has numbers or a month, move on
            print(f"Breaking in parse_location because next_item: {next_item}")
            break
        next_location_line.append(next_item)                                                               # otherwise, add next item to the new location list

    event_data["location"].append(" ".join(next_location_line))                                             # group the next set of location items and add the string to location
    return next_item

def parse_date(event_data, next_item, it):
    """
    Extracts the month, first, and last day of the event
    :param event_data: the event data dictionary of the current tour being processed
    :param next_item: the next item in the tour iterator
    :param it: the iterator of tour data
    :return:
    """
    if not next_item:
        return
    next_data_line = []

    if re.search(r"\d+\.\d+", next_item):
        return next(it, None)
    next_data_line.append(next_item)

    # while there is a month or starts with a number
    while True:
        next_item = next(it, None)
        if not next_item:
            break
        # if there is no month or no number in the next_item, break
        if not re.search(months_pattern, next_item) and not re.search("^[0-9]", next_item):
            print(f"No month or number in next item: {next_item}")
            break
        if re.search(r"\d+\.\d+", next_item):
            break
        next_data_line.append(next_item)

    event_data["dates"].append(" ".join(next_data_line))
    return next_item

def parse_gross_receipts_us(event_data, next_item):
    """
    Strips the gross receipts value of any symbols/punctuation
    :param gross_receipts:
    :return:
    """

    if next_item and next_item.startswith("$"):
        try:
            event_data["gross_receipts_us"] = float(next_item.replace("$", "").replace(",", ""))
        except ValueError:
            pass

def parse_attendance(event_data, next_item):
    '''
    Extracts the number of tickets_sold
    :param event_data:
    :param next_item:
    :return:
    '''
    if next_item and re.match(r"^[\d,]+$", next_item):
        event_data["attendance"] = float(next_item.replace(",", ""))
    else:
        logger.error(f"Attendance = {next_item}, leaving attendance as None")

def parse_additional_artist(event_data, next_item, it):
    """
    Extracts the name of the second artist on the tour

    :param event_data: the tour data dictionary of the current tour being processed
    :param next_item: the next item in the tour iterator
    :param it: the iterator of tour data
    :return:
    """
    next_artist_line = []
    next_artist_line.append(next_item)

    while True:
        next_item = next(it, None)
        if not next_item or next_item == "|":
            break
        if re.search(r"[a-z\d$]", next_item):
            break
        next_artist_line.append(next_item)

    event_data["artists"].append(" ".join(next_artist_line))

    return next_item

def parse_ticket_prices(event_data, ticket_price, it):
    '''
    Extracts the next set of ticket prices for the tour

    :param event_data:
    :param ticket_price:
    :param it:
    :return:
    '''
    ticket_price = ticket_price.replace("$", "")

    next_item = next(it, None)
    if re.search(r"^0-9\$,./-", ticket_price):
        logger.warning(f"Ticket prices = {ticket_price}, setting it back to None")
    else:
        if next_item and next_item == "&":
            ticket_price_2 = next(it, None)
            event_data["ticket_prices"].append(ticket_price + " & " + ticket_price_2)
        else:
            event_data["ticket_prices"].append(ticket_price)

    return next_item

def parse_canadian_gross(event_data, gross_receipts_canadian, it):
    '''
    Extracts the Canadian gross revenue for the tour
    :param event_data:
    :param gross_receipts_canadian:
    :param it:
    :return:
    '''
    next_item = next(it, None)
    print(f"In parse_canadian_gross: next item = {next_item}")
    if next_item == "Canadian)":
        event_data["gross_receipts_canadian"] = int(gross_receipts_canadian.replace("(", "").replace(",", "").replace("$", ""))

    next_item = next(it, None)
    return next_item

def parse_capacity(event_data, next_item):
    '''
    Extracts the capacity of the tour venue

    :param tour_data:
    :param next_item:
    :return:
    '''
    capacity = re.sub("[(),]", "", next_item)
    if capacity.isdigit():
        if event_data["attendance"] is not None and int(capacity) < event_data["attendance"]:
            logger.warning(f"Capacity = {capacity} but attendance = {event_data["attendance"]} for tour, setting capacity back to None")
        else:
            event_data["capacity"] = int(capacity)
    else:
        logger.warning(f"Capacity = {capacity}, setting it back to None")

def parse_num_sellouts_shows(event_data, number_text, it):
    '''
    Extracts the number of sellouts or number of shows

    :param tour_data:
    :param number_text:
    :param it:
    :return:
    '''
    metric = next(it, None)
    number = w2n.word_to_num(number_text)
    if metric == "sellouts":
        event_data["num_sellouts"] = int(number)
    elif metric == "shows":
        event_data["num_shows"] = int(number)
    elif levenshtein_distance(metric, "sellouts") <= 3:
        event_data["num_sellouts"] = int(number)
    elif levenshtein_distance(metric, "shows") <= 3:
        event_data["num_shows"] = int(number)

    next_item = next(it, None)
    return next_item

def parse_promoter(event_data, next_item, it):
    '''
    Extracts the next set of promoter data in the tour iterator

    :param event_data:
    :param next_item:
    :param it:
    :return:
    '''
    next_promoter_line = []
    while next_item and next_item != "|":
        next_promoter_line.append(next_item)
        next_item = next(it, None)

    event_data["promoter"].append(" ".join(next_promoter_line))

def new_event_state(bucket_name, object_key):
    '''
    Creates a new dictionary for a tour

    :param bucket_name:
    :param object_key:
    :return:
    '''
    return {
        "artists": [],
        "dates": [],
        "gross_receipts_us": None,
        "gross_receipts_canadian": None,
        "attendance": None,
        "capacity": None,
        "num_shows": None,
        "num_sellouts": None,
        "promoter": [],
        "ticket_prices": [],
        "location": [],
        "source_id": "billboard",
        "schema_id": "bb_3",
        "s3_uri": f"{bucket_name}{object_key}",
    }

def clean_event(line):
    '''
    Removes accidental symbols from a tour string

    :param line:
    :return:
    '''
    line = line.replace("§", "$").replace("‘", "").replace("—", "")

    return line.strip()

def is_number_text(text):
    '''
    Tries converting a word to a number
    :param text: a string
    :return: Yes if successfully converted to a number, otherwise False
    '''
    try:
        w2n.word_to_num(text)
        return True
    except ValueError:
        return False

def parse_additional_lines(event_data, next_item, it):
    '''
    Parses all data for consolidated tour line after the first pipe delimiter

    :param tour_data: a dictionary with a key for each section of the tour data
    :param next_item: the next item in the consolidated tour string
    :param it: the iterator of the tour string
    '''
    try:
        if not next_item or next_item == '|':
            return
        if is_number_text(next_item):
            parse_num_sellouts_shows(event_data, next_item, it)
            return
        if re.fullmatch(r"^[A-Z:,.]+$", next_item):
            next_item = parse_additional_artist(event_data, next_item, it)
        # if next item has no numbers, it should be additional venue/location data
        if next_item != "|" and not re.search("[0-9]", next_item) or levenshtein_distance(next_item, "Promotions") < 2:
            next_item = parse_location(event_data, next_item, it)
        if re.search(months_pattern, next_item) or re.search(r"^[0-9]", next_item):
            next_item = parse_date(event_data, next_item, it)
        # if next item starts with dollar sign, it is ticket prices
        if re.search(r"^\$", next_item):
            next_item = parse_ticket_prices(event_data, next_item, it)
        if re.search(r"\(\$\d*,\d*", next_item):
            next_item = parse_canadian_gross(event_data, next_item, it)
        if re.search(r"\(\d+,?.?\d+\)", next_item):
            parse_capacity(event_data, next_item)
            next_item = next(it, None)
        if levenshtein_distance(next_item, 'sellout') < 2:
            event_data["num_sellouts"] = 1
            next_item = next(it, None)
        if is_number_text(next_item):
            next_item = parse_num_sellouts_shows(event_data, next_item, it)
        if re.search(r"[0-9]", next_item):
            parse_promoter(event_data, next_item, it)
    except TypeError as e:
        print(f"TypeError = {e}. Next item = {next_item}")

def parse_event(event_str):
    event_data = new_event_state(BUCKET_NAME, object_key)
    line = clean_event(event_str)

    first_lowercase = re.search(r"[a-z]", line)
    if first_lowercase is None:
        return None
    first_lowercase_idx = first_lowercase.start()
    event_data["artists"].append(line[0:first_lowercase_idx - 2])
    rest_of_line = line[first_lowercase_idx - 1:].split()
    it = iter(rest_of_line)
    next_item = next(it, None)
    next_item = parse_location(event_data, next_item, it)
    next_item = parse_date(event_data, next_item, it)
    parse_gross_receipts_us(event_data, next_item)
    parse_attendance(event_data, next(it))
    parse_promoter(event_data, next(it), it)
    next_item = next(it, None)

    while next_item:
        parse_additional_lines(event_data, next_item, it)
        next_item = next(it, None)

    return event_data

def parse_events(tour_lines):
    '''
    Takes a string of consolidated tour lines and returns a list with a dictionary for each object
    Each component of the tour is divided into a separate key and value.

    :param tour_lines: a list of consolidated tour lines
    :return: a list of dictionaries with each tour's data broken into separate key and value
    '''
    event_objs = []

    for line in tour_lines:
        try:
            parsed_event = parse_event(line)
            print(parsed_event)
            event_objs.append(parsed_event)
        except EventParsingError as e:
            logger.warning(f"Skipping tour due to parsing error: {e}")

    return event_objs

def extract_to_csv():
    '''

    :return:
    '''
    try:
        obj = client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        pdf_bytes = obj['Body'].read()
        pdf_file = io.BytesIO(pdf_bytes)

        with (pdfplumber.open(pdf_file) as pdf):
            # loop through every page, see if any of them say "Top Boxoffice"
            # if not, try again but with OCR
            # if still not found, log comment and move on to next file
            #boxoffice_page = find_boxoffice_table(pdf, pdf_bytes)

            # magazine two is page 57
            page_text = extract_text_ocr(pdf_bytes, page_num)

            print(page_text)

            lines = page_text.splitlines()

            event_lines = extract_raw_event_lines(lines, False)

            #with open("raw_event_lines.json", "r") as f:
            #    event_lines = json.load(f)

            consolidated_event_lines = consolidate_events(event_lines)

            for event in consolidated_event_lines:
                print(event)

            event_objs = parse_events(consolidated_event_lines)

            events_df = pd.DataFrame(event_objs)
        
            file_name = object_key.split('/')[-1]
            csv_file_name = file_name.replace('.pdf', '.csv')

            csv_buffer = io.StringIO()
            events_df.to_csv(csv_buffer, index=False)

            year = object_key.split('/')[4]
            month = object_key.split('/')[5]

            try:
                client.put_object(
                    Bucket="music-industry-data-lake",
                    Key=f"processed/billboard/magazines/{year}/{month}/" + csv_file_name,
                    Body=csv_buffer.getvalue(),
                )
                print("Saved all tours report")
            except Exception as e:
                print(f"Error uploading file: {e}")

    except client.exceptions.NoSuchKey:
        print(f"Error: Object '{object_key}' not found in bucket '{BUCKET_NAME}'")
        exit()
    except Exception as e:
        print(f"Error retrieving object: {e}")
        exit()
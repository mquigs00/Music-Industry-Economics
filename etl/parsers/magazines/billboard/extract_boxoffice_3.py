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

class TourParsingError(ParsingError):
    """Raised when just the current tour should be skipped"""

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

directory_prefix = "raw/billboard/pdf/magazines/"

object_key = 'raw/billboard/pdf/magazines/1984/12/BB-1984-12-01.pdf'

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

def extract_raw_tour_lines(page_lines, should_save):
    """
    Extracts the top boxoffice table lines
    :param page_lines:
    :return:
    """
    found_boxscore = False
    tour_lines = []

    for line in page_lines:
        #print(f"Next line = {line}")
        # if the line starts with a number and is at least 15 digits long, it is the first line of a new boxoffice record
        if line == "ARTIST(S) Venue Date(s) Ticket Price(s) Capacity Promoter":
            found_boxscore = True
        elif line[0:11] == "Copyrighted":
            break
        elif found_boxscore:  # if inside box office section and the next line does not start with the next rank number, it must be overflow of the current tour data
            tour_lines.append(line)

    if should_save:
        with open("raw_tour_lines.json", "w") as f:
            json.dump(tour_lines, f)

    return tour_lines

def consolidate_tours(tour_lines):
    """
    Groups each tour into one String
    :param tour_lines:
    :return:
    """
    try:
        tours = []
        next_tour = []

        for line in tour_lines:
            if re.search(r"[A-Z]{2,}", line) and re.search(months_pattern, line):           # only the first line of a tour contains the date of the tour
                print()
                if len(next_tour) > 0:
                    tours.append(" | ".join(next_tour))
                next_tour = []
                next_tour.append(line)
            else:
                next_tour.append(line)

        tours.append(" | ".join(next_tour))

        return tours
    except Exception as e:
        print(f"Exception: {e}")

def parse_location(tour_data, next_item, it):
    """
    Pieces together the venue name, stops once it reaches a month name
    :param tour_data: the tour data dictionary of the current tour being processed
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
        if not next_item:
            break
        if not re.fullmatch("[^0-9-/]+$", next_item) or re.search(months_pattern, next_item):       # if next item has numbers or a month, move on
            print(f"Breaking in parse_location because next_item: {next_item}")
            break
        next_location_line.append(next_item)                                                               # otherwise, add next item to the new location list

    tour_data["location"].append(" ".join(next_location_line))                                             # group the next set of location items and add the string to location
    return next_item

def parse_date(tour_data, next_item, it):
    """
    Extracts the month, first, and last day of the tour
    :param tour_data: the tour data dictionary of the current tour being processed
    :param next_item: the next item in the tour iterator
    :param it: the iterator of tour data
    :return:
    """
    if not next_item:
        return
    next_data_line = []
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
        next_data_line.append(next_item)

    tour_data["dates"].append(" ".join(next_data_line))
    return next_item

def parse_gross_receipts_us(tour_data, next_item):
    """
    Strips the gross receipts value of any symbols/punctuation
    :param gross_receipts:
    :return:
    """

    if next_item and next_item.startswith("$"):
        try:
            tour_data["gross_receipts_us"] = float(next_item.replace("$", "").replace(",", ""))
        except ValueError:
            pass

def parse_tickets_sold(tour_data, next_item):
    '''
    Extracts the number of tickets_sold
    :param tour_data:
    :param next_item:
    :return:
    '''
    if next_item and re.match(r"^[\d,]+$", next_item):
        tour_data["tickets_sold"] = float(next_item.replace(",", ""))
    else:
        logger.error(f"Attendance = {next_item}, leaving tickets_sold as None")

def parse_additional_artist(tour_data, next_item, it):
    """
    Extracts the name of the second artist on the tour

    :param tour_data: the tour data dictionary of the current tour being processed
    :param next_item: the next item in the tour iterator
    :param it: the iterator of tour data
    :return:
    """
    next_artist_line = []
    next_artist_line.append(next_item)

    while True:
        next_item = next(it, None)
        if not next_item:
            break
        if re.search(r"[a-z\d$]", next_item):
            break
        next_artist_line.append(next_item)

    tour_data["artists"].append(" ".join(next_artist_line))

    return next_item

def parse_ticket_prices(tour_data, ticket_price, it):
    '''
    Extracts the next set of ticket prices for the tour

    :param tour_data:
    :param ticket_price:
    :param it:
    :return:
    '''
    next_item = next(it, None)
    if re.search(r"^0-9$,./-", ticket_price):
        logger.warning(f"Ticket prices = {ticket_price}, setting it back to None")
    else:
        if next_item and next_item == "&":
            ticket_price_2 = next(it, None)
            tour_data["ticket_prices"].append(ticket_price + " & " + ticket_price_2)
        else:
            print("APPENDING TO TICKET PRICES")
            tour_data["ticket_prices"].append(ticket_price)

    return next_item

def parse_canadian_gross(tour_data, gross_receipts_canadian, it):
    '''
    Extracts the Canadian gross revenue for the tour
    :param tour_data:
    :param gross_receipts_canadian:
    :param it:
    :return:
    '''
    next_item = next(it, None)
    print(f"In parse_canadian_gross: next item = {next_item}")
    if next_item == "Canadian)":
        tour_data["gross_receipts_canadian"] = int(gross_receipts_canadian.replace("(", "").replace(",", "").replace("$", ""))

    next_item = next(it, None)
    return next_item

def parse_capacity(tour_data, next_item):
    '''
    Extracts the capacity of the tour venue

    :param tour_data:
    :param next_item:
    :return:
    '''
    capacity = re.sub("[(),]", "", next_item)
    if capacity.isdigit():
        if tour_data["tickets_sold"] is not None and int(capacity) < tour_data["tickets_sold"]:
            logger.warning(f"Capacity = {capacity} but attendance = {tour_data["tickets_sold"]} for tour, setting capacity back to None")
        else:
            tour_data["capacity"] = int(capacity)
    else:
        logger.warning(f"Capacity = {capacity}, setting it back to None")

def parse_num_sellouts_shows(tour_data, number_text, it):
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
        tour_data["num_sellouts"] = number
    elif metric == "shows":
        tour_data["num_shows"] = number
    elif levenshtein_distance(metric, "sellouts") <= 2:
        tour_data["num_sellouts"] = number
    elif levenshtein_distance(metric, "shows") <= 2:
        tour_data["num_shows"] = number

    next_item = next(it, None)
    return next_item

def parse_promoter(tour_data, next_item, it):
    '''
    Extracts the next set of promoter data in the tour iterator

    :param tour_data:
    :param next_item:
    :param it:
    :return:
    '''
    next_promoter_line = []
    while next_item and next_item != "|":
        print(f"In parse_promoter: {next_item}")
        next_promoter_line.append(next_item)
        next_item = next(it, None)

    tour_data["promoter"].append(" ".join(next_promoter_line))

def new_tour_state(bucket_name, object_key):
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
        "tickets_sold": None,
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

def clean_tour(line):
    '''
    Removes accidental symbols from a tour string

    :param line:
    :return:
    '''
    line = line.replace("§", "$")
    line = re.sub(r"‘", "", line)
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

def parse_additional_lines(tour_data, next_item, it):
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
            parse_num_sellouts_shows(tour_data, next_item, it)
            return
        if re.fullmatch(r"^[A-Z:,.]+$", next_item):
            next_item = parse_additional_artist(tour_data, next_item, it)
        # if next item has no numbers, it should be additional venue/location data
        if not re.search("[0-9]", next_item) and levenshtein_distance(next_item, "Promotions") > 2:
            next_item = parse_location(tour_data, next_item, it)
        if re.search(months_pattern, next_item) or re.search(r"^[0-9]", next_item):
            next_item = parse_date(tour_data, next_item, it)
        # if next item starts with dollar sign, it is ticket prices
        if re.search(r"^\$", next_item):
            parse_ticket_prices(tour_data, next_item, it)
            next_item = next(it, None)
        if re.search(r"\(\$\d*,\d*", next_item):
            next_item = parse_canadian_gross(tour_data, next_item, it)
        if re.search(r"\(?\d+,?.?\d+\)?", next_item):
            parse_capacity(tour_data, next_item)
            next_item = next(it, None)
        if levenshtein_distance(next_item, 'sellout') < 2:
            tour_data["num_sellouts"] = 1
            next_item = next(it, None)
        if is_number_text(next_item):
            next_item = parse_num_sellouts_shows(tour_data, next_item, it)
        if not re.search(r"[0-9]", next_item):
            parse_promoter(tour_data, next_item, it)

    except TypeError as e:
        print(f"TypeError = {e}")

def parse_tour_lines(tour_lines):
    '''
    Takes a string of consolidated tour lines and returns a list with a dictionary for each object
    Each component of the tour is divided into a separate key and value.

    :param tour_lines: a list of consolidated tour lines
    :return: a list of dictionaries with each tour's data broken into separate key and value
    '''
    tour_objs = []

    try:
        for line in tour_lines:
            tour_data = new_tour_state(BUCKET_NAME, object_key)
            line = clean_tour(line)

            first_lowercase_idx = re.search(r"[a-z]", line).start()
            tour_data["artists"].append(line[0:first_lowercase_idx-2])
            rest_of_line = line[first_lowercase_idx-1:].split()
            it = iter(rest_of_line)
            next_item = next(it, None)
            next_item = parse_location(tour_data, next_item, it)
            next_item = parse_date(tour_data, next_item, it)
            parse_gross_receipts_us(tour_data, next_item)
            parse_tickets_sold(tour_data, next(it))
            parse_promoter(tour_data, next(it), it)
            next_item = next(it, None)

            #print(f"Artist 1 = {tour_data["artists"]}, location = {tour_data["location"]}, date = {tour_data["dates"]}, gross receipts = {tour_data["gross_receipts_us"]}, tickets sold = {tour_data["tickets_sold"]}")

            while next_item:
                parse_additional_lines(tour_data, next_item, it)
                next_item = next(it, None)
                print(f"In parse_tour_lines: {next_item}")

            print(tour_data)
            tour_objs.append(tour_data)

        return tour_objs

    except Exception as e:
        print(f"Exception: {e}")
    except TourParsingError as e:
        print(f"Tour Parsing Error: {e}")

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
            page_text = extract_text_ocr(pdf_bytes, 37)

            print(page_text)

            lines = page_text.splitlines()

            tour_lines = extract_raw_tour_lines(lines, False)

            #with open("raw_tour_lines.json", "r") as f:
            #    tour_lines = json.load(f)

            consolidated_tour_lines = consolidate_tours(tour_lines)

            for tour in consolidated_tour_lines:
                print(tour)

            '''
            tour_objs = parse_tour_lines(consolidated_tour_lines)

            df_all_tours = pd.DataFrame(tour_objs)
        
            file_name = object_key.split('/')[-1]
            csv_file_name = file_name.replace('.pdf', '.csv')

            csv_buffer = io.StringIO()
            df_all_tours.to_csv(csv_buffer, index=False)

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
            '''

    except client.exceptions.NoSuchKey:
        print(f"Error: Object '{object_key}' not found in bucket '{BUCKET_NAME}'")
        exit()
    except Exception as e:
        print(f"Error retrieving object: {e}")
        exit()
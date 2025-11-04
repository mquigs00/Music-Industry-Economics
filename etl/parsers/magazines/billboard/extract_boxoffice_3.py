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

object_key = 'raw/billboard/pdf/magazines/1984/10/BB-1984-10-20.pdf'

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
    Groups each tour into one String. Originally, tour data is in 2-3 lines
    :param tour_lines:
    :return:
    """
    try:
        tours = []
        next_tour = []
        #pattern = r'\b(?:Jan|Feb|March|April|May|June|July|Aug|Sept|Oct|Nov|Dec)'

        for line in tour_lines:
            if re.search(months_pattern, line):           # only the first line of a tour contains the date of the tour
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

def parse_venue(venue_list, it):
    """
    Pieces together the venue name, stops once it reaches a month name
    :param venue_list: the current list of words in the venue name
    :param it: the iterator of tour data
    :return: the updated venue words and iterator
    """
    next_item = next(it)

    # while no month is in the next item
    while not re.search(months_pattern, next_item):
        venue_list.append(next_item)
        next_item = next(it)

    return " ".join(venue_list), it, next_item

def parse_date(month, it):
    """
    Extracts the month, first, and last day of the tour
    :param month:
    :param it:
    :return:
    """
    tour_days = first_day = last_day = None
    month = re.sub("[.,]", "", month)

    number_in_month = re.search(r"\d", month)

    if number_in_month:
        tour_days = month[number_in_month.start():]
        month = month[:number_in_month.start()]
    else:
        tour_days = next(it)

    if '-' in tour_days:
        tour_days = tour_days.split('-')
        first_day = re.sub(r"[.,]", "", tour_days[0])
        last_day = re.sub(r"[.,]", "", tour_days[1])
    else:
        first_day = re.sub(r"[.,]", "", tour_days)

    return month, first_day, last_day, it

def parse_gross_receipts(gross_receipts):
    """
    Strips the gross receipts value of any symbols/punctuation
    :param gross_receipts:
    :return:
    """
    gross_receipts = re.sub(r"[$,‘]", "", gross_receipts)
    if gross_receipts.isdigit():
        gross_receipts = int(gross_receipts)
    else:
        logger.warning(f"Gross receipts = {gross_receipts}, setting it back to None")
        gross_receipts = None

    return gross_receipts

def parse_tickets_sold(tickets_sold):
    tickets_sold = re.sub(r"\.", ",", tickets_sold)
    tickets_sold = re.sub(r",", "", tickets_sold)

    if tickets_sold.isdigit():                                                                      # check that there are no letters or random symbols
        tickets_sold = int(re.sub(r"[$,]", "", tickets_sold))
    else:                                                                                           # if any non-numbers, set tickets_sold back to None
        logger.warning(f"Tickets sold = {tickets_sold}")
        tickets_sold = None

    return tickets_sold

def parse_capacity(next_item, tickets_sold):
    """
    Extracts the capacity or number of sold out shows for the tour
    :param next_item:
    :param tickets_sold:
    :return:
    """
    capacity = num_sellouts = None

    if re.search(r"\d", next_item):
        capacity = re.sub("[(),]", "", next_item)
        if capacity.isdigit():
            if tickets_sold is not None and int(capacity) < tickets_sold:
                logger.warning(f"Capacity = {capacity} but attendance = {tickets_sold} for tour, setting capacity back to None")
                capacity = None
        else:
            logger.warning(f"Capacity = {capacity}, setting it back to None")
    elif next_item == 'sellout':
        num_sellouts = 1
    else:
        num_sellouts = w2n.word_to_num(next_item)

    return capacity, num_sellouts

def parse_artist_2(next_item, it):
    """
    Extracts the name of the second artist on the tour
    :param next_item:
    :param it:
    :return:
    """
    artist_2 = [next_item]
    next_item = next(it, None)

    while next_item is not None and re.search(r"[A-Z]+", next_item):
        artist_2.append(next_item)
        next_item = next(it, None)

    artist_2 = " ".join(artist_2)

    return artist_2, next_item, it

def parse_location(next_item, it):
    """
    Extracts the location of the venue, could be the city, state, or country
    :param next_item:
    :param it:
    :return:
    """
    try:
        location = [next_item]
        next_item = next(it)

        while not re.search(r"\$?[0-9]", next_item):                                             # While the next item doesn't have a dollar sign, keep extracting the location
            location.append(next_item)
            next_item = next(it)

        location = " ".join(location)

        return location, next_item, it
    except Exception as e:
        print(f"Exception: {e}")

def parse_tour_lines(tour_lines):
    try:
        tour_objs = []

        for line in tour_lines:
            line = line.replace("§", "$")
            tickets_sold = num_sellouts = capacity = ticket_prices = location = last_day = artist_2 = artist_3 = None
            artists = []
            first_lowercase_idx = re.search(r"[a-z]", line).start()
            artist_1 = line[0:first_lowercase_idx-2]
            artists.append(artist_1)

            rest_of_line = line[first_lowercase_idx-1:].split()
            it = iter(rest_of_line)
            venue, it, next_item = parse_venue([], it)
            month, first_day, last_day, it = parse_date(next_item, it)
            next_item = next(it)
            gross_receipts = parse_gross_receipts(next_item)
            next_item = next(it)
            tickets_sold = parse_tickets_sold(next_item)
            next_item = next(it)
            promoter = []

            while next_item != '|':
                promoter.append(next_item)
                next_item = next(it)

            next_item = next(it)    # skip next it

            print(f"Skipped pipe delimeter. Next item: {next_item}")

            if re.fullmatch(r"[A-Z]+", next_item):                                  # if next item has multiple uppercase, it is the second artist
                artist_2, next_item, it = parse_artist_2(next_item, it)
                artists.append(artist_2)

            if not re.search(r"\$?[0-9]", next_item):                                         # if the next item does not numbers, it is the location
                print(f"Next item = {next_item}, parsing location")
                location, next_item, it = parse_location(next_item, it)

            print('Passed parse location')

            if re.search(r"^0-9\$,.\/-", next_item):                                  # if next item
                logger.warning(f"Ticket prices = {next_item}, setting it back to None")
            else:
                ticket_prices = re.sub(r"\$", "", next_item)

            print(f"artist_1 = {artist_1}, venue = {venue}, gross receipts = {gross_receipts}, tickets sold = {tickets_sold}, ticket prices = {ticket_prices}")

            next_item = next(it)
            capacity, num_sellouts = parse_capacity(next_item, tickets_sold)
            next_item = next(it, None)

            while next_item is not None:                                                       # any other text preceding the '|' delimiter is overflow of the promoter name
                if next_item == '|':
                    break
                promoter.append(next_item)
                next_item = next(it, None)

            promoter = " ".join(promoter)

            if next_item == '|':
                next_item = next(it, None)
                if re.search(r"[A-Z]+\s?[A-Z]+", next_item):
                    next_artist, next_item, it = parse_artist_2(next_item, it)
                    artists.append(next_artist)
                elif re.search(r"\$?[0-9]", next_item):
                    ticket_prices += re.sub(r"\$", "", next_item)

            next_item = next(it, None)
            if next_item == '|':
                if re.fullmatch(r"[A-Z]+", next_item):
                    next_artist, next_item, it = parse_artist_2(next_item, it)
                    artists.append(next_artist)

            next_tour = {
                "artists": '/'.join(artists),
                "venue": venue,
                "month_1": month,
                "first_day": first_day,
                "last_day": last_day,
                "gross_receipts": gross_receipts,
                "tickets_sold": tickets_sold,
                "promoter": promoter,
                "location": location,
                "ticket_prices": ticket_prices,
                "capacity": capacity,
                "num_sellouts": num_sellouts,
                "source_id": "billboard",
                "s3_uri": BUCKET_NAME + object_key
            }

            print(next_tour)
            tour_objs.append(next_tour)

        return tour_objs
    except Exception as e:
        print(f"Exception: {e}")
    except TourParsingError as e:
        print(f"Tour Parsing Error: {e}")

def extract_to_csv():
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
            page_text = extract_text_ocr(pdf_bytes, 38)

            #print(page_text)

            lines = page_text.splitlines()

            tour_lines = extract_raw_tour_lines(lines, False)

            #with open("raw_tour_lines.json", "r") as f:
            #    tour_lines = json.load(f)

            consolidated_tour_lines = consolidate_tours(tour_lines)

            for tour in consolidated_tour_lines:
                print(tour)

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

    except client.exceptions.NoSuchKey:
        print(f"Error: Object '{object_key}' not found in bucket '{BUCKET_NAME}'")
        exit()
    except Exception as e:
        print(f"Error retrieving object: {e}")
        exit()
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

def extract_raw_tour_lines(page_lines):
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

    with open("raw_tour_lines.json", "w") as f:
        json.dump(tour_lines, f)

def consolidate_tours(tour_lines):
    try:
        tours = []
        next_tour = []
        pattern = r'\b(?:Jan|Feb|March|April|May|June|July|Aug|Sept|Oct|Nov|Dec)[\.,\b]'

        for line in tour_lines:
            if re.search(pattern, line):           # only the first line of a tour contains the date of the tour
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

def parse_tour_lines(tour_lines):
    try:
        tour_objs = []
        rank = 1

        for line in tour_lines:
            line = line.replace("§", "$")
            tickets_sold = tour_days = num_sellouts = capacity = ticket_price_1 = ticket_price_2 = ticket_price_3 = location = last_day = artist_2 = artist_3 = None
            first_lowercase_idx = re.search(r"[a-z]", line).start()
            artist_1 = line[0:first_lowercase_idx-2]
            rest_of_line = line[first_lowercase_idx-1:].split()
            it = iter(rest_of_line)

            venue = []
            next_item = next(it)

            # while no month is in the next item
            pattern = r'\b(?:Jan|Feb|March|April|May|June|July|Aug|Sept|Oct|Nov|Dec)[\.,\b]'
            while not re.search(pattern, next_item):
                venue.append(next_item)
                next_item = next(it)

            venue = " ".join(venue)

            month = next_item

            print(f"Month = {month}")
            if not re.search(pattern, month):
                number_in_month = re.search(r"\d", month)

                if number_in_month:
                    tour_days = month[number_in_month.start():]
                    month = month[:number_in_month.start()]
            else:
                tour_days = next(it)

            if '-' in tour_days:
                tour_days = tour_days.split('-')
                first_day = tour_days[0]
                last_day = tour_days[1]
            else:
                first_day = tour_days

            print(f"First day = {first_day}, last day = {last_day}")

            next_item = next(it)

            gross_receipts = re.sub(r"[$,]", "", next_item)
            if gross_receipts.isdigit():
                gross_receipts = int(gross_receipts)
            else:
                logger.warning(f"Gross receipts = {gross_receipts}, setting it back to None")
                gross_receipts = None

            print(f"Gross receipts = {gross_receipts}")

            next_item = next(it)
            print(f"Next item = {next_item}")

            tickets_sold = re.sub(r"[.]", ",", next_item)
            tickets_sold = int(re.sub(r"[$,]", "", tickets_sold))
            next_item = next(it)

            promoter = []

            while next_item != '|':
                promoter.append(next_item)
                next_item = next(it)

            next_item = next(it)

            if re.search(r"^[A-Z].*[A-Z]", next_item):                         # if next item has multiple uppercase, it is the second artist
                artist_2 = [next_item]
                next_item = next(it)

                while re.fullmatch(r"[A-Z]+", next_item):
                    artist_2.append(next_item)
                    next_item = next(it)

                artist_2 = " ".join(artist_2)

            if not re.search(r"^\$", next_item):                           # if next item does not start with a dollar sign, it is the location
                location = [next_item]
                next_item = next(it)

                while not re.search(r"^\$", next_item):
                    location.append(next_item)
                    next_item = next(it)

                location = " ".join(location)

            ticket_price = next_item

            if '/' in ticket_price:
                ticket_prices = ticket_price.split('/')
                ticket_price_1 = float(re.sub(r"\$", "", ticket_prices[0]))

                if len(ticket_prices) == 2:
                    ticket_price_2 = float(re.sub(r"\$", "", ticket_prices[1]))
                elif len(ticket_prices) == 3:
                    ticket_price_2 = float(re.sub(r"\$", "", ticket_prices[1]))
                    ticket_price_3 = float(re.sub(r"\$", "", ticket_prices[2]))
            elif '-' in ticket_price:
                ticket_prices = ticket_price.split('-')
                ticket_price_1 = float(re.sub(r"\$", "", ticket_prices[0]))
                ticket_price_2 = float(re.sub(r"\$", "", ticket_prices[1]))
            else:
                ticket_price_1 = float(re.sub(r"\$", "", ticket_price))

            next_item = next(it)

            if next_item[0] == '(':
                capacity = int(re.sub("[(),]", "", next_item))
                if capacity < tickets_sold:
                    logger.warning(f"Capacity = {capacity} but attendance = {tickets_sold} for tour {rank}, setting capacity back to None")
                    capacity = None
            elif next_item == 'sellout':
                num_sellouts = 1
            else:
                #num_sellouts = w2n.word_to_num(next_item)
                num_sellouts = next_item
                next(it)

            next_item = next(it, None)

            while next_item is not None:
                if next_item == '|':
                    break
                promoter.append(next_item)
                next_item = next(it, None)

            promoter = " ".join(promoter)

            if next_item == '|':
                next_item = next(it, None)
                artist_3 = []
                while next_item is not None:
                    artist_3.append(next_item)
                    next_item = next(it, None)

                artist_3 = " ".join(artist_3)

            next_tour = {
                "rank": rank,
                "artist_1": artist_1,
                "venue": venue,
                "month_1": month,
                "first_day": first_day,
                "last_day": last_day,
                "gross_receipts": gross_receipts,
                "tickets_sold": tickets_sold,
                "promoter": promoter,
                "artist_2": artist_2,
                "artist_3": artist_3,
                "location": location,
                "ticket_price_1": ticket_price_1,
                "ticket_price_2": ticket_price_2,
                "ticket_price_3": ticket_price_3,
                "capacity": capacity,
                "num_sellouts": num_sellouts,
            }

            print(next_tour)

            tour_objs.append(next_tour)

            rank += 1

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
            #page_text = extract_text_ocr(pdf_bytes, 38)

            #print(page_text)

            #lines = page_text.splitlines()

            #extract_raw_tour_lines(lines)

            lines = []

            with open("raw_tour_lines.json", "r") as f:
                tour_lines = json.load(f)

            consolidated_tour_lines = consolidate_tours(tour_lines)

            for tour in consolidated_tour_lines:
                print(tour)

            tour_objs = parse_tour_lines(consolidated_tour_lines)

    except client.exceptions.NoSuchKey:
        print(f"Error: Object '{object_key}' not found in bucket '{BUCKET_NAME}'")
        exit()
    except Exception as e:
        print(f"Error retrieving object: {e}")
        exit()
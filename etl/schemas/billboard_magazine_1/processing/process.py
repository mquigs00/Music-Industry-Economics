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

'''
This parser is for the Billboard Boxoffice schema that ran from 1976-03-27 to 1981-09-19
'''

class ParsingError(Exception):
    """Base class for parsing errors"""

class FileParsingError(ParsingError):
    """Raised when the entire file should be skipped"""

class TourParsingError(ParsingError):
    """Raised when just the current tour should be skipped"""


pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

directory_prefix = "raw/billboard/pdf/magazines/"

object_key = 'raw/billboard/pdf/magazines/1984/09/BB-1981-09-19.pdf'

'''
    Every tour has:
    Rank: '2'
    Artist(s): 'WHO/STEVE GIBBONS BAND
    Promoter: John Bauer
    Facility: Coliseum
    City: Seattle
    Date: March 25
    Total Ticket Sales: 15,000
    Ticket Price Scale: 8.00
    Gross Receipts: 119,760* (* denotes a sellout concert)
'''

months = ["Jan.", "Feb.", "March", "April", "May", "June", "July", "Aug.", "Sept.", "Oct.", "Nov.", "Dec."]

def verify_ticket_prices(tickets_sold, ticket_price_1, ticket_price_2, gross_revenue):
    try:
        #print(f"Tickets sold = {tickets_sold} then ticket price = {ticket_price_1} and ticket price 2 = {ticket_price_2} and gross revenue = {gross_revenue}")
        if ticket_price_2 is not None:
            avg_ticket_price = (ticket_price_1 + ticket_price_2)/2
            ticket_product = avg_ticket_price * tickets_sold
        else:
            ticket_product = ticket_price_1 * tickets_sold

        if abs(gross_revenue-ticket_product) > 0.2*gross_revenue:
            print("TICKET PRICE TIMES TICKETS SOLD MORE THAN 10% OFF GROSS REVENUE")
    except Exception as e:
        print(e)

def extract_raw_tour_lines(page_lines):
    found_boxoffice = False
    tour_lines = []

    for line in page_lines:
        #print(f"Next line = {line}")
        # if the line starts with a number and is at least 15 digits long, it is the first line of a new boxoffice record
        if line == "Stadium & Festivals (20,000 & Over)":
            tour_lines.append(line)
            found_boxoffice = True
        elif line == "Arenas (6,000 To 20,000)":
            tour_lines.append(line)
            found_boxoffice = True
        elif line == "Auditoriums (Under 6,000)":
            tour_lines.append(line)
        elif found_boxoffice:  # if inside box office section and the next line does not start with the next rank number, it must be overflow of the current tour data
            tour_lines.append(line)

    with open("raw_event_lines.json", "w") as f:
        json.dump(tour_lines, f)

def restructure_tour_pieces(tour_lines):
    '''
    Takes a list of strings that make up a tour and re-arranges them
    :param tour_lines: a list of strings
    :return: the correctly structured tour string
    '''
    for line in tour_lines:
        print(line)

def consolidate_tours(page_lines):
    try:
        stadium_tours_str = []
        arena_tours_str = []  # tour dictionary for each tour's attributes
        auditorium_tours_str = []
        next_tour = []
        prev_rank = 1
        found_boxoffice = False
        venue_size = None

        for line in page_lines:
            line = re.sub(r"»", "", line).strip()           # remove any random characters and strip whitespaces from ends

            # if the line starts with a number and is at least 15 digits long, it is the first line of a new boxoffice record
            if line == "Stadium & Festivals (20,000 & Over)":
                venue_size = "Stadium"
                found_boxoffice = True
                prev_rank = 0
            elif line == "Arenas (6,000 To 20,000)":
                venue_size = "Arena"
                prev_rank = 0
                found_boxoffice = True
            elif line == "Auditoriums (Under 6,000)":
                venue_size = "Auditorium"
                prev_rank = 0
            # if the next line starts with a number, is greater than 15 digits, and the boxoffice section has started
            elif re.match(r"^[^a-zA-Z]?\d.?\s?.?[|\[\](){}].?\s?.*", line) and len(line) > 15 and found_boxoffice:
                print(f"Found next tour, first line = {line}")
                first_non_number = re.search(r"[^\d]", line)
                if first_non_number.start() == 0:
                    rank = re.sub("[^0-9]", "", line.split()[0])
                else:
                    rank = line[0:first_non_number.start()]

                # if the line starts with the next rank OR the next rank plus one random character on one end (ex. 10 --> 110)
                if int(rank) == prev_rank + 1 or re.match(fr".?{str(prev_rank+1)}.?", rank):
                    # correct the rank if there was a duplicate digit
                    if int(rank) != prev_rank + 1:
                        #line = str(prev_rank + 1) + line[line.find(" "):]
                        next_tour[0] = str(prev_rank + 1) + next_tour[0][next_tour[0].find(" "):]

                    # if there are no tours in the list yet, then this is the first tour in the magazine
                    if len(stadium_tours_str) + len(arena_tours_str) + len(stadium_tours_str) > 0:
                        # check if date data and sales data are swapped
                        print(f"Checking if tour structure is wrong: {next_tour[0]}")

                        # if the last 9 characters of the list string of the tour contains a month
                        # then the tour is not structured properly
                        if any(month in next_tour[-1][-9:] for month in months):
                            raise TourParsingError(f"Tour structure is wrong: {next_tour[-1]}")

                    next_tour = " ".join(next_tour)

                    #print(f"Next tour = {next_tour}")

                    if venue_size == "Stadium" and len(next_tour) > 15:
                        print(f'Appending new stadium tour: {next_tour}')
                        stadium_tours_str.append(next_tour)
                    elif venue_size == "Arena" and len(next_tour) > 15:
                        # if prev_rank == 0, then this is the last tour of the previous venue size
                        if prev_rank == 0:
                            print(f'Appending new stadium tour: {next_tour}')
                            stadium_tours_str.append(next_tour)
                        else:
                            print(f'Appending new arena tour: {next_tour}')
                            arena_tours_str.append(next_tour)
                    elif venue_size == "Auditorium" and len(next_tour) > 15:
                        #print(f"Adding new auditorium tour: {next_tour}")
                        # if prev_rank == 0, then this is the last tour of the previous venue size
                        if prev_rank == 0:
                            arena_tours_str.append(next_tour)
                        else:
                            auditorium_tours_str.append(next_tour)
                    else:
                        print(f"Found new rank but not adding. Rank = {rank}, prev_rank = {prev_rank}, and next tour = {next_tour}")
                    prev_rank += 1
                    next_tour = []
                else:
                    print(f"Rank not right. Rank = {rank} and prev_rank = {prev_rank}. Line = {line}")
                next_tour.append(line)

                continue
            elif found_boxoffice:          # if inside box office section and the next line does not start with the next rank number, it must be overflow of the current tour data
                print("Appending to next tour in last elif")
                next_tour.append(line)

        # make sure to add the last tour

        if next_tour:
            next_tour = " ".join(next_tour)
            # cutoff last tour where the sellout mark is to remove the rest of the page lines
            # will have to come up for a backup if the show was not sold out.
            sellout_mark_idx = next_tour.find('*')
            if sellout_mark_idx:
                next_tour = next_tour[:sellout_mark_idx+1]
    
            if venue_size == "Stadium":
                stadium_tours_str.append(next_tour)
            elif venue_size == "Arena":
                arena_tours_str.append(next_tour)
            elif venue_size == "Auditorium":
                auditorium_tours_str.append(next_tour)

        tours = {
            "stadiums": stadium_tours_str,
            "arenas": arena_tours_str,
            "auditoriums": auditorium_tours_str,
        }

        return tours

    except IndexError:
        print(f"Index out of range. Line = {line}")
    except TourParsingError:
        print(f"Tour parsing error. Line = {line}")

def parse_date(date_and_sales_data):
    try:
        month_1 = first_day = month_2 = last_day = num_shows = last_date_idx = None
        print(f'Date and sales data = {date_and_sales_data}')
        components = date_and_sales_data.split()
        it = iter(components)

        month_1 = next(it)

        if month_1 not in months:
            # if there is a number in the month, it most likely is the day attached to the month
            if bool(re.search(r"\d", month_1)):
                for month in months:
                    # check if the extracted text contains any of the possible months (e.g. 'May15')
                    if month in month_1:
                        rest_of_month = month_1[len(month):]                                                            # extract just the month from the text
                        month_1 = month                                                                                 # save the isolated month
                        first_day = re.sub(r"[^0-9]", "", rest_of_month)                                    # extract the isolated day
                        #print(f"Rest of month = {rest_of_month}, Month = {month_1} and day = {first_day}")
            else:
                distances = {}

                for month in months:
                    distance = levenshtein_distance(month, month_1)                                    # compare each month to the extracted text, calculating the levenshtein distance
                    distances[month] = distance                                                        # keep track of how differenct each month is to the extracted text

                # if the smallest difference between the extracted text and one of the months is 1, then it is safe to assume that the month has been found
                if distances[min(distances)] == 1:
                    month_1 = min(distances, key=distances.get)
                else:
                    raise TourParsingError(f"Could not find a matching month")

        next_item = next(it)

        if next_item == '|':
            next_item = next(it)

        # if first day was not attached to the month, extract it now
        if first_day is None:
            first_day = next_item

        next_item = next(it)
        used_next_item = False

        # if there is an &, then there are multiple tour dates
        if "&" in date_and_sales_data:
            print("FOUND &")
            used_next_item = True
            if next_item in months:
                month_2 = next_item
                last_day = int(re.sub(r"[^0-9]", "", next(it)))
            else:
                next_item = next(it)
                last_day = int(re.sub(r"[^0-9]", "", next_item))
        elif next_item in months:                                                             # additionally, multiple tour dates may be listed e.g. "March 12, March 13 (2)"
            used_next_item = True
            if next_item != month_1:
                month_2 = next_item
            last_day = int(re.sub(r"[^0-9]", "", next(it)))
        elif "/" in first_day:                                                                # dates will be separated by / if there is 3 dates
            days = first_day.split("/")
            first_day = int(re.sub(r"[^0-9]", "", days[0]))
            last_day = int(re.sub(r"[^0-9]", "", days[2]))
        elif "-" in first_day:                                                              # date range will have a dash if more than three days
            days = first_day.split("-")
            first_day = int(re.sub(r"[^0-9]", "", days[0]))
            last_day = int(re.sub(r"[^0-9]", "", days[1]))

        if used_next_item:
            next_item = next(it)

        if '|' in next_item and len(next_item) == 3:
            num_shows = next_item.strip('|')
            used_next_item = True
        else:
            num_shows = 1

        if used_next_item:
            rest_of_tour = " ".join(it)
        else:
            rest_of_tour = next_item + " " + " ".join(it)

        first_day = int(re.sub(r"[^0-9]", "", first_day))

        date_data = {
            "month_1": month_1,
            "first_day": first_day,
            "month_2": month_2,
            "last_day": last_day,
            "num_shows": num_shows,
        }

        return date_data, rest_of_tour
    except Exception as e:
        print(f"Error retrieving object: month_1 = {month_1}, first_day = {first_day}, month_2 = {month_2}, last_day = {last_day}, num_shows = {num_shows}")
        raise TourParsingError("Failed to parse")

def parse_sales(sales_str):
    try:
        print(f'Parsing sales: {sales_str}')

        ticket_price_1 = ticket_price_2 = gross_receipts = None
        was_sold_out = False

        sales_str = sales_str.strip("|")
        sales_components = sales_str.split()

        if len(sales_components) < 3:
            raise TourParsingError(f'Tour parsing error. Sales data = {sales_str}')

        it = iter(sales_components)

        next_item = next(it)

        tickets_sold = int(re.sub(r"[^0-9]", "", next_item))

        next_item = next(it)

        if next_item == '|':
            next_item = next(it)

        ticket_price = next_item

        if "-" in ticket_price:
            ticket_price_1 = float(ticket_price.split("-")[0])
            ticket_price_2 = float(ticket_price.split("-")[1])
        else:
            ticket_price_1 = float(ticket_price)

        next_item = next(it)

        if next_item == '|':
            next_item = next(it)

        if '*' in next_item or '*' in next(it, '*'):
            was_sold_out = True

        gross_receipts = int(re.sub(r"[^0-9]", "", next_item))

        verify_ticket_prices(tickets_sold, ticket_price_1, ticket_price_2, gross_receipts)

        sales_data = {
            "tickets_sold": tickets_sold,
            "ticket_price_1": ticket_price_1,
            "ticket_price_2": ticket_price_2,
            "gross_receipts": gross_receipts,
            "was_sold_out": was_sold_out,
        }

        return sales_data
    except Exception as e:
        print(f"Error parsing sales data: {e}")
        return {}
    except TourParsingError as e:
        print(f"Skipping tour due to invalid sales format: {e}\nSales string: {sales_str}")
        return {}
    except StopIteration:
        print("No more elements in the iterator")

def parse_tours_list(tours_str, venue_size):
    try:
        parsing_error = False
        tours_normalized = []
        states = ["Ala.", "Calif.", "D.C.", "Fla.", "Georgia", "Kansas", "La." "Maine", "Mass.", "Minn.", "New Hampshire", "Tenn.", "Va.", "Wisc."]

        for tour in tours_str:
            try:
                print(f'Next tour: {tour}')

                if tour == '':
                    raise TourParsingError('Tour parsing error. Tour string is blank')

                if not re.match(r"^\d.*", tour):
                    first_number = re.search(r"\d", tour)
                    tour = tour[first_number.start():]
                tour = re.sub(r"[\[\]{}'\"()]", "|", tour)              # replace any other vertical delimiters with the pipe delimiter
                tour = re.sub(r"~", "—", tour)
                sides = tour.split("|", 1)
                rank = sides[0].strip()  # the tour rank will always be on the left side of the first pipe |
                print(f"Rank = {rank}")
                if " " in rank:
                    raise TourParsingError(f"Space in the Rank value, Rank = {rank}")
                rest_of_tour = sides[1].strip()
                em_dash_idx = rest_of_tour.find('—')
                if em_dash_idx == -1:
                    em_dash_idx = rest_of_tour.find('-')

                if em_dash_idx == -1:
                    raise TourParsingError(f"No dash to separate Artist")

                artist = rest_of_tour[:em_dash_idx].strip()  # the artist(s) will always be to the right of the em dash
                tour_state = None

                rest_of_tour = rest_of_tour[em_dash_idx + 1:].strip()

                # the rest of the tour data can either have Promoter, Facility, City, Date(s)
                #                                        or Promoter, Facility, City, State Abr., Date(s)
                rest_of_tour = rest_of_tour.split(r",", 3)
                #print(rest_of_tour)

                # a correctly formatted tour string should have 4 items left when split by comma.
                if len(rest_of_tour) == 4:
                    promoter = re.sub("[|]", "", rest_of_tour[0])
                    venue = re.sub("[|]", "", rest_of_tour[1])
                    city = re.sub("[|]", "", rest_of_tour[2])

                    print(f"CITY = {city}")

                    if re.search(r'\d', city):
                        continue

                    print("CHECK IF NUMBER IN CITY")

                    # sometimes the month is attached to the city, raise an exception
                    for month in months:
                        if month in city:
                            raise TourParsingError(f"City contains month in: {city}")

                    print("CHECK IF MONTH IS ATTACHED TO CITY")

                    rest_of_tour = rest_of_tour[3].strip()

                    for state in states:
                        if state in rest_of_tour.split(',')[0]:
                            tour_state = rest_of_tour.split(".")[0]
                            rest_of_tour = rest_of_tour.split(None, 1)[1]       # remove the state from the rest of the tour

                    print(f"Rank = {rank}, Artist = {artist}, Promoter = {promoter}, Venue = {venue}, City = {city}, State = {tour_state}")
                    date_data, rest_of_tour = parse_date(rest_of_tour)
                    #print(f'Artist: {artist}, month_1: {date_data["month_1"]}, first_day: {date_data["first_day"]}, month_2: {date_data["month_2"]}, last_day = {date_data["last_day"]}, num_shows = {date_data["num_shows"]}')
                    sales_data = parse_sales(rest_of_tour)
                    #print("Finished getting sales data")
                else:
                    continue

                tour_data = {
                    "rank": rank,
                    "artist": artist,
                    "promoter": promoter,
                    "venue": venue,
                    "city": city,
                    "state_abbr": tour_state,
                    "month_1": date_data.get("month_1"),
                    "first_day": date_data.get("first_day"),
                    "month_2": date_data.get("month_2"),
                    "last_day": date_data.get("last_day"),
                    "num_shows": date_data.get("num_shows"),
                    "tickets_sold": sales_data.get("tickets_sold"),
                    "ticket_price_1": sales_data.get("ticket_price_1"),
                    "ticket_price_2": sales_data.get("ticket_price_2"),
                    "gross_receipts": sales_data.get("gross_receipts"),
                    "was_sold_out": sales_data.get("was_sold_out"),
                    "size": venue_size
                }

                tours_normalized.append(tour_data)
            except TourParsingError as e:
                print(f"Skipping tour")
                continue

        return tours_normalized

    except TourParsingError as e:
        print(f"Skipping tour due to invalid format: {e}\nTour String: {tour}")
    except IndexError as e:
        print(f"Skipping tour due to index error\nTour String: {tour}")

def find_boxoffice_table(pdf, pdf_bytes):
    found_boxoffice_table = False
    i = 0

    while not found_boxoffice_table and i < len(pdf.pages):
        next_page = pdf.pages[i]
        page_text = next_page.extract_text()
        if "Top Boxoffice" in page_text:
            found_boxoffice_table = True
            page_text = extract_text_ocr(pdf_bytes, i + 1)
            print("Found top boxoffice with pdfplumber")
        else:
            i += 1

    # if pdfplumber could not find the top boxoffice table, try with ocr
    if not found_boxoffice_table:
        i = 15  # jump to page 15 because ocr is more intensive and slower
        while not found_boxoffice_table and i < len(pdf.pages):
            next_page = extract_text_ocr(pdf_bytes, i)
            if "Top Boxoffice" in next_page:
                found_boxoffice_table = True
                print("Found top boxoffice with ocr")
            else:
                i += 1

    # if boxoffice table is still not found, move on to the next magazine file
    if not found_boxoffice_table:
        print("Could not find top boxoffice table")
        pass

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
            page_text = extract_text_ocr(pdf_bytes, 35)

            print(page_text)

            lines = page_text.splitlines()

            extract_raw_tour_lines(lines)

            tour_objs = []                                                                                      # save a list of all normalized tour objects from the current file
            tour_str = consolidate_tours(lines)                                                                 # create a list of all tours as one line of text

            if tour_str is None:
                raise TourParsingError("tour_str is None, cannot parse tours")

            print("CREATED TOUR LISTS")

            stadium_tours_str = tour_str["stadiums"]
            arena_tours_str = tour_str["arenas"]                                                                # get all the arena tour String
            auditorium_tours_str = tour_str["auditoriums"]                                                      # get all the auditorium tour String
            print("got each tour list per venue size")

            stadium_tour_data = parse_tours_list(stadium_tours_str, "stadium")
            arena_tour_data = parse_tours_list(arena_tours_str, "arena")                          # break each arena tour String down into its normalized parts
            auditorium_tours_data = parse_tours_list(auditorium_tours_str, "auditorium")          # break each auditorium String down into its normalized parts

            for tour in stadium_tour_data:
                print(tour)

            for tour in arena_tour_data:
                print(tour)

            for tour in auditorium_tours_data:
                print(tour)

            all_tours = stadium_tour_data + arena_tour_data + auditorium_tours_data
        
            df_all_tours = pd.DataFrame(all_tours)

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

def test():
    try:
        lines = []

        with open("raw_event_lines.json", "r") as f:
            lines =json.load(f)

        print(lines)

        tour_objs = []                                                                                      # save a list of all normalized tour objects from the current file
        tour_str = consolidate_tours(lines)                                                                 # create a list of all tours as one line of text

        if tour_str is None:
            raise TourParsingError("tour_str is None, cannot parse tours")

        print("CREATED TOUR LISTS")

        stadium_tours_str = tour_str["stadiums"]
        arena_tours_str = tour_str["arenas"]                                                                # get all the arena tour String
        auditorium_tours_str = tour_str["auditoriums"]                                                      # get all the auditorium tour String

        for stadium_tour in stadium_tours_str:
            print(stadium_tour)

        for arena_tour in arena_tours_str:
            print(arena_tour)

        for auditorium_tour in auditorium_tours_str:
            print(auditorium_tour)

        stadium_tour_data = parse_tours_list(stadium_tours_str, "stadium")
        arena_tour_data = parse_tours_list(arena_tours_str, "arena")                          # break each arena tour String down into its normalized parts
        auditorium_tours_data = parse_tours_list(auditorium_tours_str, "auditorium")          # break each auditorium String down into its normalized parts

        for tour in stadium_tour_data:
            print(tour)

        for tour in arena_tour_data:
            print(tour)

        for tour in auditorium_tours_data:
            print(tour)

    except client.exceptions.NoSuchKey:
        print(f"Error: Object '{object_key}' not found in bucket '{BUCKET_NAME}'")
        exit()
    except Exception as e:
        print(f"Error retrieving object: {e}")
        exit()

def print_magazine_names():
    directory_prefix = "raw/billboard/pdf/magazines/"

    pages = list_s3_files(directory_prefix)

    with open('billboard_magazine_files.csv', 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)

        for key in pages:
            csvwriter.writerow([key])
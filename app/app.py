import streamlit as st
import duckdb
import sys
import os
import boto3
import tempfile

tmp_path = os.path.join(tempfile.gettempdir(), 'music_warehouse.duckdb')
s3 = boto3.client(
    's3',
    aws_access_key_id = st.secrets["aws"]["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key = st.secrets["aws"]["AWS_SECRET_ACCESS_KEY"],
    region_name=st.secrets["aws"]["AWS_DEFAULT_REGION"]
)

s3.download_file('music-industry-data-lake', 'warehouse/music_warehouse.duckdb', tmp_path)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.paths import DB_PATH

conn = duckdb.connect(tmp_path, read_only=True)
event_data = conn.execute("""
    SELECT
        event.event_name AS EventName,
        artists.names AS Artists,
        strftime(event.start_date, '%m/%d/%Y') AS StartDate,
        strftime(event.end_date, '%m/%d/%Y') AS EndDate,
        venue.name AS Venue,
        prices.ticket_prices AS TicketPrices,
        event.gross_receipts_us AS GrossRevenue,
        event.attendance AS Attendance
    FROM event
    JOIN venue ON venue.id = event.venue_id
    JOIN (
        SELECT eta.event_id, GROUP_CONCAT(a.name, ', ') AS names
        FROM event_to_artist eta
        JOIN artist a ON eta.artist_id = a.id
        GROUP BY eta.event_id
    ) artists ON artists.event_id = event.id
    JOIN (
        SELECT event_id, GROUP_CONCAT(ticket_price, ', ') AS ticket_prices
        FROM event_ticket_price
        GROUP BY event_id
    ) prices ON prices.event_id = event.id
    ORDER BY event.start_date
""").df()

st.title('Live Entertainment Data')
st.dataframe(event_data, hide_index=True)

st.markdown("""
## Goal
The goal of this project was to extract semi-structured live event data from Billboard pdf's and convert the data into machine readable data that could be queried and analyzed. The
magazine archives contain ~53,000 of the top weekly entertainment events with their artist, location, ticket prices, gross revenue, and more. This data provides major insight into the
economics of live events and the rising costs of live entertainment in the last 5 decades. Manually entering this data from pdf into a system could take years.

## Data Source
The raw data comes from Billboard magazines downloaded from:\n
https://www.worldradiohistory.com/Archive-All-Music/Billboard-Magazine.htm

I downloaded about 2000 magazine issues. The first issue of Billboard containing a "Box Office" table was on March 27, 1976. From then until 2021, they went through 7 different table
schemas. All of this data comes from schema #3, which ran from October 13, 1984 to July 21, 2001. I chose this schema because it was the longest running and the closes to machine readable.

## Architecture
AWS S3
1. PDF's were downloaded to raw layer in s3 bucket
2. process.py reads raw file, finds Boxscore table data, outputs csv file in the processed layer (artists, venue, promoter are still stored as Strings)
3. curate.py reads processed csv file, applies any corrections, reads and write artists, venues, and promoters to dimension tables. Saves csv to curated layer
4. etl/load/main.py calls drop_tables(), create_tables(), load_dimensions(), and load_facts() to create and load DuckDB tables
5. Streamlit queries events table to display data

## Example Text
This is a snippet of what the table text would look like:\n
NIGHT RANGER The Paltadium June 8. $49,561 4377 Avalon Prods.
BLACK & BLUE Hollywood, Calif. $1L75 sellout
HANK WILLIAMS JR. Convention Center June 9. ‘$46,414 a3la ‘Sound Seventy Prods.
DAVID ALLAN COE Pine Bluff, Ark. $11.50 7,900
MOTLEY CRUE Stanley Theater June 12. $44,905 3,522 DiCesare-Engler Prods.
ACCEPT Pittsburgh $12.75 sellout

## Challenges
The main challenge was that these PDF files were scans and not native PDF's. Therefore, initial testing with pdfplumber outputted garbled data. I resorted to using pytesseract OCR to
scan the PDF's and then use Regex to parse the data and estimate which data belonged in which column.

Processing:\n

1. Case Sensitivity
For schema #3, all artist names were in all caps. The first line of each new event followed the same structure:\n
ARTIST Venue Date Gross Attendance Promoter

I was able to develop a complex regular expression that could detect when a new event was found by matching the pattern above
The issue was that it relied on the first string of text being in all caps to verify that an artist was present.
Artists like U2, ZZ Top, and AC/DC consistently broke this pattern because pytesseract read them as "u2", "zz top", or "Ac/Dc"
Therefore, the program did not recognize that the next line was a new event, so it skipped the event.\n

Curation:
1. Identifying, Separating, and Matching Event Names
Most events just had contained artist names under the ARTIST(S) column. But many had an event name like "MONSTERS OF ROCK" or "LOLLAPALOOZA" followed by a list of artist names. Because
event name did not have its own column, it had to be determined what events had an event name, where the event name ended, and if that event name had already been recorded in the
special events dimension table. Event names could appear in many different ways.

**Example I**\n
GUITAR GREATS: DAVID\n
GILMOUR, DAVE EDMUNDS\n
JOHNNY WINTER, BRIAN SETZER,\n
NEAL SCHON, DICKIE BETTS,\n
TONY IOMMI, STEVE CROPPER,\n
LINK WRAY\n

Features:\n
- No special event keyword
- Event name followed by a colon
- Artists listed after colon\n

**Example II**\n
LIVE AID

Features:\n
- No colon
- No artist names

**Example III**\n
- SWATCH WATCH: NEW YORK\n
CITY FRESH FESTIVAL\n

Features:\n
- Company sponsor followed by colon\n
- Event name with keyword "FESTIVAL"\n
- No artists\n

**Example IV**\n
RICHARD NADER's VALENTINE'S\n
DOO WOPP SHOW\n
LITTLE ANTHONY\n
FRED PARIS & THE LITTLE\n
SATINS\n
THE BELMONTS & MARVELETTES\n

Features:\n
- Event name with keyword "SHOW"\n
- No colon separating event name from artist names\n

**Example V**\n
ROYAL NEW YORK DOO WOPP\n
VOL. 13\n

Features:\n
- No special event keyword like "FESTIVAL", "TOUR", "SHOW"
- No colon separating event name from artists
- Tag, "VOL. 13" comes after special event base "ROYAL NEW YORK DOO WOPP"

**Example VI**\n
10TH ANNUAL TEXXAS WORLD\n
MUSIC FESTIVAL:\n
BOSTON, AEROSMITH,\n
WHITESNAKE, POISON, TESLA\n
FARRENHEIT\n

Features:\n
- Provides special event keyword "FESTIVAL"\n
- Colon\n
- Artists\n
- Tag, "10TH ANNUAL", comes before special event base "TEXXAS WORLD MUSIC FESTIVAL"
""")
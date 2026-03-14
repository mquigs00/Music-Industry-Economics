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
This is an example of the event data from the November 3rd, 1984 issue of Billboard:\n
ARTIST(S) Venue Date(s) Ticket Price(s) Capacity Promoter
BRUCE SPRINGSTEEN Oakland Coliseum Oct, 21-22 $436,272 27,267 Bill Graham Presents
Calif. $16 ‘two setlouts
DOUG HENNING Fox Theater Oct. 16-21 $401,678 W224 ~ Ray Shepardson
St. Louis $4.90-$17.90 eight shows
four seltouts
ALABAMA Reunion Arena Oct. 14 $271,436 17,512 Salem Concerts/Keith
JUICE NEWTON Dallas $15.50 setlout Fowler Productions
RICK SPRINGFIELD The Forum Oct. 6 $239,237 16,183 Avalon Attractions
COREY HART Inglewood, Calif. $15 & $12.50 sellout Jam Promotions
PATTI LABELLE Lyric Opera House ‘Oct. 9-13 $216,352 12,500 Mare Corwin/That’s
Baltimore $20/$17 915 five sellouts. Entertainment Inc.
ALABAMA Frank Erwin Center Oct. 19 $203,277 15,502 Satem Concerts/Keith
Austin $13.50 (7217) Fowler Promotions
RICK SPRINGFIELD Concord (Calif.) Pavilion Oct. 7-8 $197,300 16,950 Mederlander
$12.50 two sellouts
SAMMY HAGAR Wings Stadium Oct. 20-21 $196,087 16,046 Blue Suede Shows.
KROKUS. Kalamazoo, Mich $12.50 two seHtouts,
BARRY MANILOW Crister Arena Oct. 19 $190,095 12,582 Brass Ring Prods.
Ann Arbor, Mich. $17.50/$15 seflout
ALABAMA Convention Center Oct. 12 $188,666 12,172 Salem Concerts/Keith
2 San Antonio $15.50 (13,200 Fowler Promotions
ROD STEWART Hollywood (Fta.) Sportatorium Oct.7 $179,172 13,132 Fantasma Prods.
$4 (12,500) |
LIONEL RICHIE BSU Pavilion Oct. 10 $164,694 11,803 United Concerts.
Boise, Idaho $15 (12,045)
SAMMY HAGAR Market Square Arena Oct. 17 $141,280 13,15] ‘Sunshine Promotions
Indianapolis $11.40/$10.50 (13,500)
GEORGE BENSON Irvine Meadows Amphitheatre Oct.7 $136,569 10,358 Avalon Attractions.
Laguna Hills, Calif. $16.50/$15/$9.50 (15,000)
BARRY MANILOW Centennial Hall Oct. 18 $135,496 9,167 Belkin Prods.
Univ. of Toledo $15/$13.50 sellout
JETHRO TULL Spectrum Theater Oct. 19 $130,017 1,015 Electric Factory
HONEYMOON SUITE Philadelphia $12.50/$10 (11,882) Concerts
BILLY SQUIER Barton Coliseum Oct. 16 $120,000 10,000 Mid-South Concerts
RATT Little Rock $2 sellout
RICK SPRINGFIELD NBC Arena Oct. 12 $114,112 8,805 Jam Prods./Alan Carr
Honolulu $13.50 sellout
BILLY SQUIER Mid-South Coliseum Oct. 17 $113,207 9314 Mid-South Concerts
Memphis. $12.50/$11.50 (12,035)
DAVID COPPERFIELD Royal Oak (Mich.) Oct. 19-20 $112,057 7881 Brass Ring Prods.
Music Theater ns (8,500)
five shows
OAK RIDGE BOYS Von Braun Civic Center Oct. 14 $109,079 8777 Jerry Bentley Prods,
LEE GREENWOOD Huntsville, Ala. $12.75/$12 (8,696)
ALABAMA Stephen F. Austin College ~ Oct. 20 $107,679 8,283 Salem Concerts/Keith Fowler
Nagcogdoches, Tex. $13 sellout Promotions
CHICAGO Cal Expo Amphitheatre Oct. 12 $105,840 7,055 Bill Graham Presents
Sacramento $15 (10,000)
CHICAGO Greek Theatre Oct. 13 $103,940 7,464 Bill Graham Presents
Berkeley, Calif. $15.50/$15/$13.50 (8,500)
ALABAMA G. Rollie White Coliseum, Oct. 21 $100,495 7,882 ‘Salem Concerts/Keith
College Station, Tex. $13.50/$12.50 ‘sellout Fowler Promotions
QUIET RIOT Mclichols Sports Arena Oct. 14 $98,221 10,505 Feyline Presents
WHITESNAKE Denver $9.35 (18,483)
HELIX
FIXX ‘San Francisco Civic Auditorium Oct. 20 $70,557 5,142 Bill Graham Presents
RONNIE HAYES & THE WILD $15/$13.50 (8,500)
COMBO D
QUIET RIOT levine Meadows Amphitheatre Sept. 30 $66,969 4385 Avalon Attractions.
WHITESNAKE Laguna Hilts, Calif. S16 /$la (6.1L)
ARMORED SAINTS
THOMPSON TWINS Lawior Events Center Sept. 22 $59,238 4388 Rock ‘N’ Chair Prods.
BERLIN Reno $13.50 (7,200)
AMY GRANT Massey Hall Oct. 15-16 $59,128 467 Concert Prods.
Toronto ($73,911 Canadian) (5,000) International
$16/$15
CHARLEY PRIDE Frank Cainer Arena Oct. 20 $53,749 3,689 Jack Roberts Prods.
Behan Park, Nanaima, B.C. $15/$13 (3,722)

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

**Reflection**
In hindsight, I wish I tested more OCR engines to try and find one that would have less errors. Even small misreadings proved to have major impacts, which reveals that my
regex parsing method was brittle. This project took significantly more time and energy than I expected, but it provided me with a fascinating data set and lots of new ideas.
I was hoping to be able to successfully parse and curate more magazines issues, but I'm honestly glad that I was able to get 50 magazines issues completely curated. This project
has boosted my confidence with ETL work, and I expect that future portfolio projects will often require less messy parsing due to raw data being more clean that the OCR readings here.
""")
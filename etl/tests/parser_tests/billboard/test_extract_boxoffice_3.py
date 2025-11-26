from etl.parsers.magazines.billboard import extract_boxoffice_3 as parser

def test_parse_event_basic():
    event_line = "CULTURE CLUB Capital Centre Nov. 11 $216,736 13,983 Cellar Door Prods. | DADS Landover, Md, $15.50 (19,114)"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["CULTURE CLUB", "DADS"]
    assert event_data["location"] == ["Capital Centre", "Landover, Md,"]
    assert event_data["dates"] == ["Nov. 11"]
    assert event_data["gross_receipts_us"] == 216736
    assert event_data["tickets_sold"] == 13983
    assert event_data["promoter"] == ["Cellar Door Prods."]
    assert event_data["capacity"] == 19114
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] is None
    assert event_data["ticket_prices"] == ["15.50"]
    assert event_data["gross_receipts_canadian"] is None

def test_parse_event_u2():
    event_line = "u2 San Francisco Cwic Auditorium Dee. 15 $114,780 8.472 Bill Graham Presents | WATERBOYS $15/$13.50 sellout"
    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["u2", "WATERBOYS"]
    assert event_data["location"] == ["San Francisco Cwic Auditorium"]
    assert event_data["dates"] == ["Dee. 15"]
    assert event_data["gross_receipts_us"] == 114780
    assert event_data["tickets_sold"] == 8472
    assert event_data["promoter"] == ["Bill Graham Presents"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] == 1
    assert event_data["ticket_prices"] == ["15/13.50"]
    assert event_data["gross_receipts_canadian"] is None

def test_location_four_rows():
    event_line = "GEORGE STRAIT Strahan Coliseum Nov. 3 $46,108 4,062 Ronald & Joy Cotton | Southwest Texas State $12 (8,000) | University | San Marcos"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["GEORGE STRAIT"]
    assert event_data["location"] == ["Strahan Coliseum", "Southwest Texas State", "University", "San Marcos"]
    assert event_data["dates"] == ["Nov. 3"]
    assert event_data["gross_receipts_us"] == 46108
    assert event_data["tickets_sold"] == 4062
    assert event_data["promoter"] == ["Ronald & Joy Cotton"]
    assert event_data["capacity"] == 8000
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] is None
    assert event_data["ticket_prices"] == ["12"]
    assert event_data["gross_receipts_canadian"] is None

def test_ticket_price_letters():
    event_line = "DAVID COPPERFIELD Royal Oak (Mich.) Oct. 19-20 $112,057 7881 Brass Ring Prods. | Music Theater ns (8,500) | five shows"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["DAVID COPPERFIELD"]
    assert event_data["location"] == ["Royal Oak (Mich.)", "Music Theater ns"]
    assert event_data["dates"] == ["Oct. 19-20"]
    assert event_data["gross_receipts_us"] == 112057
    assert event_data["tickets_sold"] == 7881
    assert event_data["promoter"] == ["Brass Ring Prods."]
    assert event_data["capacity"] == 8500
    assert event_data["num_shows"] == 5
    assert event_data["num_sellouts"] is None
    assert event_data["ticket_prices"] == []
    assert event_data["gross_receipts_canadian"] is None

def test_parse_event_two_ticket_price_rows():
    # This tests parse_event when there are three artists with no other data on the third line
    event_line = ".38 SPECIAL Red Rock Amphitheater June 16. $112,020 8,991 Feyline Presents Inc. | EDDIE MONEY Denver $13.20/$12.10/$11.81/ sellout | $11.16/$10.72/$10.22"
    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == [".38 SPECIAL", "EDDIE MONEY"]
    assert event_data["location"] == ["Red Rock Amphitheater", "Denver"]
    assert event_data["dates"] == ["June 16."]
    assert event_data["gross_receipts_us"] == 112020
    assert event_data["tickets_sold"] == 8991
    assert event_data["promoter"] == ["Feyline Presents Inc."]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] == 1
    assert event_data["ticket_prices"] == ["13.20/12.10/11.81/", "11.16/10.72/10.22"]
    assert event_data["gross_receipts_canadian"] is None

def test_capacity_less_than_attendance():
    # This tests parse_event when the capacity value extracted by OCR is missing a digit and is less than the attendance
    event_line = "KENNY ROGERS Reunion Arena Sept. 30 $222,921 13,876 CK, Spurlock | EDDIE RABBITT Dallas $16.50/$13.50 (5,711) | HELEN REDDY"
    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["KENNY ROGERS", "EDDIE RABBITT", "HELEN REDDY"]
    assert event_data["location"] == ["Reunion Arena", "Dallas"]
    assert event_data["dates"] == ["Sept. 30"]
    assert event_data["gross_receipts_us"] == 222921
    assert event_data["tickets_sold"] == 13876
    assert event_data["promoter"] == ["CK, Spurlock"]
    assert event_data["capacity"] is None                                                          # the true capacity was 15,711, 5711 should be ignored
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] is None
    assert event_data["ticket_prices"] == ["16.50/13.50"]
    assert event_data["gross_receipts_canadian"] is None

def test_parse_event_many_artists():
    event_line = "GUITAR GREATS: DAVID Capital Theater Nov. 3 $41,809 3,397 Monarch Entertainment Bureau | GILMOUR, DAVE EDMUNDS, Passaic, N.J $15/$15 sellout | JOHNNY WINTER, BRIAN SETZER, | NEAL SCHON, DICKIE BETTS, | TONY IOMMI, STEVE CROPPER, | LINK WRAY"
    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["GUITAR GREATS: DAVID", "GILMOUR, DAVE EDMUNDS,", "JOHNNY WINTER, BRIAN SETZER,", "NEAL SCHON, DICKIE BETTS,", "TONY IOMMI, STEVE CROPPER,", "LINK WRAY"]
    assert event_data["location"] == ["Capital Theater", "Passaic, N.J"]
    assert event_data["dates"] == ["Nov. 3"]
    assert event_data["gross_receipts_us"] == 41809
    assert event_data["tickets_sold"] == 3397
    assert event_data["promoter"] == ["Monarch Entertainment Bureau"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] == 1
    assert event_data["ticket_prices"] == ["15/15"]
    assert event_data["gross_receipts_canadian"] is None

def test_parse_event_canadian_gross():
    event_line = "CYNDI LAUPER Maple Leaf Gardens Nov. 12 $166,353 13,500 Concert Prods. International | BANGLES Toronto ($207,942 Canadian) sellout | $16.50/$15.50"
    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["CYNDI LAUPER", "BANGLES"]
    assert event_data["location"] == ["Maple Leaf Gardens", "Toronto"]
    assert event_data["dates"] == ["Nov. 12"]
    assert event_data["gross_receipts_us"] == 166353
    assert event_data["tickets_sold"] == 13500
    assert event_data["promoter"] == ["Concert Prods. International"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] == 1
    assert event_data["ticket_prices"] == ["16.50/15.50"]
    assert event_data["gross_receipts_canadian"] == 207942

def test_dates_dollar_sign_misread():
    '''
    In this case, the OCR misread multiple items Mz is supposed to be "11-12", additional tour dates
    Additionally, "seven sellouts" is misspelled, so word2number will not be able to convert and realize it is a number
    '''
    event_line = "PRINCE Joe Louis Arena Nov. 4-5,7-9 $2,081,719 129,730 Rainbow Over America | SHELIA E. Detroit Mz 517.50 sevan sellouts"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["PRINCE", "SHELIA E."]
    assert event_data["location"] == ["Joe Louis Arena", "Detroit Mz", "sellouts"]
    assert event_data["dates"] == ["Nov. 4-5,7-9"]
    assert event_data["gross_receipts_us"] == 2081719
    assert event_data["tickets_sold"] == 129730
    assert event_data["promoter"] == ["Rainbow Over America"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] is None
    assert event_data["ticket_prices"] == []
    assert event_data["gross_receipts_canadian"] is None

def test_num_shows_and_sellouts():
    '''
        In this case, the OCR misread multiple items Mz is supposed to be "11-12", additional tour dates
        Additionally, "seven sellouts" is misspelled, so word2number will not be able to convert and realize it is a number
        '''
    event_line = "CULTURE CLUB The Centrum Nov. 20-21 $270017 18,351 Frank J, Russo | SCHEMERS Worcester, Mass. $16/$13.50 two shows, | one satlout"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["CULTURE CLUB", "SCHEMERS"]
    assert event_data["location"] == ["The Centrum", "Worcester, Mass."]
    assert event_data["dates"] == ["Nov. 20-21"]
    assert event_data["gross_receipts_us"] == 270017
    assert event_data["tickets_sold"] == 18351
    assert event_data["promoter"] == ["Frank J, Russo"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] == 2
    assert event_data["num_sellouts"] == 1
    assert event_data["ticket_prices"] == ["16/13.50"]
    assert event_data["gross_receipts_canadian"] is None

def test_numeric_num_sellouts():
    # usually the number of shows or sellouts is in English, like "seven sellouts", but in this case they wrote "11 sellouts"
    event_line = "DIANA ROSS Radio City Music Halt Sept. 19-25 $1,757,550 64,614 In-House | New York $30/$25/$20 11 sellouts"

    event_data = parser.parse_event(event_line)

    assert event_data["artists"] == ["DIANA ROSS"]
    assert event_data["location"] == ["Radio City Music Halt", "New York"]
    assert event_data["dates"] == ["Sept. 19-25"]
    assert event_data["gross_receipts_us"] == 1757550
    assert event_data["tickets_sold"] == 64614
    assert event_data["promoter"] == ["In-House"]
    assert event_data["capacity"] is None
    assert event_data["num_shows"] is None
    assert event_data["num_sellouts"] == 11
    assert event_data["ticket_prices"] == ["30/25/20"]
    assert event_data["gross_receipts_canadian"] is None

def test_consolidate_events_with_ticks():
    #
    raw_tour_lines ="""NIGHT RANGER The Paltadium June 8. $49,561 4377 Avalon Prods.
                    BLACK & BLUE Hollywood, Calif. $1L75 sellout
                    HANK WILLIAMS JR. Convention Center June 9. ‘$46,414 a3la ‘Sound Seventy Prods.
                    DAVID ALLAN COE Pine Bluff, Ark. $11.50 7,900
                    MOTLEY CRUE Stanley Theater June 12. $44,905 3,522 DiCesare-Engler Prods.
                    ACCEPT Pittsburgh $12.75 sellout"""

def test_consolidate_events_multiple_months():
    raw_tour_lines = """IRON MAIDEN Capitol Centre Jan. 28 $197,462 15.797 Cellar Door Prods.
                    TWISTED SISTER Landover, Md. $12.50 (29,023)
                    MEL TORME Fox Theater Jan, 29-Feb. 3 $188,595 28,401 Ray Shepardson
                    HELEN O'CONNELL St. Louis $17-$4.90 (37,096)
                    MICHEL LEGRAND
                    HUEY LEWIS & THE NEWS Arizona State Univ. Center Feb. 3 $144,018 10,668 Evening Stat Prods.
                    Tempe $13.50 sellout"""

def test_consolidate_events_june():
    raw_tour_lines = """MADONNA Cobo Arena May 25-26 $332,780 24,382 Brass Ring Prods.
                    BEASTIE BOYS Detroit $15/$12.50 two sellouts
                    PHIL COLLINS & HIS HOT TUB Compton Terrace June 1 $327,213 23,862 Evening Star Prods.
                    CLUB Phoenix $15/$13.50 sellout
                    DIANA ROSS Joe Louis Arena June 1 $284,450 16,296 Brass Ring Prods,
                    Detroit $17.50/$15 19,590"""

def test_consolidate_events_jun():
    raw_tour_lines = """TRIUMPH Meadowlands Arena May 10 $151,653 13,489 Monarch Entertainment Bureau/
                    ACCEPT East Rutherford, NJ. $13.50/$12.50 15,928 WNEW-FM/St. Pauli Girl Concert
                    Series
                    TEARS FOR FEARS. Massey Hall May 29-Jun1 $146,384 10,400 Concert Prods. International
                    IDLE EYES Toronto ($182,980 Canadian) four sellouts
                    $17.50
                    REO SPEEDWAGON Rwertront Coliseum May 22 $125,060 9,267 Sunshine Promotions
                    CHEAP TRICK Cincinnati $15.50/$12.50 16,000"""

def test_consolidate_events_dune():
    raw_tour_lines="""ALABAMA Thomas & Mack Center May 31 $141,927 8,009 Elks Helladorado
                    BILL MEDLEY Las Vegas $25/$17.50/$15.50 10,000
                    PATTI LABELLE Greek Theatre dune 7 $113,026 6,187 Nederlander
                    CON FUNK SHUN Los Angeles $20/$18/$12.50 sellout
                    ANNE MURRAY Chastain Park June 2 $94,846 5,685 Alex Cooley/Southern Promotions
                    Atlanta $18.50/$16.50/$13.50 6,351"""

def test_consolidate_events_not_start():
    raw_tour_lines = """AMY GRANT Veterans Memorial Auditorium Nov. 2 $54,850 5,520 Jam Prods.
                    BOS BENNETT Des Moines $12.50/$11.50 7.250
                    Richmond (Va.) Coliseum Oct. 23 $63,145 45 Callas Door Prods.
                    $14.50/$13.50 12,500
                    AMY GRANT ‘Northlands Coliseum Nov. 9 $52,928 5,462 Inside Concert Promotions
                    BOS BENNETT Edmonton, Alberta ($78,660 Canadian) 6.285 -
                    $15"""

def test_consolidate_events_additional_month():
    raw_tour_lines = """GEORGE BENSON Radio City Music Halt May 30- $439,800 16,011 Radio City Music Hall Productions
                    ROBERTA FLACK New York June L $30/$25/$20 17,538
                    MADONNA Radio City Music Hail June 6-7 $294,050 17,538 Radio City Music Hall Productions | BEASTIE BOYS New York $17.50/$15.50 three sellouts
                    MADONNA The Spectrum May 29 $237,047 15511 Stephen Star/The Concert Co. | BEASTIE BOYS Philadelphia $15.50/$13.50 sellout"""

def test_consolidate_events_table_lines():
    raw_tour_lines = """ARTIST(S) Venue Date(s) Ticket Price(s) Capacity Promoter
                    _|
                    BOSTON Alpine Valley Music Theatre Aug 6-9 Ts1us.762 100,812 Joseph Entertainment Group
                    FARRENHEIT East Troy, Wis. $25/522,50/$15 setiout
                    MADONNA Anaheim Stadium July 18 $1,417,185 62,986 Avalon Attractions
                    LEVEL 42 Anaheim, Cali. 2250 sellout"""
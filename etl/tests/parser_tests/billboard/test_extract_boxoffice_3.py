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



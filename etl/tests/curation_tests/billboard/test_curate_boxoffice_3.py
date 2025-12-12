from etl.curation.magazines.billboard import curate_boxoffice_3
from datetime import date

def curate_noise_in_dates():
    dates = ['Sept. 30 9144.zia 10,894']
    issue_year = 1984

    start_date, end_date, dates = curate_boxoffice_3.curate_date(dates, issue_year)
    assert start_date == date(1984, 9, 30)
    assert end_date == date(1984, 9, 30)

def curate_number_in_promoter():
    event_data = {'artists': ['THE BEACH BOYS'], 'dates': ['June 17,'], 'gross_receipts_us': 396457.0, 'gross_receipts_canadian': None, 'tickets_sold': None, 'capacity': None, 'num_shows': None, 'num_sellouts': None, 'promoter': ['Feyline Presents Inc.', '85,328'], 'ticket_prices': ['15/9.35/8.25/7.15/4.13'], 'location': ['Mile High Stadium', 'Denver'], 'source_id': 'billboard', 'schema_id': 'bb_3', 's3_uri': 'music-industry-data-lakeraw/billboard/pdf/magazines/1984/10/BB-1984-10-27.pdf'}

def curate_financials_in_dates():
    event_data = {'artists': ['LEON REDBONE', 'STEVE GOODMAN'], 'dates': ['June 7,', '316/87/$5 2,800'], 'gross_receipts_us': 11853.0, 'gross_receipts_canadian': None, 'tickets_sold': 1108.0, 'capacity': None, 'num_shows': None, 'num_sellouts': None, 'promoter': ['Evening Star Prods./'], 'ticket_prices': [], 'location': ['Paolo Soler', 'Santa Fe, N.M.', 'River Concerts'], 'source_id': 'billboard', 'schema_id': 'bb_3', 's3_uri': 'music-industry-data-lakeraw/billboard/pdf/magazines/1984/10/BB-1984-10-27.pdf'}

def curate_ticket_price_misread():
    # the ticket price was $11.75
    event_data = {'artists': ['RICK SPRINGFIELD'], 'dates': ['Nov. 10'], 'gross_receipts_us': 117500.0, 'gross_receipts_canadian': None, 'tickets_sold': 10000.0, 'capacity': None, 'num_shows': None, 'num_sellouts': 1, 'promoter': ['Jam Prods.'], 'ticket_prices': ['175'], 'location': ['Casper (Wya.) Events Center'], 'source_id': 'billboard', 'schema_id': 'bb_3', 's3_uri': 'music-industry-data-lakeraw/billboard/pdf/magazines/1984/11/BB-1984-11-24.pdf'}
import ast
import logging
import pandas as pd
logger = logging.getLogger()
from etl.dimensions.artists_utils import get_artist_name
from etl.schemas.billboard_magazine_3.curation.artists import curate_artists, identify_first_artist_line
from etl.schemas.billboard_magazine_3.curation.dates import identify_start_date, curate_dates
from etl.schemas.billboard_magazine_3.curation.promoters import curate_promoters
from etl.schemas.billboard_magazine_3.curation.location import identify_venue_name, curate_locations
from etl.schemas.billboard_magazine_3.curation.special_event import curate_event_name
from etl.utils.utils import load_corrections_table, get_source_id, parse_ocr_int
from etl.dimensions.location_utils import get_venue_name
import slugify
from config.config import STORAGE_MODE, STORAGE_FORMAT, BUCKET_NAME
from config.paths import S3_EVENT_CORRECTIONS_PATH, LOCAL_CORRECTION_TABLES_DIR
import re
import io
import os
import boto3

s3 = boto3.client("s3")

'''
This curation script is for the Billboard Boxscore schema that ran from 1984-10-20 to 2001-07-21
'''
test_file = "BB-1985-03-16"
object_key = f"processed/billboard/magazines/1985/03/{test_file}"

def curate_num_sellouts(processed_events_df, curated_events_df):
    mask = processed_events_df[["num_shows", "num_sellouts"]].dropna()                                                  # drop rows that are empty before validating
    all_numeric = mask["num_sellouts"].ge(0).all()                                                                      # verify all values are numeric and greater than 0
    sellouts_less_than_shows = (mask["num_sellouts"] <= mask["num_shows"]).all()                                        # num_sellouts can never be greater than num_shows

    # if both constraints met, copy the data into curated as int
    if all_numeric and sellouts_less_than_shows:
        curated_events_df["num_sellouts"] = processed_events_df["num_sellouts"].apply(lambda x: int(x) if pd.notnull(x) else pd.NA)

def find_multiple_ticket_price_symbol(ticket_price):
    """

    :param ticket_price: str
    :return:
    """
    symbols = ['/', '-', '&']

    for symbol in symbols:
        if symbol in ticket_price:
            return symbol

def clean_ticket_price(ticket_price):
    """

    :param ticket_price: str
    :return:
    """
    cleaned_ticket_price = ticket_price.strip()
    cleaned_ticket_price = cleaned_ticket_price.replace('$', '')
    cleaned_ticket_price = cleaned_ticket_price.replace('_', '')
    cleaned_ticket_price = re.sub(r'[Ss](\d)', r'\1', cleaned_ticket_price)
    cleaned_ticket_price = re.sub(r'\.$', '', cleaned_ticket_price)                                         # remove periods at the end with nothing
    cleaned_ticket_price = re.sub(r',$', '', cleaned_ticket_price)
    cleaned_ticket_price = re.sub(r'(\d{1,2}),(\d{2})', r'\1.\2', cleaned_ticket_price)

    return cleaned_ticket_price

def validate_ticket_price(ticket_price):
    NOISE = {'(', ')', '{', '}', '[', ']'}

    for noise_symbol in NOISE:
        if noise_symbol in ticket_price:
            return False

    if bool(re.search(r'\d{2,},{3.}', ticket_price)):
        print("Failed regex")
        return False

    return True

def curate_ticket_price(ticket_price):
    cleaned_ticket_price = clean_ticket_price(ticket_price)
    if validate_ticket_price(cleaned_ticket_price):
        try:
            curated_ticket_price = float(cleaned_ticket_price)
            return curated_ticket_price
        except ValueError as e:
            print(f"Error {e} curating ticket price: {cleaned_ticket_price}")
            return None
    else:
        return None

def curate_event_ticket_prices(ticket_price):
    symbols = ['/', '-', '&']

    event_prices = []
    for prices in ticket_price:
        if any(symbol in prices for symbol in symbols):
            multiple_ticket_symbol = find_multiple_ticket_price_symbol(prices)
            ticket_prices = prices.split(multiple_ticket_symbol)
            for price in ticket_prices:
                curated_ticket_price = curate_ticket_price(price)
                if curated_ticket_price:
                    event_prices.append(curated_ticket_price)
        else:
            curated_ticket_price = curate_ticket_price(prices)
            if curated_ticket_price:
                event_prices.append(curated_ticket_price)

    return event_prices

def curate_ticket_prices(processed_events_df, curated_events_df):
    clean_prices = []

    for row in processed_events_df["ticket_prices"]:
        event_prices = curate_event_ticket_prices(row)
        clean_prices.append(event_prices)

    curated_events_df["ticket_prices"] = clean_prices

def add_raw_event_signature(processed_events_df):
    processed_events_df["signature"] = processed_events_df.apply(
        lambda row:
            f"{row['first_artist_line'].lower().replace(' ', '-')}-"
            f"{row['venue_name'].lower().replace(' ', '-')}-"
            f"{row['start_date']}",
            axis=1
    )

def normalize_event_or_artists(row, dimension_tables):
    '''
    Returns either the slugified version of the event name or the name of the first artist
    :param row:
    :param dimension_tables:
    :return:
    '''
    if row["event_name"]:
        return slugify.slugify(row["event_name"])

    artist_ids = row["artist_ids"]

    if len(artist_ids) > 0:
        print(artist_ids[0])
        return slugify.slugify(get_artist_name(artist_ids[0], dimension_tables["artists"]))

    return None

def curate_event_signature(curated_events_df, dimension_tables):
    curated_events_df["signature"] = curated_events_df.apply(
        lambda row:
            f"{normalize_event_or_artists(row, dimension_tables)}-"
            f"{get_venue_name(row['venue_id'], dimension_tables["venues"]).lower().replace(' ', '-')}-"
            f"{row['start_date']}",
            axis=1
        )

def implement_corrections(processed_events_df):
    """
    Update any fields that were misread by OCR and do not have the context to autocorrect

    :param processed_events_df:
    :return:
    """
    schema_id = processed_events_df["schema_id"].iloc[0]
    if STORAGE_MODE == "local":
        schema_corrections_path = os.path.join(LOCAL_CORRECTION_TABLES_DIR, f"event_corrections_{schema_id}.csv")
        correction_dict = load_corrections_table(schema_corrections_path)
    current_event_signatures = processed_events_df["signature"].to_list()                                               # get the event signatures for all events in the current issue

    # get all corrections that need to be completed for the events in the current issue
    # one event can need corrections for multiple fields
    # {'journey-civic-center-1984-10-20': [{'event_signature': 'journey-civic-center-1984-10-20', 'field': 'tickets_sold', 'true_value': 10000},
    #                                      {'event_signature': 'journey-civic-center-1984-10-20', 'field': 'capacity', 'true_value': 12200}]
    corrections_per_event = {
        signature: correction_dict[signature]
        for signature in current_event_signatures
        if signature in correction_dict
    }

    for event_signature, corrections in corrections_per_event.items():
        event = processed_events_df.index[                                                                              # create a mask with the index where the event signature matches
            processed_events_df["signature"] == event_signature
        ]

        row_idx = event[0]                                                                                              # get the row

        for correction in corrections:
            field = correction["field"]                                                                                 # get the name of the field requiring correction
            raw_value = correction["true_value"]                                                                        # get the true value that needs to be inserted

            if isinstance(raw_value, str):
                try:
                    true_value = ast.literal_eval(raw_value)                                                            # convert the value to its natural type (list, float, int)
                except (ValueError, SyntaxError):
                    true_value = raw_value
            else:
                true_value = raw_value

            try:
                processed_events_df.at[row_idx, field] = true_value                                                     # implement the correction into the dataframe
                print(f'Corrected {raw_value}: {true_value}')
            except ValueError as e:
                print(e)
                print(f"Could not correct {field} to {true_value}")

def curate_meta_data(processed_events_df, curated_events_df):
    """
    Add the meta-data values from the processed_events_df to the curated_events_df,
    replacing strings with numeric ID's where relevant

    :param processed_events_df:
    :param curated_events_df:
    """
    curated_events_df["schema_id"] = processed_events_df["schema_id"]
    curated_events_df["source_id"] = processed_events_df["source_id"].apply(
        lambda source_id: get_source_id(source_id, storage_mode=STORAGE_MODE)
    )
    curated_events_df["s3_uri"] = processed_events_df["s3_uri"]

def validate_numeric_column(df, col_name):
    """
    True if none of the values in the column are less than 0

    :param df:
    :param col_name:
    :return:
    """
    try:
        if df[col_name].dropna().ge(0).all():                                                                           # verify all financial values are all greater than 0
            return True
    except TypeError as e:
        print(f"Error: {e} for {df[col_name]}")
        return False

def curate_numeric_fields(processed_events_df, curated_events_df):
    for col in ["gross_receipts_us", "gross_receipts_canadian", "attendance", "capacity", "num_shows"]:
        if validate_numeric_column(processed_events_df, col):
            curated_events_df[col] = processed_events_df[col].apply(parse_ocr_int)       # copy all original values as integers

def write_curated_events(events_df, s3_client=None, processed_path=None):
    """

    :param events_df:
    :param s3_client:
    :param processed_path:
    :return:
    """
    if STORAGE_MODE == "local":
        curated_path = processed_path.replace('processed', 'curated')
        os.makedirs(os.path.dirname(curated_path), exist_ok=True)

        if STORAGE_FORMAT == "csv":
            events_df.to_csv(curated_path, index=False)
        else:
            events_df.to_parquet(curated_path, index=False, engine="pyarrow")
    else:
        output_key = processed_path.replace("processed/", "curated/").replace(".csv", ".parquet")
        buffer = io.BytesIO()
        events_df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        s3_client.upload_fileobj(buffer, BUCKET_NAME, output_key)

def curate_events(processed_events_df, path, dimension_tables, s3_client=None):
    """

    :param processed_events_df:
    :param path
    :param dimension_tables:
    :param s3_client:
    """
    print(f"Curating {path} ...")
    curated_events_df = pd.DataFrame()

    identify_venue_name(processed_events_df, dimension_tables)
    identify_first_artist_line(processed_events_df)
    identify_start_date(processed_events_df, path)
    processed_events_df["promoter"] = processed_events_df["promoter"].apply(ast.literal_eval)
    processed_events_df["ticket_prices"] = processed_events_df["ticket_prices"].apply(ast.literal_eval)

    for col in ["promoter", "ticket_prices", "artists", "dates"]:
        if col in processed_events_df.columns:
            processed_events_df[col] = processed_events_df[col].astype("object")
    add_raw_event_signature(processed_events_df)
    implement_corrections(processed_events_df)

    curated_events_df["weekly_rank"] = range(1, len(processed_events_df) + 1)
    curate_event_name(processed_events_df, curated_events_df)
    curate_artists(processed_events_df, curated_events_df, dimension_tables["artists"])
    curated_events_df["venue_id"], venue_names = curate_locations(processed_events_df, dimension_tables)
    curate_promoters(processed_events_df, curated_events_df, dimension_tables["promoters"], venue_names)
    curate_dates(processed_events_df, curated_events_df, path)
    curate_event_signature(curated_events_df, dimension_tables)
    curate_numeric_fields(processed_events_df, curated_events_df)
    curate_num_sellouts(processed_events_df, curated_events_df)
    curate_ticket_prices(processed_events_df, curated_events_df)
    curate_meta_data(processed_events_df, curated_events_df)
    print(f"Finished curating {path}")

    write_curated_events(curated_events_df, s3_client=s3_client, processed_path=path)
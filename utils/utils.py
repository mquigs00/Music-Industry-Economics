from collections import defaultdict

from pdf2image import convert_from_bytes
import pytesseract
import os
import re
import csv
import pandas as pd
import slugify
from config.config import DIMENSION_TABLES
from data_cleaning.normalization import build_reverse_map
from config.paths import *
import json

def load_list_from_file(path):
    text_list = []

    with open(path, 'r') as file:
        for line in file:
            text_list.append(line.strip())

    return text_list

def sluggify_column(column_name):
    slug = column_name.replace(' ', '_').lower()
    return slug

def extract_text_ocr(pdf_bytes, page_num):
    pdf_images = convert_from_bytes(
        pdf_bytes,
        first_page=page_num,
        last_page=page_num
    )

    texts = []
    for img in pdf_images:
        texts.append(pytesseract.image_to_string(img))

    return "\n".join(texts)

def load_corrections_table(path):
    corrections_dict = {}
    with open(path, "r", newline='', encoding='cp1252') as f:
        reader = csv.DictReader(f)
        for row in reader:
            signature = row["event_signature"]
            corrections_dict.setdefault(signature, []).append(row)
    return corrections_dict

def load_dimension_table_rows(path):
    '''
    Generates a list of all of the rows in a csv file

    :param path (string)
    :return: rows (list), max_id (int)
    '''
    rows = []
    max_id = 0

    if not os.path.exists(path):
        return rows, 0

    with open(path, "r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            max_id = max(max_id, int(row["id"]))

    return rows, max_id

def load_dimension_tables():
    tables = {}

    # name = name of csv file ("artists", "promoters", etc..., meta = a key function with the necessary keys
    for name, meta in DIMENSION_TABLES.items():
        rows, max_id = load_dimension_table_rows(meta["path"])                                                                    # load the list of table rows and the maximum id value

        indexes = index_dimension(
            rows,
            key_fn = meta["key_fn"]
        )

        tables[name] = {
            "by_id": indexes["by_id"],
            "by_slug": indexes["by_slug"],
            "by_key": indexes["by_key"],
            "max_id": max_id
        }

    return tables

def index_dimension(rows, key_fn):
    '''
    Creates three versions of the dimension table as a dictionary. One allowing search by unique key, one version allowing search by slug, one by unique id number
    :param rows: list - a list of the rows of the dimension table
    :param key_fn: lambda function: a function that generates a unique key to identify the row in the dictionary
    :return: dictionary of dictionaries containing versions of the same dimension table with different indexes
    '''
    by_key = {}
    by_slug = defaultdict(list)                                                                                         # use list because slugs may not be unique keys
    by_id = {}

    for row in rows:
        key = key_fn(row)                                                                                               # key_fn generates unique key to identify the row
        by_key[key] = row                                                                                               # add the next row of data to each dictionary
        by_slug[row["slug"]].append(row)
        by_id[int(row["id"])] = row

    return {
        "by_key": by_key,
        "by_slug": dict(by_slug),
        "by_id": by_id
    }

def get_venue_name(venue_id):
    if isinstance(venue_id, str):
        if venue_id.isdigit():
            venue_id = int(venue_id)
    elif isinstance(venue_id, bool):
        raise TypeError("venue_id must be int, not bool")
    if not isinstance(venue_id, int):
        print(f"venue_id must be int, not {type(venue_id)}")

    dimension_tables = load_dimension_tables()
    dim_venues_by_id = dimension_tables["venues"]["by_id"]
    venue_name = dim_venues_by_id[venue_id]["name"]
    return venue_name

def add_slugs_to_csv(path):
    df = pd.read_csv(path)
    if "name" not in df.columns:
        raise Exception("No 'name' column in CSV file")
    if "slug" not in df.columns:
        raise Exception("No 'slug' column in CSV file")
    df["slug"] = df["name"].apply(slugify.slugify)

    df.to_csv(path, index=False)

def load_event_keywords(path):
    with open(path, "r") as f:
        event_keywords = set(json.load(f))

    return event_keywords

def parse_ocr_int(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, (int,)):
        return value
    if isinstance(value, float):
        value = str(value)
        value = re.sub(r'\.0$', '', value)
        value = value.replace('.', '')
        return int(value)
    if isinstance(value, str):
        v = value.strip()
        v = v.replace(' ', '')
        if '.' in v and ',' in v:
            v = v.replace('.', '').replace(',', '')
        else:
            v = v.replace('.', '').replace(',', '')

    v = re.sub("[^\d]", "", v)

    if  v == "":
        return pd.NA

    return int(v)

def get_source_id(source_slug):
    dim_sources = load_dimension_tables()["sources"]["by_slug"]
    source_id = dim_sources[source_slug][0]["id"]
    return source_id
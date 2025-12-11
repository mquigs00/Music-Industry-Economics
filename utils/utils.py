from pdf2image import convert_from_bytes
import pytesseract
import os
import csv
import pandas as pd
import slugify
from config.config import DIMENSION_TABLES

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

def load_table(path):
    table = {}
    max_id = 0

    if not os.path.exists(path):
        return table, 0

    with open(path, "r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["slug"]
            table[key] = row
            max_id = max(max_id, int(row["id"]))

    return table, max_id

def load_dimension_tables():
    tables = {}
    for name, path in DIMENSION_TABLES.items():
        data, max_id = load_table(path)
        tables[name] = {"data": data, "max_id": max_id}
    return tables

def add_slugs_to_csv(path):
    df = pd.read_csv(path)
    if "name" not in df.columns:
        raise Exception("No 'name' column in CSV file")
    if "slug" not in df.columns:
        raise Exception("No 'slug' column in CSV file")
    df["slug"] = df["name"].apply(slugify.slugify)

    df.to_csv(path, index=False)
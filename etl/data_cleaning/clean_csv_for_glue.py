import csv
import pandas as pd
import re

def clean(path):
    with open(path, 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)
        rows = []

        for row in csvreader:
            row = [item for item in row if item.strip()]
            rows.append(row)

    with open(path, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(header)

        for row in rows:
            csvwriter.writerow(row)

def convert_latin1_to_utf8(path):
    df = pd.read_csv(path, encoding='latin-1', on_bad_lines='skip')
    df.to_csv("official_entity_financials", index=False, encoding='utf-8')
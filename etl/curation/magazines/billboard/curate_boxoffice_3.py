import io
import re
import ast
from botocore.exceptions import ClientError
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import DIM_ARTISTS_PATH
from utils.utils import *
import pandas as pd
import os
import csv
import json
from Levenshtein import distance as levenshtein_distance
import logging
import slugify
logger = logging.getLogger()

'''
This curation script is for the Billboard Boxscore schema that ran from 1984-10-20 to 2001-07-21
'''

object_key = "processed/billboard/magazines/1984/10/BB-1984-10-20.csv"

def load_artists(path):
    artists = {}
    max_id = 0

    if not os.path.exists(path):
        return artists, 0

    with open(path, "r", newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["artist_slug"]
            artists[key] = row
            max_id = max(max_id, int(row["artist_id"]))

    return artists, max_id

def append_artist_table(path, artist_id, name):
    with open(path, "a", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([artist_id, name, slugify.slugify(name)])

def curate_artists(artists, existing_artists, max_artists_id):
    event_name = None
    i = 0

    # if there is a colon, save a new event
    if ":" in artists[0]:
        event_name = artists[0][:artists[0].find(":")]                                      # save everything up to the colon as the event name
        print(f"Event name: {event_name}")
        i += 1

    for artist in artists[i:]:
        print(f"Artist = {artist}, max id = {max_artists_id}")
        key = slugify.slugify(artist)

        # if the artist does not already exist in the artists reference table
        if key not in existing_artists:
            max_artists_id += 1                                                                    # increment to get the next artist id number
            existing_artists[key] = {                                                      # record the new artist
                "artist_id": max_artists_id,
                "artist_name": artist
            }
            append_artist_table(DIM_ARTISTS_PATH, max_artists_id, artist)                          # write the new artist to the csv file
            artists[i] = max_artists_id                                                            # replace the artist name with the new id number
        else:
            artist_id = existing_artists[key]["artist_id"]                                 # get the id number of the artist
            artists[i] = artist_id                                                         # replace the artist name with their id number

        i += 1
        return max_artists_id

def curate_to_parquet():
    #processed_data = pd.read_csv(f"s3://{BUCKET_NAME}/{object_key}")
    processed_events = pd.read_csv("test_files/BB-1984-10-20.csv")
    dim_artists, max_artist_id = load_artists(DIM_ARTISTS_PATH)
    curated_events = []

    # for every event in the current issue
    for event in processed_events:
        curate_artists(event["artists"], dim_artists, max_artist_id)

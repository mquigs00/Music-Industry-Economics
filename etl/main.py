from config.paths import DIM_CITIES_PATH
from etl.parsers.magazines.billboard import extract_boxoffice_1
from etl.parsers.magazines.billboard import extract_boxoffice_3
from etl.curation.magazines.billboard import curate_boxoffice_3 as curator
from config import paths
from utils import utils
from data_cleaning import clean_csv_for_glue
import re
import os
from datetime import date

if __name__ == "__main__":
    #extract_boxoffice_3.extract_to_csv()
    curator.curate_events()
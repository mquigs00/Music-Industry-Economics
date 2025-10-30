from etl.parsers.magazines.billboard import extract_boxoffice_1
from etl.parsers.magazines.billboard import extract_boxscore_2
from data_cleaning import clean_csv_for_glue
import re

if __name__ == "__main__":
    extract_boxscore_2.extract_to_csv()

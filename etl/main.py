from etl.parsers.magazines.billboard import extract_boxoffice_1
from etl.parsers.magazines.billboard import extract_boxoffice_3
from data_cleaning import clean_csv_for_glue
import re

if __name__ == "__main__":
    extract_boxoffice_3.extract_to_csv()

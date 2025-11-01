import pdfplumber
import io
import re
from utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import NON_MUSICIANS_PATH, NOISY_SYMBOLS_PATH
from utils.utils import *
import pandas as pd
import csv
import json
from Levenshtein import distance as levenshtein_distance

'''
This parser is for the Billboard Boxscore that ran from 1981-10-03 to 1984-10-13
'''

class ParsingError(Exception):
    """Base class for parsing errors"""

class FileParsingError(ParsingError):
    """Raised when the entire file should be skipped"""

class TourParsingError(ParsingError):
    """Raised when just the current tour should be skipped"""


pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

directory_prefix = "raw/billboard/pdf/magazines/"

object_key = 'raw/billboard/pdf/magazines/1981/09/BB-1981-09-19.pdf'

'''
    Every tour has:
    Artist(s): 'FOREIGNER, BILLY SQUIER',
    Gross Revenue,
    Attendance,
    Capacity,
    Ticket Price(s)
    City: Seattle
    Date: March 25
    Total Ticket Sales: 15,000
    Ticket Price Scale: 8.00
    Gross Receipts: 119,760* (* denotes a sellout concert)
'''
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_CLEANING_DIR = os.path.join(PROJECT_ROOT, "data_cleaning")
NON_MUSICIANS_PATH = os.path.join(DATA_CLEANING_DIR, "non_musicians.txt")
NOISY_SYMBOLS_PATH = os.path.join(DATA_CLEANING_DIR, "noisy_symbols.txt")
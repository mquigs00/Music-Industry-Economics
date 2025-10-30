import pytesseract

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

# s3 bucket configuration
BUCKET_NAME = "music-industry-data-lake"

NON_MUSICIANS_PATH = "data_cleaning/non_musicians.txt"
NOISY_SYMBOLS_PATH = "data_cleaning/noisy_symbols.txt"
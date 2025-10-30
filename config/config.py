import pytesseract
import os

pytesseract.pytesseract.tesseract_cmd = r"C:\Users\mquig\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"

# s3 bucket configuration
BUCKET_NAME = "music-industry-data-lake"

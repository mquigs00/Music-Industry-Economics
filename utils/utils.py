from pdf2image import convert_from_bytes
import pytesseract

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
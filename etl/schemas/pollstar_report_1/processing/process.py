import pdfplumber
import io
import re
from etl.utils.s3_utils import list_s3_files, client
from config import BUCKET_NAME
from config.paths import NON_MUSICIANS_PATH, NOISY_SYMBOLS_PATH


def clean_text(text):
    text = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")

    for symbol in noisy_symbols:
        text = text.replace(symbol, "")

    text = text.strip()
    # text = re.sub(r"\s*", "\s", text)
    fields = text.split()
    text = " ".join(fields)

    return text

non_musicians = load_list_from_file(NON_MUSICIANS_PATH)
noisy_symbols = load_list_from_file(NOISY_SYMBOLS_PATH)

directory_prefix = "raw/pollstar_dir/pdf/reports_pack/top-tours/"

pages = list_s3_files(directory_prefix)

print("Pages returned: ", pages)

def extract_to_csv():
    for key in pages:
        if not key.endswith(".pdf"):
            continue

        try:
            file = client.get_object(Bucket=BUCKET_NAME, Key=key)
            print("Extracting second file")
            pdf_bytes = file['Body'].read()
            pdf_file = io.BytesIO(pdf_bytes)

            with pdfplumber.open(pdf_file) as pdf:
                top_tours_all = []
                top_tours_musicians = []

                for page_num, page in enumerate(pdf.pages, start=1):
                    page_text = page.extract_text()

                    if not page_text:
                        print("PDF Plumber did not find text. Attempting to extract text with pytesseract OCR")
                        page_text = extract_text_ocr(pdf_bytes, page_num)

                    if not page_text:
                        print("No text found with pdfplumber or OCR, skipping page")
                        continue

                    lines = page_text.splitlines()

                    for line in lines:
                        if re.search(r"^\d", line) and len(line) >= 40:
                            try:
                                path_parts = key.split("/")
                                scope = path_parts[-2]
                                year = path_parts[-3]
                                line = clean_text(line)

                                tour_data = re.split(r"\s", line, 2)
                                rank = tour_data[0]
                                gross_millions = tour_data[1]
                                tour_data = tour_data[2]

                                end_of_artist_name_idx = re.search(r"\d*\.\d*", tour_data)
                                artist = tour_data[0:end_of_artist_name_idx.start() - 1]
                                tour_data = tour_data[end_of_artist_name_idx.start():]

                                view_remaining_columns = re.split(r"\s", tour_data, 5)

                                if "/" in view_remaining_columns[4]:
                                    tour_data = re.split(r"\s", tour_data, 5)
                                    avg_ticket_price = tour_data[0]
                                    avg_num_tickets = tour_data[1]
                                    total_num_tickets = tour_data[2]
                                    avg_gross = tour_data[3]
                                    cities_shows = tour_data[4]
                                    agency = tour_data[5]

                                    tour_dict = {
                                        "year": year,
                                        "rank": rank,
                                        "revenue": gross_millions,
                                        'currency': 'usd',
                                        "artist_id": sluggify_column(artist),
                                        "avg_ticket_price": avg_ticket_price,
                                        "avg_tickets_per_show": avg_num_tickets,
                                        "total_tickets_sold": total_num_tickets,
                                        "avg_revenue_per_show": avg_gross,
                                        "num_cities_shows": cities_shows,
                                        "agency_id": sluggify_column(agency),
                                        "scope": scope,
                                        "source_id": "pollstar_dir"
                                    }

                                    top_tours_all.append(tour_dict)

                                    if artist not in non_musicians:
                                        top_tours_musicians.append(tour_dict)

                                else:
                                    tour_data = re.split(r"\s", tour_data, 5)
                                    avg_ticket_price = tour_data[0]
                                    avg_num_tickets = tour_data[1]
                                    avg_gross = tour_data[2]
                                    rank2 = tour_data[3]
                                    gross_millions2 = tour_data[4]

                                    tour_data = tour_data[5]
                                    end_of_artist_name_idx = re.search(r"\s\d*\.\d*", tour_data)
                                    artist2 = tour_data[0:end_of_artist_name_idx.start() - 1]
                                    tour_data = tour_data[end_of_artist_name_idx.start():]
                                    tour_data = re.split(r"\s", tour_data)

                                    avg_ticket_price2 = tour_data[0]
                                    avg_num_tickets2 = tour_data[1]
                                    avg_gross2 = tour_data[2]

                                    tour_dict1 = {
                                        "year": year,
                                        "rank": rank,
                                        "revenue": gross_millions,
                                        'currency': 'usd',
                                        "artist_id": sluggify_column(artist),
                                        "avg_ticket_price": avg_ticket_price,
                                        "avg_tickets_per_show": avg_num_tickets,
                                        "total_tickets_sold": None,
                                        "avg_revenue_per_show": avg_gross,
                                        "num_cities_shows": None,
                                        "agency_id": None,
                                        "scope": scope,
                                        "source_id": "pollstar_dir"
                                    }

                                    tour_dict2 = {
                                        "year": year,
                                        "rank": rank2,
                                        "revenue": gross_millions2,
                                        'currency': 'usd',
                                        "artist_id": sluggify_column(artist2),
                                        "avg_ticket_price": avg_ticket_price2,
                                        "avg_tickets_per_show": avg_num_tickets2,
                                        "total_tickets_sold": None,
                                        "avg_revenue_per_show": avg_gross2,
                                        "num_cities_shows": None,
                                        "agency_id": None,
                                        "scope": scope,
                                        "source_id": "pollstar_dir"
                                    }

                                    top_tours_all.append(tour_dict1)
                                    top_tours_all.append(tour_dict2)

                                    if artist not in non_musicians:
                                        top_tours_musicians.append(tour_dict1)

                                    if artist2 not in non_musicians:
                                        top_tours_musicians.append(tour_dict2)

                            except Exception as e:
                                print("Error parsing tour record: ", e)

                df_all_tours = pd.DataFrame(top_tours_all)
                df_musician_tours = pd.DataFrame(top_tours_musicians)

                # get name of the file and add .csv instead of .pdf
                file_path = key
                file_name = file_path.split('/')[-1]
                csv_file_name = file_name.replace('.pdf', '.csv')
                # print(csv_file_name)

                # df_all_tours.to_csv("./csv/" + csv_file_name)

                # save all tour df as csv in all-tours folder

                csv_buffer = io.StringIO()
                df_all_tours.to_csv(csv_buffer, index=False)

                try:
                    client.put_object(
                        Bucket="music-industry-data-lake",
                        Key="processed/pollstar_dir/reports_pack/top-tours/all-tours/" + csv_file_name,
                        Body=csv_buffer.getvalue(),
                    )
                    print("Saved all tours report")
                except Exception as e:
                    print(f"Error uploading file: {e}")

                df_musician_tours["org_rank"] = df_musician_tours["rank"]

                df_musician_tours = df_musician_tours.reset_index(drop=True)
                df_musician_tours.index = df_musician_tours.index + 1
                df_musician_tours.index.name = "musician_rank"

                csv_buffer = io.StringIO()
                df_musician_tours.to_csv(csv_buffer, index=True)

                try:
                    client.put_object(
                        Bucket="music-industry-data-lake",
                        Key="processed/pollstar_dir/reports_pack/top-tours/musician-tours/" + csv_file_name,
                        Body=csv_buffer.getvalue()
                    )

                    print(f"Uploaded file: {csv_file_name}")
                except Exception as e:
                    print(f"Error uploading file {csv_file_name}: {e}")
        except client.exceptions.NoSuchKey:
            print(f"Error: Object '{key}' not found in bucket '{BUCKET_NAME}'")
            exit()
        except Exception as e:
            print(f"Error retrieving object: {e}")
            exit()
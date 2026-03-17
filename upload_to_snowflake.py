import os
import pdfplumber
import snowflake.connector
from dotenv import load_dotenv
from tqdm import tqdm
import pytesseract
from PIL import Image

# Load environment variables
load_dotenv()

# Snowflake config
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

# 🔥 SET YOUR ROOT FOLDER HERE
ROOT_FOLDER = "/Users/shaivibhandekar/Desktop/Pradeepthi Medical Agent"


# -------------------------------
# PDF TEXT EXTRACTION (WITH OCR)
# -------------------------------
def extract_text_from_pdf(file_path):
    text = ""

    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"

        # 🔥 OCR fallback if no text found
        if not text.strip():
            print(f"🔍 OCR fallback for: {file_path}")

            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    img = page.to_image(resolution=300)
                    pil_img = img.original
                    ocr_text = pytesseract.image_to_string(pil_img)
                    text += ocr_text + "\n"

    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")

    return text.strip()


# -------------------------------
# CLEAN TEXT
# -------------------------------
def clean_text(text):
    return " ".join(text.split())


# -------------------------------
# GET ALL PDF FILES
# -------------------------------
def get_pdf_files(root_folder):
    pdf_data = []

    for year_folder in os.listdir(root_folder):
        year_path = os.path.join(root_folder, year_folder)

        if os.path.isdir(year_path):
            for file in os.listdir(year_path):
                if file.lower().endswith(".pdf"):
                    file_path = os.path.join(year_path, file)
                    pdf_data.append((year_folder, file, file_path))

    return pdf_data


# -------------------------------
# CONNECT TO SNOWFLAKE
# -------------------------------
def connect_snowflake():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)


# -------------------------------
# INSERT DATA
# -------------------------------
def insert_records(conn, records):
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO medical_db.records.medical_records (year, file_name, content)
    VALUES (%s, %s, %s)
    """

    try:
        cursor.executemany(insert_query, records)
        conn.commit()
        print("✅ Data inserted into Snowflake!")
    except Exception as e:
        print(f"❌ Insert error: {e}")
    finally:
        cursor.close()


# -------------------------------
# MAIN FUNCTION
# -------------------------------
def main():
    print("🚀 Starting PDF upload process...")

    if not os.path.exists(ROOT_FOLDER):
        print(f"❌ Folder not found: {ROOT_FOLDER}")
        return

    pdf_files = get_pdf_files(ROOT_FOLDER)
    print(f"📂 Found {len(pdf_files)} PDF files")

    if not pdf_files:
        print("⚠️ No PDFs found.")
        return

    records_to_insert = []

    for year, file_name, file_path in tqdm(pdf_files):
        print(f"📄 Processing: {file_name}")

        text = extract_text_from_pdf(file_path)

        if not text:
            print(f"⚠️ Skipping empty file: {file_name}")
            continue

        cleaned = clean_text(text)

        records_to_insert.append((year, file_name, cleaned))

    print(f"📦 Total records prepared: {len(records_to_insert)}")

    if not records_to_insert:
        print("❌ No data to insert. Check OCR or PDFs.")
        return

    print("🔌 Connecting to Snowflake...")
    conn = connect_snowflake()
    print("✅ Connected!")

    insert_records(conn, records_to_insert)

    conn.close()
    print("🎉 Upload completed successfully!")


# -------------------------------
if __name__ == "__main__":
    main()
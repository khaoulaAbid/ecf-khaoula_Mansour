import json
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
from utils.logger import get_logger
from utils.minio_client import get_minio_client

# ===============================================================================
# DDL Script: Create Bronze books Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for books  
# ===============================================================================


logger = get_logger("bronze.scrape_books")

BUCKET = "bronze"
BASE_URL = "https://books.toscrape.com/catalogue/page-{}.html"
HEADERS = {"User-Agent": "DataPulseBot/1.0"}

DB_CONN = psycopg2.connect(
    host="db",
    database="datapulse",
    user="admin",
    password="admin"
)


def run():
    logger.info("START Bronze scraping → PostgreSQL & MinIO (Books)")

    cursor = DB_CONN.cursor()

    # Ensure bronze schema & table exist
    cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.books_raw (
            title TEXT,
            price TEXT,
            rating TEXT,
            category TEXT,
            ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT
        );
    """)

    # minio_client = get_minio_client()

    # if not minio_client.bucket_exists(BUCKET):
    #     minio_client.make_bucket(BUCKET)

    books = []

    for page in range(1, 51):
        try:
            url = BASE_URL.format(page)
            logger.info(f"Scraping page {page}")

            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()

        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            continue

        soup = BeautifulSoup(r.text, "html.parser")

        for b in soup.select("article.product_pod"):
            record = {
                "title": b.h3.a["title"],
                "price": b.select_one(".price_color").text[1:],
                "rating": b.p["class"][1],  # Raw value (e.g. 'Three')
                "category": "Books",
                "source": "books.toscrape.com"
            }

            books.append(record)

            cursor.execute("""
                INSERT INTO bronze.books_raw (
                    title, price, rating, category, source
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                record["title"],
                record["price"],
                record["rating"],
                record["category"],
                record["source"]
            ))

        time.sleep(1)  # polite scraping

    DB_CONN.commit()
    cursor.close()
    DB_CONN.close()

    logger.info("Uploading books raw data to MinIO")

    data_bytes = json.dumps(books, indent=2).encode("utf-8")

    # minio_client.put_object(
    #     BUCKET,
    #     "books/books_raw.json",
    #     data=data_bytes,
    #     length=len(data_bytes),
    #     content_type="application/json"
    # )

    logger.info("SUCCESS Bronze books ingestion completed")
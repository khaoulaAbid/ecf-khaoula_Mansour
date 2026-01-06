import json
import time
import requests
import psycopg2
from bs4 import BeautifulSoup
from utils.logger import get_logger
from utils.minio_client import get_minio_client
import io


# ===============================================================================
# DDL Script: Create Bronze quotes Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for quotes api scrapping 
# ===============================================================================



logger = get_logger("bronze.scrape_quotes")

BASE_URL = "https://quotes.toscrape.com"
HEADERS = {"User-Agent": "DataPulseBot/1.0 (ECF project)"}
BUCKET = "bronze"

conn = psycopg2.connect(
    host="db",
    database="datapulse",
    user="admin",
    password="admin"
)


def run():
    logger.info("START Bronze scraping → PostgreSQL & MinIO (Quotes)")

    cur = conn.cursor()

    # Ensure bronze schema & table exist
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.quotes_raw (
            quote TEXT,
            author TEXT,
            tags TEXT,
            ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
         
        );
    """)

    minio_client = get_minio_client()

    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)

    quotes = []
    page = 1

    while True:
        url = f"{BASE_URL}/page/{page}/"
        logger.info(f"Scraping page {page}")

        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 404:
                break
            r.raise_for_status()
        except Exception as e:
            logger.error(f"HTTP error page {page}: {e}")
            break

        soup = BeautifulSoup(r.text, "html.parser")
        items = soup.select(".quote")

        if not items:
            break

        for q in items:
            tags_list = [t.text for t in q.select(".tag")]
            tags_str = ",".join(tags_list)

            record = {
                "quote": q.select_one(".text").text.strip(),
                "author": q.select_one(".author").text.strip(),
                "tags": tags_str
                
            }

            quotes.append(record)

            cur.execute("""
                INSERT INTO bronze.quotes_raw (
                    quote, author, tags
                )
                VALUES (%s, %s, %s)
            """, (
                record["quote"],
                record["author"],
                record["tags"]
                
            ))

        page += 1
        time.sleep(1)  # polite scraping

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Uploading quotes raw data to MinIO")


    
    logger.info("SUCCESS Bronze books ingestion completed")

    data_bytes = json.dumps(quotes, indent=2).encode("utf-8")
    data_stream = io.BytesIO(data_bytes)


    minio_client.put_object(
            bucket_name=BUCKET,
            object_name="quotes/quotes_raw.json",
            data=data_stream,
            length=len(data_bytes),
            content_type="application/json"
        )
    logger.info("SUCCESS Bronze quotes ingestion completed")
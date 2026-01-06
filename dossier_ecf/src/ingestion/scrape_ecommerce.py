import time
import requests
import pandas as pd
import psycopg2
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from utils.logger import get_logger

# ===============================================================================
# DDL Script: Create Bronze ecommerce Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for ecommerce api scrapping 
# ===============================================================================


logger = get_logger("bronze.scrape_products")

BASE_URL = "https://webscraper.io/test-sites/e-commerce/allinone"
HEADERS = {"User-Agent": "DataPulseBot/1.0"}

conn = psycopg2.connect(
    host="db",
    database="datapulse",
    user="admin",
    password="admin"
)


def run():
    logger.info("START Bronze scraping – E-commerce")

    cur = conn.cursor()

    # Ensure bronze schema & table exist
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.ecommerce_raw (
            product_name TEXT,
            price NUMERIC,
            description TEXT,
            category TEXT,
            ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT
        );
    """)

    try:
        r = requests.get(BASE_URL, headers=HEADERS, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"Homepage error: {e}")
        return

    soup = BeautifulSoup(r.text, "html.parser")

    products_count = 0

    for p in soup.select(".thumbnail"):
        try:
            product_name = p.select_one(".title").text.strip()
            price = float(p.select_one(".price").text.replace("$", ""))
            description = p.select_one(".description").text.strip()
            category = p.select_one(".caption a")["href"].split("/")[-1]

            cur.execute("""
                INSERT INTO bronze.ecommerce_raw (
                    product_name, price, description, category, source
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                product_name,
                price,
                description,
                category,
                "webscraper.io"
            ))

            products_count += 1

        except Exception as e:
            logger.warning(f"Product parse error: {e}")

        time.sleep(0.2)

    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"{products_count} products ingested into bronze.ecommerce_raw")
    logger.info("END Bronze scraping – E-commerce")
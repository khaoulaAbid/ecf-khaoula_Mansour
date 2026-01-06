import requests
import psycopg2
from bs4 import BeautifulSoup


# ===============================================================================
# DDL Script: Create Bronze quotes Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for quotes api scrapping 
# ===============================================================================



conn = psycopg2.connect(
    host="db",
    database="supershop",
    user="admin",
    password="admin"
)

def scrape_quotes():
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS bronze.quotes_raw (
            quote TEXT,
            author TEXT
        )
    """)

    response = requests.get(
        "https://quotes.toscrape.com",
        headers={"User-Agent": "DataPulse-ECF"}
    )

    soup = BeautifulSoup(response.text, "html.parser")
    quotes = soup.select(".quote")

    for q in quotes:
        text = q.select_one(".text").text
        author = q.select_one(".author").text

        cur.execute(
            "INSERT INTO bronze.quotes_raw VALUES (%s, %s)",
            (text, author)
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    scrape_quotes()

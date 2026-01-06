import requests
import time
import psycopg2
from bs4 import BeautifulSoup

# ===============================================================================
# DDL Script: Create Bronze books Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for books api scrapping 
# ===============================================================================



DB_CONN = psycopg2.connect(
    host="db",
    database="supershop",
    user="admin",
    password="admin"
)

BASE_URL = "https://books.toscrape.com/"

def scrape_books():
    cursor = DB_CONN.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bronze.books_raw (
            title TEXT,
            price NUMERIC,
            rating INTEGER,
            category TEXT
        )
    """)

    for page in range(1, 3):
        response = requests.get(BASE_URL.format(page), headers={
            "User-Agent": "DataPulse-ECF-Scraper"
        })

        soup = BeautifulSoup(response.text, "html.parser")
        books = soup.select(".product_pod")

        for book in books:
            title = book.h3.a["title"]
            price = float(book.select_one(".price_color").text[1:])
            rating = len(book.select_one(".star-rating")["class"]) - 1

            cursor.execute(
                "INSERT INTO bronze.books_raw VALUES (%s, %s, %s, %s)",
                (title, price, rating, "Books")
            )

        DB_CONN.commit()
        time.sleep(1)

    cursor.close()
    DB_CONN.close()

if __name__ == "__main__":
    scrape_books()

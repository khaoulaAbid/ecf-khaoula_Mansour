import os

os.system("python src/ingestion/scrape_books.py")
os.system("python src/ingestion/scrape_quotes.py")
os.system("python src/ingestion/import_excel.py")
os.system("python src/ingestion/scrape_api_geo.py")

os.system("python src/transformation/bronze_to_silver.py")
os.system("python src/transformation/silver_to_gold.py")

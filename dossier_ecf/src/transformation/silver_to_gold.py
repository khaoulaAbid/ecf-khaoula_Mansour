# src/transformation/silver_to_gold.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.logger import get_logger


# ===============================================================================
# Script Purpose:
#     This script creates views for the Gold layer in the data warehouse. 
#     The Gold layer represents the final dimension and fact tables 

#     Each view performs transformations and combines data from the Silver layer 
#     to produce a clean, enriched, and business-ready dataset.

# Usage:
#     - These views can be queried directly for analytics and reporting.
# ===============================================================================

logger = get_logger("silver_to_gold")

DB_URI = "postgresql+psycopg2://admin:admin@db:5432/datapulse"

# ==============================================================================
# DIMENSIONS
# ==============================================================================
def create_dim_books(engine):
    logger.info("Creating gold.dim_books")

    df = pd.read_sql(
        "SELECT DISTINCT title, category, price FROM silver.books",
        engine
    )

    if df.empty:
        logger.warning("silver.books is empty – skipping dim_books")
        return

    df = df.reset_index(drop=True)
    df["book_key"] = df.index + 1

    df = df[["book_key", "title", "category", "price"]]

    df.to_sql("dim_books", engine, schema="gold", if_exists="replace", index=False)


def create_dim_authors(engine):
    logger.info("Creating gold.dim_authors")

    df = pd.read_sql(
        "SELECT DISTINCT author FROM silver.quotes WHERE author IS NOT NULL",
        engine
    )

    if df.empty:
        logger.warning("silver.quotes is empty – skipping dim_authors")
        return

    df = df.reset_index(drop=True)
    df["author_key"] = df.index + 1
    df.rename(columns={"author": "author_name"}, inplace=True)

    df = df[["author_key", "author_name"]]

    df.to_sql("dim_authors", engine, schema="gold", if_exists="replace", index=False)


def create_dim_geo(engine):
    logger.info("Creating gold.dim_geo")

    try:
        df = pd.read_sql("""
            SELECT DISTINCT
                city,
                postal_code
            FROM silver.librairies_clean
            WHERE city IS NOT NULL
        """, engine)
    except Exception:
        logger.warning("silver.librairies_clean missing – skipping dim_geo")
        return

def create_dim_products(engine):
    logger.info("Creating gold.dim_products")

    df = pd.read_sql("""
        SELECT DISTINCT
            product_name,
            category,
            price
        FROM silver.products_clean
    """, engine)

    if df.empty:
        logger.warning("silver.products_clean is empty – skipping dim_products")
        return

    df = df.reset_index(drop=True)
    df["product_key"] = df.index + 1

    df = df[["product_key", "product_name", "category", "price"]]

    df.to_sql("dim_products", engine, schema="gold", if_exists="replace", index=False)


# ==============================================================================
# FACT TABLES
# ==============================================================================
def create_fact_sales_books(engine):
    logger.info("Creating gold.fact_sales_books")

    try:
        books = pd.read_sql("SELECT book_key, price FROM gold.dim_books", engine)
        authors = pd.read_sql("SELECT author_key FROM gold.dim_authors", engine)
    except Exception:
        logger.warning("Missing book/author dimensions – skipping fact_sales_books")
        return

    if books.empty or authors.empty:
        logger.warning("Empty dimensions – skipping fact_sales_books")
        return

    # 👉 GEO OPTIONNEL
    try:
        geo = pd.read_sql("SELECT geo_key FROM gold.dim_geo", engine)
        fact = books.merge(authors, how="cross").merge(geo, how="cross")
    except Exception:
        logger.warning("No geo dimension – building fact without geo")
        fact = books.merge(authors, how="cross")
        fact["geo_key"] = None

    fact = fact.head(100)
    fact["sales_date"] = pd.Timestamp.today().normalize()
    fact["quantity"] = 1
    fact["sales_amount"] = fact["quantity"] * fact["price"]

    fact.to_sql(
        "fact_sales_books",
        engine,
        schema="gold",
        if_exists="replace",
        index=False
    )

    logger.info("gold.fact_sales_books created")


def create_fact_sales_products(engine):
    logger.info("Creating gold.fact_sales_products")

    products = pd.read_sql(
        "SELECT product_key, price FROM gold.dim_products",
        engine
    )

    if products.empty:
        logger.warning("gold.dim_products is empty – skipping fact_sales_products")
        return

    fact = products.copy()
    fact["sales_date"] = pd.Timestamp.today().normalize()
    fact["quantity"] = 2
    fact["sales_amount"] = fact["quantity"] * fact["price"]

    fact = fact[[
        "product_key",
        "sales_date",
        "quantity",
        "price",
        "sales_amount"
    ]]

    fact.to_sql(
        "fact_sales_products",
        engine,
        schema="gold",
        if_exists="replace",
        index=False
    )


# ==============================================================================
# RUN
# ==============================================================================
def run():
    logger.info("START Silver → Gold")

    engine = create_engine(DB_URI)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))

    create_dim_books(engine)
    create_dim_authors(engine)
    create_dim_geo(engine)
    create_dim_products(engine)

    create_fact_sales_books(engine)
    create_fact_sales_products(engine)

    logger.info("END Silver → Gold")
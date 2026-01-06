# src/transformation/bronze_to_silver.py

import pandas as pd
from sqlalchemy import create_engine, text
from utils.logger import get_logger

# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'silver' schema
# 	  Run this script to re-define the DDL structure of 'silver' Tables
# ===============================================================================
# Transformations réalisées
# ✔ Nettoyage des valeurs nulles
# ✔ Normalisation (dates, devises)
# ✔ Déduplication
# ✔ Enrichissement géographique
# ✔ RGPD : Email & téléphone supprimés 
# ===============================================================================



logger = get_logger("bronze_to_silver")

DB_URI = "postgresql+psycopg2://admin:admin@db:5432/datapulse"


# ==============================================================================
# BOOKS
# ==============================================================================
def clean_books(engine):
    logger.info("Cleaning books data")

    df = pd.read_sql("SELECT * FROM bronze.books_raw", engine)

    df.drop_duplicates(subset=["title"], inplace=True)
    df.dropna(subset=["title", "price"], inplace=True)

    # Nettoyage devises
    df["price"] = (
        df["price"]
        .astype(str)
        .str.replace("£", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("€", "", regex=False)
    )

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df.dropna(subset=["price"], inplace=True)

    df["title"] = df["title"].str.strip()
    df["category"] = df["category"].fillna("Unknown")

    df.to_sql(
        "books",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )

    logger.info("silver.books created")


# ==============================================================================
# QUOTES
# ==============================================================================
def clean_quotes(engine):
    logger.info("Cleaning quotes data")

    df = pd.read_sql("SELECT * FROM bronze.quotes_raw", engine)

    df.drop_duplicates(subset=["quote"], inplace=True)
    df["author"] = df["author"].str.strip()

    df.to_sql(
        "quotes",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )

    logger.info("silver.quotes created")


# ==============================================================================
# LIBRAIRIES (RGPD)
# ==============================================================================
def clean_librairies(engine):
    logger.info("Cleaning librairies data with RGPD")

    df = pd.read_sql("SELECT * FROM bronze.librairies_raw", engine)

    # Sécurisation colonnes RGPD
    for col in ["contact_email", "contact_telephone"]:
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

    if "contact_nom" in df.columns:
        df["contact_nom"] = df["contact_nom"].apply(
            lambda x: f"user_{abs(hash(str(x))) % 10000}"
        )

    if "date_partenariat" in df.columns:
        df["date_partenariat"] = pd.to_datetime(df["date_partenariat"], errors="coerce")

    df.to_sql(
        "librairies_clean",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )

    logger.info("silver.librairies_clean created")


# ==============================================================================
# GEO ENRICHMENT
# ==============================================================================
def enrich_geo(engine):
    logger.info("Enriching librairies with geocoding")

    geo = pd.read_sql("SELECT * FROM bronze.geocoding_raw", engine)
    libs = pd.read_sql("SELECT * FROM silver.librairies_clean", engine)

    df = libs.merge(
        geo,
        left_on="adresse",
        right_on="address",
        how="left"
    )

    df.to_sql(
        "librairies_geo",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )

    logger.info("silver.librairies_geo created")


# ==============================================================================
# E-COMMERCE
# ==============================================================================
def clean_ecommerce(engine):
    logger.info("Cleaning ecommerce data")

    df = pd.read_sql("SELECT * FROM bronze.ecommerce_raw", engine)

    df.dropna(subset=["product_name", "price"], inplace=True)
    df.drop_duplicates(
        subset=["product_name", "category", "price"],
        inplace=True
    )

    df["product_name"] = df["product_name"].str.strip()
    df["category"] = df["category"].str.lower()

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df.dropna(subset=["price"], inplace=True)

    df.to_sql(
        "products_clean",
        engine,
        schema="silver",
        if_exists="replace",
        index=False
    )

    logger.info("silver.products_clean created")


# ==============================================================================
# RUN
# ==============================================================================
def run():
    logger.info("START Bronze → Silver")

    engine = create_engine(DB_URI)

    # 🔑 CRÉATION DU SCHÉMA SILVER (OBLIGATOIRE)
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver"))

    clean_books(engine)
    clean_quotes(engine)
    clean_librairies(engine)
    enrich_geo(engine)
    clean_ecommerce(engine)

    logger.info("END Bronze → Silver")
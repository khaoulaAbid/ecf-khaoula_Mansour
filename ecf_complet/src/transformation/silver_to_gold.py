
import psycopg2

===============================================================================
##DDL Script: Transformations réalisées : Create Bronze Tables
===============================================================================
# Script Purpose:
#     This script creates views for the Gold layer in the data warehouse. 
#     The Gold layer represents the final dimension and fact tables (Star Schema)

#     Each view performs transformations and combines data from the Silver layer 
#     to produce a clean, enriched, and business-ready dataset.

# Usage:
#     - These views can be queried directly for analytics and reporting.
===============================================================================
# Transformations réalisées
# ✔ Nettoyage des valeurs nulles
# ✔ Normalisation (dates, devises)
# ✔ Déduplication
# ✔ Enrichissement géographique
# ✔ RGPD : ❌ Email & téléphone supprimés 
===============================================================================




conn = psycopg2.connect(
    host="db",
    database="supershop",
    user="admin",
    password="admin"
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE gold.dim_books AS
    SELECT DISTINCT
        ROW_NUMBER() OVER () AS book_key,
        title,
        price
    FROM silver.books
""")

cur.execute("""
    CREATE TABLE gold.fact_sales AS
    SELECT
        book_key,
        1 AS quantity,
        price,
        price AS sales_amount
    FROM gold.dim_books
""")

conn.commit()
conn.close()

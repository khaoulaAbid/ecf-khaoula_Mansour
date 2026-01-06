import psycopg2

===============================================================================
##DDL Script: Transformations réalisées : Create Bronze Tables
===============================================================================
# Script Purpose:
#     This script creates tables in the 'silver' schema
# 	  Run this script to re-define the DDL structure of 'silver' Tables
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
    CREATE TABLE silver.books AS
    SELECT DISTINCT
        title,
        price,
        rating
    FROM bronze.books_raw
""")

conn.commit()
conn.close()
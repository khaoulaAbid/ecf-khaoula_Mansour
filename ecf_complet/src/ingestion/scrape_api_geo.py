import requests
import psycopg2


# ===============================================================================
# DDL Script: Create Bronze geocoding Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for geocoding - api scrapping 
# ===============================================================================

conn = psycopg2.connect(
    host="db",
    database="supershop",
    user="admin",
    password="admin"
)

cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS bronze.geocoding_raw (
        address TEXT,
        city TEXT,
        lat NUMERIC,
        lon NUMERIC
    )
""")

addresses = ["20 avenue de Ségur Paris"]

for addr in addresses:
    r = requests.get(
        "https://api-adresse.data.gouv.fr/search/",
        params={"q": addr, "limit": 50}
    )

    if r.json()["features"]:
        geo = r.json()["features"][0]
        cur.execute(
            "INSERT INTO bronze.geocoding_raw VALUES (%s, %s, %s, %s)",
            (
                addr,
                geo["properties"]["city"],
                geo["geometry"]["coordinates"][1],
                geo["geometry"]["coordinates"][0]
            )
        )

conn.commit()
conn.close()
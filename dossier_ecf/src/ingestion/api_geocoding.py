import json
import time
import requests
import psycopg2
from utils.logger import get_logger
from utils.minio_client import get_minio_client
import io

# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for geocoding - api scrapping 
# ===============================================================================

logger = get_logger("bronze.api_geocoding")

BUCKET = "bronze"
API_URL = "https://api-adresse.data.gouv.fr/search/"

conn = psycopg2.connect(
    host="db",
    database="datapulse",
    user="admin",
    password="admin"
)


def run():
    logger.info("START Bronze geocoding → PostgreSQL & MinIO")

    cur = conn.cursor()

    logger.info("Create Bronze schema & table if not exists")

    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.geocoding_raw (
            address TEXT,
            city TEXT,
            postal_code TEXT,
            latitude NUMERIC,
            longitude NUMERIC,
            ingestion_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    addresses = [
        "15 rue des Francs Bourgeois Paris",
        "10 rue de Rivoli Paris"
    ]

    results = []

    for addr in addresses:
        try:
            logger.info(f"Geocoding address: {addr}")

            r = requests.get(
                API_URL,
                params={"q": addr, "limit": 1},
                timeout=10
            )
            r.raise_for_status()

            data = r.json()

            if not data.get("features"):
                logger.warning(f"No result for address: {addr}")
                continue

            f = data["features"][0]

            record = {
                "address": addr,
                "city": f["properties"].get("city"),
                "postal_code": f["properties"].get("postcode"),
                "latitude": f["geometry"]["coordinates"][1],
                "longitude": f["geometry"]["coordinates"][0]
            }

            results.append(record)

            cur.execute("""
                INSERT INTO bronze.geocoding_raw (
                    address, city, postal_code, latitude, longitude
                )
                VALUES (%s, %s, %s, %s, %s)
            """, (
                record["address"],
                record["city"],
                record["postal_code"],
                record["latitude"],
                record["longitude"]
            ))

        except Exception as e:
            logger.error(f"Error while geocoding '{addr}': {e}")

        time.sleep(0.05)

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Uploading geocoding raw data to MinIO")

    minio_client = get_minio_client()
    

    data_bytes = json.dumps(results, indent=2).encode("utf-8")
    data_stream = io.BytesIO(data_bytes)

    minio_client.put_object(
        bucket_name=BUCKET,
        object_name="geocoding/geocoding_raw.json",
        data=data_stream,
        length=len(data_bytes),
        content_type="application/json"
    )
    logger.info("SUCCESS Bronze geocoding ingestion completed")
import json
import pandas as pd
from sqlalchemy import create_engine, text
from utils.logger import get_logger
from utils.minio_client import get_minio_client



# ===============================================================================
# DDL Script: Create Bronze partenaire_librairies Table
# ===============================================================================
# Script Purpose:
#     This script creates tables in the 'bronze' schema for partenaire_librairies excel

#nom_librairie
#adresse
#code_postal
#ville
#contact_nom
#contact_email
#contact_telephone
#ca_annuel
#date_partenariat
#specialite


#NB : pandas.to_sql() ne fonctionne PAS avec psycopg directement , il faut SQLAlchemy

# ===============================================================================




logger = get_logger("bronze.import_excel")
DB_URI = "postgresql+psycopg2://admin:admin@db:5432/datapulse"
engine = create_engine(DB_URI)

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
# BUCKET = "bronze"
EXCEL_PATH = "data/partenaire_librairies.xlsx"


def run():
    logger.info("START Bronze Excel → PostgreSQL & MinIO")

    # Load Excel file
    df = pd.read_excel(EXCEL_PATH)
    logger.info(f"{len(df)} rows loaded from Excel")

    # Add Bronze metadata
    df["ingestion_date"] = pd.Timestamp.utcnow()
    df["source"] = "excel_partenaire"

    # Ensure bronze schema exists
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze;"))

    logger.info("Insert data into bronze.librairies_raw")

    df.to_sql(
        name="librairies_raw",
        con=engine,
        schema="bronze",
        if_exists="append",
        index=False
    )

    # Store raw data in MinIO
    logger.info("Uploading raw Excel data to MinIO")

    # minio = get_minio_client()

    # if not minio.bucket_exists(BUCKET):
    #     minio.make_bucket(BUCKET)

    # data_bytes = df.to_json(orient="records", indent=2).encode("utf-8")

    # minio.put_object(
    #     BUCKET,
    #     "librairies/librairies_raw.json",
    #     data=data_bytes,
    #     length=len(data_bytes),
    #     content_type="application/json"
    # )

    logger.info("SUCCESS Bronze Excel ingestion completed")
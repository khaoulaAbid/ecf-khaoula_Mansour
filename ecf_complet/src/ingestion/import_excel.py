import pandas as pd
import psycopg

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

# ===============================================================================

conn = psycopg.connect(
    host="db",
    database="supershop",
    user="admin",
    password="admin"
)

df = pd.read_excel("data/partenaire_librairies.xlsx")

# RGPD : suppression email / téléphone
df.drop(columns=["contact_email", "contact_telephone"], inplace=True)

df.to_sql(
    "librairies_raw",
    con=conn,
    schema="bronze",
    if_exists="replace",
    index=False
)

conn.close()

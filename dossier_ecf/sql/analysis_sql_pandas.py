# -- =============================================================================
# -- analyses_sql_pandas.sql
# -- Source officielle : PostgreSQL (Gold layer)
# -- Visualisation : pandas + matplotlib
# -- =============================================================================


import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# ============================
# Connexion PostgreSQL (Docker)
# ============================
DB_URI = "postgresql+psycopg2://admin:admin@db:5432/datapulse"

engine = create_engine(DB_URI)

# ============================
# SQL – Chiffre d’affaires total (Livres)
# ============================
SQL_CA_TOTAL_BOOKS = """
SELECT
    SUM(sales_amount) AS chiffre_affaires_total
FROM gold.fact_sales_books;
"""

df_ca_books = pd.read_sql(SQL_CA_TOTAL_BOOKS, engine)
print("Chiffre d’affaires total (Livres) :")
print(df_ca_books)

# ============================
# SQL – Top 10 livres par CA
# ============================
SQL_TOP_BOOKS = """
SELECT
    b.title,
    SUM(f.quantity) AS total_quantite,
    SUM(f.sales_amount) AS total_ventes
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
GROUP BY b.title
ORDER BY total_ventes DESC
LIMIT 10;
"""

df_books = pd.read_sql(SQL_TOP_BOOKS, engine)

# ============================
# Visualisation – Top 10 livres
# ============================
plt.figure(figsize=(10, 5))
plt.bar(df_books["title"], df_books["total_ventes"])
plt.xticks(rotation=45, ha="right")
plt.title("Top 10 livres par chiffre d’affaires")
plt.ylabel("Chiffre d’affaires (€)")
plt.tight_layout()
plt.show()

# ============================
# SQL – Classement des livres (Window Function)
# ============================
SQL_RANKING_BOOKS = """
SELECT
    b.title,
    SUM(f.sales_amount) AS total_ventes,
    RANK() OVER (
        ORDER BY SUM(f.sales_amount) DESC
    ) AS classement
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
GROUP BY b.title;
"""

df_rank = pd.read_sql(SQL_RANKING_BOOKS, engine)
print("\n Classement des livres :")
print(df_rank.head(10))

# ============================
# SQL – Chiffre d’affaires par catégorie
# ============================
SQL_CA_CATEGORY = """
SELECT
    b.category,
    SUM(f.sales_amount) AS chiffre_affaires
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
GROUP BY b.category
ORDER BY chiffre_affaires DESC;
"""

df_category = pd.read_sql(SQL_CA_CATEGORY, engine)

# ============================
# Visualisation – CA par catégorie
# ============================
plt.figure(figsize=(8, 5))
plt.bar(df_category["category"], df_category["chiffre_affaires"])
plt.title("Chiffre d’affaires par catégorie de livres")
plt.ylabel("CA (€)")
plt.tight_layout()
plt.show()

# ============================
# SQL – CA E-commerce (2e source de données)
# ============================
SQL_CA_PRODUCTS = """
SELECT
    p.product_name,
    SUM(f.sales_amount) AS chiffre_affaires
FROM gold.fact_sales_products f
JOIN gold.dim_products p
    ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY chiffre_affaires DESC
LIMIT 10;
"""

df_products = pd.read_sql(SQL_CA_PRODUCTS, engine)
print("\n Top produits e-commerce :")
print(df_products)

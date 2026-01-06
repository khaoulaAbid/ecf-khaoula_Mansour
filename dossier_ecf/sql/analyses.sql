-- =============================================================================
-- analyses.sql
-- Objectif : démontrer la valeur analytique de la plateforme DataPulse
-- Couche : GOLD (Data Mart)
-- =============================================================================



-- ============================================================================
-- 1. Requête avec jointure
--    → Ventes par livre et par auteur
-- ============================================================================
SELECT
    b.title AS book_title,
    a.author_name,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.sales_amount) AS total_sales_amount
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
JOIN gold.dim_authors a
    ON f.author_key = a.author_key
GROUP BY b.title, a.author_name
ORDER BY total_sales_amount DESC;


-- ============================================================================
-- 2. Requête avec fonction de fenêtrage (Window Function)
--    → Classement global des livres par chiffre d'affaires
-- ============================================================================
SELECT
    b.title,
    SUM(f.sales_amount) AS total_sales,
    RANK() OVER (
        ORDER BY SUM(f.sales_amount) DESC
    ) AS sales_rank
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
GROUP BY b.title;

-- ============================================================================
-- 3. Requête de classement (Top N)
--    → Top 5 des livres les plus vendus
-- ============================================================================
SELECT
    b.title,
    SUM(f.sales_amount) AS total_sales_amount
FROM gold.fact_sales_books f
JOIN gold.dim_books b
    ON f.book_key = b.book_key
GROUP BY b.title
ORDER BY total_sales_amount DESC
LIMIT 5;


-- ============================================================================
-- 4. Requête croisant au moins 2 sources de données
--    → Comparaison ventes Livres vs E-commerce
-- ============================================================================
SELECT
    'Books' AS source,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales_books

UNION ALL

SELECT
    'E-commerce' AS source,
    SUM(sales_amount) AS total_sales
FROM gold.fact_sales_products;

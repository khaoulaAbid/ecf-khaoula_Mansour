
-- 1. Agrégation
SELECT category, SUM(sales_amount)
FROM gold.fact_sales fs
JOIN gold.dim_books b USING(book_key)
GROUP BY category;

-- 2. Jointure
SELECT author_name, COUNT(*)
FROM gold.fact_sales fs
JOIN gold.dim_authors a USING(author_key)
GROUP BY author_name;

-- 3. Window function
SELECT category,
       SUM(sales_amount) OVER (PARTITION BY category)
FROM gold.fact_sales fs
JOIN gold.dim_books b USING(book_key);

-- 4. Top N
SELECT title, SUM(sales_amount)
FROM gold.fact_sales fs
JOIN gold.dim_books b USING(book_key)
GROUP BY title
ORDER BY SUM(sales_amount) DESC
LIMIT 5;

-- 5. Analyse croisée
SELECT city, category, SUM(sales_amount)
FROM gold.fact_sales fs
JOIN gold.dim_geo g USING(geo_key)
JOIN gold.dim_books b USING(book_key)
GROUP BY city, category;

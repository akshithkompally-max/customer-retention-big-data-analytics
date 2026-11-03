-- =====================================================================
-- File: 02_load_and_clean.sql
-- STEP A  Load the raw CSV into the staging table (choose ONE method)
--   Method 1 (psql / server can read the file):
--     \copy retail.stg_online_retail FROM 'data/online_retail_raw.csv' CSV HEADER
--   Method 2 (DBeaver): right-click stg_online_retail -> Import Data -> CSV
--     -> pick data/online_retail_raw.csv -> map columns by name -> Proceed
-- Expected: 541,909 rows
-- =====================================================================
SET search_path TO retail;

SELECT COUNT(*) AS staged_rows FROM stg_online_retail;      -- expect 541909

-- STEP B  Basic validation / data-quality checks
SELECT
  COUNT(*)                                          AS total_rows,
  COUNT(*) FILTER (WHERE customer_id IS NULL)       AS missing_customer,
  COUNT(*) FILTER (WHERE description IS NULL)       AS missing_description,
  COUNT(*) FILTER (WHERE invoice_no LIKE 'C%')      AS cancellation_lines,
  COUNT(*) FILTER (WHERE quantity <= 0)             AS non_positive_qty,
  COUNT(*) FILTER (WHERE unit_price <= 0)           AS non_positive_price,
  COUNT(DISTINCT invoice_no)                        AS invoices,
  COUNT(DISTINCT customer_id)                       AS customers,
  COUNT(DISTINCT country)                           AS countries,
  MIN(invoice_date) AS first_date, MAX(invoice_date) AS last_date
FROM stg_online_retail;

-- exact duplicate rows
SELECT COUNT(*) - (SELECT COUNT(*) FROM (SELECT DISTINCT * FROM stg_online_retail) d) AS exact_duplicates
FROM stg_online_retail;

-- STEP C  De-duplicated, typed working set
CREATE TEMP TABLE t_dedup AS SELECT DISTINCT * FROM stg_online_retail;

-- STEP D  Populate dimensions and facts
INSERT INTO dim_customer (customer_id, country)
SELECT customer_id::INT, MODE() WITHIN GROUP (ORDER BY country)
FROM t_dedup WHERE customer_id IS NOT NULL
GROUP BY customer_id;

INSERT INTO dim_product (stock_code, description)
SELECT stock_code, MODE() WITHIN GROUP (ORDER BY TRIM(description))
FROM t_dedup
GROUP BY stock_code;

-- valid sales lines only: no cancellations, positive qty & price, real products, known customer
CREATE TEMP TABLE t_sales AS
SELECT * FROM t_dedup
WHERE invoice_no NOT LIKE 'C%'
  AND quantity > 0 AND unit_price > 0
  AND customer_id IS NOT NULL
  AND stock_code NOT IN ('POST','D','M','DOT','CRUK','BANK CHARGES','S','AMAZONFEE','B','PADS','C2');

INSERT INTO fact_invoice (invoice_no, customer_id, invoice_date)
SELECT invoice_no, MIN(customer_id)::INT, MIN(invoice_date)
FROM t_sales GROUP BY invoice_no;

INSERT INTO fact_invoice_line (invoice_no, stock_code, quantity, unit_price)
SELECT invoice_no, stock_code, quantity, unit_price FROM t_sales;

INSERT INTO fact_return_line (invoice_no, customer_id, stock_code, quantity, unit_price, invoice_date)
SELECT invoice_no, customer_id::INT, stock_code, quantity, unit_price, invoice_date
FROM t_dedup
WHERE invoice_no LIKE 'C%' AND customer_id IS NOT NULL
  AND stock_code NOT IN ('POST','D','M','DOT','CRUK','BANK CHARGES','S','AMAZONFEE','B','PADS','C2');

-- STEP E  Reconciliation (must match the Python notebook)
SELECT 'fact_invoice_line' AS tbl, COUNT(*) AS rows FROM fact_invoice_line   -- expect 391150
UNION ALL SELECT 'fact_invoice', COUNT(*) FROM fact_invoice                  -- expect 18402
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'fact_return_line', COUNT(*) FROM fact_return_line;

-- Convenience views
CREATE OR REPLACE VIEW v_sales AS
SELECT l.line_id, i.invoice_no, i.invoice_date, i.customer_id, c.country,
       l.stock_code, p.description, l.quantity, l.unit_price,
       ROUND(l.quantity * l.unit_price, 2) AS revenue
FROM fact_invoice_line l
JOIN fact_invoice  i ON i.invoice_no  = l.invoice_no
JOIN dim_customer  c ON c.customer_id = i.customer_id
JOIN dim_product   p ON p.stock_code  = l.stock_code;

-- =====================================================================
-- File: 03_analysis_queries.sql  -  filtering, sorting, aggregation, joins
-- =====================================================================
SET search_path TO retail;

-- Q1 FILTER + SORT: 10 largest single invoices
SELECT invoice_no, customer_id, invoice_date, ROUND(SUM(revenue),2) AS invoice_value
FROM v_sales GROUP BY invoice_no, customer_id, invoice_date
ORDER BY invoice_value DESC LIMIT 10;

-- Q2 AGGREGATION: headline KPIs
SELECT ROUND(SUM(revenue),2)                         AS total_revenue,
       COUNT(DISTINCT invoice_no)                    AS orders,
       COUNT(DISTINCT customer_id)                   AS customers,
       ROUND(SUM(revenue)/COUNT(DISTINCT invoice_no),2) AS avg_order_value
FROM v_sales;

-- Q3 AGGREGATION: monthly revenue, orders, active customers
SELECT DATE_TRUNC('month', invoice_date)::date AS month,
       ROUND(SUM(revenue),0) AS revenue,
       COUNT(DISTINCT invoice_no) AS orders,
       COUNT(DISTINCT customer_id) AS active_customers
FROM v_sales GROUP BY 1 ORDER BY 1;

-- Q4 AGGREGATION + SORT: revenue by country with share
SELECT country, ROUND(SUM(revenue),0) AS revenue,
       ROUND(100*SUM(revenue)/SUM(SUM(revenue)) OVER (),1) AS pct_of_total
FROM v_sales GROUP BY country ORDER BY revenue DESC LIMIT 10;

-- Q5 JOIN: top 10 products by revenue
SELECT p.stock_code, p.description, SUM(l.quantity) AS units,
       ROUND(SUM(l.quantity*l.unit_price),0) AS revenue
FROM fact_invoice_line l JOIN dim_product p USING (stock_code)
GROUP BY p.stock_code, p.description ORDER BY revenue DESC LIMIT 10;

-- Q6 JOIN: customer with returns value (sales vs returns)
SELECT s.customer_id, ROUND(s.sales,0) AS sales, ROUND(COALESCE(r.returns,0),0) AS returns_value,
       ROUND(100*COALESCE(r.returns,0)/s.sales,1) AS return_pct
FROM (SELECT customer_id, SUM(revenue) AS sales FROM v_sales GROUP BY 1) s
LEFT JOIN (SELECT customer_id, -SUM(quantity*unit_price) AS returns FROM fact_return_line GROUP BY 1) r
       USING (customer_id)
ORDER BY sales DESC LIMIT 15;

-- Q7 PARETO: share of revenue from top 20% customers
WITH c AS (
  SELECT customer_id, SUM(revenue) AS rev,
         NTILE(5) OVER (ORDER BY SUM(revenue) DESC) AS quintile
  FROM v_sales GROUP BY customer_id)
SELECT quintile, COUNT(*) AS customers, ROUND(SUM(rev),0) AS revenue,
       ROUND(100*SUM(rev)/SUM(SUM(rev)) OVER (),1) AS pct_revenue
FROM c GROUP BY quintile ORDER BY quintile;

-- Q8 REPEAT vs ONE-TIME customers
SELECT CASE WHEN orders = 1 THEN 'One-time' ELSE 'Repeat' END AS type,
       COUNT(*) AS customers, ROUND(SUM(rev),0) AS revenue
FROM (SELECT customer_id, COUNT(DISTINCT invoice_no) AS orders, SUM(revenue) AS rev
      FROM v_sales GROUP BY 1) t GROUP BY 1;

-- Q9 COHORT: first-purchase month and customers active in month 0..3
WITH first_m AS (
  SELECT customer_id, DATE_TRUNC('month', MIN(invoice_date)) AS cohort FROM fact_invoice GROUP BY 1),
act AS (
  SELECT DISTINCT i.customer_id, f.cohort,
         (EXTRACT(YEAR FROM AGE(DATE_TRUNC('month', i.invoice_date), f.cohort))*12 +
          EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', i.invoice_date), f.cohort)))::int AS m
  FROM fact_invoice i JOIN first_m f USING (customer_id))
SELECT cohort::date, COUNT(*) FILTER (WHERE m=0) AS m0,
       ROUND(100.0*COUNT(*) FILTER (WHERE m=1)/COUNT(*) FILTER (WHERE m=0),1) AS m1_pct,
       ROUND(100.0*COUNT(*) FILTER (WHERE m=3)/COUNT(*) FILTER (WHERE m=0),1) AS m3_pct
FROM act GROUP BY cohort ORDER BY cohort;

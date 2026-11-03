-- =====================================================================
-- File: 04_rfm_segmentation.sql - RFM scoring and customer segments in SQL
-- Snapshot date = day after last invoice (2011-12-10)
-- =====================================================================
SET search_path TO retail;

DROP TABLE IF EXISTS customer_rfm;
CREATE TABLE customer_rfm AS
WITH base AS (
  SELECT customer_id,
         DATE '2011-12-10' - MAX(invoice_date)::date AS recency,
         COUNT(DISTINCT invoice_no)                  AS frequency,
         ROUND(SUM(revenue),2)                       AS monetary
  FROM v_sales GROUP BY customer_id),
scored AS (
  SELECT *,
    NTILE(5) OVER (ORDER BY recency DESC)  AS r,   -- 5 = most recent
    NTILE(5) OVER (ORDER BY frequency)     AS f,
    NTILE(5) OVER (ORDER BY monetary)      AS m
  FROM base)
SELECT *, (f + m) / 2.0 AS fm,
  CASE
    WHEN r >= 4 AND (f+m)/2.0 >= 4.5 THEN 'Champions'
    WHEN r >= 3 AND (f+m)/2.0 >= 3.5 THEN 'Loyal Customers'
    WHEN r >= 4 AND (f+m)/2.0 >= 2.5 THEN 'Potential Loyalists'
    WHEN r >= 4                      THEN 'New / Promising'
    WHEN r >= 3                      THEN 'Need Attention'
    WHEN r <= 2 AND (f+m)/2.0 >= 4   THEN 'Cant Lose Them'
    WHEN r <= 2 AND (f+m)/2.0 >= 2.5 THEN 'At Risk'
    ELSE 'Hibernating / Lost'
  END AS segment
FROM scored;

ALTER TABLE customer_rfm ADD PRIMARY KEY (customer_id);

-- Segment summary (compare with Python segment_summary.csv)
SELECT segment, COUNT(*) AS customers,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (),1) AS pct_customers,
       ROUND(SUM(monetary),0) AS revenue,
       ROUND(100.0*SUM(monetary)/SUM(SUM(monetary)) OVER (),1) AS pct_revenue,
       ROUND(AVG(recency),0) AS avg_recency_days, ROUND(AVG(frequency),1) AS avg_orders,
       ROUND(AVG(monetary),0) AS avg_spend
FROM customer_rfm GROUP BY segment ORDER BY revenue DESC;

-- Top 20 customers
SELECT c.customer_id, d.country, c.recency, c.frequency, c.monetary, c.segment
FROM customer_rfm c JOIN dim_customer d USING (customer_id)
ORDER BY monetary DESC LIMIT 20;

-- Retention worklist: valuable customers who have gone quiet
SELECT customer_id, recency, frequency, monetary, segment
FROM customer_rfm
WHERE segment IN ('Cant Lose Them','At Risk') AND monetary >= 1000
ORDER BY monetary DESC;

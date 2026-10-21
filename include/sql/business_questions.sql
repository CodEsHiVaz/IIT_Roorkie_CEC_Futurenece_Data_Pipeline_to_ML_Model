-- include/sql/business_questions.sql
-- The six business questions, answered from the ELT marts / staging tables.
-- (ETL equivalents use etl_fact_orders / etl_* tables — same numbers.)

-- Q1. Daily net revenue by zone and store (delivered, after discount)
SELECT order_date, zone, store_id, store_name, net_revenue
FROM mart_daily_store_kpis
ORDER BY order_date, zone, store_id;

-- Q1b. Rolled up to zone
SELECT zone, ROUND(SUM(net_revenue),2) AS net_revenue
FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC;

-- Q2. Revenue and gross margin by product category
SELECT category, revenue, gross_margin FROM mart_category_margin ORDER BY revenue DESC;

-- Q3. Average delivery time per store and % of delivered orders within 15 minutes
SELECT store_id, store_name, avg_delivery_minutes, pct_within_15
FROM mart_delivery_sla ORDER BY avg_delivery_minutes;

-- Q4a. Revenue by payment mode (delivered orders)
SELECT payment_mode, ROUND(SUM(order_amount),2) AS revenue
FROM stg_orders WHERE dq_status='PASS' AND order_status='DELIVERED'
GROUP BY payment_mode ORDER BY revenue DESC;

-- Q4b. Total amount that does not reconcile with the payment gateway
SELECT reason, COUNT(*) AS n, ROUND(SUM(COALESCE(order_amount,amount_paid)),2) AS amount
FROM mart_payment_reconciliation GROUP BY reason ORDER BY amount DESC;

-- Q5. Top 10 customers by spend, with loyalty tier (no raw email/phone)
SELECT customer_id, full_name, loyalty_tier, email_masked, delivered_orders, total_spend
FROM mart_top_customers ORDER BY total_spend DESC;

-- Q6. Rows rejected today, by file and by reason (from the latest run's quality scorecard)
SELECT split_part(check_name,'_',1) AS file_group, check_name, dimension, severity, rows_failed
FROM dq_results
WHERE run_id = (SELECT max(run_id) FROM dq_results WHERE dag_id='nammamart_elt_shivaz')
  AND rows_failed > 0
ORDER BY rows_failed DESC;

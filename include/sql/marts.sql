-- include/sql/marts.sql
-- ELT mart layer: business-ready tables (SQL only) over the PASS rows in staging.
-- net revenue counts DELIVERED orders only.

-- Q1 + Q3: daily KPIs by store/zone. LEFT JOIN to the clean (PASS, deduped)
-- store dim keeps every delivered order (matching the ETL fact LEFT JOIN); an
-- order at a quarantined store just shows a null zone.
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
  o.order_date, o.store_id, s.store_name, s.zone,
  COUNT(*) AS delivered_orders,
  ROUND(SUM(o.order_amount),2) AS net_revenue,
  ROUND(AVG(o.delivery_minutes),2) AS avg_delivery_minutes,
  ROUND(100.0*AVG(CASE WHEN o.delivery_minutes<=15 THEN 1 ELSE 0 END),1) AS pct_within_15
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status='PASS') s ON o.store_id=s.store_id
WHERE o.dq_status='PASS' AND o.order_status='DELIVERED'
GROUP BY 1,2,3,4 ORDER BY 1,4,2;

-- Q2: revenue and gross margin by category
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
  p.category,
  ROUND(SUM(o.order_amount),2) AS revenue,
  ROUND(SUM(o.order_amount - o.quantity*p.cost_price),2) AS gross_margin
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_products WHERE dq_status='PASS') p ON o.product_id=p.product_id
WHERE o.dq_status='PASS' AND o.order_status='DELIVERED'
GROUP BY 1 ORDER BY revenue DESC;

-- Q3: delivery SLA per store
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
  o.store_id, s.store_name,
  ROUND(AVG(o.delivery_minutes),2) AS avg_delivery_minutes,
  ROUND(100.0*AVG(CASE WHEN o.delivery_minutes<=15 THEN 1 ELSE 0 END),1) AS pct_within_15
FROM stg_orders o
LEFT JOIN (SELECT * FROM stg_stores WHERE dq_status='PASS') s ON o.store_id=s.store_id
WHERE o.dq_status='PASS' AND o.order_status='DELIVERED'
GROUP BY 1,2 ORDER BY avg_delivery_minutes;

-- Q4: payment reconciliation (one row per mismatch)
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
SELECT * FROM (
  SELECT
    COALESCE(o.order_id,p.order_id) AS order_id,
    o.order_status, o.order_amount, p.amount_paid, p.payment_status,
    CASE
      WHEN o.order_id IS NULL THEN 'payment_without_order'
      WHEN o.order_status='DELIVERED' AND p.payment_status IS NULL THEN 'missing_payment_for_delivered'
      WHEN o.order_status='DELIVERED' AND p.payment_status<>'SUCCESS' THEN 'delivered_but_payment_'||lower(p.payment_status)
      WHEN o.order_status='DELIVERED' AND abs(COALESCE(p.amount_paid,0)-COALESCE(o.order_amount,0))>0.01 THEN 'amount_mismatch'
      WHEN o.order_status='CANCELLED' AND p.payment_status IS NOT NULL THEN 'payment_for_cancelled'
      ELSE NULL END AS reason
  FROM (SELECT * FROM stg_orders WHERE dq_status='PASS') o
  FULL OUTER JOIN (SELECT * FROM stg_payments WHERE dq_status='PASS') p
    ON o.order_id=p.order_id
) WHERE reason IS NOT NULL;

-- Q5: top 10 customers by spend (no raw email/phone)
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
  c.customer_id, c.full_name, c.loyalty_tier, c.email_masked,
  COUNT(*) AS delivered_orders,
  ROUND(SUM(o.order_amount),2) AS total_spend
FROM stg_orders o
JOIN stg_customers c ON o.customer_id=c.customer_id AND c.dq_status='PASS'
WHERE o.dq_status='PASS' AND o.order_status='DELIVERED'
GROUP BY 1,2,3,4 ORDER BY total_spend DESC LIMIT 10;

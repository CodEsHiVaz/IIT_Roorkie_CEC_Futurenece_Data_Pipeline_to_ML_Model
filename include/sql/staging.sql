-- include/sql/staging.sql
-- ELT staging layer: cast types, standardise, dedupe, mask PII, apply the same
-- critical quality rules as the ETL pipeline. dq_status = 'QUARANTINE' means the
-- row is excluded from the marts (but kept here for audit). Masters are built
-- first because orders reference their clean keys.
-- RUN_DATE for timeliness checks = 2026-10-01.

-- ── stg_customers ─────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_customers AS
WITH d AS (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY rowid) AS rn
  FROM raw_customers
)
SELECT
  customer_id, full_name,
  CASE WHEN email LIKE '%@%'
       THEN left(split_part(email,'@',1),2) || '***@' || split_part(email,'@',2) END AS email_masked,
  CASE WHEN length(regexp_replace(coalesce(phone,''),'[^0-9]','','g'))=10
       THEN left(regexp_replace(phone,'[^0-9]','','g'),2) || '******'
            || right(regexp_replace(phone,'[^0-9]','','g'),2) END AS phone_masked,
  city, area, pincode, signup_date,
  CASE upper(trim(loyalty_tier))
       WHEN 'BRONZE' THEN 'Bronze' WHEN 'SILVER' THEN 'Silver'
       WHEN 'GOLD' THEN 'Gold' WHEN 'PLATINUM' THEN 'Platinum'
       ELSE trim(loyalty_tier) END AS loyalty_tier,
  is_active,
  concat_ws(';',
    CASE WHEN rn>1 THEN 'customers_customer_id_unique' END,
    CASE WHEN TRY_CAST(signup_date AS DATE) > DATE '2026-10-01' THEN 'customers_signup_not_future' END
  ) AS failure_reason,
  CASE WHEN rn>1 OR TRY_CAST(signup_date AS DATE) > DATE '2026-10-01'
       THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status
FROM d;

-- ── stg_products ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_products AS
WITH d AS (
  SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY rowid) AS rn
  FROM raw_products
)
SELECT
  product_id, product_name, category, sub_category, brand,
  TRY_CAST(mrp AS DOUBLE) AS mrp, TRY_CAST(cost_price AS DOUBLE) AS cost_price,
  unit, is_perishable, launch_date,
  concat_ws(';',
    CASE WHEN rn>1 THEN 'products_product_id_unique' END,
    CASE WHEN category IS NULL THEN 'products_category_present' END,
    CASE WHEN NOT (TRY_CAST(mrp AS DOUBLE) > 0) THEN 'products_mrp_positive' END,
    CASE WHEN TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE) THEN 'products_cost_le_mrp' END
  ) AS failure_reason,
  CASE WHEN rn>1 OR category IS NULL OR NOT (TRY_CAST(mrp AS DOUBLE) > 0)
            OR TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE)
       THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status
FROM d;

-- ── stg_stores ────────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_stores AS
WITH d AS (
  SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY rowid) AS rn
  FROM raw_stores
)
SELECT
  store_id, store_name, zone, area, pincode, open_date, manager_name,
  TRY_CAST(capacity_orders_per_day AS INTEGER) AS capacity_orders_per_day,
  concat_ws(';',
    CASE WHEN rn>1 THEN 'stores_store_id_unique' END,
    CASE WHEN zone IS NULL THEN 'stores_zone_present' END
  ) AS failure_reason,
  CASE WHEN rn>1 OR zone IS NULL THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status
FROM d;

-- ── stg_orders (MERGE/upsert: increment wins, then first base row) ────────────
CREATE OR REPLACE TABLE stg_orders AS
WITH d AS (
  SELECT *,
    (_source_file LIKE '%increment%') AS is_inc,
    row_number() OVER (PARTITION BY order_id
                       ORDER BY (_source_file LIKE '%increment%') DESC, rowid) AS rn
  FROM raw_orders
),
one AS (SELECT * FROM d WHERE rn=1),
calc AS (
  SELECT *,
    upper(trim(order_status)) AS status_std,
    CASE lower(trim(payment_mode))
      WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
      WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
      ELSE trim(payment_mode) END AS paymode_std,
    TRY_CAST(order_ts AS TIMESTAMP) AS order_ts_t,
    TRY_CAST(quantity AS DOUBLE) AS qty_n,
    TRY_CAST(unit_price AS DOUBLE) AS price_n,
    TRY_CAST(discount_pct AS DOUBLE) AS disc_n,
    TRY_CAST(order_amount AS DOUBLE) AS amt_n,
    TRY_CAST(delivery_minutes AS DOUBLE) AS dmin_n
  FROM one
)
SELECT
  order_id, order_ts_t AS order_ts, CAST(order_ts_t AS DATE) AS order_date,
  customer_id, store_id, product_id,
  qty_n AS quantity, price_n AS unit_price, disc_n AS discount_pct, amt_n AS order_amount,
  paymode_std AS payment_mode, status_std AS order_status, channel, dmin_n AS delivery_minutes,
  concat_ws(';',
    CASE WHEN customer_id IS NULL THEN 'orders_customer_id_present' END,
    CASE WHEN status_std='DELIVERED' AND dmin_n IS NULL THEN 'orders_delivered_has_delivery_min' END,
    CASE WHEN order_ts IS NULL THEN 'orders_order_ts_present' END,
    CASE WHEN status_std NOT IN ('DELIVERED','CANCELLED','RETURNED') THEN 'orders_status_valid' END,
    CASE WHEN NOT (disc_n BETWEEN 0 AND 100) THEN 'orders_discount_pct_0_100' END,
    CASE WHEN order_ts IS NOT NULL AND order_ts_t IS NULL THEN 'orders_order_ts_parses' END,
    CASE WHEN NOT (qty_n > 0) THEN 'orders_quantity_positive' END,
    CASE WHEN abs(round(qty_n*price_n*(1-disc_n/100),2) - amt_n) > 0.01 THEN 'orders_amount_matches_formula' END,
    CASE WHEN dmin_n IS NOT NULL AND dmin_n > 120 THEN 'orders_delivery_min_believable' END,
    CASE WHEN customer_id IS NOT NULL AND customer_id NOT IN (SELECT customer_id FROM raw_customers) THEN 'orders_customer_exists' END,
    CASE WHEN product_id NOT IN (SELECT product_id FROM raw_products) THEN 'orders_product_exists' END,
    CASE WHEN store_id NOT IN (SELECT store_id FROM raw_stores) THEN 'orders_store_exists' END,
    CASE WHEN CAST(order_ts_t AS DATE) > DATE '2026-10-01' THEN 'orders_order_ts_not_future' END
  ) AS failure_reason,
  CASE WHEN
       customer_id IS NULL
    OR (status_std='DELIVERED' AND dmin_n IS NULL)
    OR order_ts IS NULL
    OR status_std NOT IN ('DELIVERED','CANCELLED','RETURNED')
    OR NOT (disc_n BETWEEN 0 AND 100)
    OR (order_ts IS NOT NULL AND order_ts_t IS NULL)
    OR NOT (qty_n > 0)
    OR abs(round(qty_n*price_n*(1-disc_n/100),2) - amt_n) > 0.01
    OR (dmin_n IS NOT NULL AND dmin_n > 120)
    OR (customer_id IS NOT NULL AND customer_id NOT IN (SELECT customer_id FROM raw_customers))
    OR product_id NOT IN (SELECT product_id FROM raw_products)
    OR store_id NOT IN (SELECT store_id FROM raw_stores)
    OR CAST(order_ts_t AS DATE) > DATE '2026-10-01'
       THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status
FROM calc;

-- ── stg_payments ──────────────────────────────────────────────────────────────
CREATE OR REPLACE TABLE stg_payments AS
WITH d AS (
  SELECT *,
    row_number() OVER (PARTITION BY payment_id ORDER BY rowid) AS rn_pid,
    row_number() OVER (PARTITION BY order_id  ORDER BY rowid) AS rn_oid
  FROM raw_payments
)
SELECT
  payment_id, order_id, payment_ts,
  CASE lower(trim(payment_mode))
    WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet'
    WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery'
    ELSE trim(payment_mode) END AS payment_mode,
  TRY_CAST(amount_paid AS DOUBLE) AS amount_paid, payment_status, gateway_ref,
  concat_ws(';',
    CASE WHEN rn_pid>1 THEN 'payments_payment_id_unique' END,
    CASE WHEN rn_oid>1 THEN 'payments_one_per_order' END,
    CASE WHEN order_id NOT IN (SELECT order_id FROM raw_orders) THEN 'payments_order_exists' END
  ) AS failure_reason,
  CASE WHEN rn_pid>1 OR rn_oid>1
            OR order_id NOT IN (SELECT order_id FROM raw_orders)
       THEN 'QUARANTINE' ELSE 'PASS' END AS dq_status
FROM d;

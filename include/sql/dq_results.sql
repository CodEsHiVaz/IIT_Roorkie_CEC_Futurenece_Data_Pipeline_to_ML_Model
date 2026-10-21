-- include/sql/dq_results.sql
-- Populate the quality scorecard for one ELT run. One row per check.
-- Critical checks are read from staging failure_reason; warning checks and
-- orders-uniqueness are computed from the raw tables. Placeholders {run_id},
-- {dag_id}, {checked_at} are substituted by the DAG before execution.
INSERT INTO dq_results
SELECT run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed,
       CASE WHEN rows_failed=0 THEN 'PASS' ELSE 'FAIL' END AS status, checked_at
FROM (
  -- orders (critical value checks from staging failure_reason)
  SELECT '{run_id}' run_id,'{dag_id}' dag_id,'orders_customer_id_present' check_name,'Completeness' dimension,'Critical' severity,(SELECT count(*) FROM stg_orders) rows_checked,(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_customer_id_present%') rows_failed,'{checked_at}' checked_at
  UNION ALL SELECT '{run_id}','{dag_id}','orders_delivered_has_delivery_min','Completeness','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_delivered_has_delivery_min%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_order_ts_present','Completeness','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_order_ts_present%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_order_id_unique','Uniqueness','Critical',(SELECT count(*) FROM raw_orders WHERE _source_file NOT LIKE '%increment%'),(SELECT count(*)-count(DISTINCT order_id) FROM raw_orders WHERE _source_file NOT LIKE '%increment%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_status_valid','Validity','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_status_valid%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_discount_pct_0_100','Validity','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_discount_pct_0_100%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_order_ts_parses','Validity','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_order_ts_parses%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_quantity_positive','Accuracy','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_quantity_positive%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_amount_matches_formula','Accuracy','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_amount_matches_formula%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_delivery_min_believable','Accuracy','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_delivery_min_believable%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_customer_exists','Consistency','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_customer_exists%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_product_exists','Consistency','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_product_exists%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_store_exists','Consistency','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_store_exists%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_order_ts_not_future','Timeliness','Critical',(SELECT count(*) FROM stg_orders),(SELECT count(*) FROM stg_orders WHERE failure_reason LIKE '%orders_order_ts_not_future%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','orders_paymode_valid','Validity','Warning',(SELECT count(*) FROM raw_orders),(SELECT count(*) FROM raw_orders WHERE (CASE lower(trim(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery' ELSE trim(payment_mode) END) NOT IN ('UPI','Card','Cash on Delivery','Wallet')),'{checked_at}'
  -- payments
  UNION ALL SELECT '{run_id}','{dag_id}','payments_payment_id_unique','Uniqueness','Critical',(SELECT count(*) FROM stg_payments),(SELECT count(*) FROM stg_payments WHERE failure_reason LIKE '%payments_payment_id_unique%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','payments_one_per_order','Uniqueness','Critical',(SELECT count(*) FROM stg_payments),(SELECT count(*) FROM stg_payments WHERE failure_reason LIKE '%payments_one_per_order%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','payments_order_exists','Consistency','Critical',(SELECT count(*) FROM stg_payments),(SELECT count(*) FROM stg_payments WHERE failure_reason LIKE '%payments_order_exists%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','payments_paymode_valid','Validity','Warning',(SELECT count(*) FROM raw_payments),(SELECT count(*) FROM raw_payments WHERE (CASE lower(trim(payment_mode)) WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'wallet' THEN 'Wallet' WHEN 'cash on delivery' THEN 'Cash on Delivery' WHEN 'cod' THEN 'Cash on Delivery' ELSE trim(payment_mode) END) NOT IN ('UPI','Card','Cash on Delivery','Wallet')),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','payments_success_has_gateway_ref','Completeness','Warning',(SELECT count(*) FROM raw_payments),(SELECT count(*) FROM raw_payments WHERE payment_status='SUCCESS' AND gateway_ref IS NULL),'{checked_at}'
  -- customers
  UNION ALL SELECT '{run_id}','{dag_id}','customers_customer_id_unique','Uniqueness','Critical',(SELECT count(*) FROM stg_customers),(SELECT count(*) FROM stg_customers WHERE failure_reason LIKE '%customers_customer_id_unique%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','customers_signup_not_future','Timeliness','Critical',(SELECT count(*) FROM stg_customers),(SELECT count(*) FROM stg_customers WHERE failure_reason LIKE '%customers_signup_not_future%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','customers_email_format','Validity','Warning',(SELECT count(*) FROM raw_customers),(SELECT count(*) FROM raw_customers WHERE coalesce(email,'') NOT LIKE '%@%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','customers_phone_10_digit','Validity','Warning',(SELECT count(*) FROM raw_customers),(SELECT count(*) FROM raw_customers WHERE length(regexp_replace(coalesce(phone,''),'[^0-9]','','g'))<>10),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','customers_pincode_6_digit','Validity','Warning',(SELECT count(*) FROM raw_customers),(SELECT count(*) FROM raw_customers WHERE NOT regexp_full_match(coalesce(pincode,''),'[0-9]{6}')),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','customers_city_present','Completeness','Warning',(SELECT count(*) FROM raw_customers),(SELECT count(*) FROM raw_customers WHERE city IS NULL),'{checked_at}'
  -- products
  UNION ALL SELECT '{run_id}','{dag_id}','products_product_id_unique','Uniqueness','Critical',(SELECT count(*) FROM stg_products),(SELECT count(*) FROM stg_products WHERE failure_reason LIKE '%products_product_id_unique%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','products_category_present','Completeness','Critical',(SELECT count(*) FROM stg_products),(SELECT count(*) FROM stg_products WHERE failure_reason LIKE '%products_category_present%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','products_mrp_positive','Accuracy','Critical',(SELECT count(*) FROM stg_products),(SELECT count(*) FROM stg_products WHERE failure_reason LIKE '%products_mrp_positive%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','products_cost_le_mrp','Accuracy','Critical',(SELECT count(*) FROM stg_products),(SELECT count(*) FROM stg_products WHERE failure_reason LIKE '%products_cost_le_mrp%'),'{checked_at}'
  -- stores
  UNION ALL SELECT '{run_id}','{dag_id}','stores_store_id_unique','Uniqueness','Critical',(SELECT count(*) FROM stg_stores),(SELECT count(*) FROM stg_stores WHERE failure_reason LIKE '%stores_store_id_unique%'),'{checked_at}'
  UNION ALL SELECT '{run_id}','{dag_id}','stores_zone_present','Completeness','Critical',(SELECT count(*) FROM stg_stores),(SELECT count(*) FROM stg_stores WHERE failure_reason LIKE '%stores_zone_present%'),'{checked_at}'
);

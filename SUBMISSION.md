# NammaMart ETL & ELT — Submission (shivaz)

Forward Deployed Engineer deliverable for NammaMart Quick Commerce. Both pipelines
run on Apache Airflow (Astro Runtime 3.3 / Airflow 3) and write to one DuckDB
warehouse, `include/warehouse_shivaz.duckdb`.

## What was built

| File | Purpose |
| --- | --- |
| `dags/nammamart_etl_shivaz.py` | ETL: Extract → Validate → Quarantine → Transform → Load star schema |
| `dags/nammamart_elt_shivaz.py` | ELT: Extract → Load raw → SQL staging → SQL marts |
| `include/nm_logic.py` | Shared Python: masking, standardisation, the 6-dimension quality-check registry, increment merge, ETL transform/reconcile |
| `include/sql/staging.sql` | ELT staging layer (cast, standardise, dedupe, mask, quality rules) |
| `include/sql/marts.sql` | ELT business-ready marts |
| `include/sql/dq_results.sql` | ELT quality scorecard insert |
| `include/sql/business_questions.sql` | The six business questions |

### How to run

```bash
astro dev start
astro dev run dags test nammamart_etl_shivaz     # ETL once
astro dev run dags test nammamart_elt_shivaz     # ELT once
```

Both DAGs parse with no import errors (`astro dev parse` → “No errors detected”).

### Headline result — the pipelines reconcile

Verified on a live run (Airflow 3.3). September delivered net revenue:

| Pipeline | Delivered net revenue | Delivered orders | Rows loaded |
| --- | --- | --- | --- |
| ETL (`etl_fact_orders`) | **₹1,211,087.30** | 2,228 | 2,460 |
| ELT (`mart_daily_store_kpis`) | **₹1,211,087.30** | 2,228 | 2,460 |

The two pipelines apply identical critical rules, so the clean delivered set is
identical and the totals match exactly.

---

## Part 0 — Discovery & Data Issues Log

Profiled all six files (row counts, nulls, duplicate keys, numeric ranges,
distinct categories, referential integrity, business-rule recompute). Issues were
found in **every** file. Counts below are the actual measured numbers.

| File | Column | Problem (example) | Dimension | Proposed rule | Severity |
| --- | --- | --- | --- | --- | --- |
| orders | customer_id | 18 rows null | Completeness | require customer_id | Critical |
| orders | order_ts | 1 null, 5 unparseable, 7 future (to 2027-01-01) | Completeness / Validity / Timeliness | must parse, not future | Critical |
| orders | order_id | 12 duplicate ids | Uniqueness | keep first, quarantine rest | Critical |
| orders | order_status | `Pending??` (3), `delivered` lowercase (2) | Validity | upper + allowed list | Critical (fix casing) |
| orders | payment_mode | `upi`, `UPI `, `CARD`, `cod`, `Cashh` | Validity | standardise; `Cashh` unmappable | Warning |
| orders | quantity | negative, min −3 (10 rows) | Accuracy | quantity > 0 | Critical |
| orders | discount_pct | 250 (> 100) (3 rows) | Validity | 0–100 | Critical |
| orders | order_amount | 18 rows break `qty×price×(1−disc/100)` | Accuracy | recompute & compare | Critical |
| orders | delivery_minutes | up to 1440 (hours, 6 rows); 8 DELIVERED missing | Accuracy / Completeness | ≤120; DELIVERED must have it | Critical |
| orders | product_id / store_id | 9 / 5 ids not in masters | Consistency | FK must exist in source master | Critical |
| payments | payment_id | 4 duplicate ids | Uniqueness | unique | Critical |
| payments | order_id | 4 orders paid twice; 8 ids not in orders | Uniqueness / Consistency | one per order; FK exists | Critical |
| payments | amount_paid | 14 SUCCESS rows ≠ order_amount | Consistency | reconcile to order | → reconciliation table |
| payments | gateway_ref | 285 SUCCESS rows null (mostly COD) | Completeness | ref for non-COD | Warning |
| customers | customer_id | 5 duplicate ids | Uniqueness | unique | Critical |
| customers | signup_date | 3 future dates | Timeliness | not future | Critical |
| customers | email | 9 malformed / no `@` | Validity | contains `@` | Warning |
| customers | phone | 7 not 10 digits | Validity | 10 digits | Warning |
| customers | pincode | 3 not 6 digits | Validity | 6 digits | Warning |
| customers | city | 6 null | Completeness | present | Warning |
| customers | loyalty_tier | `BRONZE`, `bronze`, `SILVER` (8) | Consistency | title-case to one casing | Warning |
| products | product_id | 2 duplicate ids | Uniqueness | unique | Critical |
| products | category | 3 null | Completeness | present | Critical |
| products | mrp / cost_price | 1 mrp ≤ 0; 5 rows cost_price > mrp | Accuracy | mrp > 0; cost ≤ mrp | Critical |
| stores | zone | 1 null | Completeness | present | Critical |
| increment | — | 10 September orders corrected to RETURNED + 150 new orders | — | upsert on order_id | — |

### Three questions I would ask the client before building

1. **Negative quantity** (−3, etc.) — is this a logged return/refund (so I should flip the sign and keep it) or a data-entry error (so I should reject it)? Today I quarantine it.
2. **SUCCESS payment ≠ order_amount** (14 rows) — which system is the source of truth, the order ledger or the payment gateway? This decides the direction of the reconciliation adjustment.
3. **Orphan foreign keys** (9 products, 5 stores missing) — are these late-arriving master rows I should hold and retry, or permanently invalid? And is the **120-minute** delivery ceiling the correct cut-off for “impossible”?

---

## Part A / B — ETL and ELT design, and why totals match

- **ETL** rejects bad rows *before* the warehouse. `validate_quarantine` runs the
  shared 6-dimension checks, writes critical failures to
  `include/quarantine/shivaz/<file>_failed_<runid>.csv` with a `failure_reason`
  column, and only clean rows flow to the star schema (`etl_fact_orders`,
  `etl_dim_customer/product/store`). Load is **idempotent** (`CREATE OR REPLACE`).
- **ELT** keeps every raw row (`raw_*`, all columns text, plus `_loaded_at`,
  `_source_file`, `_run_id`), then cleans in SQL (`stg_*` with `dq_status` +
  `failure_reason`) and builds marts. The increment is **MERGE/upsert on
  order_id** in staging (increment wins, then first base row).
- **Why totals match:** staging encodes the *same* critical rules as the ETL
  Python checks, and the marts `LEFT JOIN` the clean dimensions exactly as the ETL
  fact does — so the clean DELIVERED set is identical. Measured: both = ₹1,211,087.30.
- **Referential integrity** is checked against the *full source-master key set* (a
  key either exists in the source or it doesn’t), so a dimension row with its own
  quality flag (e.g. a product with `cost > mrp`) does **not** cascade-reject the
  orders that reference it. Those orders still count as revenue; only their margin
  is flagged. This is why quarantine is ~2 % of orders, not ~20 %.

A quarantined store (null zone) and three null-category products appear honestly
in both pipelines as a `NaN` zone (₹85,382) and `NaN` category (₹71,348) — the gap
is surfaced, not hidden, and both pipelines agree on it.

---

## Part C — Scheduling (9:00 AM IST, Mon–Sat)

Both DAGs: `schedule="0 9 * * 1-6"`, `start_date=pendulum.datetime(2026,9,1,
tz="Asia/Kolkata")`, `catchup=False`, and `default_args` with `retries=2`,
`retry_delay=1 min`, `execution_timeout=15 min`.

- **Cron expression:** `0 9 * * 1-6` (09:00, Monday–Saturday).
- **Time shown in UTC in the UI:** IST = UTC+5:30, so a 09:00 IST run shows as
  **03:30 UTC** in the Airflow UI.
- **If `start_date` were left in UTC:** `0 9` would fire at 09:00 **UTC = 14:30 IST**
  — the number would land at 2:30 PM, five hours after the 9:30 AM stand-up. The
  timezone-aware `start_date` is what pins it to 09:00 IST.
- **If the laptop/Docker is off at 9 AM:** the scheduler isn’t running, so the run
  doesn’t fire on time. With `catchup=False` there is **no backfill** of the missed
  morning when it restarts — it simply runs the next scheduled slot. You’d trigger
  that day’s run manually.
- **Why `catchup=False`:** the business only cares about *today’s* number. We do not
  want Airflow to replay every day back to the Sept 1 `start_date` on first deploy.

---

## Part D — Data quality dimensions

All six dimensions are implemented in **both** pipelines (Python registry in
`nm_logic.CHECKS` for ETL; SQL `CASE` expressions in `staging.sql` for ELT) with
Critical/Warning severities. Every check, every run, is written to **`dq_results`**
(`run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed,
status, checked_at`). Measured failure counts for one run:

| Dimension | Example check | Severity | Rows failed |
| --- | --- | --- | --- |
| Completeness | `orders_customer_id_present` | Critical | 18 |
| Completeness | `payments_success_has_gateway_ref` | Warning | 285 |
| Uniqueness | `orders_order_id_unique` | Critical | 12 |
| Validity | `orders_discount_pct_0_100` | Critical | 3 |
| Validity | `customers_email_format` | Warning | 9 |
| Accuracy | `orders_amount_matches_formula` | Critical | 18 |
| Accuracy | `orders_delivery_min_believable` (≤120) | Critical | 6 |
| Consistency | `orders_product_exists` | Critical | 9 |
| Timeliness | `orders_order_ts_not_future` | Critical | 7 |
| Timeliness | `customers_signup_not_future` | Critical | 3 |

32 checks run per pipeline per run. **Critical** rows are quarantined; **Warning**
rows load but are logged and counted.

---

## Part E — Data governance

1. **Ownership.** Orders & payments → Finance/Ops data owner; customers → CRM owner
   (and the DPO for PII); products → Catalogue team; stores → Ops; warehouse tables
   → the data-engineering team as custodian. A change to a quality rule (e.g. the
   120-minute delivery ceiling or the allowed `order_status` list) is a change to
   `nm_logic.CHECKS` / `staging.sql` and must be approved by the **Ops + Finance**
   owners via PR review, because it moves revenue.
2. **Data dictionary (fact + key dims).**

   | Table.column | Type | Meaning | Source | Allowed values | PII |
   | --- | --- | --- | --- | --- | --- |
   | etl_fact_orders.order_id | text | order key | orders.csv | ORDxxxxxx | no |
   | .order_date | date | order date | order_ts | 2026-09.. | no |
   | .net_revenue | double | revenue if DELIVERED else 0 | derived | ≥0 | no |
   | .gross_margin | double | net_revenue − qty×cost_price | derived | — | no |
   | .within_15_min | bool | delivered ≤15 min | derived | T/F | no |
   | etl_dim_customer.email | text | **masked** email | customers.csv | `ab***@…` | masked |
   | etl_dim_customer.phone | text | **masked** phone | customers.csv | `98******21` | masked |
   | etl_dim_product.cost_price | double | unit cost | products.csv | ≤ mrp | no |
   | etl_dim_store.zone | text | store zone | stores.csv | North/South/East/West | no |
3. **Lineage.** Pick `mart_daily_store_kpis` net_revenue for store S08 on a date →
   trace to `stg_orders` PASS DELIVERED rows for S08 → to `raw_orders` (with
   `_source_file`, `_run_id`) → to `orders.csv`. In **ELT** this is a single SQL
   walk because raw is retained with source metadata — *easier*. In **ETL** the raw
   row only survives if it was clean (else it’s in the quarantine CSV), so tracing a
   rejected number means joining logs + quarantine files.
4. **Personal data.** PII columns: `full_name`, `email`, `phone`, and
   address-ish `area`/`pincode`. Email and phone are **masked** (`pr***@…`,
   `98******21`) in every reporting table. We **mask, not delete**, because the
   masked value still supports analytics (dedupe, “top customers”) while removing the
   ability to contact/identify. India’s **DPDP Act 2023** applies: purpose limitation,
   data minimisation, and the right to erasure — masking in the serving layer plus
   access-controlled raw satisfies minimisation for analysts.
5. **Access control.** Analysts/BI → `mart_*` and `stg_*` only. Data engineers →
   all layers. Quarantine → data engineering + the owning team. Raw `customers`
   (unmasked phone) → only the CRM owner / DPO, never analysts.
6. **Retention.** Raw: keep ~13 months for audit/replay then archive cold. Quarantine
   files: ~90 days (long enough for an ops investigation). Masked customer data:
   retain while the customer is active; purge on erasure request. After the window,
   delete or move to encrypted cold storage.
7. **Secrets.** A `SLACK_WEBHOOK_URL` (bonus alert) is read with `Variable.get()`
   from an Airflow Variable, never hard-coded — a DAG file is committed to GitHub, so
   a secret in it would leak to everyone with repo access and to git history forever.
8. **Audit — “show a rejected order.”** ELT: `SELECT * FROM raw_orders WHERE
   order_id=…` shows it exactly as received; `stg_orders.failure_reason` shows the
   rule it broke; `WHERE dq_status='QUARANTINE'` proves it is excluded from every
   mart. ETL: the row is in `include/quarantine/shivaz/orders_failed_*.csv` with its
   `failure_reason`, and absent from `etl_fact_orders` — so it cannot be in the CFO total.
9. **Change history.** Order DELIVERED on 3 Sep, RETURNED on 1 Oct: after the
   incremental load both pipelines show it as **RETURNED**, so it drops out of
   September delivered revenue. Finance wants the **corrected** (point-in-time-now)
   view for the official number. To keep *both* views, add an SCD-2 style history
   (raw already preserves the original DELIVERED row with its `_run_id`/`_loaded_at`,
   so “as-of 30 Sep” revenue is still answerable from raw).

---

## Part F — Observability

**Run audit.** A final `run_audit` task (`trigger_rule="all_done"`) writes one row
per run to **`pipeline_runs`** (`run_id, dag_id, start_time, end_time,
rows_extracted, rows_loaded, rows_quarantined, status`). Measured:

| dag | rows_extracted | rows_loaded | rows_quarantined | status |
| --- | --- | --- | --- | --- |
| nammamart_etl_shivaz | 5362 | 2460 | 133 | success |
| nammamart_etl_shivaz | 5362 | 2460 | 133 | success |
| nammamart_elt_shivaz | 5372 | 2460 | 121 | success |
| nammamart_elt_shivaz | 5372 | 2460 | 121 | success |

(ETL counts the 12 duplicate order rows as quarantined; ELT de-dupes them in staging
before counting — both reach the same 2,460 clean rows.)

### 10 experiments

✅ = run and verified on the live environment. The rest give the expected result and
the exact step to reproduce (restore the file afterwards).

| # | Experiment | Expected → Observed → Where | What I’d change |
| --- | --- | --- | --- |
| 1 | Happy path ✅ | Expected clean load → **Observed** ETL fact 2,460, 133 quarantined; ELT identical revenue → `pipeline_runs`, task logs | nothing |
| 2 | Re-run idempotency ✅ | Expected no change → **Observed** fact stays 2,460, `raw_orders` stays 2,572, revenue unchanged across 2 runs → `pipeline_runs` (two identical rows) | nothing |
| 3 | Incremental load ✅ | Expected 150 new + 10 updated, no dupes → **Observed** log “10 updated, 150 inserted”; the 10 flip to RETURNED and leave Sept revenue → extract log / `stg_orders` | nothing |
| 4 | Missing source file | Rename `products.csv`; extract/staging task fails `up_for_retry`→`failed`, downstream `upstream_failed`; `pipeline_runs.status='failed'` | add an email/Slack alert on failure |
| 5 | New bad data | Add 5 bad rows (null customer, −qty, unknown store, `SHIPPED`, 2027 ts) → each lands in quarantine with its reason; `dq_results` counts rise | already covered by the registry |
| 6 | Schema change | Rename `order_amount`→`total` in a copy → ETL transform / staging cast fails **loudly** (column not found), no silent wrong load | add an explicit schema check at extract |
| 7 | Volume anomaly | Run with 50 orders → nothing warns today (only row-level checks) | add a volume check: fail if row count < 50 % of trailing average |
| 8 | Distribution drift | ×100 one store’s `unit_price` → `amount_matches_formula` catches most; store revenue spikes on the mart | add a per-store revenue z-score check |
| 9 | Freshness | Orders 30 days old → `orders_order_ts_not_future` still passes; staleness only logged | add a Warning check: latest order_ts older than 1 day |
| 10 | Recovery | After fixing #4, **Clear** the failed task → only it + downstream re-run | nothing |

### Five pillars of observability

- **Freshness** — timeliness checks (`order_ts`/`signup_date` not future). *Weakest:* no “stale” upper-bound check yet (exp 9).
- **Volume** — `rows_extracted`/`rows_loaded` in `pipeline_runs`. *Weak:* no automatic drop alert (exp 7).
- **Schema** — typed loads + explicit column lists fail loudly (exp 6).
- **Distribution** — formula/range accuracy checks (exp 8).
- **Lineage** — **strongest**: ELT raw keeps `_source_file`/`_run_id`, so any mart
  number traces back to source rows in SQL.

---

## Part G — Recommendation to Sameer (CTO)

**Recommendation: adopt the ELT architecture (raw → staging → marts) as the primary
pipeline, with the ETL quality rules implemented as the staging gate.**

- **Auditability** — ELT keeps every raw record with `_source_file`/`_run_id`, so
  Finance can be shown any rejected order exactly as received and proven out of the
  CFO total in one SQL query. ETL loses that unless you also read quarantine files.
- **Data-quality guarantee** — the staging layer applies the *same* critical rules,
  so the mart holds only validated rows (verified: mart revenue = ETL fact revenue,
  ₹1,211,087.30). Finance gets the clean-only guarantee *and* compliance gets raw.
- **Ease of change** — a new rule or KPI is a SQL edit in `include/sql/`, version
  controlled and reviewable, not a pandas rewrite.
- **Performance at 1000× growth** — ELT pushes compute into the warehouse
  (DuckDB now, a cloud warehouse later) instead of pulling everything into a pandas
  worker; the raw load is a bulk copy, not row-by-row Python.
- **Cost** — one warehouse doing set-based SQL is cheaper to operate and scale than
  large pandas workers; storage for raw is cheap relative to re-deriving history.

**Top 3 risks of the ELT design**
1. **Raw holds unmasked PII** — needs strict access control + retention/erasure, or it becomes a privacy liability.
2. **Quality logic lives in SQL** — it must be tested and reviewed, or rules drift silently and bad rows slip into marts.
3. **Warehouse is the single point** — DuckDB is a single-file local store; a 1000× plan must migrate raw/marts to a managed warehouse with backups and concurrency.

---

## Business questions (results from the live warehouse)

SQL in `include/sql/business_questions.sql`. ETL answers are identical via
`etl_daily_zone_revenue` / `etl_category_margin` / `etl_fact_orders`.

**Q1 — net revenue by zone** (delivered, after discount):
East 314,056.20 · West 312,413.00 · South 256,214.55 · North 243,021.55 · (null-zone store 85,382.00)

**Q2 — revenue & gross margin by category:**
Staples 268,583.30 / 67,143.14 · Household 212,105.05 / 54,466.15 · Personal Care
210,889.10 / 47,054.58 · Dairy & Bakery 179,568.95 / 47,633.00 · Snacks & Beverages
174,988.60 / 41,429.67 · Fruits & Vegetables 93,604.20 / 22,205.38.

**Q3 — delivery SLA:** avg delivery ~20–22 min across stores; % within 15 min ranges
21.6 % (S06 Banashankari) to 35.4 % (S08 Whitefield). The 10-minute promise is not
being met — a real finding for Ops.

**Q4 — revenue by payment mode & unreconciled:** UPI 638,752.15 · Card 251,882.30 ·
Cash on Delivery 181,668.40 · Wallet 138,118.45 · (`Cashh` 666.00 — an unmapped
value, flagged Warning). Total that does **not** reconcile ≈ **₹203,102** across:
missing payment for delivered (177, ₹96,316.55), payment without order (88,
₹76,259.98), delivered-but-failed payment (42, ₹25,722.25), amount mismatch (14,
₹4,803.95).

**Q5 — top 10 customers by spend** (masked contact): led by C00136 Sanjay Bhat
(Bronze) ₹8,738.10, C00115 ₹8,037.70, C00283 ₹7,934.60, C00236 (Platinum) ₹7,831.80 …
No raw email/phone shown (`sa***@example.com`).

**Q6 — rows rejected today, by reason:** top rejects — gateway_ref missing (285,
Warning), amount≠formula (18), customer_id null (18), duplicate order_id (12),
quantity≤0 (10), product FK missing (9). Full list in `dq_results` for the run.

"""
dags/nammamart_etl_shivaz.py
----------------------------
NammaMart ETL pipeline:  Extract -> Validate -> Quarantine -> Transform -> Load.

Finance view: only rows that pass every CRITICAL quality check reach the star
schema. Bad rows are quarantined with a reason. Loads are idempotent, so a
re-run never double-counts revenue.
"""

import sys, os, glob
sys.path.insert(0, "/usr/local/airflow/include")

from io import StringIO
from datetime import timedelta
import pandas as pd
import duckdb
import pendulum
try:
    from airflow.sdk import dag, task          # Airflow 3 (Astro Runtime 3.x)
except ImportError:
    from airflow.decorators import dag, task   # Airflow 2 fallback

import nm_logic as nm

DAG_ID = "nammamart_etl_shivaz"

default_args = {
    "owner": "shivaz",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}


def _read(name):
    return pd.read_csv(f"{nm.DATA_DIR}/{name}", dtype=str, keep_default_na=False, na_values=[""])


def _to_csv(df):
    return df.to_csv(index=False)


def _from_csv(s):
    return pd.read_csv(StringIO(s), dtype=str, keep_default_na=False, na_values=[""])


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                       # 09:00 Mon–Sat
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "etl"],
)
def nammamart_etl():

    # ── EXTRACT (5 parallel tasks) ────────────────────────────────────────────
    @task
    def extract_orders():
        df = _read("orders.csv")
        inc_files = glob.glob(f"{nm.DATA_DIR}/orders_*_increment.csv")
        for f in inc_files:
            inc = pd.read_csv(f, dtype=str, keep_default_na=False, na_values=[""])
            df, upd, ins = nm.apply_increment(df, inc)
            print(f"[Extract] increment {os.path.basename(f)}: {upd} updated, {ins} inserted")
        print(f"[Extract] orders: {len(df)} rows")
        return _to_csv(df)

    @task
    def extract_payments():
        df = _read("payments.csv"); print(f"[Extract] payments: {len(df)}"); return _to_csv(df)

    @task
    def extract_customers():
        df = _read("customers.csv"); print(f"[Extract] customers: {len(df)}"); return _to_csv(df)

    @task
    def extract_products():
        df = _read("products.csv"); print(f"[Extract] products: {len(df)}"); return _to_csv(df)

    @task
    def extract_stores():
        df = _read("stores.csv"); print(f"[Extract] stores: {len(df)}"); return _to_csv(df)

    # ── VALIDATE + QUARANTINE ─────────────────────────────────────────────────
    @task
    def validate_quarantine(orders, payments, customers, products, stores):
        ctx_now = pendulum.now("Asia/Kolkata")
        run_id = ctx_now.format("YYYYMMDD_HHmmss")
        checked_at = ctx_now.to_iso8601_string()
        os.makedirs(nm.QUARANTINE_DIR, exist_ok=True)

        frames = {k: _from_csv(v) for k, v in
                  dict(orders=orders, payments=payments, customers=customers,
                       products=products, stores=stores).items()}

        dq_rows, clean = [], {}

        def split(key, df, ctx):
            fail, warn, rows = nm.validate(key, df, ctx, run_id, DAG_ID, checked_at)
            dq_rows.extend(rows)
            bad = df[fail != ""].copy()
            if len(bad):
                bad["failure_reason"] = fail[fail != ""].values
                path = f"{nm.QUARANTINE_DIR}/{key}_failed_{run_id}.csv"
                bad.to_csv(path, index=False)
                print(f"[Quarantine] {key}: {len(bad)} rows -> {path}")
            warn_n = int((warn != "").sum())
            if warn_n:
                print(f"[Warning] {key}: {warn_n} rows passed with warnings")
            return df[fail == ""].copy()

        # RI checks reference the FULL source-master key set (a key either exists
        # in the source or it doesn't — independent of that row's own quality).
        clean["customers"] = split("customers", frames["customers"], {})
        clean["products"] = split("products", frames["products"], {})
        clean["stores"] = split("stores", frames["stores"], {})
        clean["orders"] = split("orders", frames["orders"], {
            "customer_ids": set(frames["customers"].customer_id),
            "product_ids": set(frames["products"].product_id),
            "store_ids": set(frames["stores"].store_id)})
        clean["payments"] = split("payments", frames["payments"],
                                  {"order_ids": set(frames["orders"].order_id)})

        quarantined = sum(len(frames[k]) - len(clean[k]) for k in frames)
        extracted = sum(len(frames[k]) for k in frames)
        print(f"[Validate] extracted={extracted} quarantined={quarantined}")
        return {
            "run_id": run_id, "checked_at": checked_at,
            "clean": {k: _to_csv(v) for k, v in clean.items()},
            "dq_rows": dq_rows,
            "counts": {"extracted": extracted, "quarantined": quarantined},
        }

    # ── TRANSFORM ─────────────────────────────────────────────────────────────
    @task
    def transform(v):
        clean = {k: _from_csv(v["clean"][k]) for k in
                 ["orders", "customers", "products", "stores"]}
        tbls = nm.build_etl_tables(clean)
        print(f"[Transform] fact rows: {len(tbls['fact'])}")
        return {k: _to_csv(df) for k, df in tbls.items()}

    # ── LOAD (idempotent: CREATE OR REPLACE) ──────────────────────────────────
    @task
    def load(t):
        con = duckdb.connect(nm.WAREHOUSE_DB)
        for tbl, key in [("etl_fact_orders", "fact"),
                         ("etl_dim_customer", "dim_customer"),
                         ("etl_dim_product", "dim_product"),
                         ("etl_dim_store", "dim_store")]:
            df = pd.read_csv(StringIO(t[key]))   # let numeric columns infer types
            con.register("df", df)
            con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM df")
            con.unregister("df")
        n = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()
        print(f"[Load] etl_fact_orders = {n} rows (idempotent replace)")
        return int(n)

    # ── RECONCILE ─────────────────────────────────────────────────────────────
    @task
    def reconcile(v):
        recon = nm.build_reconciliation(_from_csv(v["clean"]["orders"]),
                                        _from_csv(v["clean"]["payments"]))
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.register("recon", recon)
        con.execute("CREATE OR REPLACE TABLE etl_payment_reconciliation AS SELECT * FROM recon")
        con.unregister("recon")
        con.close()
        print(f"[Reconcile] {len(recon)} mismatches")
        print(recon.reason.value_counts().to_string())
        return len(recon)

    # ── REPORT ────────────────────────────────────────────────────────────────
    @task
    def report():
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT order_date, zone, store_id, store_name,
                   ROUND(SUM(net_revenue), 2) AS net_revenue
            FROM etl_fact_orders WHERE order_status='DELIVERED'
            GROUP BY 1,2,3,4 ORDER BY 1,2,3""")
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT category,
                   ROUND(SUM(net_revenue), 2) AS revenue,
                   ROUND(SUM(gross_margin), 2) AS gross_margin
            FROM etl_fact_orders WHERE order_status='DELIVERED'
            GROUP BY 1 ORDER BY revenue DESC""")
        print("[Report] etl_daily_zone_revenue (head):")
        print(con.execute("SELECT * FROM etl_daily_zone_revenue LIMIT 10").fetchdf().to_string(index=False))
        print("[Report] etl_category_margin:")
        print(con.execute("SELECT * FROM etl_category_margin").fetchdf().to_string(index=False))
        con.close()

    # ── DQ AUDIT ──────────────────────────────────────────────────────────────
    @task
    def dq_audit(v):
        # Column order is pinned explicitly: XCom may reorder dict keys, so never
        # rely on positional "SELECT *" into a typed table.
        cols = ["run_id", "dag_id", "check_name", "dimension", "severity",
                "rows_checked", "rows_failed", "status", "checked_at"]
        df = pd.DataFrame(v["dq_rows"], columns=cols)
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked INTEGER, rows_failed INTEGER,
            status VARCHAR, checked_at VARCHAR)""")
        con.register("df", df)
        con.execute(f"INSERT INTO dq_results ({','.join(cols)}) SELECT {','.join(cols)} FROM df")
        con.unregister("df")
        con.close()
        print(f"[DQ] {len(df)} checks written to dq_results")
        return len(df)        # sentinel: non-null means the pipeline reached here

    # ── RUN AUDIT (always runs) ───────────────────────────────────────────────
    @task(trigger_rule="all_done")
    def run_audit():
        try:
            from airflow.sdk import get_current_context       # Airflow 3
        except ImportError:
            from airflow.operators.python import get_current_context  # Airflow 2
        ctx = get_current_context()
        ti, dr = ctx["ti"], ctx.get("dag_run")
        run_id = (getattr(dr, "run_id", None) or ctx.get("run_id") or "manual")
        v = ti.xcom_pull(task_ids="validate_quarantine")
        counts = v.get("counts", {}) if isinstance(v, dict) else {}
        loaded = ti.xcom_pull(task_ids="load") or 0
        status = "success" if ti.xcom_pull(task_ids="dq_audit") else "failed"
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
            rows_extracted INTEGER, rows_loaded INTEGER, rows_quarantined INTEGER,
            status VARCHAR)""")
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)", [
            run_id, DAG_ID,
            str(getattr(dr, "start_date", "")), pendulum.now("UTC").to_iso8601_string(),
            int(counts.get("extracted", 0)), int(loaded),
            int(counts.get("quarantined", 0)), status])
        con.close()
        print(f"[RunAudit] extracted={counts.get('extracted',0)} "
              f"loaded={loaded} quarantined={counts.get('quarantined',0)} status={status}")

    # ── WIRING ────────────────────────────────────────────────────────────────
    # Extracts run in parallel; warehouse writers are chained because DuckDB
    # allows only one writer per file at a time.
    o, p, c, pr, s = (extract_orders(), extract_payments(), extract_customers(),
                      extract_products(), extract_stores())
    v = validate_quarantine(o, p, c, pr, s)
    t = transform(v)
    loaded = load(t)
    rec = reconcile(v)
    rep = report()
    dq = dq_audit(v)
    loaded >> rec >> rep >> dq >> run_audit()


nammamart_etl()

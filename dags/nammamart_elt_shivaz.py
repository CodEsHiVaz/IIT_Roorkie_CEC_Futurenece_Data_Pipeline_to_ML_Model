"""
dags/nammamart_elt_shivaz.py
----------------------------
NammaMart ELT pipeline:  Extract -> Load raw -> Transform in DuckDB with SQL.

Analytics/compliance view: every raw record is kept exactly as received (all
columns as text) for audit and replay. Cleaning happens in SQL staging; marts
are business-ready. The increment file is upserted on order_id in staging.
"""

import sys, os, glob
from datetime import timedelta
import duckdb
import pendulum
try:
    from airflow.sdk import dag, task          # Airflow 3 (Astro Runtime 3.x)
except ImportError:
    from airflow.decorators import dag, task   # Airflow 2 fallback

sys.path.insert(0, "/usr/local/airflow/include")
import nm_logic as nm

DAG_ID  = "nammamart_elt_shivaz"
SQL_DIR = "/usr/local/airflow/include/sql"

default_args = {
    "owner": "shivaz",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=15),
}


def _run_sql_file(con, path):
    with open(path) as f:
        sql = f.read()
    for stmt in nm.split_sql(sql):
        con.execute(stmt)


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                       # 09:00 Mon–Sat
    start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
    catchup=False,
    default_args=default_args,
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt():

    # ── LOAD RAW (exact text copy, idempotent CREATE OR REPLACE) ──────────────
    @task
    def load_raw():
        run_id = pendulum.now("Asia/Kolkata").format("YYYYMMDD_HHmmss")
        con = duckdb.connect(nm.WAREHOUSE_DB)
        files = {"raw_orders": "orders.csv", "raw_payments": "payments.csv",
                 "raw_customers": "customers.csv", "raw_products": "products.csv",
                 "raw_stores": "stores.csv"}
        for tbl, fname in files.items():
            con.execute(f"""
                CREATE OR REPLACE TABLE {tbl} AS
                SELECT *, now() AS _loaded_at, '{fname}' AS _source_file, '{run_id}' AS _run_id
                FROM read_csv('{nm.DATA_DIR}/{fname}', all_varchar=true, header=true)""")
            n = con.execute(f"SELECT count(*) FROM {tbl}").fetchone()[0]
            print(f"[Raw] {tbl}: {n} rows")
        con.close()
        return run_id

    # ── INCREMENTAL RAW APPEND (idempotent: wipe prior increment rows first) ──
    @task
    def load_increment(rid):     # not 'run_id' — that name is a reserved task-context key
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("DELETE FROM raw_orders WHERE _source_file LIKE '%increment%'")
        total = 0
        for f in glob.glob(f"{nm.DATA_DIR}/orders_*_increment.csv"):
            fname = os.path.basename(f)
            con.execute(f"""
                INSERT INTO raw_orders
                SELECT *, now() AS _loaded_at, '{fname}' AS _source_file, '{rid}' AS _run_id
                FROM read_csv('{f}', all_varchar=true, header=true)""")
            n = con.execute("SELECT count(*) FROM raw_orders WHERE _source_file=?", [fname]).fetchone()[0]
            total += n
            print(f"[Increment] appended {n} rows from {fname}")
        print(f"[Increment] raw_orders now {con.execute('SELECT count(*) FROM raw_orders').fetchone()[0]} rows")
        con.close()
        return total

    # ── BUILD STAGING (SQL) ───────────────────────────────────────────────────
    @task
    def build_staging():
        con = duckdb.connect(nm.WAREHOUSE_DB)
        _run_sql_file(con, f"{SQL_DIR}/staging.sql")
        for t in ["stg_orders", "stg_payments", "stg_customers", "stg_products", "stg_stores"]:
            tot = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            bad = con.execute(f"SELECT count(*) FROM {t} WHERE dq_status='QUARANTINE'").fetchone()[0]
            print(f"[Staging] {t}: {tot} rows, {bad} quarantined")
        con.close()

    # ── BUILD MARTS (SQL) ──────────────────────────────────────────────────────
    @task
    def build_marts():
        con = duckdb.connect(nm.WAREHOUSE_DB)
        _run_sql_file(con, f"{SQL_DIR}/marts.sql")
        rev = con.execute("SELECT ROUND(SUM(net_revenue),2) FROM mart_daily_store_kpis").fetchone()[0]
        print(f"[Marts] total delivered net revenue = {rev}")
        print(con.execute("SELECT * FROM mart_category_margin").fetchdf().to_string(index=False))
        con.close()

    # ── DQ SCORECARD (SQL) ─────────────────────────────────────────────────────
    @task
    def dq_audit(rid):           # not 'run_id' — reserved task-context key
        checked_at = pendulum.now("Asia/Kolkata").to_iso8601_string()
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS dq_results (
            run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
            severity VARCHAR, rows_checked INTEGER, rows_failed INTEGER,
            status VARCHAR, checked_at VARCHAR)""")
        with open(f"{SQL_DIR}/dq_results.sql") as f:
            sql = f.read()
        sql = (sql.replace("{run_id}", rid).replace("{dag_id}", DAG_ID)
                  .replace("{checked_at}", checked_at))
        for stmt in nm.split_sql(sql):
            con.execute(stmt)
        n = con.execute("SELECT count(*) FROM dq_results WHERE run_id=?", [rid]).fetchone()[0]
        con.close()
        print(f"[DQ] {n} checks written to dq_results")
        return int(n)         # sentinel: non-null means the pipeline reached here

    # ── RUN AUDIT (always runs) ────────────────────────────────────────────────
    @task(trigger_rule="all_done")
    def run_audit():
        try:
            from airflow.sdk import get_current_context       # Airflow 3
        except ImportError:
            from airflow.operators.python import get_current_context  # Airflow 2
        ctx = get_current_context()
        ti, dr = ctx["ti"], ctx.get("dag_run")
        run_id = (getattr(dr, "run_id", None) or ctx.get("run_id") or "manual")
        con = duckdb.connect(nm.WAREHOUSE_DB)
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time VARCHAR, end_time VARCHAR,
            rows_extracted INTEGER, rows_loaded INTEGER, rows_quarantined INTEGER,
            status VARCHAR)""")

        def q(sql):
            try:
                return int(con.execute(sql).fetchone()[0] or 0)
            except Exception:
                return 0

        extracted = sum(q(f"SELECT count(*) FROM {t}") for t in
                        ["raw_orders", "raw_payments", "raw_customers", "raw_products", "raw_stores"])
        loaded = q("SELECT count(*) FROM stg_orders WHERE dq_status='PASS'")
        quarantined = sum(q(f"SELECT count(*) FROM {t} WHERE dq_status='QUARANTINE'") for t in
                          ["stg_orders", "stg_payments", "stg_customers", "stg_products", "stg_stores"])
        status = "success" if ti.xcom_pull(task_ids="dq_audit") else "failed"
        con.execute("INSERT INTO pipeline_runs VALUES (?,?,?,?,?,?,?,?)", [
            run_id, DAG_ID, str(getattr(dr, "start_date", "")),
            pendulum.now("UTC").to_iso8601_string(),
            extracted, loaded, quarantined, status])
        con.close()
        print(f"[RunAudit] extracted={extracted} loaded={loaded} quarantined={quarantined}")

    # ── WIRING ─────────────────────────────────────────────────────────────────
    # Writers are chained because DuckDB allows only one writer per file at a time.
    rid = load_raw()
    inc = load_increment(rid)
    stg = build_staging()
    marts = build_marts()
    dq = dq_audit(rid)
    rid >> inc >> stg >> marts >> dq >> run_audit()


nammamart_elt()

"""
include/nm_logic.py
-------------------
Shared NammaMart logic for the ETL pipeline (pure pandas, no Airflow import so
it can be unit-tested locally). Holds: paths, masking, value standardisation,
and the data-quality check registry that covers all six DQ dimensions.

A check returns a boolean Series where True = the row FAILS the check.
Critical failures are quarantined; Warning failures load but are logged.
"""

import re
import pandas as pd

# ── Paths (inside the Astro/Docker container) ─────────────────────────────────
DATA_DIR       = "/usr/local/airflow/include/data/nammamart"
WAREHOUSE_DB   = "/usr/local/airflow/include/warehouse_shivaz.duckdb"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/shivaz"

# ── Allowed value lists (from the client's business rules) ─────────────────────
ALLOWED_STATUS   = {"DELIVERED", "CANCELLED", "RETURNED"}
ALLOWED_PAYMODE  = {"UPI", "Card", "Cash on Delivery", "Wallet"}
ALLOWED_TIER     = {"Bronze", "Silver", "Gold", "Platinum"}
MAX_DELIVERY_MIN = 120          # client's believable-delivery ceiling
RUN_DATE         = "2026-10-01"  # "today" for timeliness checks (data is Sept 2026)


# ── Masking (PII) ──────────────────────────────────────────────────────────────
def mask_email(e):
    if not isinstance(e, str) or "@" not in e:
        return None
    user, domain = e.split("@", 1)
    return (user[:2] + "***@" + domain) if len(user) >= 2 else ("***@" + domain)


def mask_phone(p):
    s = re.sub(r"\D", "", str(p or ""))
    return (s[:2] + "******" + s[-2:]) if len(s) == 10 else None


# ── Value standardisation ──────────────────────────────────────────────────────
def std_paymode(x):
    if not isinstance(x, str):
        return x
    key = x.strip().lower()
    return {
        "upi": "UPI", "card": "Card", "wallet": "Wallet",
        "cash on delivery": "Cash on Delivery", "cod": "Cash on Delivery",
    }.get(key, x.strip())


def std_status(x):
    return x.strip().upper() if isinstance(x, str) else x


def std_tier(x):
    return x.strip().title() if isinstance(x, str) else x


# ── Small helpers ──────────────────────────────────────────────────────────────
def num(s):
    return pd.to_numeric(s, errors="coerce")


def ts(s):
    return pd.to_datetime(s, errors="coerce")


# ── Check registry ──────────────────────────────────────────────────────────────
# Each entry: (check_name, dimension, severity, fail_fn)
# fail_fn(df, ctx) -> boolean Series, True where the row FAILS.
def _checks_orders():
    return [
        ("orders_customer_id_present", "Completeness", "Critical",
         lambda d, c: d.customer_id.isna()),
        ("orders_delivered_has_delivery_min", "Completeness", "Critical",
         lambda d, c: (d.order_status.map(std_status) == "DELIVERED") & d.delivery_minutes.isna()),
        ("orders_order_ts_present", "Completeness", "Critical",
         lambda d, c: d.order_ts.isna()),
        ("orders_order_id_unique", "Uniqueness", "Critical",
         lambda d, c: d.order_id.duplicated(keep="first")),
        ("orders_status_valid", "Validity", "Critical",
         lambda d, c: ~d.order_status.map(std_status).isin(ALLOWED_STATUS)),
        ("orders_discount_pct_0_100", "Validity", "Critical",
         lambda d, c: ~num(d.discount_pct).between(0, 100)),
        ("orders_order_ts_parses", "Validity", "Critical",
         lambda d, c: d.order_ts.notna() & ts(d.order_ts).isna()),
        ("orders_quantity_positive", "Accuracy", "Critical",
         lambda d, c: ~(num(d.quantity) > 0)),
        ("orders_amount_matches_formula", "Accuracy", "Critical",
         lambda d, c: (
             (num(d.quantity) * num(d.unit_price) * (1 - num(d.discount_pct) / 100)).round(2)
             - num(d.order_amount)).abs() > 0.01),
        ("orders_delivery_min_believable", "Accuracy", "Critical",
         lambda d, c: num(d.delivery_minutes).notna() & (num(d.delivery_minutes) > MAX_DELIVERY_MIN)),
        ("orders_customer_exists", "Consistency", "Critical",
         lambda d, c: d.customer_id.notna() & ~d.customer_id.isin(c["customer_ids"])),
        ("orders_product_exists", "Consistency", "Critical",
         lambda d, c: ~d.product_id.isin(c["product_ids"])),
        ("orders_store_exists", "Consistency", "Critical",
         lambda d, c: ~d.store_id.isin(c["store_ids"])),
        ("orders_order_ts_not_future", "Timeliness", "Critical",
         lambda d, c: ts(d.order_ts).dt.normalize() > pd.Timestamp(RUN_DATE)),
        ("orders_paymode_valid", "Validity", "Warning",
         lambda d, c: ~d.payment_mode.map(std_paymode).isin(ALLOWED_PAYMODE)),
    ]


def _checks_payments():
    return [
        ("payments_payment_id_unique", "Uniqueness", "Critical",
         lambda d, c: d.payment_id.duplicated(keep="first")),
        ("payments_one_per_order", "Uniqueness", "Critical",
         lambda d, c: d.order_id.duplicated(keep="first")),
        ("payments_order_exists", "Consistency", "Critical",
         lambda d, c: ~d.order_id.isin(c["order_ids"])),
        ("payments_paymode_valid", "Validity", "Warning",
         lambda d, c: ~d.payment_mode.map(std_paymode).isin(ALLOWED_PAYMODE)),
        ("payments_success_has_gateway_ref", "Completeness", "Warning",
         lambda d, c: (d.payment_status == "SUCCESS") & d.gateway_ref.isna()),
    ]


def _checks_customers():
    return [
        ("customers_customer_id_unique", "Uniqueness", "Critical",
         lambda d, c: d.customer_id.duplicated(keep="first")),
        ("customers_signup_not_future", "Timeliness", "Critical",
         lambda d, c: ts(d.signup_date) > pd.Timestamp(RUN_DATE)),
        ("customers_email_format", "Validity", "Warning",
         lambda d, c: ~d.email.fillna("").str.contains("@")),
        ("customers_phone_10_digit", "Validity", "Warning",
         lambda d, c: ~d.phone.fillna("").str.replace(r"\D", "", regex=True).str.fullmatch(r"\d{10}")),
        ("customers_pincode_6_digit", "Validity", "Warning",
         lambda d, c: ~d.pincode.fillna("").str.fullmatch(r"\d{6}")),
        ("customers_city_present", "Completeness", "Warning",
         lambda d, c: d.city.isna()),
    ]


def _checks_products():
    return [
        ("products_product_id_unique", "Uniqueness", "Critical",
         lambda d, c: d.product_id.duplicated(keep="first")),
        ("products_category_present", "Completeness", "Critical",
         lambda d, c: d.category.isna()),
        ("products_mrp_positive", "Accuracy", "Critical",
         lambda d, c: ~(num(d.mrp) > 0)),
        ("products_cost_le_mrp", "Accuracy", "Critical",
         lambda d, c: num(d.cost_price) > num(d.mrp)),
    ]


def _checks_stores():
    return [
        ("stores_store_id_unique", "Uniqueness", "Critical",
         lambda d, c: d.store_id.duplicated(keep="first")),
        ("stores_zone_present", "Completeness", "Critical",
         lambda d, c: d.zone.isna()),
    ]


CHECKS = {
    "orders": _checks_orders(),
    "payments": _checks_payments(),
    "customers": _checks_customers(),
    "products": _checks_products(),
    "stores": _checks_stores(),
}


def split_sql(sql):
    """Split a multi-statement SQL string on ';'. Ignores ';' inside '...' string
    literals and strips '--' line comments (so ';' in a comment is harmless)."""
    out, buf, in_str, i, n = [], [], False, 0, len(sql)
    while i < n:
        ch = sql[i]
        if not in_str and ch == "-" and i + 1 < n and sql[i + 1] == "-":
            while i < n and sql[i] != "\n":   # skip comment to end of line
                i += 1
            continue
        if ch == "'":
            in_str = not in_str
        if ch == ";" and not in_str:
            if "".join(buf).strip():
                out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    if "".join(buf).strip():
        out.append("".join(buf))
    return out


def apply_increment(base, inc):
    """Merge the next-day increment into base orders.

    Overlapping order_ids are corrections (overwrite the base row); the rest are
    new orders (appended). Deterministic, so a re-run rebuilds the same result.
    Genuine in-file duplicates in base are left intact for the uniqueness check.
    Returns (combined_df, n_updated, n_inserted).
    """
    corr = inc[inc.order_id.isin(base.order_id)]
    new = inc[~inc.order_id.isin(base.order_id)]
    base_kept = base[~base.order_id.isin(corr.order_id)]
    combined = pd.concat([base_kept, corr, new], ignore_index=True)
    return combined, len(corr), len(new)


def build_etl_tables(clean):
    """Standardise, mask, join and derive the ETL star schema from clean frames.

    `clean` is a dict of already critical-clean dataframes (orders, customers,
    products, stores). Returns {fact, dim_customer, dim_product, dim_store}.
    """
    o, c, p, s = (clean["orders"].copy(), clean["customers"].copy(),
                  clean["products"].copy(), clean["stores"].copy())
    o["order_status"] = o.order_status.map(std_status)
    o["payment_mode"] = o.payment_mode.map(std_paymode)
    c["loyalty_tier"] = c.loyalty_tier.map(std_tier)
    c["email"] = c.email.map(mask_email)
    c["phone"] = c.phone.map(mask_phone)

    f = (o.merge(p[["product_id", "category", "cost_price"]], on="product_id", how="left")
           .merge(s[["store_id", "zone", "store_name"]], on="store_id", how="left"))
    f["order_ts"] = ts(f.order_ts)
    f["order_date"] = f.order_ts.dt.date.astype(str)
    for col in ["quantity", "unit_price", "discount_pct", "order_amount",
                "delivery_minutes", "cost_price"]:
        f[col] = num(f[col])
    delivered = f.order_status == "DELIVERED"
    f["net_revenue"] = f.order_amount.where(delivered, 0).round(2)
    f["gross_margin"] = (f.order_amount - f.quantity * f.cost_price).where(delivered, 0).round(2)
    f["within_15_min"] = delivered & (f.delivery_minutes <= 15)

    keep = ["order_id", "order_date", "customer_id", "store_id", "store_name",
            "zone", "product_id", "category", "quantity", "unit_price",
            "discount_pct", "order_amount", "payment_mode", "order_status",
            "channel", "delivery_minutes", "net_revenue", "gross_margin", "within_15_min"]
    return {"fact": f[keep], "dim_customer": c, "dim_product": p, "dim_store": s}


def build_reconciliation(clean_orders, clean_payments):
    """One row per order/payment mismatch, with a reason."""
    o = clean_orders.copy()
    p = clean_payments.copy()
    o["order_status"] = o.order_status.map(std_status)
    o["order_amount"] = num(o.order_amount)
    p["amount_paid"] = num(p.amount_paid)
    m = o[["order_id", "order_status", "order_amount"]].merge(
        p[["order_id", "amount_paid", "payment_status"]], on="order_id", how="outer")

    def reason(r):
        if pd.isna(r.order_status):
            return "payment_without_order"
        if r.order_status == "DELIVERED":
            if pd.isna(r.payment_status):
                return "missing_payment_for_delivered"
            if r.payment_status != "SUCCESS":
                return "delivered_but_payment_" + str(r.payment_status).lower()
            if abs((r.amount_paid or 0) - (r.order_amount or 0)) > 0.01:
                return "amount_mismatch"
        if r.order_status == "CANCELLED" and pd.notna(r.payment_status):
            return "payment_for_cancelled"
        return ""

    m["reason"] = m.apply(reason, axis=1)
    return m[m.reason != ""]


def validate(file_key, df, ctx, run_id, dag_id, checked_at):
    """Run every check for one file.

    Returns (fail_reason, warn_reason, dq_rows):
      fail_reason : Series of ';'-joined Critical failures ('' = clean)
      warn_reason : Series of ';'-joined Warning failures
      dq_rows     : one dict per check for the dq_results table
    """
    fail_reason = pd.Series([""] * len(df), index=df.index)
    warn_reason = pd.Series([""] * len(df), index=df.index)
    dq_rows = []
    for name, dim, sev, fn in CHECKS[file_key]:
        failed = fn(df, ctx).fillna(False).astype(bool)
        target = fail_reason if sev == "Critical" else warn_reason
        target.loc[failed] = (target.loc[failed] + ";" + name).str.lstrip(";")
        dq_rows.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": name,
            "dimension": dim, "severity": sev,
            "rows_checked": int(len(df)), "rows_failed": int(failed.sum()),
            "status": "PASS" if failed.sum() == 0 else "FAIL",
            "checked_at": checked_at,
        })
    return fail_reason, warn_reason, dq_rows

"""
include/gx_checks.py
--------------------
Great Expectations validation for the Meridian pipeline.

Returns:
    {
        "passed":    <JSON of clean rows>,
        "failed":    <JSON of bad rows>,
        "summary":   { column: [list of issues] },
        "passed_count": int,
        "failed_count": int,
    }
"""

import pandas as pd
import json
from datetime import datetime


def run_gx_checks(sales_json: str, customers_json: str, inventory_json: str) -> dict:
    sales     = pd.read_json(sales_json)
    customers = pd.read_json(customers_json)
    inventory = pd.read_json(inventory_json)

    failed_masks = []
    summary      = {}

    # ── 1. Completeness: customer_id must not be null ──────────────────────
    mask_null_cust = sales["customer_id"].isnull() | (sales["customer_id"].astype(str).str.strip() == "")
    if mask_null_cust.any():
        summary["customer_id"] = summary.get("customer_id", [])
        summary["customer_id"].append(f"null/empty customer_id in {int(mask_null_cust.sum())} rows")
    failed_masks.append(mask_null_cust)

    # ── 2. Uniqueness: order_id must be unique ─────────────────────────────
    mask_dup_order = sales["order_id"].duplicated(keep=False)
    if mask_dup_order.any():
        summary["order_id"] = summary.get("order_id", [])
        summary["order_id"].append(f"duplicate order_id in {int(mask_dup_order.sum())} rows")
    failed_masks.append(mask_dup_order)

    # ── 3. Timeliness: order_date must not be in the future ────────────────
    sales["order_date"] = pd.to_datetime(sales["order_date"], errors="coerce")
    mask_future = sales["order_date"] > pd.Timestamp.now()
    if mask_future.any():
        summary["order_date"] = summary.get("order_date", [])
        summary["order_date"].append(f"future-dated orders in {int(mask_future.sum())} rows")
    failed_masks.append(mask_future)

    # ── 4. Accuracy: revenue must be >= 0 ─────────────────────────────────
    mask_neg_rev = sales["revenue"] < 0
    if mask_neg_rev.any():
        summary["revenue"] = summary.get("revenue", [])
        summary["revenue"].append(f"negative revenue in {int(mask_neg_rev.sum())} rows")
    failed_masks.append(mask_neg_rev)

    # ── 5. Consistency: product_id must exist in inventory ────────────────
    valid_products = set(inventory["product_id"].astype(str))
    mask_bad_product = ~sales["product_id"].astype(str).isin(valid_products)
    if mask_bad_product.any():
        summary["product_id"] = summary.get("product_id", [])
        summary["product_id"].append(f"unknown product_id in {int(mask_bad_product.sum())} rows")
    failed_masks.append(mask_bad_product)

    # ── Combine all failure masks ──────────────────────────────────────────
    combined_fail = failed_masks[0]
    for m in failed_masks[1:]:
        combined_fail = combined_fail | m

    failed_df = sales[combined_fail].copy()
    passed_df = sales[~combined_fail].copy()

    print(f"[GX] Passed: {len(passed_df)} rows | Failed: {len(failed_df)} rows")
    print(f"[GX] Issues: {json.dumps(summary, indent=2)}")

    return {
        "passed":        passed_df.to_json(),
        "failed":        failed_df.to_json(),
        "customers":     customers.to_json(),
        "inventory":     inventory.to_json(),
        "summary":       summary,
        "passed_count":  len(passed_df),
        "failed_count":  len(failed_df),
        "checked_at":    datetime.utcnow().isoformat(),
    }

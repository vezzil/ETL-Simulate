"""
transform_orders.py
───────────────────
Cleans and enriches the raw orders + order_items extracts from Olist.

Operations performed:
  1. Parse datetime columns from string to proper datetime
  2. Drop rows missing order_id or customer_id (not recoverable)
  3. Validate order status against allowed values
  4. Join orders ↔ order_items to produce one row per line item
  5. Validate price > 0 and quantity >= 1
  6. Build the date_key column (YYYYMMDD integer) for dim_date FK lookup
  7. Enrich with order_events: take the latest status from the operational DB

Output: data/processed/orders_clean.csv
         data/processed/dim_date.csv
"""

import os
import sys
from datetime import datetime

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("transform.orders")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_date_key(dt_series: pd.Series) -> pd.Series:
    """Convert a datetime Series to YYYYMMDD integer keys."""
    return dt_series.dt.strftime("%Y%m%d").astype("Int64")


def _build_dim_date(dates: pd.Series) -> pd.DataFrame:
    """
    Given a Series of unique date objects, build a full dim_date DataFrame.
    """
    unique_dates = pd.to_datetime(dates.dropna().unique())
    rows = []
    for d in sorted(unique_dates):
        rows.append(
            {
                "date_key": int(d.strftime("%Y%m%d")),
                "full_date": d.strftime("%Y-%m-%d"),
                "year": d.year,
                "month": d.month,
                "day": d.day,
                "quarter": (d.month - 1) // 3 + 1,
                "weekday": d.strftime("%A"),
            }
        )
    return pd.DataFrame(rows)


# ── Main transform ─────────────────────────────────────────────────────────────

def transform_orders(config: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read raw_orders.csv and raw_order_items.csv, clean and join them.

    Returns:
        (orders_df, dim_date_df)
        orders_df   — one row per order line item, ready for fact_orders load
        dim_date_df — deduplicated date dimension rows
    """
    cfg = config or load_config()
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])
    valid_statuses = set(cfg["validation"]["valid_statuses"])
    min_price = float(cfg["validation"]["min_price"])

    # ── Read raw files ────────────────────────────────────────────────────────
    orders_path = os.path.join(raw_dir, "raw_orders.csv")
    items_path = os.path.join(raw_dir, "raw_order_items.csv")

    log.info(f"Reading {orders_path}")
    orders = pd.read_csv(orders_path, low_memory=False)
    log.info(f"Reading {items_path}")
    items = pd.read_csv(items_path, low_memory=False)

    log.info(f"  Raw orders: {len(orders):,} | Raw order items: {len(items):,}")

    # ── 1. Parse timestamps ───────────────────────────────────────────────────
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # ── 2. Drop rows missing critical keys ───────────────────────────────────
    before = len(orders)
    orders.dropna(subset=["order_id", "customer_id"], inplace=True)
    log.info(f"  Dropped {before - len(orders):,} rows with null order_id/customer_id")

    # ── 3. Validate status ────────────────────────────────────────────────────
    invalid_status = ~orders["order_status"].isin(valid_statuses)
    if invalid_status.sum():
        log.warning(
            f"  {invalid_status.sum():,} rows have unexpected status values: "
            f"{orders.loc[invalid_status, 'order_status'].unique().tolist()}"
        )
    orders = orders[~invalid_status].copy()

    # ── 4. Join orders ↔ order items ─────────────────────────────────────────
    items["order_item_id"] = pd.to_numeric(items["order_item_id"], errors="coerce").fillna(1).astype(int)
    items["price"] = pd.to_numeric(items["price"], errors="coerce")
    items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce").fillna(0.0)

    merged = orders.merge(items[["order_id", "order_item_id", "product_id", "price", "freight_value"]],
                          on="order_id", how="inner")
    log.info(f"  After join: {len(merged):,} line items")

    # ── 5. Validate price ─────────────────────────────────────────────────────
    before = len(merged)
    merged = merged[merged["price"] > min_price].copy()
    log.info(f"  Dropped {before - len(merged):,} line items with price ≤ {min_price}")

    # ── 6. Build date_key (integer YYYYMMDD) ──────────────────────────────────
    merged["purchase_date"] = merged["order_purchase_timestamp"].dt.normalize()
    merged["date_key"] = _make_date_key(merged["order_purchase_timestamp"])

    # ── 7. Enrich with latest event status from operational DB ────────────────
    events_path = os.path.join(raw_dir, "order_events.csv")
    if os.path.exists(events_path):
        events = pd.read_csv(events_path, usecols=["order_id", "status", "event_timestamp"])
        events["event_timestamp"] = pd.to_datetime(events["event_timestamp"], errors="coerce")
        latest_events = (
            events.sort_values("event_timestamp")
            .groupby("order_id", as_index=False)
            .last()[["order_id", "status"]]
            .rename(columns={"status": "latest_event_status"})
        )
        merged = merged.merge(latest_events, on="order_id", how="left")
        # Override order_status with the latest operational event if available
        mask = merged["latest_event_status"].notna()
        merged.loc[mask, "order_status"] = merged.loc[mask, "latest_event_status"]
        merged.drop(columns=["latest_event_status"], inplace=True)
        log.info(f"  Enriched {mask.sum():,} rows with latest operational event status")
    else:
        log.warning("  order_events.csv not found — skipping operational status enrichment")

    # ── Final column selection ────────────────────────────────────────────────
    merged = merged.rename(columns={"order_item_id": "order_item_seq"})
    orders_clean = merged[[
        "order_id", "order_item_seq", "customer_id", "product_id",
        "date_key", "order_status", "price", "freight_value",
    ]].copy()

    # quantity is always 1 per line item in this dataset
    orders_clean["quantity"] = 1

    # ── Build dim_date ────────────────────────────────────────────────────────
    dim_date = _build_dim_date(merged["purchase_date"])
    log.info(f"  Built dim_date: {len(dim_date):,} unique dates")
    log.info(f"  Final clean order line items: {len(orders_clean):,}")

    return orders_clean, dim_date


def save_orders(orders_df: pd.DataFrame, dim_date_df: pd.DataFrame, config: dict | None = None) -> None:
    cfg = config or load_config()
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    os.makedirs(processed_dir, exist_ok=True)

    orders_path = os.path.join(processed_dir, cfg["staging_files"]["orders"])
    date_path = os.path.join(processed_dir, cfg["staging_files"]["dim_date"])

    orders_df.to_csv(orders_path, index=False, encoding="utf-8")
    dim_date_df.to_csv(date_path, index=False, encoding="utf-8")

    log.info(f"Saved clean orders → {orders_path}")
    log.info(f"Saved dim_date     → {date_path}")


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== TRANSFORM ORDERS: START ===")
    cfg = load_config()
    orders_df, dim_date_df = transform_orders(cfg)
    save_orders(orders_df, dim_date_df, cfg)
    log.info(f"=== TRANSFORM ORDERS: DONE — {len(orders_df):,} line items | {len(dim_date_df):,} date records ===")


if __name__ == "__main__":
    run()

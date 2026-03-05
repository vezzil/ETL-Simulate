"""
extract_csv.py
──────────────
Reads the four Olist CSV source files from the raw landing zone,
performs minimal structural validation, then writes clean copies to the
processed staging directory.

Called as an Airflow task (no arguments — paths come from config.yaml).
Can also be run standalone for testing:

    python src/extract/extract_csv.py
"""

import os
import sys

import pandas as pd

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("extract.csv")

# ── Required columns per file ────────────────────────────────────────────────
REQUIRED_COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "customer_unique_id", "customer_city", "customer_state", "customer_zip_code_prefix"],
    "orders": ["order_id", "customer_id", "order_status", "order_purchase_timestamp"],
    "order_items": ["order_id", "order_item_id", "product_id", "price", "freight_value"],
    "products": ["product_id", "product_category_name"],
}


def _check_columns(df: pd.DataFrame, expected: list[str], filename: str) -> None:
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(
            f"[{filename}] Missing expected columns: {missing}. "
            f"Found: {list(df.columns)}"
        )


def extract_csv(config: dict | None = None) -> dict[str, pd.DataFrame]:
    """
    Read all Olist CSV source files.

    Returns a dict with keys: 'customers', 'orders', 'order_items', 'products'.
    Each value is a raw (unmodified) DataFrame.
    """
    cfg = config or load_config()
    csv_dir = resolve_path(cfg["paths"]["csv_dir"])
    source_files = cfg["source_files"]

    dataframes: dict[str, pd.DataFrame] = {}

    for key, filename in source_files.items():
        filepath = os.path.join(csv_dir, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Source file not found: {filepath}\n"
                f"  → Download from https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce\n"
                f"  → Place CSV files in: {csv_dir}"
            )

        log.info(f"Reading {filename} …")
        df = pd.read_csv(filepath, low_memory=False)
        _check_columns(df, REQUIRED_COLUMNS[key], filename)
        log.info(f"  {filename}: {len(df):,} rows × {len(df.columns)} columns")
        dataframes[key] = df

    return dataframes


def save_raw_extracts(dataframes: dict[str, pd.DataFrame], config: dict | None = None) -> None:
    """
    Persist the raw extracted DataFrames into the raw landing zone.
    Files are written as UTF-8 CSVs with no index column.
    """
    cfg = config or load_config()
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])
    os.makedirs(raw_dir, exist_ok=True)

    mapping = {
        "customers": "raw_customers.csv",
        "orders": "raw_orders.csv",
        "order_items": "raw_order_items.csv",
        "products": "raw_products.csv",
    }

    for key, df in dataframes.items():
        out_path = os.path.join(raw_dir, mapping[key])
        df.to_csv(out_path, index=False, encoding="utf-8")
        log.info(f"Saved raw extract → {out_path} ({len(df):,} rows)")


def run(**kwargs) -> None:
    """Airflow-compatible entry point (accepts **kwargs for task context)."""
    log.info("=== EXTRACT CSV: START ===")
    cfg = load_config()
    dataframes = extract_csv(cfg)
    save_raw_extracts(dataframes, cfg)
    total = sum(len(df) for df in dataframes.values())
    log.info(f"=== EXTRACT CSV: DONE — {total:,} total rows extracted ===")


if __name__ == "__main__":
    run()

"""
transform_customers.py
──────────────────────
Cleans and standardises the raw customer extract from Olist.

Operations performed:
  1. Drop rows with null customer_id or customer_unique_id
  2. Deduplicate by customer_unique_id (keep first occurrence)
  3. Standardise city names (title-case, strip whitespace)
  4. Standardise state codes (upper-case, 2-char region)
  5. Cast zip code prefix to string (left-pad with zeros to 5 chars)
  6. Rename columns to match dim_customer schema

Output: data/processed/customers_clean.csv
"""

import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("transform.customers")


def transform_customers(config: dict | None = None) -> pd.DataFrame:
    """Read raw customers CSV, clean it, and return the clean DataFrame."""
    cfg = config or load_config()
    raw_path = os.path.join(resolve_path(cfg["paths"]["raw_dir"]), "raw_customers.csv")

    log.info(f"Reading raw customers from {raw_path}")
    df = pd.read_csv(raw_path, dtype=str)
    raw_count = len(df)
    log.info(f"  Raw rows: {raw_count:,}")

    # ── 1. Drop rows with critical nulls ─────────────────────────────────────
    df.dropna(subset=["customer_id", "customer_unique_id"], inplace=True)
    after_null_drop = len(df)
    log.info(f"  After null drop: {after_null_drop:,} (dropped {raw_count - after_null_drop:,})")

    # ── 2. Deduplicate by customer_unique_id ──────────────────────────────────
    df.drop_duplicates(subset=["customer_unique_id"], keep="first", inplace=True)
    after_dedup = len(df)
    log.info(f"  After dedup: {after_dedup:,} (dropped {after_null_drop - after_dedup:,} duplicates)")

    # ── 3. Standardise city names ─────────────────────────────────────────────
    df["customer_city"] = (
        df["customer_city"]
        .str.strip()
        .str.title()
        .fillna("Unknown")
    )

    # ── 4. Standardise state codes ────────────────────────────────────────────
    df["customer_state"] = (
        df["customer_state"]
        .str.strip()
        .str.upper()
        .fillna("XX")
    )

    # ── 5. Zero-pad zip prefix to 5 digits ───────────────────────────────────
    df["customer_zip_code_prefix"] = (
        df["customer_zip_code_prefix"]
        .str.strip()
        .str.zfill(5)
        .fillna("00000")
    )

    # ── 6. Rename to match dim_customer target schema ─────────────────────────
    df = df.rename(
        columns={
            "customer_city": "city",
            "customer_state": "state",
            "customer_zip_code_prefix": "zip_code",
        }
    )[["customer_id", "customer_unique_id", "city", "state", "zip_code"]]

    log.info(f"  Final clean rows: {len(df):,}")
    return df


def save_customers(df: pd.DataFrame, config: dict | None = None) -> str:
    """Write clean customers to the processed staging area."""
    cfg = config or load_config()
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    os.makedirs(processed_dir, exist_ok=True)
    out_path = os.path.join(processed_dir, cfg["staging_files"]["customers"])
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"Saved clean customers → {out_path}")
    return out_path


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== TRANSFORM CUSTOMERS: START ===")
    cfg = load_config()
    df = transform_customers(cfg)
    save_customers(df, cfg)
    log.info(f"=== TRANSFORM CUSTOMERS: DONE — {len(df):,} clean customer records ===")


if __name__ == "__main__":
    run()

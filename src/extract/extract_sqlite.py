"""
extract_sqlite.py
─────────────────
Reads order event data from the operational SQLite database (created by
seeds/seed_operational_db.py) and writes it as a CSV to the raw landing zone.

Simulates extracting from an OLTP / operational source database.

Called as an Airflow task (no arguments — config drives paths).
Can also be run standalone:

    python src/extract/extract_sqlite.py
"""

import os
import sys

import pandas as pd

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import get_connection, load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("extract.sqlite")

# ── Queries ───────────────────────────────────────────────────────────────────
ORDER_EVENTS_QUERY = """
    SELECT
        oe.event_id,
        oe.order_id,
        oe.status,
        oe.event_timestamp,
        oe.seller_id,
        oe.notes,
        ro.customer_id,
        ro.created_at        AS order_created_at,
        ro.expected_delivery,
        ro.channel
    FROM order_events oe
    LEFT JOIN raw_orders ro ON ro.order_id = oe.order_id
    ORDER BY oe.order_id, oe.event_timestamp
"""


def extract_order_events(config: dict | None = None) -> pd.DataFrame:
    """
    Query the operational SQLite DB and return order events as a DataFrame.
    Raises FileNotFoundError if the operational DB has not been seeded yet.
    """
    cfg = config or load_config()
    db_path = resolve_path(cfg["paths"]["operational_db"])

    if not os.path.exists(db_path):
        raise FileNotFoundError(
            f"Operational DB not found: {db_path}\n"
            f"  → Run first: python seeds/seed_operational_db.py"
        )

    log.info(f"Connecting to operational DB: {db_path}")

    with get_connection(db_path) as conn:
        df = pd.read_sql_query(ORDER_EVENTS_QUERY, conn)

    log.info(f"Extracted {len(df):,} order_event rows from operational DB")
    return df


def save_db_extract(df: pd.DataFrame, config: dict | None = None) -> str:
    """
    Persist the extracted DataFrame as a CSV in the raw landing zone.
    Returns the output file path.
    """
    cfg = config or load_config()
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])
    os.makedirs(raw_dir, exist_ok=True)

    out_path = os.path.join(raw_dir, cfg["staging_files"]["order_events"])
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"Saved DB extract → {out_path} ({len(df):,} rows)")
    return out_path


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== EXTRACT SQLITE: START ===")
    cfg = load_config()
    df = extract_order_events(cfg)
    save_db_extract(df, cfg)
    log.info(f"=== EXTRACT SQLITE: DONE — {len(df):,} events extracted ===")


if __name__ == "__main__":
    run()

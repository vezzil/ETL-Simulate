"""
load_dwh.py
───────────
Loads all clean/processed staging files into the SQLite star-schema DWH.

Load order (to satisfy FK constraints):
  1. dim_date       (date_key is standalone — no FK dependencies)
  2. dim_status     (status_key is standalone — no FK dependencies)
  3. dim_customer   (SCD Type 1 — INSERT OR REPLACE)
  4. dim_product    (SCD Type 1 — INSERT OR REPLACE)
  5. fact_orders    (references all four dims above)

SCD Type 1 strategy: INSERT OR REPLACE overwrites existing dim rows on
primary-key conflict, keeping the latest version of each dimension record.

Called as an Airflow task. Can also be run standalone:

    python src/load/load_dwh.py
"""

import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import create_dwh_schema, get_connection, load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("load.dwh")


# ─────────────────────────────────────────────────────────────────────────────
# 1. dim_date
# ─────────────────────────────────────────────────────────────────────────────

def load_dim_date(conn, processed_dir: str, staging_files: dict) -> int:
    path = os.path.join(processed_dir, staging_files["dim_date"])
    df = pd.read_csv(path)

    rows = list(df[["date_key", "full_date", "year", "month", "day", "quarter", "weekday"]].itertuples(index=False))
    conn.executemany(
        """
        INSERT OR IGNORE INTO dim_date (date_key, full_date, year, month, day, quarter, weekday)
        VALUES (?,?,?,?,?,?,?)
        """,
        rows,
    )
    loaded = conn.execute("SELECT COUNT(*) FROM dim_date").fetchone()[0]
    log.info(f"  dim_date: {loaded:,} rows in DWH (inserted from {len(df):,} staging rows)")
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# 2. dim_status
# ─────────────────────────────────────────────────────────────────────────────

def load_dim_status(conn, valid_statuses: list[str]) -> int:
    conn.executemany(
        "INSERT OR IGNORE INTO dim_status (status_label) VALUES (?)",
        [(s,) for s in valid_statuses],
    )
    loaded = conn.execute("SELECT COUNT(*) FROM dim_status").fetchone()[0]
    log.info(f"  dim_status: {loaded:,} status labels loaded")
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# 3. dim_customer
# ─────────────────────────────────────────────────────────────────────────────

def load_dim_customer(conn, processed_dir: str, staging_files: dict) -> int:
    path = os.path.join(processed_dir, staging_files["customers"])
    df = pd.read_csv(path, dtype=str).fillna("")

    rows = list(df[["customer_id", "city", "state", "zip_code"]].itertuples(index=False))
    conn.executemany(
        """
        INSERT INTO dim_customer (customer_id, city, state, zip_code, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'))
        ON CONFLICT(customer_id) DO UPDATE SET
            city       = excluded.city,
            state      = excluded.state,
            zip_code   = excluded.zip_code,
            updated_at = datetime('now')
        """,
        rows,
    )
    loaded = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    log.info(f"  dim_customer: {loaded:,} rows in DWH (upserted from {len(df):,} staging rows)")
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# 4. dim_product
# ─────────────────────────────────────────────────────────────────────────────

def load_dim_product(conn, processed_dir: str, staging_files: dict) -> int:
    path = os.path.join(processed_dir, staging_files["products"])
    df = pd.read_csv(path).fillna("")

    rows = [(
        str(row.product_id),
        str(row.category),
        str(row.name),
        float(row.api_price) if row.api_price != "" else 0.0,
        float(row.api_rating) if row.api_rating != "" else 0.0,
    ) for row in df.itertuples(index=False)]

    conn.executemany(
        """
        INSERT INTO dim_product (product_id, category, name, api_price, api_rating, updated_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(product_id) DO UPDATE SET
            category   = excluded.category,
            name       = excluded.name,
            api_price  = excluded.api_price,
            api_rating = excluded.api_rating,
            updated_at = datetime('now')
        """,
        rows,
    )
    loaded = conn.execute("SELECT COUNT(*) FROM dim_product").fetchone()[0]
    log.info(f"  dim_product: {loaded:,} rows in DWH (upserted from {len(df):,} staging rows)")
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# 5. fact_orders
# ─────────────────────────────────────────────────────────────────────────────

def load_fact_orders(conn, processed_dir: str, staging_files: dict) -> int:
    path = os.path.join(processed_dir, staging_files["orders"])
    df = pd.read_csv(path)
    log.info(f"  Loading {len(df):,} order line items into fact_orders …")

    # ── Build lookup maps from DWH dims ──────────────────────────────────────
    customer_map: dict[str, int] = dict(
        conn.execute("SELECT customer_id, customer_key FROM dim_customer").fetchall()
    )
    product_map: dict[str, int] = dict(
        conn.execute("SELECT product_id, product_key FROM dim_product").fetchall()
    )
    status_map: dict[str, int] = dict(
        conn.execute("SELECT status_label, status_key FROM dim_status").fetchall()
    )

    rows_inserted = 0
    rows_skipped = 0

    for row in df.itertuples(index=False):
        customer_key = customer_map.get(str(row.customer_id))
        product_key = product_map.get(str(row.product_id))
        status_key = status_map.get(str(row.order_status))
        date_key = int(row.date_key) if pd.notna(row.date_key) else None

        if customer_key is None or product_key is None or date_key is None:
            rows_skipped += 1
            continue

        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO fact_orders
                    (order_id, order_item_seq, customer_key, product_key,
                     date_key, status_key, quantity, price, freight_value)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    str(row.order_id),
                    int(row.order_item_seq),
                    customer_key,
                    product_key,
                    date_key,
                    status_key,
                    int(row.quantity),
                    float(row.price),
                    float(row.freight_value),
                ),
            )
            rows_inserted += 1
        except Exception as exc:
            log.warning(f"  Skipping row {row.order_id}/{row.order_item_seq}: {exc}")
            rows_skipped += 1

    loaded = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    log.info(
        f"  fact_orders: {loaded:,} rows in DWH "
        f"(inserted {rows_inserted:,} | skipped {rows_skipped:,})"
    )
    return loaded


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrate all loads
# ─────────────────────────────────────────────────────────────────────────────

def load_all(config: dict | None = None) -> dict[str, int]:
    """
    Run all dimension and fact loads in dependency order.
    Returns a dict of {table_name: row_count}.
    """
    cfg = config or load_config()
    dwh_db = resolve_path(cfg["paths"]["dwh_db"])
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    staging_files = cfg["staging_files"]
    valid_statuses = cfg["validation"]["valid_statuses"]

    create_dwh_schema(dwh_db)
    log.info(f"DWH schema created/verified at {dwh_db}")

    counts: dict[str, int] = {}
    with get_connection(dwh_db) as conn:
        counts["dim_date"] = load_dim_date(conn, processed_dir, staging_files)
        counts["dim_status"] = load_dim_status(conn, valid_statuses)
        counts["dim_customer"] = load_dim_customer(conn, processed_dir, staging_files)
        counts["dim_product"] = load_dim_product(conn, processed_dir, staging_files)
        counts["fact_orders"] = load_fact_orders(conn, processed_dir, staging_files)

    return counts


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== LOAD DWH: START ===")
    counts = load_all()
    summary = " | ".join(f"{t}: {n:,}" for t, n in counts.items())
    log.info(f"=== LOAD DWH: DONE — {summary} ===")


if __name__ == "__main__":
    run()

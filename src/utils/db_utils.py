"""
SQLite connection + schema helpers shared across ETL layers.
"""
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

import yaml

# ── Resolve project root & config ────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "config.yaml")


def load_config() -> dict:
    """Load and return the central config.yaml as a dict."""
    with open(_CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def resolve_path(relative_path: str) -> str:
    """Convert a config-relative path to an absolute filesystem path."""
    return os.path.join(_PROJECT_ROOT, relative_path)


@contextmanager
def get_connection(db_path: str) -> Generator[sqlite3.Connection, None, None]:
    """
    Context manager that yields a SQLite connection and commits on success,
    rolls back on exception, and always closes the connection.

    Usage::
        with get_connection(cfg["paths"]["dwh_db"]) as conn:
            conn.execute("SELECT 1")
    """
    abs_path = resolve_path(db_path) if not os.path.isabs(db_path) else db_path
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    conn = sqlite3.connect(abs_path)
    conn.execute("PRAGMA journal_mode=WAL")   # better concurrency
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── DWH Schema DDL ────────────────────────────────────────────────────────────

DWH_SCHEMA_SQL = """
-- ── Dimension: Customer ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key   INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id    TEXT    NOT NULL UNIQUE,
    city           TEXT,
    state          TEXT,
    zip_code       TEXT,
    updated_at     TEXT    DEFAULT (datetime('now'))
);

-- ── Dimension: Product ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id     TEXT    NOT NULL UNIQUE,
    category       TEXT,
    name           TEXT,
    api_price      REAL,
    api_rating     REAL,
    updated_at     TEXT    DEFAULT (datetime('now'))
);

-- ── Dimension: Date ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_date (
    date_key       INTEGER PRIMARY KEY,   -- YYYYMMDD integer
    full_date      TEXT    NOT NULL,      -- ISO 8601 YYYY-MM-DD
    year           INTEGER NOT NULL,
    month          INTEGER NOT NULL,
    day            INTEGER NOT NULL,
    quarter        INTEGER NOT NULL,
    weekday        TEXT    NOT NULL       -- Monday … Sunday
);

-- ── Dimension: Status ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_status (
    status_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    status_label   TEXT    NOT NULL UNIQUE
);

-- ── Fact: Orders ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_orders (
    fact_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id       TEXT    NOT NULL,
    order_item_seq INTEGER NOT NULL DEFAULT 1,
    customer_key   INTEGER REFERENCES dim_customer(customer_key),
    product_key    INTEGER REFERENCES dim_product(product_key),
    date_key       INTEGER REFERENCES dim_date(date_key),
    status_key     INTEGER REFERENCES dim_status(status_key),
    quantity       INTEGER NOT NULL DEFAULT 1,
    price          REAL    NOT NULL DEFAULT 0.0,
    freight_value  REAL    NOT NULL DEFAULT 0.0,
    loaded_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(order_id, order_item_seq)
);

-- ── Indexes for common analytical queries ────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_product  ON fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_date     ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_status   ON fact_orders(status_key);
"""


def create_dwh_schema(db_path: str) -> None:
    """Create all DWH tables and indexes (idempotent — IF NOT EXISTS)."""
    with get_connection(db_path) as conn:
        conn.executescript(DWH_SCHEMA_SQL)

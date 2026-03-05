"""
seed_operational_db.py
──────────────────────
Generates a realistic SQLite operational database that simulates an
e-commerce operations system.  Creates two tables:

  order_events  — status-change audit log (one row per status transition)
  raw_orders    — lightweight order header snapshot

Run once before starting the Airflow pipeline:

    python seeds/seed_operational_db.py

Output: data/raw/operational.db
"""

import os
import random
import sqlite3
import sys
from datetime import datetime, timedelta

# ── Allow running from project root or seeds/ directory ──────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _PROJECT_ROOT)

try:
    from faker import Faker
except ImportError:
    print("[ERROR] Faker not installed. Run: pip install Faker==22.0.0")
    sys.exit(1)

import yaml

# ── Config ────────────────────────────────────────────────────────────────────
_CONFIG_PATH = os.path.join(_PROJECT_ROOT, "config", "config.yaml")
with open(_CONFIG_PATH) as f:
    _cfg = yaml.safe_load(f)

DB_PATH = os.path.join(_PROJECT_ROOT, _cfg["paths"]["operational_db"])
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ── Constants ─────────────────────────────────────────────────────────────────
NUM_ORDERS = 1_500
STATUSES = ["created", "approved", "processing", "invoiced", "shipped", "delivered"]
CANCEL_PROB = 0.08    # 8 % of orders get cancelled mid-flow
SEED = 42
random.seed(SEED)
fake = Faker("pt_BR")   # Brazilian locale to match Olist flavour
Faker.seed(SEED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS order_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT    NOT NULL,
    status          TEXT    NOT NULL,
    event_timestamp TEXT    NOT NULL,
    seller_id       TEXT,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS raw_orders (
    order_id            TEXT PRIMARY KEY,
    customer_id         TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    expected_delivery   TEXT,
    channel             TEXT
);

CREATE INDEX IF NOT EXISTS idx_oe_order_id ON order_events(order_id);
CREATE INDEX IF NOT EXISTS idx_oe_status   ON order_events(status);
"""


def _generate_order_id() -> str:
    """Return a UUID-style hex string similar to Olist order IDs."""
    import uuid
    return uuid.uuid4().hex


def _random_datetime(start: datetime, span_days: int = 365) -> datetime:
    return start + timedelta(
        days=random.randint(0, span_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )


def seed(db_path: str = DB_PATH) -> None:
    print(f"[seed] Creating operational DB at: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.executescript(SCHEMA)

    start_dt = datetime(2017, 1, 1)
    channels = ["web", "mobile", "marketplace", "app"]
    event_rows = []
    order_rows = []

    for _ in range(NUM_ORDERS):
        order_id = _generate_order_id()
        customer_id = fake.uuid4().replace("-", "")
        created_at = _random_datetime(start_dt, 700)
        channel = random.choice(channels)
        expected_delivery = created_at + timedelta(days=random.randint(3, 30))

        order_rows.append(
            (
                order_id,
                customer_id,
                created_at.isoformat(),
                expected_delivery.isoformat(),
                channel,
            )
        )

        # Walk through the status pipeline
        current_time = created_at
        cancelled = False
        for idx, status in enumerate(STATUSES):
            # Small random gap between status transitions
            current_time = current_time + timedelta(
                hours=random.randint(1, 72)
            )
            seller_id = fake.uuid4().replace("-", "") if idx >= 2 else None
            notes = None
            if status == "delivered":
                notes = random.choice(
                    [None, None, None, "Left at door", "Signed by neighbour"]
                )

            event_rows.append(
                (order_id, status, current_time.isoformat(), seller_id, notes)
            )

            # Randomly cancel some orders after processing
            if not cancelled and status == "processing" and random.random() < CANCEL_PROB:
                cancel_time = current_time + timedelta(hours=random.randint(1, 12))
                event_rows.append(
                    (order_id, "canceled", cancel_time.isoformat(), seller_id, "Customer requested cancellation")
                )
                cancelled = True
                break

    # ── Insert ────────────────────────────────────────────────────────────────
    conn.executemany(
        "INSERT OR IGNORE INTO raw_orders VALUES (?,?,?,?,?)", order_rows
    )
    conn.executemany(
        "INSERT INTO order_events (order_id, status, event_timestamp, seller_id, notes) VALUES (?,?,?,?,?)",
        event_rows,
    )
    conn.commit()

    # ── Summary ───────────────────────────────────────────────────────────────
    order_count = conn.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0]
    event_count = conn.execute("SELECT COUNT(*) FROM order_events").fetchone()[0]
    conn.close()

    print(f"[seed] Done — {order_count:,} orders | {event_count:,} events")


if __name__ == "__main__":
    seed()

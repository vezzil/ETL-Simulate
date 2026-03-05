"""
generate_sample_data.py
───────────────────────
Generates synthetic Olist-compatible CSV files so the full pipeline can run
without downloading from Kaggle.

Creates ~5,000 orders, ~8,000 order items, ~3,000 customers, and ~500 products
with realistic-looking Brazilian e-commerce data.

Output: data/raw/csv/  (all four files the pipeline needs)

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --orders 10000
"""

import argparse
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _PROJECT_ROOT)

try:
    import pandas as pd
    from faker import Faker
except ImportError:
    print("[ERROR] Run: python -m pip install pandas Faker")
    sys.exit(1)

import yaml

with open(os.path.join(_PROJECT_ROOT, "config", "config.yaml")) as f:
    _cfg = yaml.safe_load(f)

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
fake = Faker("pt_BR")
Faker.seed(RANDOM_SEED)

STATUSES = ["delivered", "shipped", "canceled", "processing", "invoiced", "approved"]
STATUS_WEIGHTS = [0.60, 0.15, 0.08, 0.05, 0.06, 0.06]

CATEGORIES = [
    "eletronicos", "informatica_acessorios", "telefonia",
    "cama_mesa_banho", "fashion_roupa_feminina", "fashion_roupa_masculina",
    "joias_relogios", "esporte_lazer", "brinquedos", "beleza_saude",
    "moveis_decoracao", "automotivo", "livros_tecnicos", "pet_shop",
]

BR_STATES = ["SP", "RJ", "MG", "RS", "PR", "BA", "SC", "GO", "PE", "CE"]
STATE_WEIGHTS = [0.33, 0.14, 0.12, 0.07, 0.07, 0.06, 0.05, 0.04, 0.04, 0.04]


def _uid() -> str:
    return uuid.uuid4().hex


def _rand_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_customers(n: int) -> pd.DataFrame:
    print(f"  Generating {n:,} customers …")
    rows = []
    for _ in range(n):
        state = random.choices(BR_STATES, STATE_WEIGHTS)[0]
        rows.append({
            "customer_id": _uid(),
            "customer_unique_id": _uid(),
            "customer_zip_code_prefix": str(random.randint(10000, 99999)),
            "customer_city": fake.city(),
            "customer_state": state,
        })
    return pd.DataFrame(rows)


def generate_products(n: int) -> pd.DataFrame:
    print(f"  Generating {n:,} products …")
    rows = []
    for _ in range(n):
        rows.append({
            "product_id": _uid(),
            "product_category_name": random.choice(CATEGORIES),
            "product_name_lenght": random.randint(10, 60),
            "product_description_lenght": random.randint(100, 1000),
            "product_photos_qty": random.randint(1, 5),
            "product_weight_g": random.randint(100, 5000),
            "product_length_cm": random.randint(10, 60),
            "product_height_cm": random.randint(5, 40),
            "product_width_cm": random.randint(10, 50),
        })
    return pd.DataFrame(rows)


def generate_orders(customers_df: pd.DataFrame, n_orders: int) -> pd.DataFrame:
    print(f"  Generating {n_orders:,} orders …")
    customer_ids = customers_df["customer_id"].tolist()
    start = datetime(2017, 1, 1)
    end   = datetime(2018, 8, 31)

    rows = []
    for _ in range(n_orders):
        purchase_dt = _rand_date(start, end)
        approved_dt = purchase_dt + timedelta(minutes=random.randint(10, 120))
        carrier_dt  = approved_dt + timedelta(days=random.randint(1, 5))
        delivered_dt = carrier_dt + timedelta(days=random.randint(1, 20))
        estimated_dt = purchase_dt + timedelta(days=random.randint(7, 40))
        status = random.choices(STATUSES, STATUS_WEIGHTS)[0]

        rows.append({
            "order_id": _uid(),
            "customer_id": random.choice(customer_ids),
            "order_status": status,
            "order_purchase_timestamp": purchase_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "order_approved_at": approved_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "order_delivered_carrier_date": carrier_dt.strftime("%Y-%m-%d %H:%M:%S") if status not in ("processing", "approved") else "",
            "order_delivered_customer_date": delivered_dt.strftime("%Y-%m-%d %H:%M:%S") if status == "delivered" else "",
            "order_estimated_delivery_date": estimated_dt.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return pd.DataFrame(rows)


def generate_order_items(orders_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    print(f"  Generating order items …")
    order_ids  = orders_df["order_id"].tolist()
    product_ids = products_df["product_id"].tolist()

    rows = []
    for order_id in order_ids:
        n_items = random.choices([1, 2, 3], weights=[0.70, 0.22, 0.08])[0]
        for seq in range(1, n_items + 1):
            rows.append({
                "order_id": order_id,
                "order_item_id": seq,
                "product_id": random.choice(product_ids),
                "seller_id": _uid(),
                "shipping_limit_date": "",
                "price": round(random.uniform(9.90, 999.90), 2),
                "freight_value": round(random.uniform(5.00, 60.00), 2),
            })
    return pd.DataFrame(rows)


def main(n_orders: int = 5000) -> None:
    n_customers = max(1000, n_orders // 2)
    n_products  = 500

    print(f"\n[generate] Synthetic Olist-compatible data")
    print(f"  orders={n_orders:,}  customers={n_customers:,}  products={n_products:,}")

    customers = generate_customers(n_customers)
    products  = generate_products(n_products)
    orders    = generate_orders(customers, n_orders)
    items     = generate_order_items(orders, products)

    csv_dir = os.path.join(_PROJECT_ROOT, _cfg["paths"]["csv_dir"])
    os.makedirs(csv_dir, exist_ok=True)

    file_map = {
        "olist_customers_dataset.csv":   customers,
        "olist_orders_dataset.csv":      orders,
        "olist_order_items_dataset.csv": items,
        "olist_products_dataset.csv":    products,
    }
    for filename, df in file_map.items():
        path = os.path.join(csv_dir, filename)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"  Saved {filename} ({len(df):,} rows) → {path}")

    print(f"\n[generate] Done — {len(orders):,} orders | {len(items):,} items | {len(customers):,} customers\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic Olist CSV data")
    parser.add_argument("--orders", type=int, default=5000, help="Number of orders to generate (default: 5000)")
    args = parser.parse_args()
    main(n_orders=args.orders)

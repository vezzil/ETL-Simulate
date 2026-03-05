"""
transform_products.py
─────────────────────
Builds the clean product dimension by merging two sources:

  1. Olist raw_products.csv (product_id, category_name, dimensions/weight)
  2. FakeStoreAPI api_products.json (id, title, price, category, rating)

Since the product IDs from Olist and FakeStoreAPI are different systems,
enrichment is done by fuzzy category mapping: we map Olist Portuguese category
names to their English equivalents and then attach average API price/rating
per category as a representative enrichment.

Operations:
  1. Load & clean Olist products (null category → 'uncategorized')
  2. Load FakeStoreAPI products JSON
  3. Build a PT→EN category mapping
  4. Compute per-category average api_price and api_rating from API data
  5. Merge enrichment into Olist products
  6. Rename columns to match dim_product schema

Output: data/processed/products_clean.csv
"""

import json
import os
import sys

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("transform.products")

# ── Portuguese → English category mapping (Olist → FakeStoreAPI) ─────────────
# FakeStoreAPI has 4 categories: electronics, jewelery, men's clothing, women's clothing
PT_TO_EN_CATEGORY: dict[str, str] = {
    "eletronicos": "electronics",
    "informatica_acessorios": "electronics",
    "telefonia": "electronics",
    "tablets_impressao_imagem": "electronics",
    "audio": "electronics",
    "eletrodomesticos": "electronics",
    "eletrodomesticos_2": "electronics",
    "utilidades_domesticas": "electronics",
    "cama_mesa_banho": "women's clothing",
    "fashion_bolsas_e_acessorios": "women's clothing",
    "fashion_roupa_feminina": "women's clothing",
    "fashion_roupa_masculina": "men's clothing",
    "fashion_calcados": "men's clothing",
    "fashion_underwear_e_moda_praia": "men's clothing",
    "fashion_esporte": "men's clothing",
    "joias_relogios": "jewelery",
    "relogios_presentes": "jewelery",
    "bijuterias_e_jóias": "jewelery",
}

DEFAULT_CATEGORY = "electronics"   # fallback for unmapped categories


def _load_olist_products(raw_dir: str) -> pd.DataFrame:
    path = os.path.join(raw_dir, "raw_products.csv")
    log.info(f"Reading {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.dropna(subset=["product_id"], inplace=True)
    df["product_category_name"] = (
        df["product_category_name"].str.strip().str.lower().fillna("uncategorized")
    )
    return df


def _load_api_products(raw_dir: str, staging_files: dict) -> list[dict]:
    path = os.path.join(raw_dir, staging_files["api_products"])
    if not os.path.exists(path):
        log.warning(f"API products file not found at {path} — enrichment will use defaults")
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_api_enrichment(api_products: list[dict]) -> pd.DataFrame:
    """
    Compute average price and rating per API category.
    Returns a DataFrame with columns: api_category, avg_price, avg_rating.
    """
    if not api_products:
        return pd.DataFrame(columns=["api_category", "avg_price", "avg_rating"])

    rows = []
    for p in api_products:
        rating = p.get("rating", {})
        rows.append(
            {
                "api_category": str(p.get("category", "")).lower(),
                "price": float(p.get("price", 0.0)),
                "rating": float(rating.get("rate", 0.0)) if isinstance(rating, dict) else 0.0,
            }
        )
    df = pd.DataFrame(rows)
    enrichment = (
        df.groupby("api_category")
        .agg(avg_price=("price", "mean"), avg_rating=("rating", "mean"))
        .reset_index()
    )
    return enrichment


def transform_products(config: dict | None = None) -> pd.DataFrame:
    """
    Merge Olist product data with FakeStoreAPI enrichment and return a
    clean DataFrame shaped for dim_product.
    """
    cfg = config or load_config()
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])
    staging_files = cfg["staging_files"]

    # ── Load sources ──────────────────────────────────────────────────────────
    olist = _load_olist_products(raw_dir)
    api_products = _load_api_products(raw_dir, staging_files)
    api_enrichment = _build_api_enrichment(api_products)
    log.info(f"  Olist products: {len(olist):,} | API enrichment categories: {len(api_enrichment)}")

    # ── Map Olist PT category → EN category ───────────────────────────────────
    olist["api_category"] = (
        olist["product_category_name"]
        .map(PT_TO_EN_CATEGORY)
        .fillna(DEFAULT_CATEGORY)
    )

    # ── Merge enrichment ──────────────────────────────────────────────────────
    if not api_enrichment.empty:
        merged = olist.merge(api_enrichment, on="api_category", how="left")
        merged["avg_price"] = merged["avg_price"].fillna(0.0)
        merged["avg_rating"] = merged["avg_rating"].fillna(0.0)
    else:
        merged = olist.copy()
        merged["avg_price"] = 0.0
        merged["avg_rating"] = 0.0

    # ── Build dim_product columns ─────────────────────────────────────────────
    # Use Olist category as the canonical category name; api_category as English label
    dim_product = merged[[
        "product_id",
        "product_category_name",
        "api_category",
        "avg_price",
        "avg_rating",
    ]].rename(
        columns={
            "product_category_name": "category",
            "api_category": "name",
            "avg_price": "api_price",
            "avg_rating": "api_rating",
        }
    ).drop_duplicates(subset=["product_id"])

    log.info(f"  Final dim_product rows: {len(dim_product):,}")
    return dim_product


def save_products(df: pd.DataFrame, config: dict | None = None) -> str:
    cfg = config or load_config()
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    os.makedirs(processed_dir, exist_ok=True)
    out_path = os.path.join(processed_dir, cfg["staging_files"]["products"])
    df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"Saved clean products → {out_path}")
    return out_path


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== TRANSFORM PRODUCTS: START ===")
    cfg = load_config()
    df = transform_products(cfg)
    save_products(df, cfg)
    log.info(f"=== TRANSFORM PRODUCTS: DONE — {len(df):,} product records ===")


if __name__ == "__main__":
    run()

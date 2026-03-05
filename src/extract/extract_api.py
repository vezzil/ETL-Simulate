"""
extract_api.py
──────────────
Fetches product catalog data from the FakeStoreAPI REST endpoint
(https://fakestoreapi.com/products) and writes it as a JSON file to the
raw landing zone.

Simulates a real API extract with:
  - Retry logic with exponential back-off
  - Timeout enforcement
  - Schema validation on response items
  - Safe file write (atomic rename)

Called as an Airflow task (no arguments — config drives behaviour).
Can also be run standalone:

    python src/extract/extract_api.py
"""

import json
import os
import sys
import time
from typing import Any

import requests

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("extract.api")

# ── Expected fields in each product record ────────────────────────────────────
REQUIRED_FIELDS = {"id", "title", "price", "category", "rating"}


def _validate_record(record: dict) -> bool:
    """Return True if all required fields are present."""
    return REQUIRED_FIELDS.issubset(record.keys())


def fetch_products(config: dict | None = None) -> list[dict[str, Any]]:
    """
    Call the FakeStoreAPI /products endpoint and return the parsed JSON list.

    Retries up to `max_retries` times with exponential back-off on transient
    HTTP errors (5xx) or connection failures.
    """
    cfg = config or load_config()
    api_cfg = cfg["api"]

    base_url: str = api_cfg["base_url"]
    endpoint: str = api_cfg["products_endpoint"]
    timeout: int = api_cfg["timeout_seconds"]
    max_retries: int = api_cfg["max_retries"]

    url = base_url.rstrip("/") + endpoint
    log.info(f"Fetching products from {url} (timeout={timeout}s, max_retries={max_retries})")

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            products: list[dict] = response.json()

            if not isinstance(products, list):
                raise ValueError(f"Expected a JSON array, got: {type(products)}")

            # ── Validate records ──────────────────────────────────────────────
            valid = [p for p in products if _validate_record(p)]
            invalid_count = len(products) - len(valid)
            if invalid_count:
                log.warning(f"Dropped {invalid_count} records missing required fields")

            log.info(f"Fetched {len(valid)} valid product records from API")
            return valid

        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response else "?"
            if exc.response is not None and exc.response.status_code < 500:
                # 4xx — no point retrying
                raise RuntimeError(f"API returned client error {status_code}: {exc}") from exc
            last_exc = exc
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc

        wait = 2 ** attempt
        log.warning(f"Attempt {attempt}/{max_retries} failed. Retrying in {wait}s … ({last_exc})")
        time.sleep(wait)

    raise RuntimeError(
        f"API extraction failed after {max_retries} attempts. Last error: {last_exc}"
    )


def save_api_extract(products: list[dict], config: dict | None = None) -> str:
    """
    Write the products list as a pretty-printed JSON file to the raw landing zone.
    Uses an atomic write (temp file + rename) to avoid partial writes.

    Returns the path to the saved file.
    """
    cfg = config or load_config()
    raw_dir = resolve_path(cfg["paths"]["raw_dir"])
    os.makedirs(raw_dir, exist_ok=True)

    out_path = os.path.join(raw_dir, cfg["staging_files"]["api_products"])
    tmp_path = out_path + ".tmp"

    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    os.replace(tmp_path, out_path)   # atomic on most OS
    log.info(f"Saved API extract → {out_path} ({len(products)} records)")
    return out_path


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== EXTRACT API: START ===")
    cfg = load_config()
    products = fetch_products(cfg)
    save_api_extract(products, cfg)
    log.info(f"=== EXTRACT API: DONE — {len(products)} products ===")


if __name__ == "__main__":
    run()

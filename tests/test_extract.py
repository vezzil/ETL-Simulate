"""
test_extract.py
───────────────
Unit tests for the extract layer.
Tests use small fixture files and a temp SQLite DB — no real network or file I/O.
"""

import json
import os
import sqlite3
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ── Path setup ────────────────────────────────────────────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _PROJECT_ROOT)

FIXTURES = os.path.join(_HERE, "fixtures")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_config(tmp_dir: str, csv_dir: str | None = None) -> dict:
    raw_dir = os.path.join(tmp_dir, "raw")
    os.makedirs(raw_dir, exist_ok=True)
    return {
        "paths": {
            "raw_dir": raw_dir,
            "csv_dir": csv_dir or os.path.join(raw_dir, "csv"),
            "logs_dir": os.path.join(tmp_dir, "logs"),
            "operational_db": os.path.join(raw_dir, "operational.db"),
        },
        "source_files": {
            "customers": "olist_customers_dataset.csv",
            "orders": "olist_orders_dataset.csv",
            "order_items": "olist_order_items_dataset.csv",
            "products": "olist_products_dataset.csv",
        },
        "staging_files": {
            "api_products": "api_products.json",
            "order_events": "order_events.csv",
        },
        "api": {
            "base_url": "https://fakestoreapi.com",
            "products_endpoint": "/products",
            "timeout_seconds": 10,
            "max_retries": 1,
        },
    }


def _seed_csv_fixtures(csv_dir: str) -> None:
    """Copy sample fixture CSVs into csv_dir with the expected Olist names."""
    import shutil
    os.makedirs(csv_dir, exist_ok=True)
    mapping = {
        "sample_customers.csv": "olist_customers_dataset.csv",
        "sample_orders.csv": "olist_orders_dataset.csv",
        "sample_order_items.csv": "olist_order_items_dataset.csv",
        "sample_products.csv": "olist_products_dataset.csv",
    }
    for src_name, dst_name in mapping.items():
        shutil.copy(os.path.join(FIXTURES, src_name), os.path.join(csv_dir, dst_name))


def _seed_operational_db(db_path: str) -> None:
    """Create a minimal operational DB for testing."""
    conn = sqlite3.connect(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id TEXT PRIMARY KEY, customer_id TEXT,
            created_at TEXT, expected_delivery TEXT, channel TEXT
        );
        CREATE TABLE IF NOT EXISTS order_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT, status TEXT, event_timestamp TEXT,
            seller_id TEXT, notes TEXT
        );
    """)
    conn.execute("INSERT INTO raw_orders VALUES ('ord1','cust1','2017-01-01','2017-01-20','web')")
    conn.executemany(
        "INSERT INTO order_events (order_id, status, event_timestamp, seller_id, notes) VALUES (?,?,?,?,?)",
        [
            ("ord1", "created", "2017-01-01 10:00:00", None, None),
            ("ord1", "shipped", "2017-01-05 10:00:00", "seller1", None),
        ],
    )
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CSV Extract Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractCsv:

    def test_extract_reads_all_four_files(self, tmp_path):
        csv_dir = str(tmp_path / "csv")
        _seed_csv_fixtures(csv_dir)
        cfg = _make_config(str(tmp_path), csv_dir=csv_dir)

        from src.extract.extract_csv import extract_csv
        dfs = extract_csv(cfg)

        assert set(dfs.keys()) == {"customers", "orders", "order_items", "products"}
        assert len(dfs["customers"]) > 0
        assert len(dfs["orders"]) > 0

    def test_extract_raises_on_missing_file(self, tmp_path):
        cfg = _make_config(str(tmp_path))   # csv_dir has no files
        os.makedirs(cfg["paths"]["csv_dir"], exist_ok=True)

        from src.extract.extract_csv import extract_csv
        with pytest.raises(FileNotFoundError, match="olist_customers"):
            extract_csv(cfg)

    def test_save_raw_extracts_writes_files(self, tmp_path):
        csv_dir = str(tmp_path / "csv")
        _seed_csv_fixtures(csv_dir)
        cfg = _make_config(str(tmp_path), csv_dir=csv_dir)

        from src.extract.extract_csv import extract_csv, save_raw_extracts
        dfs = extract_csv(cfg)
        save_raw_extracts(dfs, cfg)

        raw_dir = cfg["paths"]["raw_dir"]
        assert os.path.exists(os.path.join(raw_dir, "raw_customers.csv"))
        assert os.path.exists(os.path.join(raw_dir, "raw_orders.csv"))


# ─────────────────────────────────────────────────────────────────────────────
# API Extract Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractApi:

    def _mock_response(self, data: list) -> MagicMock:
        mock = MagicMock()
        mock.json.return_value = data
        mock.raise_for_status = MagicMock()
        mock.status_code = 200
        return mock

    def test_fetch_products_returns_valid_records(self, tmp_path):
        with open(os.path.join(FIXTURES, "sample_api_products.json")) as f:
            sample = json.load(f)

        cfg = _make_config(str(tmp_path))
        with patch("requests.get", return_value=self._mock_response(sample)):
            from src.extract.extract_api import fetch_products
            result = fetch_products(cfg)

        assert len(result) == len(sample)
        assert all("id" in p and "price" in p for p in result)

    def test_fetch_products_drops_invalid_records(self, tmp_path):
        bad_data = [{"id": 1, "title": "ok", "price": 10.0, "category": "electronics", "rating": {"rate": 4.0, "count": 5}},
                    {"id": 2}]  # missing required fields

        cfg = _make_config(str(tmp_path))
        with patch("requests.get", return_value=self._mock_response(bad_data)):
            from src.extract.extract_api import fetch_products
            result = fetch_products(cfg)

        assert len(result) == 1

    def test_save_api_extract_writes_json(self, tmp_path):
        products = [{"id": 1, "title": "Test", "price": 9.99, "category": "electronics", "rating": {"rate": 4.0, "count": 50}}]
        cfg = _make_config(str(tmp_path))

        from src.extract.extract_api import save_api_extract
        path = save_api_extract(products, cfg)

        assert os.path.exists(path)
        with open(path) as f:
            loaded = json.load(f)
        assert loaded[0]["id"] == 1

    def test_fetch_retries_on_server_error(self, tmp_path):
        import requests as req_module

        cfg = _make_config(str(tmp_path))
        mock_exc = req_module.exceptions.ConnectionError("timeout")

        with patch("requests.get", side_effect=mock_exc):
            from src.extract import extract_api
            # Re-import to get fresh module state
            import importlib
            importlib.reload(extract_api)
            with pytest.raises(RuntimeError, match="failed after"):
                extract_api.fetch_products(cfg)


# ─────────────────────────────────────────────────────────────────────────────
# SQLite Extract Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractSqlite:

    def test_extract_order_events_returns_dataframe(self, tmp_path):
        db_path = str(tmp_path / "raw" / "operational.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        _seed_operational_db(db_path)

        cfg = _make_config(str(tmp_path))
        cfg["paths"]["operational_db"] = db_path

        from src.extract.extract_sqlite import extract_order_events
        df = extract_order_events(cfg)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert "order_id" in df.columns
        assert "status" in df.columns

    def test_extract_raises_when_db_missing(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        # operational_db path points to non-existent file

        from src.extract.extract_sqlite import extract_order_events
        with pytest.raises(FileNotFoundError, match="Operational DB not found"):
            extract_order_events(cfg)

    def test_save_db_extract_writes_csv(self, tmp_path):
        db_path = str(tmp_path / "raw" / "operational.db")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        _seed_operational_db(db_path)

        cfg = _make_config(str(tmp_path))
        cfg["paths"]["operational_db"] = db_path

        from src.extract.extract_sqlite import extract_order_events, save_db_extract
        df = extract_order_events(cfg)
        path = save_db_extract(df, cfg)

        assert os.path.exists(path)
        loaded = pd.read_csv(path)
        assert len(loaded) == 2

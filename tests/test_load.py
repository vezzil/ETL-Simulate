"""
test_load.py
────────────
Unit tests for the load layer (load_dwh.py) and quality checks (quality_check.py).
Uses an in-memory or temp-file SQLite DWH.
"""

import os
import shutil
import sqlite3
import sys

import pandas as pd
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _PROJECT_ROOT)

FIXTURES = os.path.join(_HERE, "fixtures")


def _make_config(tmp_dir: str) -> dict:
    processed_dir = os.path.join(tmp_dir, "processed")
    dwh_dir = os.path.join(tmp_dir, "dwh")
    logs_dir = os.path.join(tmp_dir, "logs")
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(dwh_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    return {
        "paths": {
            "processed_dir": processed_dir,
            "dwh_db": os.path.join(dwh_dir, "ecommerce_dwh.db"),
            "dwh_dir": dwh_dir,
            "logs_dir": logs_dir,
        },
        "staging_files": {
            "customers": "customers_clean.csv",
            "orders": "orders_clean.csv",
            "products": "products_clean.csv",
            "dim_date": "dim_date.csv",
            "api_products": "api_products.json",
            "order_events": "order_events.csv",
        },
        "validation": {
            "valid_statuses": ["delivered", "shipped", "processing", "canceled",
                               "invoiced", "approved", "unavailable", "created"],
            "min_price": 0.0,
            "max_null_ratio_fact_fk": 0.05,
        },
    }


def _write_processed_fixtures(processed_dir: str) -> None:
    """Write minimal clean staging CSVs that the load module expects."""

    # customers_clean.csv
    pd.DataFrame({
        "customer_id": ["cust1", "cust2", "cust3"],
        "customer_unique_id": ["u1", "u2", "u3"],
        "city": ["São Paulo", "Rio De Janeiro", "Curitiba"],
        "state": ["SP", "RJ", "PR"],
        "zip_code": ["01310", "20040", "80010"],
    }).to_csv(os.path.join(processed_dir, "customers_clean.csv"), index=False)

    # products_clean.csv
    pd.DataFrame({
        "product_id": ["prod1", "prod2"],
        "category": ["eletronicos", "joias_relogios"],
        "name": ["electronics", "jewelery"],
        "api_price": [99.00, 250.00],
        "api_rating": [4.2, 4.6],
    }).to_csv(os.path.join(processed_dir, "products_clean.csv"), index=False)

    # dim_date.csv
    pd.DataFrame({
        "date_key": [20171002, 20171105],
        "full_date": ["2017-10-02", "2017-11-05"],
        "year": [2017, 2017],
        "month": [10, 11],
        "day": [2, 5],
        "quarter": [4, 4],
        "weekday": ["Monday", "Sunday"],
    }).to_csv(os.path.join(processed_dir, "dim_date.csv"), index=False)

    # orders_clean.csv
    pd.DataFrame({
        "order_id": ["order001", "order002"],
        "order_item_seq": [1, 1],
        "customer_id": ["cust1", "cust2"],
        "product_id": ["prod1", "prod2"],
        "date_key": [20171002, 20171105],
        "order_status": ["delivered", "shipped"],
        "price": [99.90, 149.00],
        "freight_value": [8.72, 12.50],
        "quantity": [1, 1],
    }).to_csv(os.path.join(processed_dir, "orders_clean.csv"), index=False)


# ─────────────────────────────────────────────────────────────────────────────
# Load DWH Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadDwh:

    def test_creates_all_tables(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        from src.utils.db_utils import resolve_path

        # Override resolve_path by passing absolute paths directly in config
        cfg["paths"]["dwh_db"] = os.path.join(str(tmp_path), "dwh", "ecommerce_dwh.db")
        counts = load_all(cfg)

        tables = set(counts.keys())
        assert {"fact_orders", "dim_customer", "dim_product", "dim_date", "dim_status"}.issubset(tables)

    def test_dim_customer_rows_loaded(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        counts = load_all(cfg)

        assert counts["dim_customer"] == 3

    def test_dim_product_rows_loaded(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        counts = load_all(cfg)

        assert counts["dim_product"] == 2

    def test_fact_orders_loaded(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        counts = load_all(cfg)

        assert counts["fact_orders"] == 2

    def test_upsert_updates_existing_customer(self, tmp_path):
        """Second run should update existing dim_customer rows (SCD Type 1)."""
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        load_all(cfg)   # first load

        # Update city for cust1
        customers_path = os.path.join(cfg["paths"]["processed_dir"], "customers_clean.csv")
        df = pd.read_csv(customers_path)
        df.loc[df["customer_id"] == "cust1", "city"] = "Campinas"
        df.to_csv(customers_path, index=False)

        load_all(cfg)   # second load (upsert)

        db_path = cfg["paths"]["dwh_db"]
        conn = sqlite3.connect(db_path)
        row = conn.execute("SELECT city FROM dim_customer WHERE customer_id = 'cust1'").fetchone()
        conn.close()

        assert row[0] == "Campinas"

    def test_no_duplicate_facts_on_rerun(self, tmp_path):
        """Running load twice should not duplicate fact_orders rows."""
        cfg = _make_config(str(tmp_path))
        _write_processed_fixtures(cfg["paths"]["processed_dir"])

        from src.load.load_dwh import load_all
        load_all(cfg)
        counts_second = load_all(cfg)

        assert counts_second["fact_orders"] == 2


# ─────────────────────────────────────────────────────────────────────────────
# Quality Check Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestQualityCheck:

    def _run_full_load(self, cfg: dict) -> None:
        _write_processed_fixtures(cfg["paths"]["processed_dir"])
        from src.load.load_dwh import load_all
        load_all(cfg)

    def test_quality_check_passes_on_good_data(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        self._run_full_load(cfg)

        from src.load.quality_check import run_quality_checks
        summary = run_quality_checks(cfg)

        assert summary["failures"] == 0

    def test_quality_check_writes_report_file(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        self._run_full_load(cfg)

        from src.load.quality_check import run_quality_checks
        summary = run_quality_checks(cfg)

        assert os.path.exists(summary["report_path"])

    def test_quality_check_fails_on_empty_fact(self, tmp_path):
        """If fact_orders is empty the row_count check should fail and raise."""
        cfg = _make_config(str(tmp_path))

        # Create schema but leave tables empty
        from src.utils.db_utils import create_dwh_schema
        create_dwh_schema(cfg["paths"]["dwh_db"])

        from src.load.quality_check import run_quality_checks
        with pytest.raises(ValueError, match="Data quality check FAILED"):
            run_quality_checks(cfg)

    def test_all_checks_return_status_field(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        self._run_full_load(cfg)

        from src.load.quality_check import run_quality_checks
        summary = run_quality_checks(cfg)

        for result in summary["results"]:
            assert "check" in result
            assert "status" in result
            assert result["status"] in ("PASS", "FAIL", "WARN")

"""
test_transform.py
─────────────────
Unit tests for the transform layer.
Uses small fixture CSVs copied into a temp directory.
"""

import os
import shutil
import sys

import pandas as pd
import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
sys.path.insert(0, _PROJECT_ROOT)

FIXTURES = os.path.join(_HERE, "fixtures")


def _make_config(tmp_dir: str) -> dict:
    raw_dir = os.path.join(tmp_dir, "raw")
    processed_dir = os.path.join(tmp_dir, "processed")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    return {
        "paths": {
            "raw_dir": raw_dir,
            "processed_dir": processed_dir,
            "logs_dir": os.path.join(tmp_dir, "logs"),
        },
        "staging_files": {
            "customers": "customers_clean.csv",
            "orders": "orders_clean.csv",
            "products": "products_clean.csv",
            "api_products": "api_products.json",
            "dim_date": "dim_date.csv",
            "order_events": "order_events.csv",
        },
        "validation": {
            "valid_statuses": ["delivered", "shipped", "processing", "canceled",
                               "invoiced", "approved", "unavailable", "created"],
            "min_price": 0.0,
            "max_null_ratio_fact_fk": 0.01,
        },
    }


def _stage_raw_files(raw_dir: str) -> None:
    """Copy fixture CSVs to the raw directory with the names that transform modules expect."""
    shutil.copy(os.path.join(FIXTURES, "sample_customers.csv"), os.path.join(raw_dir, "raw_customers.csv"))
    shutil.copy(os.path.join(FIXTURES, "sample_orders.csv"), os.path.join(raw_dir, "raw_orders.csv"))
    shutil.copy(os.path.join(FIXTURES, "sample_order_items.csv"), os.path.join(raw_dir, "raw_order_items.csv"))
    shutil.copy(os.path.join(FIXTURES, "sample_products.csv"), os.path.join(raw_dir, "raw_products.csv"))
    shutil.copy(os.path.join(FIXTURES, "sample_api_products.json"), os.path.join(raw_dir, "api_products.json"))


# ─────────────────────────────────────────────────────────────────────────────
# Transform Customers
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformCustomers:

    def test_deduplicates_by_unique_id(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        # uniq001 appears twice in fixture — should be deduplicated to 1
        assert df["customer_id"].duplicated().sum() == 0

    def test_drops_null_customer_id(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        assert df["customer_id"].isna().sum() == 0

    def test_city_is_title_case(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        for city in df["city"]:
            assert city == city.title() or city == "Unknown", f"City not title-cased: {city}"

    def test_state_is_upper(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        for state in df["state"]:
            assert state == state.upper()

    def test_zip_zero_padded(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        for z in df["zip_code"]:
            assert len(z) == 5

    def test_output_columns_match_schema(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_customers import transform_customers
        df = transform_customers(cfg)

        assert set(df.columns) == {"customer_id", "customer_unique_id", "city", "state", "zip_code"}


# ─────────────────────────────────────────────────────────────────────────────
# Transform Orders
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformOrders:

    def test_returns_tuple_of_two_dataframes(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        result = transform_orders(cfg)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], pd.DataFrame)
        assert isinstance(result[1], pd.DataFrame)

    def test_drops_orders_with_null_customer_id(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        orders_df, _ = transform_orders(cfg)

        # order004 has null customer_id — should be excluded
        assert "order004" not in orders_df["order_id"].values

    def test_filters_zero_price_items(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        orders_df, _ = transform_orders(cfg)

        assert (orders_df["price"] <= 0).sum() == 0

    def test_date_key_is_yyyymmdd_integer(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        orders_df, _ = transform_orders(cfg)

        for dk in orders_df["date_key"].dropna():
            dk_int = int(dk)
            assert 20150101 <= dk_int <= 20300101, f"Unexpected date_key: {dk_int}"

    def test_dim_date_has_correct_columns(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        _, dim_date = transform_orders(cfg)

        expected_cols = {"date_key", "full_date", "year", "month", "day", "quarter", "weekday"}
        assert expected_cols.issubset(set(dim_date.columns))

    def test_quantity_column_is_always_one(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_orders import transform_orders
        orders_df, _ = transform_orders(cfg)

        assert (orders_df["quantity"] == 1).all()


# ─────────────────────────────────────────────────────────────────────────────
# Transform Products
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformProducts:

    def test_returns_dataframe(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_no_duplicate_product_ids(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        assert df["product_id"].duplicated().sum() == 0

    def test_drops_null_product_id(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        assert df["product_id"].isna().sum() == 0

    def test_output_columns_match_schema(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        assert set(df.columns) == {"product_id", "category", "name", "api_price", "api_rating"}

    def test_api_price_enrichment_applied(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        _stage_raw_files(cfg["paths"]["raw_dir"])

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        # Electronics products should have non-zero api_price from fixture
        electronics = df[df["name"] == "electronics"]
        if not electronics.empty:
            assert (electronics["api_price"] > 0).all()

    def test_gracefully_handles_missing_api_file(self, tmp_path):
        cfg = _make_config(str(tmp_path))
        # Only copy product CSV, not the API JSON
        shutil.copy(os.path.join(FIXTURES, "sample_products.csv"),
                    os.path.join(cfg["paths"]["raw_dir"], "raw_products.csv"))

        from src.transform.transform_products import transform_products
        df = transform_products(cfg)

        # Should still return products, just with 0 api_price and api_rating
        assert len(df) > 0
        assert (df["api_price"] == 0.0).all()

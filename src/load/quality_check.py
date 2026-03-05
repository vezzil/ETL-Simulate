"""
quality_check.py
────────────────
Post-load data quality checks on the DWH.

Checks performed:
  1. Row count — fact_orders must have rows
  2. Row count — all dimension tables must have rows
  3. Null FK checks — fact_orders must not have unexpected null FKs
  4. Price range — all prices in fact_orders must be > 0
  5. Orphan FKs — fact_orders FKs must all resolve in their dim tables
  6. Staging row count vs fact count — loaded rows should be ≥ threshold

Results are written to logs/quality_YYYYMMDD.log and also returned as a
structured dict for Airflow task logging.

Called as an Airflow task. Can also be run standalone:

    python src/load/quality_check.py
"""

import os
import sys
from datetime import datetime

import pandas as pd

_HERE = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
sys.path.insert(0, _PROJECT_ROOT)

from src.utils.db_utils import get_connection, load_config, resolve_path
from src.utils.logger import get_logger

log = get_logger("quality.check")

QC_PASS = "PASS"
QC_FAIL = "FAIL"
QC_WARN = "WARN"


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_row_counts(conn) -> list[dict]:
    results = []
    tables = ["fact_orders", "dim_customer", "dim_product", "dim_date", "dim_status"]
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        status = QC_PASS if count > 0 else QC_FAIL
        results.append({
            "check": f"row_count_{table}",
            "status": status,
            "detail": f"{count:,} rows",
        })
    return results


def _check_null_fks(conn, max_null_ratio: float) -> list[dict]:
    fk_cols = ["customer_key", "product_key", "date_key"]
    total = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    results = []

    for col in fk_cols:
        null_count = conn.execute(
            f"SELECT COUNT(*) FROM fact_orders WHERE {col} IS NULL"
        ).fetchone()[0]
        ratio = null_count / total if total > 0 else 0.0
        status = QC_PASS if ratio <= max_null_ratio else QC_FAIL
        results.append({
            "check": f"null_fk_{col}",
            "status": status,
            "detail": f"{null_count:,} nulls ({ratio:.2%})",
        })

    return results


def _check_price_positive(conn) -> list[dict]:
    bad = conn.execute("SELECT COUNT(*) FROM fact_orders WHERE price <= 0").fetchone()[0]
    status = QC_PASS if bad == 0 else QC_FAIL
    return [{
        "check": "price_positive",
        "status": status,
        "detail": f"{bad:,} rows with price ≤ 0",
    }]


def _check_orphan_fks(conn) -> list[dict]:
    checks = [
        ("customer_key", "dim_customer", "customer_key"),
        ("product_key", "dim_product", "product_key"),
        ("date_key", "dim_date", "date_key"),
        ("status_key", "dim_status", "status_key"),
    ]
    results = []
    for fact_col, dim_table, dim_col in checks:
        orphans = conn.execute(f"""
            SELECT COUNT(*) FROM fact_orders f
            WHERE f.{fact_col} IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM {dim_table} d WHERE d.{dim_col} = f.{fact_col}
              )
        """).fetchone()[0]
        status = QC_PASS if orphans == 0 else QC_FAIL
        results.append({
            "check": f"orphan_fk_{fact_col}",
            "status": status,
            "detail": f"{orphans:,} orphan references",
        })
    return results


def _check_staging_vs_fact(conn, processed_dir: str, staging_files: dict) -> list[dict]:
    """Warn if more than 30% of staging rows were dropped during load."""
    staging_path = os.path.join(processed_dir, staging_files["orders"])
    if not os.path.exists(staging_path):
        return [{"check": "staging_vs_fact", "status": QC_WARN, "detail": "Staging file not found"}]

    staging_count = sum(1 for _ in open(staging_path)) - 1   # subtract header
    fact_count = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]

    if staging_count == 0:
        return [{"check": "staging_vs_fact", "status": QC_WARN, "detail": "Staging file is empty"}]

    ratio = fact_count / staging_count
    status = QC_PASS if ratio >= 0.70 else QC_WARN
    return [{
        "check": "staging_vs_fact",
        "status": status,
        "detail": f"{fact_count:,}/{staging_count:,} staging rows loaded ({ratio:.1%})",
    }]


# ─────────────────────────────────────────────────────────────────────────────
# Report writer
# ─────────────────────────────────────────────────────────────────────────────

def _write_report(results: list[dict], logs_dir: str) -> str:
    os.makedirs(logs_dir, exist_ok=True)
    report_path = os.path.join(logs_dir, f"quality_{datetime.now():%Y%m%d_%H%M%S}.log")

    lines = [
        "=" * 60,
        f"  ETL Data Quality Report — {datetime.now():%Y-%m-%d %H:%M:%S}",
        "=" * 60,
    ]
    failures = 0
    warnings = 0
    for r in results:
        icon = "✓" if r["status"] == QC_PASS else ("⚠" if r["status"] == QC_WARN else "✗")
        lines.append(f"  [{r['status']:4s}] {icon}  {r['check']:<30s}  {r['detail']}")
        if r["status"] == QC_FAIL:
            failures += 1
        elif r["status"] == QC_WARN:
            warnings += 1

    lines += [
        "=" * 60,
        f"  Total: {len(results)} checks | {failures} FAILED | {warnings} WARNINGS",
        "=" * 60,
    ]

    report_text = "\n".join(lines)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_text + "\n")

    return report_path


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_quality_checks(config: dict | None = None) -> dict:
    """
    Execute all quality checks and return a summary dict::

        {
            "results": [...],
            "failures": 2,
            "warnings": 1,
            "report_path": "/path/to/quality_20240315_120000.log",
        }
    """
    cfg = config or load_config()
    dwh_db = resolve_path(cfg["paths"]["dwh_db"])
    logs_dir = resolve_path(cfg["paths"]["logs_dir"])
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    staging_files = cfg["staging_files"]
    max_null_ratio = float(cfg["validation"]["max_null_ratio_fact_fk"])

    all_results = []
    with get_connection(dwh_db) as conn:
        all_results += _check_row_counts(conn)
        all_results += _check_null_fks(conn, max_null_ratio)
        all_results += _check_price_positive(conn)
        all_results += _check_orphan_fks(conn)
        all_results += _check_staging_vs_fact(conn, processed_dir, staging_files)

    report_path = _write_report(all_results, logs_dir)

    failures = sum(1 for r in all_results if r["status"] == QC_FAIL)
    warnings = sum(1 for r in all_results if r["status"] == QC_WARN)

    # Log each result
    for r in all_results:
        level = log.info if r["status"] == QC_PASS else (log.warning if r["status"] == QC_WARN else log.error)
        level(f"[{r['status']}] {r['check']}: {r['detail']}")

    log.info(f"Quality report written to: {report_path}")

    if failures > 0:
        raise ValueError(
            f"Data quality check FAILED: {failures} check(s) failed. "
            f"See report: {report_path}"
        )

    return {
        "results": all_results,
        "failures": failures,
        "warnings": warnings,
        "report_path": report_path,
    }


def run(**kwargs) -> None:
    """Airflow-compatible entry point."""
    log.info("=== DATA QUALITY CHECK: START ===")
    summary = run_quality_checks()
    log.info(
        f"=== DATA QUALITY CHECK: DONE — "
        f"{len(summary['results'])} checks | "
        f"{summary['failures']} failed | "
        f"{summary['warnings']} warnings ==="
    )


if __name__ == "__main__":
    run()

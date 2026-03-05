"""
run_pipeline.py
───────────────
Standalone end-to-end ETL pipeline runner — no Airflow required.
Runs all stages in order with colour-coded console output.

Usage:
    python run_pipeline.py                  # full pipeline
    python run_pipeline.py --stage extract  # only extract
    python run_pipeline.py --stage transform
    python run_pipeline.py --stage load
    python run_pipeline.py --stage quality
    python run_pipeline.py --skip-extract   # skip extract (reuse existing raw files)
"""

import argparse
import sys
import time
import traceback
from datetime import datetime

# ── Colour helpers (Windows-safe via colorama if available) ───────────────────
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    GREEN  = Fore.GREEN
    RED    = Fore.RED
    YELLOW = Fore.YELLOW
    CYAN   = Fore.CYAN
    BOLD   = Style.BRIGHT
    RESET  = Style.RESET_ALL
except ImportError:
    GREEN = RED = YELLOW = CYAN = BOLD = RESET = ""


def _header(text: str) -> None:
    print(f"\n{BOLD}{CYAN}{'─' * 60}{RESET}")
    print(f"{BOLD}{CYAN}  {text}{RESET}")
    print(f"{BOLD}{CYAN}{'─' * 60}{RESET}")


def _ok(label: str, elapsed: float) -> None:
    print(f"  {GREEN}✓  {label:<35}{RESET}  {elapsed:.2f}s")


def _fail(label: str, exc: Exception) -> None:
    print(f"  {RED}✗  {label:<35}{RESET}  FAILED")
    print(f"     {RED}{exc}{RESET}")


def run_stage(label: str, fn, *args, **kwargs) -> bool:
    """Run a single ETL function, prints status, returns True on success."""
    t0 = time.perf_counter()
    try:
        fn(*args, **kwargs)
        _ok(label, time.perf_counter() - t0)
        return True
    except Exception as exc:
        _fail(label, exc)
        traceback.print_exc()
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL-Simulate standalone runner")
    parser.add_argument("--stage", choices=["extract", "transform", "load", "quality"],
                        help="Run only a specific stage")
    parser.add_argument("--skip-extract", action="store_true",
                        help="Skip extraction (reuse existing data/raw files)")
    args = parser.parse_args()

    # ── Import all ETL modules ─────────────────────────────────────────────────
    from src.extract.extract_csv    import run as extract_csv
    from src.extract.extract_api    import run as extract_api
    from src.extract.extract_sqlite import run as extract_sqlite
    from src.transform.transform_customers import run as transform_customers
    from src.transform.transform_orders    import run as transform_orders
    from src.transform.transform_products  import run as transform_products
    from src.load.load_dwh       import run as load_dwh
    from src.load.quality_check  import run as quality_check

    _header(f"ETL-Simulate Pipeline  —  {datetime.now():%Y-%m-%d %H:%M:%S}")
    overall_start = time.perf_counter()
    failures: list[str] = []

    def step(label, fn):
        ok = run_stage(label, fn)
        if not ok:
            failures.append(label)
        return ok

    # ── Extract ────────────────────────────────────────────────────────────────
    if args.stage in (None, "extract") and not args.skip_extract:
        _header("EXTRACT")
        step("Extract CSVs  (Olist)",     extract_csv)
        step("Extract API   (FakeStore)", extract_api)
        step("Extract DB    (Operational)", extract_sqlite)
    elif args.skip_extract:
        print(f"\n  {YELLOW}⚠  Extract skipped — using existing raw files{RESET}")

    # ── Transform ──────────────────────────────────────────────────────────────
    if args.stage in (None, "transform"):
        _header("TRANSFORM")
        step("Transform Customers", transform_customers)
        step("Transform Orders",    transform_orders)
        step("Transform Products",  transform_products)

    # ── Load ───────────────────────────────────────────────────────────────────
    if args.stage in (None, "load"):
        _header("LOAD → DWH")
        step("Load DWH  (star schema)", load_dwh)

    # ── Quality ────────────────────────────────────────────────────────────────
    if args.stage in (None, "quality"):
        _header("DATA QUALITY CHECKS")
        step("Quality checks", quality_check)

    # ── Summary ────────────────────────────────────────────────────────────────
    elapsed = time.perf_counter() - overall_start
    _header("SUMMARY")
    if failures:
        print(f"  {RED}{BOLD}FAILED — {len(failures)} stage(s) failed:{RESET}")
        for f in failures:
            print(f"    {RED}• {f}{RESET}")
        sys.exit(1)
    else:
        print(f"  {GREEN}{BOLD}ALL STAGES PASSED  ({elapsed:.1f}s total){RESET}")
        print(f"\n  DWH location : data/dwh/ecommerce_dwh.db")
        print(f"  Quality log  : logs/")
        print(f"\n  Query the DWH:")
        print(f"    python -c \"import sqlite3, pandas as pd; conn = sqlite3.connect('data/dwh/ecommerce_dwh.db'); print(pd.read_sql('SELECT COUNT(*) as facts FROM fact_orders', conn))\"")


if __name__ == "__main__":
    main()

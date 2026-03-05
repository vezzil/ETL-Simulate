"""
etl_ecommerce_dag.py
────────────────────
Apache Airflow DAG for the end-to-end e-commerce ETL pipeline.

DAG graph (fan-in pattern):

    extract_csv ──┐
    extract_api ──┼──► transform_customers ──┐
    extract_db  ──┘    transform_orders    ──┼──► load_dwh ──► data_quality_check ──► notify_success
                       transform_products  ──┘

All extract tasks run in parallel.
All transform tasks run in parallel (after all extracts are done).
Load and quality check run sequentially after all transforms.

Scheduling: daily at midnight UTC
Retries:    1 retry with 5-minute delay per task
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ── Make src/ importable inside the Airflow container ───────────────────────
# The docker-compose mounts the project at /opt/airflow, so the src/ dir is:
_AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
sys.path.insert(0, _AIRFLOW_HOME)

# ── Import ETL modules ────────────────────────────────────────────────────────
from src.extract.extract_csv import run as extract_csv_run
from src.extract.extract_api import run as extract_api_run
from src.extract.extract_sqlite import run as extract_sqlite_run
from src.transform.transform_customers import run as transform_customers_run
from src.transform.transform_orders import run as transform_orders_run
from src.transform.transform_products import run as transform_products_run
from src.load.load_dwh import run as load_dwh_run
from src.load.quality_check import run as quality_check_run

# ── Load config for DAG-level settings ───────────────────────────────────────
import yaml

_CONFIG_PATH = os.path.join(_AIRFLOW_HOME, "config", "config.yaml")
with open(_CONFIG_PATH) as f:
    _cfg = yaml.safe_load(f)

_dag_cfg = _cfg["dag"]

# ── Default task arguments ────────────────────────────────────────────────────
default_args = {
    "owner": "etl-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": _dag_cfg.get("retries", 1),
    "retry_delay": timedelta(minutes=_dag_cfg.get("retry_delay_minutes", 5)),
}


def _on_failure_callback(context: dict) -> None:
    """Log failure details; extend with Slack/email in production."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    exception = context.get("exception")
    print(
        f"[ALERT] DAG '{dag_id}' task '{task_id}' FAILED.\n"
        f"  Exception : {exception}\n"
        f"  Log URL   : {log_url}"
    )


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id=_dag_cfg["dag_id"],
    description="E-Commerce ETL: CSV + API + SQLite → Star Schema DWH",
    default_args=default_args,
    schedule_interval=_dag_cfg["schedule"],
    start_date=datetime.strptime(_dag_cfg["start_date"], "%Y-%m-%d"),
    catchup=_dag_cfg.get("catchup", False),
    max_active_runs=_dag_cfg.get("max_active_runs", 1),
    tags=["etl", "ecommerce", "olist", "sqlite"],
    on_failure_callback=_on_failure_callback,
) as dag:

    # ── Start sentinel ────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Extract layer (run in parallel) ───────────────────────────────────────
    t_extract_csv = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Extract** — Read Olist CSV files from `data/raw/csv/` into raw landing zone.",
    )

    t_extract_api = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Extract** — Fetch product catalog from FakeStoreAPI → `data/raw/api_products.json`.",
    )

    t_extract_db = PythonOperator(
        task_id="extract_db",
        python_callable=extract_sqlite_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Extract** — Query operational SQLite DB → `data/raw/order_events.csv`.",
    )

    # ── Transform layer (run in parallel, after ALL extracts) ─────────────────
    t_transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=transform_customers_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Transform** — Clean & deduplicate customers → `data/processed/customers_clean.csv`.",
    )

    t_transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=transform_orders_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Transform** — Clean orders, join items, build dim_date → `data/processed/orders_clean.csv`.",
    )

    t_transform_products = PythonOperator(
        task_id="transform_products",
        python_callable=transform_products_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Transform** — Merge Olist products + API enrichment → `data/processed/products_clean.csv`.",
    )

    # ── Load layer ────────────────────────────────────────────────────────────
    t_load_dwh = PythonOperator(
        task_id="load_dwh",
        python_callable=load_dwh_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Load** — Upsert all dimensions and insert fact_orders into the SQLite star schema DWH.",
    )

    # ── Data quality gate ─────────────────────────────────────────────────────
    t_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=quality_check_run,
        on_failure_callback=_on_failure_callback,
        doc_md="**Quality** — Row counts, null FK checks, price ranges, orphan FKs. Fails DAG on critical errors.",
    )

    # ── End sentinel ──────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="notify_success")

    # ── Wire dependencies ─────────────────────────────────────────────────────
    #
    #  start
    #    ├── extract_csv ──────────────────────────┐
    #    ├── extract_api ──────────────────────────┤ (fan-in)
    #    └── extract_db ───────────────────────────┘
    #                    ├── transform_customers ──┐
    #                    ├── transform_orders    ──┤ (fan-in)
    #                    └── transform_products  ──┘
    #                                              └── load_dwh ──► data_quality_check ──► notify_success

    extracts = [t_extract_csv, t_extract_api, t_extract_db]
    transforms = [t_transform_customers, t_transform_orders, t_transform_products]

    start >> extracts
    for extract in extracts:
        extract >> transforms
    transforms >> t_load_dwh >> t_quality_check >> end

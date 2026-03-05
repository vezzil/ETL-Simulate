# ETL-Simulate: End-to-End E-Commerce ETL Pipeline

A fully simulated, real-world ETL pipeline for an e-commerce domain, orchestrated with **Apache Airflow**, pulling from multiple source types, and loading into a **SQLite Star Schema data warehouse**.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW DAG                          │
│                                                                     │
│  [extract_csv] ──┐                                                  │
│  [extract_api] ──┼──► [transform_clean] ──► [load_dwh]             │
│  [extract_db]  ──┘         │                    │                   │
│                        (staging)           (star schema)            │
│                                                 │                   │
│                                    [data_quality_check]             │
│                                                 │                   │
│                                       [notify_success]              │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Sources

| Layer | Source | Type | Description |
|---|---|---|---|
| Extract | Olist Brazilian E-Commerce Dataset | CSV | ~100k real orders, customers, products |
| Extract | FakeStoreAPI (`/products`) | REST API | Product catalog enrichment (prices, ratings) |
| Extract | Operational SQLite DB | SQLite | Order event logs (status changes) |

## Target: Star Schema DWH

```
fact_orders ──► dim_customer
            ──► dim_product
            ──► dim_date
            ──► dim_status
```

---

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Python 3.10+ (for running the seed script locally)
- ~2 GB free disk space

### Step 1 — Download Olist Dataset

Download the free dataset from Kaggle:
**https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce**

Extract the following files into `data/raw/csv/`:
```
data/raw/csv/
├── olist_customers_dataset.csv
├── olist_orders_dataset.csv
├── olist_order_items_dataset.csv
└── olist_products_dataset.csv
```

### Step 2 — Seed the Operational Database

```bash
pip install -r requirements.txt
python seeds/seed_operational_db.py
```

This creates `data/raw/operational.db` with ~1,500 mock order events.

### Step 3 — Start Airflow

```bash
# Initialize the database (first run only)
docker-compose up airflow-init

# Start all services
docker-compose up -d
```

Airflow UI → http://localhost:8080  
Username: `airflow` | Password: `airflow`

### Step 4 — Trigger the DAG

1. Open the Airflow UI
2. Find the `etl_ecommerce_pipeline` DAG
3. Toggle it **ON**
4. Click **Trigger DAG** to run manually

### Step 5 — Verify Results

```bash
# Query the DWH
python -c "
import sqlite3, pandas as pd
conn = sqlite3.connect('data/dwh/ecommerce_dwh.db')
print('fact_orders:', pd.read_sql('SELECT COUNT(*) FROM fact_orders', conn).iloc[0,0])
print('dim_customer:', pd.read_sql('SELECT COUNT(*) FROM dim_customer', conn).iloc[0,0])
print('dim_product:', pd.read_sql('SELECT COUNT(*) FROM dim_product', conn).iloc[0,0])
print('dim_date:', pd.read_sql('SELECT COUNT(*) FROM dim_date', conn).iloc[0,0])
conn.close()
"

# Run tests
pytest tests/ -v
```

---

## Project Structure

```
ETL-Simulate/
├── dags/
│   └── etl_ecommerce_dag.py          # Airflow DAG — task graph & scheduling
├── src/
│   ├── extract/
│   │   ├── extract_csv.py            # Reads Olist CSV files
│   │   ├── extract_api.py            # Calls FakeStoreAPI REST endpoint
│   │   └── extract_sqlite.py         # Queries operational SQLite DB
│   ├── transform/
│   │   ├── transform_orders.py       # Clean, join, validate orders+items
│   │   ├── transform_customers.py    # Deduplicate & standardize customers
│   │   └── transform_products.py     # Merge CSV + API, build dim_date
│   ├── load/
│   │   └── load_dwh.py               # Upsert dims, insert facts (SCD Type 1)
│   └── utils/
│       ├── logger.py                 # Structured file + console logger
│       └── db_utils.py               # SQLite connection helpers
├── data/
│   ├── raw/                          # Landing zone (extracted data)
│   │   └── csv/                      # ← place Olist CSVs here
│   ├── processed/                    # Cleaned/transformed staging files
│   └── dwh/
│       └── ecommerce_dwh.db          # Target SQLite data warehouse
├── seeds/
│   └── seed_operational_db.py        # Generates mock operational DB
├── tests/
│   ├── fixtures/                     # Small CSV/JSON fixtures for unit tests
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── config/
│   └── config.yaml                   # Central config (paths, URLs, settings)
├── logs/                             # ETL quality check logs
├── docker-compose.yaml               # Airflow stack (webserver, scheduler, worker)
├── requirements.txt
├── .env                              # Airflow UID env var (auto-created)
└── .gitignore
```

---

## ETL Phases Explained

### Extract
Each source type has a dedicated extractor:
- **CSV**: Reads Olist flat files → writes to `data/raw/` staging CSVs
- **API**: GETs `https://fakestoreapi.com/products` → writes `api_products.json`
- **SQLite**: Queries `order_events` in operational DB → writes `order_events.csv`

### Transform
- Null handling: drop rows with missing order IDs / customer IDs
- Date parsing: `order_purchase_timestamp` → Python `datetime`, build `dim_date`
- Deduplication: customers deduplicated by `customer_unique_id`
- Enrichment: product dim merges Olist category names + API prices/ratings
- Validation: price > 0, quantity > 0, valid status values

### Load (SCD Type 1)
- Dimensions loaded first (INSERT OR REPLACE → overwrite on conflict)
- Facts inserted after all dimensions are populated
- FK integrity enforced in application layer

### Data Quality Checks
- Row count: raw file rows vs rows loaded into DWH
- Null FK checks on `fact_orders`
- Price range validation
- Results written to `logs/quality_YYYYMMDD.log`

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Language | Python 3.10+ |
| Data manipulation | pandas 2.x |
| HTTP | requests |
| Config | PyYAML |
| Database (DWH) | SQLite 3 |
| Testing | pytest |
| Containerization | Docker + Docker Compose |

---

## Key Design Decisions

- **SQLite as DWH** — portable, zero-infra, runs everywhere, supports real SQL analytics
- **Filesystem staging** — tasks communicate via `data/raw/` and `data/processed/` folders mounted as Docker volumes (avoids Airflow XCom size limits)
- **SCD Type 1** — dimension rows overwritten on re-load (appropriate for intermediate simulation)
- **Fan-in DAG pattern** — 3 extracts run in parallel, transform waits on all 3 (realistic dependency modelling)

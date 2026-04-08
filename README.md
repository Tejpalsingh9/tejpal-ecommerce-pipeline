# E-Commerce Sales Pipeline — ShopEasy
### End-to-End Data Engineering Project on Databricks

---

## Project Overview

A complete end-to-end data pipeline built for a fictional e-commerce company **ShopEasy** using **Databricks** and **Delta Lake**. Raw CSV files are ingested into a Bronze layer, cleaned and enriched in a Silver layer, and aggregated into a business-ready Gold layer. The pipeline is orchestrated using Databricks Workflows, governed using Unity Catalog, and includes structured streaming with Auto Loader.

---

## Architecture

```
Raw CSV Files (Volume)
        │
        ▼
  ┌─────────────┐
  │   BRONZE    │  Raw ingestion with metadata columns
  │   Layer     │  Delta tables with constraints + history
  └─────┬───────┘
        │
        ▼
  ┌─────────────┐
  │   SILVER    │  Cleaned, deduplicated, enriched
  │   Layer     │  SCD Type 2, Window Functions, CDF
  └─────┬───────┘
        │
        ▼
  ┌─────────────┐
  │    GOLD     │  Aggregated business views
  │   Layer     │  SQL Dashboard + Live Streaming
  └─────────────┘
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Databricks | Cloud data platform |
| Delta Lake | ACID-compliant storage layer |
| PySpark | Distributed data processing |
| Auto Loader | Incremental file ingestion |
| Databricks SQL | Gold layer querying + dashboards |
| Unity Catalog | Data governance and access control |
| Databricks Workflows | Pipeline orchestration |
| GitHub Repos | Version control |

---

## Dataset

Three synthetic CSV files representing ShopEasy platform:

**orders.csv** — order_id, customer_id, product_id, order_date, quantity, unit_price, status, region

**customers.csv** — customer_id, name, email (PII), city, loyalty_tier, signup_date

**products.csv** — product_id, product_name, category, base_price

---

## Notebooks

| Notebook | Description |
|----------|-------------|
| `tejpal_ecommerce_pipeline` | Workspace setup, catalog/schema/volume creation, secrets API |
| `bronze` | Raw CSV ingestion into Delta bronze tables |
| `silver` | Transformations, SCD Type 2, CDF, OPTIMIZE |
| `Gold` | Gold views for monthly revenue and top products |
| `Streaming` | Auto Loader streaming pipeline with watermark + checkpoint |
| `GOVERNANCE` | Column masking, row-level filter, GRANT privileges |

---

## Pipeline Details

### Bronze Layer
- Reads `customers.csv`, `orders.csv`, `products.csv` from Unity Catalog Volume
- Adds `_ingested_at` (current_timestamp) and `_source_file` metadata columns
- Writes to Delta tables: `tejpal_bronze_customers`, `tejpal_bronze_orders`, `tejpal_bronze_products`
- NOT NULL constraint on `order_id`
- Delta transaction history verified via `DESCRIBE HISTORY`

### Silver Layer
- Deduplication on `order_id`, `customer_id`, `product_id`
- Casts `order_date` to DateType, computes `revenue = quantity * unit_price`
- Window function: `cumulative_revenue` per customer ordered by date
- Joins orders + customers + products into enriched `tejpal_silver_orders`
- Partitioned by `region`
- **SCD Type 2** on `tejpal_silver_customers` using `MERGE INTO` — tracks `loyalty_tier` changes with `is_current`, `effective_start_date`, `effective_end_date`
- Change Data Feed enabled on silver orders
- `OPTIMIZE` + `ZORDER BY (customer_id, order_date)`

### Gold Layer
- `tejpal_gold_monthly_revenue` — monthly revenue aggregated by year, month, region
- `tejpal_gold_top_products` — products ranked by revenue using `RANK()` partitioned by category
- Databricks SQL Dashboard with bar chart (monthly revenue) and top products table

### Streaming
- Auto Loader reads new CSV files incrementally from raw volume
- Watermark: 10-minute threshold on `order_date`
- Writes to `tejpal_gold_live_orders` in append mode
- Checkpoint stored in Unity Catalog Volume — guarantees no duplicate rows on restart
- Uses `.trigger(once=True)` for controlled batch execution

### Workflow Orchestration
- 4-task Databricks Workflow DAG: `Ingest_Bronze → Transform_Silver → Build_Gold → Run_Streaming`
- Job cluster with 30-minute auto-termination
- 2 retries with 5-minute gap on Bronze task
- Email notification on failure
- Scheduled daily at 6 AM UTC

### Unity Catalog Governance
- **Column Masking** — `mask_email()` function masks customer emails to first 2 chars + `****`
- **Row-Level Filter** — `region_filter()` limits analysts to rows matching their region
- **GRANT** — SELECT privilege on gold monthly revenue view granted to analyst user
- All tables registered under `de_workspace26.tejpal_shop`

---

## Setup Instructions

1. Clone this repo into Databricks Repos
2. Upload CSV files to `/Volumes/de_workspace26/tejpal_shop/tejpal_raw_volume/`
3. Run notebooks in this order:
   - `tejpal_ecommerce_pipeline` (setup)
   - `bronze`
   - `silver`
   - `Gold`
   - `Streaming`
   - `GOVERNANCE`
4. Or trigger the Databricks Workflow `tejpal_ecommerce_pipeline` which runs all steps automatically

---

## Author

**Tejpal Singh**
tejpal.singh@sigmoidanalytics.com

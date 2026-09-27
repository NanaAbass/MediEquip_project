# MediEquip Ghana — dbt + PostgreSQL ETL Pipeline
 
**Stack:** dbt Core + Self-hosted PostgreSQL + GitHub Actions

---

## Architecture Overview

```
S3 Raw CSVs
    │
    ▼
ingest_from_s3.py                  ← pandas + boto3 + SQLAlchemy
    │  Downloads CSVs from S3, loads into PostgreSQL raw schema
    ▼
PostgreSQL: raw.*                  ← 5 tables (sales, shipments, inventory, ar, suppliers)
    │
    ▼
dbt test (sources)                 ← validates raw data before any transformation
    │  All assert_* from mediequip_utils.py → declarative YAML tests
    ▼
dbt build
  ├── models/staging/              ← views: type casting, column swap fix, trimming
  │     stg_sales_transactions
  │     stg_import_shipments
  │     stg_inventory_snapshots
  │     stg_accounts_receivable
  │     stg_supplier_performance
  │
  └── models/marts/
      ├── dimensions/              ← built FIRST; fact tables depend on these
      │     dim_date               (generated — no source data needed)
      │     dim_customer           (derived from fact_sales + fact_ar)
      │     dim_product            (derived from fact_sales + fact_inventory)
      │     dim_supplier           (derived from fact_shipments + fact_supplier_performance)
      │
      └── facts/                  ← surrogate FKs resolved against dimensions
            fact_sales              (incremental — insert-only on transaction_id)
            fact_shipments          (incremental — insert-only on shipment_id)
            fact_inventory          (incremental — insert-only on date_key+product_name)
            fact_ar                 (incremental — delete+insert upsert on invoice_id)
            fact_supplier_performance (full refresh — 92 rows rebuilt every run)
    │
    ▼
PostgreSQL: marts.*               ← complete star schema — Power BI connects here
    │                                (dims + facts in one schema)
    ▼
export_parquet_to_s3.py           ← pandas + pyarrow → S3 Parquet (audit/archival)
    │
    ▼
S3 Processed/ (Parquet)           ← date-partitioned snapshots
```

**Orchestration:** GitHub Actions cron (`0 2 * * *` = 2:00 AM GMT)

---

## Star Schema (mediequip_dw.marts)

Power BI connects to the `marts` schema and sees a complete star schema:

```
                    ┌─────────────┐
                    │  dim_date   │
                    │─────────────│
                    │ date_key PK │◄────────────────────────────────────┐
                    │ full_date   │                                     │
                    │ year        │        ┌──────────────┐             │
                    │ quarter     │        │ dim_customer │             │
                    │ month       │        │──────────────│             │
                    │ week        │        │customer_key  │◄──────┐     │
                    │ is_weekend  │        │customer_name │       │     │
                    └─────────────┘        │ghana_region  │       │     │
                                           │customer_type │       │     │
                                           │ownership     │       │     │
          ┌──────────────┐                 └──────────────┘       │     │
          │ dim_product  │                                        │     │
          │──────────────│                                        │     │
          │ product_key  │◄───────────────────────────────┐      │     │
          │ product_name │                                 │      │     │
          │ category     │     ┌─────────────────────┐    │      │     │
          │ unit_cost    │     │     fact_sales       │    │      │     │
          │ reorder_lvl  │     │─────────────────────│    │      │     │
          └──────────────┘     │ transaction_id  PK  │    │      │     │
                               │ date_key        FK  │────┼──────┼─────┘
          ┌──────────────┐     │ product_key     FK  │────┘      │
          │ dim_supplier │     │ customer_key    FK  │───────────┘
          │──────────────│     │ supplier_key    FK  │──────────────────┐
          │ supplier_key │◄────│ revenue_usd         │                  │
          │ supplier     │  ┌──│ cogs_usd            │                  │
          │ country      │  │  │ gross_profit_usd    │                  │
          │ tier         │  │  │ quantity            │                  │
          │ composite    │  │  └─────────────────────┘                  │
          │ otd_pct      │  │                                           │
          └──────────────┘  │  ┌─────────────────────┐                 │
               ▲            │  │   fact_shipments     │                 │
               │            │  │─────────────────────│                 │
               └────────────┼──│ shipment_id     PK  │                 │
                            │  │ ship_date_key   FK  │                 │
                            │  │ arrival_date_key FK │                 │
                            │  │ product_key     FK  │                 │
                            │  │ supplier_key    FK  │                 │
                            │  │ landed_cost_usd     │                 │
                            │  └─────────────────────┘                 │
                            │                                           │
                            │  ┌─────────────────────┐                 │
                            │  │   fact_inventory     │                 │
                            │  │─────────────────────│                 │
                            │  │ date_key        FK  │                 │
                            │  │ product_key     FK  │                 │
                            │  │ qty_on_hand         │                 │
                            │  │ stock_value_usd     │                 │
                            │  │ below_reorder       │                 │
                            │  └─────────────────────┘                 │
                            │                                           │
                            │  ┌─────────────────────┐                 │
                            │  │      fact_ar         │                 │
                            │  │─────────────────────│                 │
                            │  │ invoice_id      PK  │                 │
                            │  │ invoice_date_key FK │                 │
                            │  │ due_date_key    FK  │                 │
                            │  │ customer_key    FK  │                 │
                            │  │ outstanding_usd     │                 │
                            │  │ ageing_bucket       │                 │
                            │  │ credit_risk_flag    │                 │
                            │  └─────────────────────┘                 │
                            │                                           │
                            │  ┌─────────────────────────────┐         │
                            └──│  fact_supplier_performance   │─────────┘
                               │─────────────────────────────│
                               │ supplier_key            FK  │
                               │ year                        │
                               │ on_time_delivery_pct        │
                               │ defect_rate_pct             │
                               │ composite_score             │
                               │ supplier_tier               │
                               └─────────────────────────────┘
```

---

## Cost Comparison

| Component | Old (AWS) | New (dbt + PostgreSQL) |
|---|---|---|
| Compute | AWS Glue ~$0.44/DPU-hr × 5 jobs | GitHub Actions — **free** |
| Orchestration | Step Functions + EventBridge | GitHub Actions cron — **free** |
| Database | RDS SQL Server ($100–400/mo) | Local PostgreSQL — **free** |
| Secrets | AWS Secrets Manager | GitHub Actions Secrets — **free** |
| Monitoring | CloudWatch ($3–10/mo) | GitHub Actions logs — **free** |
| **Total** | **~$150–450/month** | **~$1–5/month (S3 only)** |

---

## Project Structure

```
mediequip_dbt/
├── dbt_project.yml                      # project config — profile: mediequip_postgres
├── profiles.yml                         # PostgreSQL connection (copy to ~/.dbt/)
├── packages.yml                         # dbt_utils dependency
├── requirements.txt                     # Python dependencies
├── setup_postgres.sql                   # ONE-TIME: create DB, user, schemas, pgcrypto
├── ingest_from_s3.py                    # Stage 1: S3 CSVs → PostgreSQL raw schema
├── export_parquet_to_s3.py              # Stage 4: PostgreSQL marts → S3 Parquet
│
├── models/
│   ├── staging/
│   │   ├── sources.yml                  # source declarations + all raw DQ tests
│   │   ├── stg_sales_transactions.sql
│   │   ├── stg_import_shipments.sql
│   │   ├── stg_inventory_snapshots.sql
│   │   ├── stg_accounts_receivable.sql
│   │   └── stg_supplier_performance.sql
│   │
│   └── marts/
│       ├── schema.yml                   # fact table docs + DQ tests
│       ├── fact_sales.sql
│       ├── fact_shipments.sql
│       ├── fact_inventory.sql
│       ├── fact_ar.sql                  # upsert via delete+insert on invoice_id
│       ├── fact_supplier_performance.sql
│       │
│       └── dimensions/
│           ├── schema.yml               # dimension docs + DQ tests
│           ├── dim_date.sql             # generated calendar — no source needed
│           ├── dim_customer.sql         # derived from fact_sales + fact_ar
│           ├── dim_product.sql          # derived from fact_sales + fact_inventory
│           └── dim_supplier.sql         # derived from fact_shipments + fact_supplier_performance
│
├── macros/
│   ├── mediequip_macros.sql             # date_key(), audit_cols(), row_hash(), custom tests
│   └── generate_schema_name.sql        # clean schema names (staging / marts)
│
└── .github/
    └── workflows/
        └── nightly_etl.yml             # 2AM GMT schedule — replaces Step Functions
```

---

## dbt Build Order (DAG)

dbt resolves dependencies automatically via `ref()`. The build order on every
`dbt build` run is:

```
[1] Staging views (parallel)
    stg_sales_transactions
    stg_import_shipments
    stg_inventory_snapshots
    stg_accounts_receivable
    stg_supplier_performance

        ↓

[2] Dimension tables (parallel — depend on staging)
    dim_date               ← no staging dependency (pure generate_series)
    dim_customer           ← stg_sales_transactions + stg_accounts_receivable
    dim_product            ← stg_sales_transactions + stg_inventory_snapshots
    dim_supplier           ← stg_import_shipments + stg_supplier_performance

        ↓

[3] Fact tables (parallel — depend on staging + dimensions)
    fact_sales                  ← dim_product, dim_customer, dim_supplier
    fact_shipments              ← dim_product, dim_supplier
    fact_inventory              ← dim_product
    fact_ar                     ← dim_customer
    fact_supplier_performance   ← dim_supplier
```

Dimensions are always built before facts. You never need to manage this order manually.

---

## One-Time Setup (Local Development)

### 1. Install PostgreSQL

**macOS:**
```bash
brew install postgresql@16
brew services start postgresql@16
```

**Ubuntu/Debian:**
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql
```

### 2. Initialise the database

```bash
# Edit setup_postgres.sql first — replace 'your_strong_password_here'
sudo -u postgres psql -f setup_postgres.sql
```

This creates:
- Database: `mediequip_dw`
- User: `mediequip_dbt` (read/write — used by dbt and ingestion scripts)
- Schemas: `raw`, `staging`, `marts`
- Extension: `pgcrypto` (required for SHA-256 row hashing)
- Optional: `powerbi_reader` read-only user (uncomment in `setup_postgres.sql`)

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
dbt deps
```

### 4. Configure dbt connection

```bash
# Copy profiles.yml to dbt's default location
cp profiles.yml ~/.dbt/profiles.yml

# Set your database password
export MEDIEQUIP_DB_PASSWORD=your_strong_password_here

# Verify connection before first run
dbt debug
```

### 5. Create a .env file for local development

```bash
cat > .env << 'ENVEOF'
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-west-1
S3_RAW_BUCKET=mediequip-datalake-raw-ACCOUNT_ID
S3_PROCESSED_BUCKET=mediequip-datalake-processed-ACCOUNT_ID
MEDIEQUIP_DB_PASSWORD=your_strong_password_here
MEDIEQUIP_DB_HOST=localhost
ENVEOF
```

---

## Running the Pipeline Locally

```bash
# Load environment variables
source .env

# Stage 1: Download S3 CSVs → PostgreSQL raw schema
python ingest_from_s3.py

# Stage 2: Validate raw sources (runs all DQ tests against raw.*)
dbt test --select "source:mediequip_raw"

# Stage 3: Build everything — dimensions first, then facts (dbt handles order)
dbt build

# Stage 4: Export fact + dimension tables to S3 as Parquet
python export_parquet_to_s3.py
```

### Useful one-off commands

```bash
# Full rebuild from scratch (drops and recreates all incremental tables)
dbt build --full-refresh

# Rebuild and test a single model
dbt run --select fact_ar
dbt test --select fact_ar

# Rebuild all dimensions only
dbt run --select "marts.dimensions"

# Rebuild a dimension and every fact that depends on it
dbt run --select "dim_customer+"

# Run all tests without re-running transforms
dbt test

# View the full lineage graph in a browser
dbt docs generate && dbt docs serve
```

---

## GitHub Actions Setup (Production)

Add these secrets under **GitHub repo → Settings → Secrets → Actions**:

| Secret | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | AWS credential for S3 access |
| `AWS_SECRET_ACCESS_KEY` | AWS credential for S3 access |
| `S3_RAW_BUCKET` | e.g. `mediequip-datalake-raw-123456789` |
| `S3_PROCESSED_BUCKET` | e.g. `mediequip-datalake-processed-123456789` |
| `MEDIEQUIP_DB_HOST` | Server IP or hostname (e.g. `192.168.1.10`) |
| `MEDIEQUIP_DB_PASSWORD` | PostgreSQL password for `mediequip_dbt` user |
| `SLACK_WEBHOOK_URL` | *(optional)* Slack alert webhook |

The pipeline fires automatically at **2:00 AM GMT** every night. Trigger it manually
from the GitHub Actions tab with options for full refresh or targeting a single model.

---

## Power BI Connection

Power BI connects directly to the `marts` schema where both dimensions and facts live.

### Connection steps
1. Open Power BI Desktop → **Get Data → PostgreSQL database**
2. Server: `localhost` (or your VPS IP for production)
3. Database: `mediequip_dw`
4. Credentials: use the `powerbi_reader` read-only user (see `setup_postgres.sql`)
5. Select the `marts` schema — all 9 tables appear:

| Table | Type | Description |
|---|---|---|
| `dim_date` | Dimension | Calendar table 2020–2030; join on `date_key` |
| `dim_customer` | Dimension | Customers with region, type, ownership |
| `dim_product` | Dimension | Products with category, cost, reorder level |
| `dim_supplier` | Dimension | Suppliers with tier, scorecard, lifetime spend |
| `fact_sales` | Fact | Transaction line items |
| `fact_shipments` | Fact | Import shipments with landed cost |
| `fact_inventory` | Fact | Monthly stock snapshots |
| `fact_ar` | Fact | Invoices with ageing bucket + credit risk flag |
| `fact_supplier_performance` | Fact | Annual supplier scorecard |

### Relationships to define in Power BI Model view

| From (Fact) | Key | To (Dimension) |
|---|---|---|
| `fact_sales.date_key` | → | `dim_date.date_key` |
| `fact_sales.product_key` | → | `dim_product.product_key` |
| `fact_sales.customer_key` | → | `dim_customer.customer_key` |
| `fact_sales.supplier_key` | → | `dim_supplier.supplier_key` |
| `fact_shipments.ship_date_key` | → | `dim_date.date_key` |
| `fact_shipments.product_key` | → | `dim_product.product_key` |
| `fact_shipments.supplier_key` | → | `dim_supplier.supplier_key` |
| `fact_inventory.date_key` | → | `dim_date.date_key` |
| `fact_inventory.product_key` | → | `dim_product.product_key` |
| `fact_ar.invoice_date_key` | → | `dim_date.date_key` |
| `fact_ar.customer_key` | → | `dim_customer.customer_key` |
| `fact_supplier_performance.supplier_key` | → | `dim_supplier.supplier_key` |

> **Note:** `fact_shipments` has two date FKs (`ship_date_key` and `arrival_date_key`).
> In Power BI, set `ship_date_key` as the active relationship and `arrival_date_key`
> as inactive — use `USERELATIONSHIP()` in DAX measures that need the arrival date.

> **Tip:** Never expose the `mediequip_dbt` write-user credentials to Power BI.
> Always use the read-only `powerbi_reader` account.

---

## Key Design Decisions

### Dimension sourcing strategy

| Dimension | Primary sources | Deduplication logic |
|---|---|---|
| `dim_date` | `generate_series()` — no source data | N/A — mathematically complete |
| `dim_customer` | `fact_sales` + `fact_ar` | `DISTINCT ON (customer_name)` — most complete row wins |
| `dim_product` | `fact_sales` (supplier) + `fact_inventory` (cost/reorder) | `FULL OUTER JOIN` — latest inventory snapshot wins |
| `dim_supplier` | `fact_shipments` (port) + `fact_supplier_performance` (scorecard) | `DISTINCT ON (supplier) ORDER BY year DESC` — most recent year wins |

### Surrogate key strategy
All four dimensions use `ROW_NUMBER() OVER (ORDER BY natural_key)` to generate
stable integer surrogate keys. Because dimensions are full-refresh tables rebuilt
on every run, `ROW_NUMBER()` produces the same key for the same natural key value
on every run — as long as the underlying data doesn't change. Fact tables join
against these via `LEFT JOIN ... COALESCE(dim.key, -1)`: the `-1` fallback ensures
new fact rows never fail to insert if a dimension hasn't caught up yet.

### Incremental load strategy

| Model | Strategy | Key | Rationale |
|---|---|---|---|
| `fact_sales` | insert-only | `transaction_id` | Transactions are immutable once posted |
| `fact_shipments` | insert-only | `shipment_id` | Shipments don't change after arrival |
| `fact_inventory` | insert-only | `(date_key, product_name)` | Monthly snapshots are point-in-time |
| `fact_ar` | delete+insert | `invoice_id` | Outstanding balance changes as payments arrive |
| `fact_supplier_performance` | full refresh | — | 92 rows — faster to rebuild than merge |
| All dimensions | full refresh | — | Small tables; rebuilt to capture any attribute changes |

### Column swap fix (staging layer)
The source data bug where `customer_type` contains Ghana region names and
`region` contains hospital/customer type labels is corrected in the staging
layer — `stg_sales_transactions.sql` and `stg_accounts_receivable.sql`.
Dimensions and facts downstream all read the corrected column names.

### pgcrypto dependency
The `row_hash()` macro requires the `pgcrypto` extension for `DIGEST()`.
Enabled by `setup_postgres.sql`. If you see `function digest(text, unknown) does not exist`:
```sql
\connect mediequip_dw
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

---

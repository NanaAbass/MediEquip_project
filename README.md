# MediEquip Ghana — dbt + PostgreSQL ETL Pipeline

**Data Engineer:** Ama Boateng
**Migrated from:** AWS Glue + Step Functions + RDS SQL Server
**Stack:** dbt Core + Self-hosted PostgreSQL + GitHub Actions

---

## Architecture Overview

```
S3 Raw CSVs
    │
    ▼
ingest_from_s3.py              ← pandas + boto3 + SQLAlchemy
    │  Downloads CSVs from S3, loads into PostgreSQL raw schema
    ▼
PostgreSQL: raw.*              ← 5 tables (sales, shipments, inventory, ar, suppliers)
    │
    ▼
dbt test (sources)             ← validates raw data before any transformation
    │  All assert_* from mediequip_utils.py → declarative YAML tests
    ▼
dbt build
  ├── models/staging/          ← views: type casting, column swap fix, trimming
  │     stg_sales_transactions
  │     stg_import_shipments
  │     stg_inventory_snapshots
  │     stg_accounts_receivable
  │     stg_supplier_performance
  │
  └── models/marts/            ← tables: business logic, derived cols, incremental
        fact_sales              (incremental — insert-only on transaction_id)
        fact_shipments          (incremental — insert-only on shipment_id)
        fact_inventory          (incremental — insert-only on date_key+product_name)
        fact_ar                 (incremental — delete+insert upsert on invoice_id)
        fact_supplier_performance (full refresh — 92 rows rebuilt every run)
    │
    ▼
PostgreSQL: marts.*            ← Power BI connects here
    │
    ▼
export_parquet_to_s3.py        ← pandas + pyarrow → S3 Parquet (audit/archival)
    │
    ▼
S3 Processed/ (Parquet)        ← date-partitioned snapshots
```

**Orchestration:** GitHub Actions cron (`0 2 * * *` = 2:00 AM GMT)

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
├── dbt_project.yml                  # project config — profile: mediequip_postgres
├── profiles.yml                     # PostgreSQL connection (copy to ~/.dbt/)
├── packages.yml                     # dbt_utils dependency
├── requirements.txt                 # Python dependencies
├── setup_postgres.sql               # ONE-TIME: create DB, user, schemas, pgcrypto
├── ingest_from_s3.py                # Stage 1: S3 CSVs → PostgreSQL raw schema
├── export_parquet_to_s3.py          # Stage 4: PostgreSQL marts → S3 Parquet
│
├── models/
│   ├── staging/
│   │   ├── sources.yml              # source declarations + all raw DQ tests
│   │   ├── stg_sales_transactions.sql
│   │   ├── stg_import_shipments.sql
│   │   ├── stg_inventory_snapshots.sql
│   │   ├── stg_accounts_receivable.sql
│   │   └── stg_supplier_performance.sql
│   │
│   └── marts/
│       ├── schema.yml               # model docs + mart-level DQ tests
│       ├── fact_sales.sql
│       ├── fact_shipments.sql
│       ├── fact_inventory.sql
│       ├── fact_ar.sql              # upsert via delete+insert on invoice_id
│       └── fact_supplier_performance.sql
│
├── macros/
│   ├── mediequip_macros.sql         # date_key(), audit_cols(), row_hash(), tests
│   └── generate_schema_name.sql    # ensures clean schema names (staging / marts)
│
└── .github/
    └── workflows/
        └── nightly_etl.yml          # 2AM GMT schedule — replaces Step Functions
```

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
# Run as the postgres superuser
# Edit setup_postgres.sql first — replace 'your_strong_password_here'
sudo -u postgres psql -f setup_postgres.sql
```

This creates:
- Database: `mediequip_dw`
- User: `mediequip_dbt`
- Schemas: `raw`, `staging`, `marts`
- Extension: `pgcrypto` (required for SHA-256 row hashing)

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
dbt deps
```

### 4. Configure dbt connection

```bash
# Copy profiles.yml to dbt's default location
cp profiles.yml ~/.dbt/profiles.yml

# Set your database password in the environment
export MEDIEQUIP_DB_PASSWORD=your_strong_password_here

# Verify connection
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
# Load .env
source .env

# Stage 1: Ingest S3 CSVs → PostgreSQL raw schema
python ingest_from_s3.py

# Stage 2: Validate raw sources
dbt test --select "source:mediequip_raw"

# Stage 3: Build all staging views + 5 fact tables
dbt build

# Stage 4: Export fact tables to S3 as Parquet
python export_parquet_to_s3.py
```

### Useful one-off commands

```bash
# Full rebuild from scratch (drops and recreates all incremental tables)
dbt build --full-refresh

# Rebuild a single model only
dbt run --select fact_ar
dbt test --select fact_ar

# Check for test failures without re-running transforms
dbt test

# View the DAG (dependency graph)
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
| `MEDIEQUIP_DB_HOST` | Your server IP or hostname (e.g. `192.168.1.10`) |
| `MEDIEQUIP_DB_PASSWORD` | PostgreSQL password for `mediequip_dbt` user |
| `SLACK_WEBHOOK_URL` | *(optional)* Slack alert webhook |

The pipeline fires automatically at **2:00 AM GMT** every night. You can also
trigger it manually from the GitHub Actions tab with options for full refresh
or targeting a single model.

---

## Power BI Connection

Power BI connects directly to the `marts` schema in PostgreSQL.

1. Open Power BI Desktop → **Get Data → PostgreSQL database**
2. Server: `localhost` (or your server IP for production)
3. Database: `mediequip_dw`
4. Use the `powerbi_reader` read-only user (uncomment in `setup_postgres.sql`)
5. Navigate to the `marts` schema — all five fact tables are here:
   - `marts.fact_sales`
   - `marts.fact_shipments`
   - `marts.fact_inventory`
   - `marts.fact_ar`
   - `marts.fact_supplier_performance`

> **Tip:** Create the `powerbi_reader` read-only user (commented section in
> `setup_postgres.sql`) before connecting Power BI. Never expose the
> `mediequip_dbt` write-user credentials to Power BI.

---

## Key Design Decisions

### PostgreSQL-specific changes from the DuckDB version

| Area | DuckDB | PostgreSQL |
|---|---|---|
| Date key formatting | `STRFTIME(col, '%Y%m%d')` | `TO_CHAR(col, 'YYYYMMDD')` |
| Row hashing | `SHA256(CONCAT_WS(...))` | `ENCODE(DIGEST(..., 'sha256'), 'hex')` via pgcrypto |
| String cast | `CAST(x AS VARCHAR)` | `CAST(x AS TEXT)` |
| AR upsert strategy | `merge` | `delete+insert` |
| DB connection | File path | Host/port/credentials via env vars |
| Schema naming | Automatic | `generate_schema_name` macro for clean names |

### Incremental Load Strategy

| Model | Strategy | Key | Rationale |
|---|---|---|---|
| `fact_sales` | insert-only | `transaction_id` | Transactions are immutable once posted |
| `fact_shipments` | insert-only | `shipment_id` | Shipments don't change after arrival |
| `fact_inventory` | insert-only | `(date_key, product_name)` | Monthly snapshots are point-in-time |
| `fact_ar` | delete+insert | `invoice_id` | Outstanding balance changes as payments arrive |
| `fact_supplier_performance` | full refresh | — | 92 rows — faster to rebuild than merge |

### Column Swap Fix (Jobs 01 & 04)
The source data bug where `customer_type` contains Ghana region names and
`region` contains hospital/customer type labels is corrected in the staging
layer — `stg_sales_transactions.sql` and `stg_accounts_receivable.sql`.

### pgcrypto Dependency
The `row_hash()` macro requires the `pgcrypto` extension for `DIGEST()`.
This is enabled by `setup_postgres.sql`. If you see an error like
`function digest(text, unknown) does not exist`, run:
```sql
\connect mediequip_dw
CREATE EXTENSION IF NOT EXISTS pgcrypto;
```

---

## Complete AWS → PostgreSQL Component Map

| Old Component | New Component |
|---|---|
| AWS Glue (5 jobs) | dbt models (5 staging + 5 mart) |
| `mediequip_utils.py` | `macros/mediequip_macros.sql` + `ingest_from_s3.py` |
| Step Functions state machine | GitHub Actions workflow stages |
| EventBridge cron | GitHub Actions `schedule: cron` |
| AWS Secrets Manager | GitHub Actions Secrets + env vars |
| AWS RDS SQL Server | Self-hosted PostgreSQL (`mediequip_dw`) |
| CloudWatch alarms | GitHub Actions failure notifications |
| S3 Parquet writes | `export_parquet_to_s3.py` |

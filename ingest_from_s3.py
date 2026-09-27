#!/usr/bin/env python3
"""
ingest_from_s3.py
─────────────────────────────────────────────────────────────────────────────
Pre-dbt ingestion script for MediEquip Ghana pipeline.

ROLE IN THE PIPELINE:
  This script runs BEFORE `dbt build`. It:
    1. Downloads the five raw CSVs from S3 into pandas DataFrames
    2. Loads them into the `raw` schema of the local PostgreSQL database
  dbt then reads from the `raw` schema and transforms into the `staging`
  and `marts` schemas.



TARGET DATABASE: Self-hosted PostgreSQL (local machine for dev)

USAGE:
  # Local development
  python ingest_from_s3.py

  # GitHub Actions
  python ingest_from_s3.py --env prod

ENVIRONMENT VARIABLES REQUIRED:
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION       (default: eu-west-1)
  S3_RAW_BUCKET            (e.g. mediequip-datalake-raw-123456789)
  MEDIEQUIP_DB_PASSWORD    PostgreSQL password for mediequip_dbt user
  MEDIEQUIP_DB_HOST        (default: localhost)
  MEDIEQUIP_DB_PORT        (default: 5432)
  MEDIEQUIP_DB_NAME        (default: mediequip_dw)
  MEDIEQUIP_DB_USER        (default: mediequip_dbt)

Author : Ama Boateng, Data Engineer — MediEquip Ghana
"""

import os
import sys
import urllib.parse
import logging
import argparse
from io import BytesIO
from datetime import datetime, timezone

import boto3
import pandas as pd
from sqlalchemy import create_engine, text

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mediequip_ingest")


# ─── Configuration ────────────────────────────────────────────────────────────
SOURCE_TABLES = {
    "sales_transactions":   "stg_sales",
    "import_shipments":     "stg_shipments",
    "inventory_snapshots":  "stg_inventory",
    "accounts_receivable":  "stg_receivables",
    "supplier_performance": "stg_supplier",
}

MIN_ROW_COUNTS = {
    "sales_transactions":   1000,
    "import_shipments":     100,
    "inventory_snapshots":  100,
    "accounts_receivable":  50,
    "supplier_performance": 20,
}

RAW_SCHEMA = "raw"


# ─── Database connection ──────────────────────────────────────────────────────
def get_pg_engine():
    """Build a SQLAlchemy engine for the self-hosted PostgreSQL database."""
    host     = os.environ.get("MEDIEQUIP_DB_HOST", "localhost")
    port     = os.environ.get("MEDIEQUIP_DB_PORT", "5433")
    dbname   = os.environ.get("MEDIEQUIP_DB_NAME", "mediequip_dw")
    user     = os.environ.get("MEDIEQUIP_DB_USER", "mediequip_dbt")
    raw_password = os.environ.get("MEDIEQUIP_DB_PASSWORD")
    safe_password = urllib.parse.quote_plus(raw_password) if raw_password else None

    if not safe_password:
        log.error("MEDIEQUIP_DB_PASSWORD environment variable not set. Aborting.")
        sys.exit(1)

    url = f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}"
    engine = create_engine(url, pool_pre_ping=True)
    log.info("PostgreSQL connection: %s@%s:%s/%s", user, host, port, dbname)
    return engine


# ─── S3 helpers ───────────────────────────────────────────────────────────────
def get_s3_client():
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    return boto3.client("s3", region_name=region)


def list_csv_keys(s3, bucket: str, prefix: str) -> list[str]:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                keys.append(obj["Key"])
    return keys


def read_csv_from_s3(s3, bucket: str, prefix: str) -> pd.DataFrame:
    keys = list_csv_keys(s3, bucket, prefix)
    if not keys:
        raise FileNotFoundError(f"No CSV files found at s3://{bucket}/{prefix}")

    frames = []
    for key in keys:
        log.info("  Reading s3://%s/%s", bucket, key)
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(obj["Body"].read()), encoding="cp1252")  # handle special chars
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    log.info("  Total rows: %d  Cols: %d", len(combined), len(combined.columns))
    return combined


# ─── DQ pre-check ─────────────────────────────────────────────────────────────
def assert_min_rows(df: pd.DataFrame, table_name: str, minimum: int) -> None:
    if len(df) < minimum:
        raise ValueError(
            f"[{table_name}] Row count FAILED — expected >= {minimum}, got {len(df)}."
        )
    log.info("[%s] Row count OK: %d rows", table_name, len(df))


# ─── PostgreSQL write ─────────────────────────────────────────────────────────
def ensure_raw_schema(engine) -> None:
    """Create the raw schema if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}"))
    log.info("Schema '%s' ready", RAW_SCHEMA)


def load_to_postgres(engine, df: pd.DataFrame, table_name: str) -> None:
    """
    Load a pandas DataFrame into PostgreSQL raw.<table_name>.
    Uses if_exists='replace' — raw layer is always rebuilt fresh each run.
    pandas to_sql with method='multi' batches inserts for performance.
    """
    full_name = f"{RAW_SCHEMA}.{table_name}"
    log.info("Loading %d rows → %s", len(df), full_name)

    # Sanitise column names: lowercase, replace spaces with underscores
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    df.to_sql(
        name=table_name,
        con=engine,
        schema=RAW_SCHEMA,
        if_exists="replace",       # drop + recreate on each run (raw layer)
        index=False,
        method="multi",            # batch inserts — faster than row-by-row
        chunksize=1000,
    )

    # Verify row count in database
    with engine.connect() as conn:
        count = conn.execute(
            text(f"SELECT COUNT(*) FROM {full_name}")
        ).scalar()
    log.info("Loaded %d rows → %s ✓", count, full_name)


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="MediEquip S3 → PostgreSQL ingestion")
    parser.add_argument("--env", default="dev", choices=["dev", "prod"])
    args = parser.parse_args()

    bucket = os.environ.get("S3_RAW_BUCKET")
    if not bucket:
        log.error("S3_RAW_BUCKET environment variable not set. Aborting.")
        sys.exit(1)

    log.info("=" * 60)
    log.info("MediEquip Ghana — S3 → PostgreSQL Ingestion")
    log.info("  Environment : %s", args.env)
    log.info("  S3 bucket   : s3://%s", bucket)
    log.info("  UTC time    : %s", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    s3     = get_s3_client()
    engine = get_pg_engine()
    ensure_raw_schema(engine)

    failed_tables = []

    for table_name, prefix in SOURCE_TABLES.items():
        log.info("── Ingesting: %s ──────────────────────────", table_name)
        try:
            df = read_csv_from_s3(s3, bucket, prefix)
            assert_min_rows(df, table_name, MIN_ROW_COUNTS[table_name])
            load_to_postgres(engine, df, table_name)
        except Exception as exc:
            log.error("[%s] FAILED: %s", table_name, exc)
            failed_tables.append(table_name)

    engine.dispose()

    if failed_tables:
        log.error("Ingestion FAILED for: %s", ", ".join(failed_tables))
        sys.exit(1)

    log.info("=" * 60)
    log.info("All 5 tables ingested into PostgreSQL raw schema.")
    log.info("Ready for `dbt build`.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()

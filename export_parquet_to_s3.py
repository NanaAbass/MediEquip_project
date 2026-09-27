#!/usr/bin/env python3
"""
export_parquet_to_s3.py
─────────────────────────────────────────────────────────────────────────────
Post-dbt export script for MediEquip Ghana pipeline.

ROLE IN THE PIPELINE:
  Runs AFTER `dbt build`. Reads each fact table from PostgreSQL marts schema,
  exports to Parquet, and uploads to the S3 processed/ bucket.

TARGET DATABASE: Self-hosted PostgreSQL

ENVIRONMENT VARIABLES REQUIRED:
  AWS_ACCESS_KEY_ID
  AWS_SECRET_ACCESS_KEY
  AWS_DEFAULT_REGION       (default: eu-west-1)
  S3_PROCESSED_BUCKET      (e.g. mediequip-datalake-processed-123456789)
  MEDIEQUIP_DB_PASSWORD
  MEDIEQUIP_DB_HOST        (default: localhost)
  MEDIEQUIP_DB_PORT        (default: 5433)
  MEDIEQUIP_DB_NAME        (default: mediequip_dw)
  MEDIEQUIP_DB_USER        (default: mediequip_dbt)

Author : Ama Boateng, Data Engineer — MediEquip Ghana
"""

import os
import sys
import urllib.parse
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mediequip_export")

MARTS_SCHEMA = "marts"

EXPORT_TABLES = {
    "fact_sales":                "sales/",
    "fact_shipments":            "shipments/",
    "fact_inventory":            "inventory/",
    "fact_ar":                   "accounts-receivable/",
    "fact_supplier_performance": "supplier-performance/",
}


def get_pg_engine():
    host     = os.environ.get("MEDIEQUIP_DB_HOST", "localhost")
    port     = os.environ.get("MEDIEQUIP_DB_PORT", "5433")
    dbname   = os.environ.get("MEDIEQUIP_DB_NAME", "mediequip_dw")
    user     = os.environ.get("MEDIEQUIP_DB_USER", "mediequip_dbt")
    raw_password = os.environ.get("MEDIEQUIP_DB_PASSWORD")
    safe_password = urllib.parse.quote_plus(raw_password)
    if not safe_password:
        log.error("MEDIEQUIP_DB_PASSWORD not set.")
        sys.exit(1)
    return create_engine(
        f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}",
        pool_pre_ping=True,
    )


def export_table_to_s3(engine, s3, table: str, bucket: str, prefix: str, run_date: str) -> None:
    """Read a PostgreSQL mart table → Parquet → upload to S3."""
    temp_dir = os.path.join(os.getcwd(), "tmp")
    os.makedirs(temp_dir, exist_ok=True)
    local_path = os.path.join(temp_dir, f"{table}_{run_date}.parquet")
    s3_key     = f"{prefix}{run_date}/{table}.parquet"

    log.info("Exporting %s.%s → s3://%s/%s", MARTS_SCHEMA, table, bucket, s3_key)

    df = pd.read_sql_table(table, con=engine, schema=MARTS_SCHEMA)
    df.to_parquet(local_path, engine="pyarrow", compression="snappy", index=False)

    s3.upload_file(local_path, bucket, s3_key)
    log.info("Uploaded %d rows → s3://%s/%s ✓", len(df), bucket, s3_key)
    os.remove(local_path)


def main():
    bucket = os.environ.get("S3_PROCESSED_BUCKET")
    if not bucket:
        log.error("S3_PROCESSED_BUCKET not set.")
        sys.exit(1)

    region   = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("MediEquip Ghana — PostgreSQL → S3 Parquet Export")
    log.info("  S3 bucket : s3://%s", bucket)
    log.info("  Run date  : %s", run_date)
    log.info("=" * 60)

    s3     = boto3.client("s3", region_name=region)
    engine = get_pg_engine()

    failed = []
    for table, prefix in EXPORT_TABLES.items():
        try:
            export_table_to_s3(engine, s3, table, bucket, prefix, run_date)
        except Exception as exc:
            log.error("[%s] Export FAILED: %s", table, exc)
            failed.append(table)

    engine.dispose()

    if failed:
        log.error("Export FAILED for: %s", ", ".join(failed))
        sys.exit(1)

    log.info("All 5 tables exported to S3 ✓")


if __name__ == "__main__":
    main()

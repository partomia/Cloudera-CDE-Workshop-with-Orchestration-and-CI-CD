"""Unit tests for ingest_raw.py."""

import pytest
import os
import tempfile
from pyspark.sql import SparkSession

from jobs.ingest.ingest_raw import ingest


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-ingest")
        .getOrCreate()
    )


@pytest.fixture
def sample_csv(tmp_path):
    csv_content = (
        "order_id,customer_id,product_id,order_date,quantity,unit_price,category\n"
        "ORD001,CUST01,PROD01,2026-01-15,2,29.99,electronics\n"
        "ORD002,CUST02,PROD02,2026-01-16,1,9.99,books\n"
    )
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(csv_content)
    return str(tmp_path)


def test_ingest_returns_correct_count(spark, sample_csv, tmp_path):
    output_path = str(tmp_path / "raw")
    count = ingest(spark, sample_csv, output_path)
    assert count == 2


def test_ingest_writes_parquet(spark, sample_csv, tmp_path):
    output_path = str(tmp_path / "raw")
    ingest(spark, sample_csv, output_path)
    df = spark.read.parquet(output_path)
    assert df.count() == 2


def test_ingest_adds_audit_columns(spark, sample_csv, tmp_path):
    output_path = str(tmp_path / "raw")
    ingest(spark, sample_csv, output_path)
    df = spark.read.parquet(output_path)
    assert "_ingested_at" in df.columns
    assert "_source_file" in df.columns

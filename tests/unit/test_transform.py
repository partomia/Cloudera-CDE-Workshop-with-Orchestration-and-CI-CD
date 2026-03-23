"""Unit tests for transform.py business logic."""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from jobs.transform.transform import clean, enrich, aggregate_by_customer, aggregate_by_category


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test-transform")
        .getOrCreate()
    )


@pytest.fixture
def sample_schema():
    return StructType([
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("product_id", StringType()),
        StructField("order_date", StringType()),
        StructField("quantity", StringType()),
        StructField("unit_price", StringType()),
        StructField("category", StringType()),
    ])


@pytest.fixture
def sample_data(spark, sample_schema):
    rows = [
        ("ORD001", "CUST01", "PROD01", "2026-01-15", "2", "29.99", "electronics"),
        ("ORD002", "CUST02", "PROD02", "2026-01-16", "1", "9.99",  "  Books  "),
        ("ORD003", "CUST01", "PROD03", "2026-01-17", "0", "19.99", "clothing"),  # invalid qty
        ("ORD004", None,     "PROD04", "2026-01-18", "3", "5.00",  "food"),       # null customer
    ]
    return spark.createDataFrame(rows, schema=sample_schema)


class TestClean:
    def test_drops_null_customer_id(self, sample_data):
        result = clean(sample_data)
        customer_ids = [r.customer_id for r in result.collect()]
        assert None not in customer_ids

    def test_filters_zero_quantity(self, sample_data):
        result = clean(sample_data)
        quantities = [r.quantity for r in result.collect()]
        assert all(q > 0 for q in quantities)

    def test_uppercases_category(self, sample_data):
        result = clean(sample_data)
        categories = [r.category for r in result.collect()]
        assert all(c == c.upper() for c in categories)

    def test_trims_category_whitespace(self, sample_data):
        result = clean(sample_data)
        categories = [r.category for r in result.collect()]
        assert "BOOKS" in categories
        assert "  Books  " not in categories


class TestEnrich:
    def test_revenue_column_added(self, spark, sample_schema):
        rows = [("ORD001", "C1", "P1", "2026-01-15", "3", "10.00", "FOOD")]
        df = spark.createDataFrame(rows, schema=sample_schema)
        df_clean = clean(df)
        result = enrich(df_clean)
        assert "revenue" in result.columns

    def test_revenue_calculation(self, spark, sample_schema):
        rows = [("ORD001", "C1", "P1", "2026-01-15", "3", "10.00", "FOOD")]
        df = spark.createDataFrame(rows, schema=sample_schema)
        result = enrich(clean(df)).collect()
        assert abs(result[0].revenue - 30.0) < 0.001

    def test_order_month_format(self, spark, sample_schema):
        rows = [("ORD001", "C1", "P1", "2026-01-15", "1", "5.00", "FOOD")]
        df = spark.createDataFrame(rows, schema=sample_schema)
        result = enrich(clean(df)).collect()
        assert result[0].order_month == "2026-01"


class TestAggregations:
    @pytest.fixture
    def enriched_df(self, spark, sample_schema):
        rows = [
            ("ORD001", "C1", "P1", "2026-01-15", "2", "10.00", "FOOD"),
            ("ORD002", "C1", "P1", "2026-01-20", "1", "10.00", "FOOD"),
            ("ORD003", "C2", "P2", "2026-01-10", "3", "20.00", "ELECTRONICS"),
        ]
        df = spark.createDataFrame(rows, schema=sample_schema)
        return enrich(clean(df))

    def test_customer_aggregation_columns(self, enriched_df):
        result = aggregate_by_customer(enriched_df)
        assert "total_revenue" in result.columns
        assert "order_count" in result.columns

    def test_category_aggregation_columns(self, enriched_df):
        result = aggregate_by_category(enriched_df)
        assert "total_revenue" in result.columns
        assert "avg_unit_price" in result.columns

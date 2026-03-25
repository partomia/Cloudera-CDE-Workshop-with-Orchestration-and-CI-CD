"""
Inline retail orders dataset — shared by all demo scripts.
No file system or S3 dependency. Works seamlessly on CDE Spark Connect.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

SCHEMA = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("order_date",  StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("category",    StringType(),  True),
])

ORDERS = [
    ("ORD0001", "CUST001", "PROD001", "2026-01-05", 2,  29.99, "electronics"),
    ("ORD0002", "CUST002", "PROD002", "2026-01-06", 1,   9.99, "books"),
    ("ORD0003", "CUST003", "PROD003", "2026-01-06", 3,  49.99, "clothing"),
    ("ORD0004", "CUST001", "PROD004", "2026-01-07", 1, 199.99, "electronics"),
    ("ORD0005", "CUST004", "PROD005", "2026-01-08", 5,   4.99, "food"),
    ("ORD0006", "CUST005", "PROD006", "2026-01-08", 2,  89.99, "furniture"),
    ("ORD0007", "CUST002", "PROD001", "2026-01-09", 1,  29.99, "electronics"),
    ("ORD0008", "CUST006", "PROD007", "2026-01-10", 4,  14.99, "sports"),
    ("ORD0009", "CUST003", "PROD008", "2026-01-11", 2,  24.99, "clothing"),
    ("ORD0010", "CUST007", "PROD009", "2026-01-12", 1,   7.99, "books"),
    ("ORD0011", "CUST001", "PROD010", "2026-01-13", 3,  12.99, "food"),
    ("ORD0012", "CUST008", "PROD011", "2026-01-14", 1, 299.99, "electronics"),
    ("ORD0013", "CUST004", "PROD012", "2026-01-15", 2,  39.99, "sports"),
    ("ORD0014", "CUST009", "PROD003", "2026-01-16", 1,  49.99, "clothing"),
    ("ORD0015", "CUST010", "PROD013", "2026-01-17", 6,   3.49, "food"),
    ("ORD0016", "CUST005", "PROD014", "2026-01-18", 1, 149.99, "furniture"),
    ("ORD0017", "CUST002", "PROD015", "2026-01-19", 2,  19.99, "books"),
    ("ORD0018", "CUST006", "PROD016", "2026-01-20", 3,  59.99, "sports"),
    ("ORD0019", "CUST011", "PROD017", "2026-01-21", 1,   9.99, "food"),
    ("ORD0020", "CUST012", "PROD001", "2026-01-22", 2,  29.99, "electronics"),
]

# Intentionally bad rows — used in demo_data_quality.py to show GE catching errors
BAD_ORDERS = list(ORDERS) + [
    (None,      "CUST099", "PROD099", "2026-01-23", 1,   9.99, "electronics"),  # null order_id
    ("ORD0001", "CUST001", "PROD001", "2026-01-05", 2,  29.99, "electronics"),  # duplicate order_id
    ("ORD0021", "CUST099", "PROD099", "2026-01-24", 0,   9.99, "electronics"),  # zero quantity
    ("ORD0022", "CUST099", "PROD099", "2026-01-25", 1,  -5.00, "books"),        # negative price
    ("ORD0023", "CUST099", "PROD099", "2026-01-26", 1,   9.99, "unknown"),      # invalid category
    ("ORD0024", "CUST099", "PROD099", "20260127",   1,   9.99, "food"),         # bad date format
]

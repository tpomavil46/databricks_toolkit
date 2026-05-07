"""
tests/fixtures/scd_data.py — Shared in-memory DataFrames for SCD unit tests.

All fixtures are plain PySpark DataFrames created from Python literals.
No file I/O, no Databricks connection required.

Domain
------
A customer dimension with:
  customer_id  — business key (string)
  name         — attribute (rarely changes)
  email        — attribute (changes when customer updates contact info)
  tier         — attribute (changes when customer upgrades/downgrades)

The fixtures represent a typical SCD scenario:
  - C001: existing customer whose email + tier changed in the new batch
  - C002: existing customer with no changes
  - C003: brand new customer not yet in the dimension table
  - C004: existing customer with a NULL value in a tracked column
"""

from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SOURCE_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
])

TARGET_BASE_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
])

TARGET_SCD2_SCHEMA = StructType([
    StructField("scd_key", StringType(), True),
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("effective_start", TimestampType(), True),
    StructField("effective_end", TimestampType(), True),
    StructField("is_current", BooleanType(), True),
])

TARGET_SCD3_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("prev_email", StringType(), True),
    StructField("prev_tier", StringType(), True),
])


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------

T0 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
T1 = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Source DataFrames (incoming batch from upstream)
# ---------------------------------------------------------------------------

def source_batch(spark: SparkSession):
    """
    Incoming source batch.

    C001: email + tier changed vs. target
    C002: unchanged vs. target
    C003: brand new entity (not in target)
    C004: tier changed to NULL vs. target (tests null-safe change detection)
    """
    data = [
        ("C001", "Alice", "alice@new.com", "Gold"),
        ("C002", "Bob", "bob@example.com", "Silver"),
        ("C003", "Carol", "carol@example.com", "Bronze"),
        ("C004", "Dave", "dave@example.com", None),
    ]
    return spark.createDataFrame(data, SOURCE_SCHEMA)


def source_empty(spark: SparkSession):
    """Empty source batch — nothing to process."""
    return spark.createDataFrame([], SOURCE_SCHEMA)


def source_all_new(spark: SparkSession):
    """Source batch where every row is a new entity."""
    data = [
        ("C010", "Erin", "erin@example.com", "Bronze"),
        ("C011", "Frank", "frank@example.com", "Silver"),
    ]
    return spark.createDataFrame(data, SOURCE_SCHEMA)


# ---------------------------------------------------------------------------
# Target DataFrames (current state of dimension table)
# ---------------------------------------------------------------------------

def target_scd1(spark: SparkSession):
    """
    Current state of a Type-1/Type-0 dimension.

    C001: old email + old tier (will detect change vs. source)
    C002: matches source exactly (unchanged)
    C004: tier is 'Gold' (will detect change when source sends NULL)
    """
    data = [
        ("C001", "Alice", "alice@old.com", "Silver"),
        ("C002", "Bob", "bob@example.com", "Silver"),
        ("C004", "Dave", "dave@example.com", "Gold"),
    ]
    return spark.createDataFrame(data, TARGET_BASE_SCHEMA)


def target_scd2_current_only(spark: SparkSession):
    """
    Current-only rows from a Type-2 dimension (is_current = True).

    Used to test change classification without the full history.
    """
    data = [
        ("hash_c001_v1", "C001", "Alice", "alice@old.com", "Silver", T0, None, True),
        ("hash_c002_v1", "C002", "Bob", "bob@example.com", "Silver", T0, None, True),
        ("hash_c004_v1", "C004", "Dave", "dave@example.com", "Gold", T0, None, True),
    ]
    return spark.createDataFrame(data, TARGET_SCD2_SCHEMA)


def target_scd2_with_history(spark: SparkSession):
    """
    Full Type-2 dimension with both historical and current rows.

    C001 has one historical row (v1) and one current row (v2).
    C002 has one current row (v1).
    """
    data = [
        ("hash_c001_v1", "C001", "Alice", "alice@v1.com", "Bronze", T0, T1, False),
        ("hash_c001_v2", "C001", "Alice", "alice@old.com", "Silver", T1, None, True),
        ("hash_c002_v1", "C002", "Bob", "bob@example.com", "Silver", T0, None, True),
        ("hash_c004_v1", "C004", "Dave", "dave@example.com", "Gold", T0, None, True),
    ]
    return spark.createDataFrame(data, TARGET_SCD2_SCHEMA)


def target_scd3(spark: SparkSession):
    """
    Current state of a Type-3 dimension with prev_ columns.
    """
    data = [
        ("C001", "Alice", "alice@old.com", "Silver", "alice@original.com", "Bronze"),
        ("C002", "Bob", "bob@example.com", "Silver", None, None),
        ("C004", "Dave", "dave@example.com", "Gold", None, None),
    ]
    return spark.createDataFrame(data, TARGET_SCD3_SCHEMA)

"""
tests/fixtures/upsert_data.py — In-memory DataFrames for upsert unit tests.

Domain: same customer dimension as scd_data.py, extended with:
- An 'op' column for CDC delete-aware tests ('I'=insert, 'U'=update, 'D'=delete)
- An 'event_ts' column for late-arriving data tests
- A 'phone' column that exists only in v2 batches (schema evolution tests)
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

T_OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
T_MID = datetime(2024, 6, 1, tzinfo=timezone.utc)
T_NEW = datetime(2024, 12, 1, tzinfo=timezone.utc)
T_LATE = datetime(2024, 3, 1, tzinfo=timezone.utc)  # older than T_MID

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
])

CDC_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("op", StringType(), True),
])

TIMESTAMPED_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("event_ts", TimestampType(), True),
])

EVOLVED_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("tier", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("loyalty_score", StringType(), True),
])


# ---------------------------------------------------------------------------
# Basic merge source/target
# ---------------------------------------------------------------------------

def source_basic(spark: SparkSession):
    """Incoming batch: 1 new (C003), 1 updated (C001), 1 unchanged (C002)."""
    data = [
        ("C001", "Alice", "alice@new.com", "Gold"),
        ("C002", "Bob", "bob@example.com", "Silver"),
        ("C003", "Carol", "carol@example.com", "Bronze"),
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


def target_basic(spark: SparkSession):
    """Current target state: C001 (old email/tier), C002 (unchanged)."""
    data = [
        ("C001", "Alice", "alice@old.com", "Silver"),
        ("C002", "Bob", "bob@example.com", "Silver"),
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


def source_empty(spark: SparkSession):
    return spark.createDataFrame([], CUSTOMER_SCHEMA)


# ---------------------------------------------------------------------------
# Delete-aware (CDC) source
# ---------------------------------------------------------------------------

def source_cdc(spark: SparkSession):
    """
    CDC batch with mixed operations:
    C001: update (op='U')
    C002: no-op update (values same, op='U')
    C003: new insert (op='I')
    C004: delete (op='D')
    """
    data = [
        ("C001", "Alice", "alice@new.com", "Gold", "U"),
        ("C002", "Bob", "bob@example.com", "Silver", "U"),
        ("C003", "Carol", "carol@example.com", "Bronze", "I"),
        ("C004", "Dave", "dave@example.com", "Bronze", "D"),
    ]
    return spark.createDataFrame(data, CDC_SCHEMA)


def target_cdc(spark: SparkSession):
    """Target state: C001, C002, C004 present."""
    data = [
        ("C001", "Alice", "alice@old.com", "Silver"),
        ("C002", "Bob", "bob@example.com", "Silver"),
        ("C004", "Dave", "dave@example.com", "Gold"),
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


# ---------------------------------------------------------------------------
# Idempotent source (with duplicates)
# ---------------------------------------------------------------------------

def source_with_duplicates(spark: SparkSession):
    """Source has two rows for C001 — deduplication needed before MERGE."""
    data = [
        ("C001", "Alice", "alice@v1.com", "Gold"),
        ("C001", "Alice", "alice@v2.com", "Platinum"),  # duplicate key, later version
        ("C002", "Bob", "bob@example.com", "Silver"),
    ]
    return spark.createDataFrame(data, CUSTOMER_SCHEMA)


def source_timestamped_with_duplicates(spark: SparkSession):
    """Duplicates identified by timestamp — most recent should win."""
    data = [
        ("C001", "Alice", "alice@v1.com", "Gold", T_MID),
        ("C001", "Alice", "alice@v2.com", "Platinum", T_NEW),  # newer, should win
        ("C002", "Bob", "bob@example.com", "Silver", T_MID),
    ]
    return spark.createDataFrame(data, TIMESTAMPED_SCHEMA)


# ---------------------------------------------------------------------------
# Late-arriving data
# ---------------------------------------------------------------------------

def source_late_arriving(spark: SparkSession):
    """
    Source batch with mixed recency:
    C001 T_NEW  — newer than target, should be applied
    C002 T_LATE — older than target (T_MID), should be ignored
    C003 T_NEW  — new entity, should be inserted
    """
    data = [
        ("C001", "Alice", "alice@new.com", "Gold", T_NEW),
        ("C002", "Bob", "bob@late.com", "Bronze", T_LATE),
        ("C003", "Carol", "carol@example.com", "Silver", T_NEW),
    ]
    return spark.createDataFrame(data, TIMESTAMPED_SCHEMA)


def target_timestamped(spark: SparkSession):
    """Target with timestamps: C001 and C002 exist at T_MID."""
    data = [
        ("C001", "Alice", "alice@old.com", "Silver", T_MID),
        ("C002", "Bob", "bob@example.com", "Silver", T_MID),
    ]
    return spark.createDataFrame(data, TIMESTAMPED_SCHEMA)


# ---------------------------------------------------------------------------
# Schema evolution
# ---------------------------------------------------------------------------

def source_evolved(spark: SparkSession):
    """V2 source: adds 'phone' and 'loyalty_score' columns not in target."""
    data = [
        ("C001", "Alice", "alice@new.com", "Gold", "+1-555-0001", "950"),
        ("C002", "Bob", "bob@example.com", "Silver", "+1-555-0002", "720"),
        ("C003", "Carol", "carol@example.com", "Bronze", None, None),
    ]
    return spark.createDataFrame(data, EVOLVED_SCHEMA)

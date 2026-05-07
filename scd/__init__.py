"""
scd — Slowly Changing Dimension (SCD) patterns for Delta Lake.

Implements SCD Types 0, 1, 2, 3, 4, and 6, each in both PySpark and
Spark SQL.  Every type has a corresponding pytest test file and a Great
Expectations suite stub in great_expectations/suites/.

Quick reference
---------------
Type 0  Fixed: new rows only, ignore all changes to existing rows.
Type 1  Overwrite: always keep latest value, no history.
Type 2  Full history: effective dates + is_current flag + surrogate key.
Type 3  Partial history: one previous value column per tracked attribute.
Type 4  Separate history table: current table (Type 1) + history table.
Type 6  Hybrid: current row has latest values, historical rows via Type 2,
        prev_<col> on current row via Type 3.

Entry points
------------
from scd.type2_pyspark import apply_scd2, classify_changes
from scd.type2_sql import apply_scd2_sql, build_scd2_sql
from scd.config import SCDConfig
from scd.surrogate_key import add_surrogate_key
"""

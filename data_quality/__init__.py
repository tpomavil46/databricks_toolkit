"""
data_quality/ — Data Quality Checks, Quarantine, and DQ Logging.

Modules
-------
config           DQConfig dataclass and DQResult output type.
checks_pyspark   DataFrame-level DQ checks: null, unique, range, regex, ref integrity.
checks_sql       SQL-generating DQ checks: same checks expressed as aggregate SELECTs.
quarantine_pyspark  Tag and split records into passing / quarantine DataFrames.
quarantine_sql   SQL quarantine INSERT and good-record SELECT builders.
logging_pyspark  Write DQ results to a Delta audit table.
"""

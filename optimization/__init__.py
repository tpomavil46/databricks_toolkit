"""
optimization — Query and Write Performance Tuning for Delta Tables.

Covers the four most impactful levers available in Databricks:

  partitioning_sql   Partition strategy DDL, ZORDER, Liquid Clustering, ANALYZE.
  small_files_sql    OPTIMIZE, auto-compaction, optimizeWrite, target file size.
  bloom_filters_sql  CREATE BLOOMFILTER INDEX — high-cardinality point lookups.
  join_sql           SQL join hints: BROADCAST, MERGE, SHUFFLE_HASH, SKEW.
  skew_pyspark       Skew detection, broadcast join helpers, salt-based skew fix.
"""

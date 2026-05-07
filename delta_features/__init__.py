"""
delta_features/ — Delta Lake Feature Reference.

Modules
-------
time_travel_pyspark   Read historical snapshots via PySpark API options.
time_travel_sql       VERSION AS OF / TIMESTAMP AS OF / RESTORE TABLE SQL.
optimize_sql          OPTIMIZE, ZORDER BY, Liquid Clustering DDL.
vacuum_sql            VACUUM with retention enforcement and dry-run.
cdf_pyspark           Change Data Feed: enable, read as stream or batch.
cdf_sql               table_changes() SQL, CDF filtering, downstream CDC.
deletion_vectors      Deletion Vector DDL and trade-off reference.
table_properties      ALTER TABLE SET/UNSET TBLPROPERTIES helpers.
"""

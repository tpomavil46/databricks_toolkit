from databricks_toolkit.bronze.ingest_customer import run
from pyspark.sql import SparkSession
import shutil
import os


def test_ingest_creates_table(tmp_path):
    # Clean spark-warehouse to avoid overlap
    warehouse_path = tmp_path / "spark-warehouse"
    os.environ["ENV"] = "local"
    if warehouse_path.exists():
        shutil.rmtree(warehouse_path)

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("TestIngestCustomer")
        .config("spark.sql.warehouse.dir", str(warehouse_path))
        .getOrCreate()
    )

    run(spark)

    # Confirm table exists
    tables = [t.name for t in spark.catalog.listTables()]
    assert "customers_cleaned" in tables

    # Confirm record count (from local CSV)
    df = spark.table("customers_cleaned")
    assert df.count() == 3

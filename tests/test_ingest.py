# tests/test_ingest.py

from jobs.bronze.ingest import ingest_data
from utils.session import MyDatabricksSession
from utils.structs import get_column_mapping


def test_ingest_nytaxi_data():
    spark = MyDatabricksSession.get_spark()

    column_mapping = {
        "yellow": get_column_mapping("yellow"),
        "green": get_column_mapping("green"),
    }

    df = ingest_data(
        spark=spark,
        dataset="nyctaxi",
        input_paths={
            "yellow": "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz",
            "green": "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz",
        },
        bronze_output="main.default.test_nytaxi_bronze",
        column_mapping=column_mapping,
        format="delta",
        normalize=True,
    )

    assert df.count() > 0
    assert "vendor" in df.columns
    assert "source" in df.columns
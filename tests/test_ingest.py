def test_ingest_nytaxi_data():
    from jobs.bronze.ingest import ingest_data
    from databricks.connect import DatabricksSession
    from jobs.structs import nyctaxi_schema  # <- Move schema here if not already

    spark = DatabricksSession.builder.getOrCreate()

    df = ingest_data(
        spark=spark,
        dataset="nyctaxi",
        input_paths={
            "yellow": "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz",
            "green": "dbfs:/databricks-datasets/nyctaxi/tripdata/green/green_tripdata_2019-12.csv.gz",
        },
        bronze_output="main.default.test_nytaxi_bronze",
        schema=nyctaxi_schema,
        format="delta",
    )

    assert df.count() > 0
    assert "vendor" in df.columns
    assert "source" in df.columns

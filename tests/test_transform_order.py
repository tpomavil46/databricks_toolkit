import os
import pytest
from pyspark.sql import SparkSession
from jobs.transform_orders import run


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[2]")
        .appName("TestTransformOrders")
        .getOrCreate()
    )


def test_transform_orders_run(spark):
    # Setup test file
    input_path = "data/test_orders.csv"
    os.makedirs("data", exist_ok=True)
    with open(input_path, "w") as f:
        f.write("order_id,description\n1,order_one\n2,order_two\n")

    # Run job with local path override
    run(spark, input_path=input_path, output_table="orders_transformed_test")

    # Verify the output table exists
    df = spark.sql("SELECT * FROM orders_transformed_test")
    rows = df.collect()
    assert len(rows) == 2
    assert rows[0]["description"] == "order_one"

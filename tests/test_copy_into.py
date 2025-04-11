# tests/test_copy_into.py

from bootstrap.table_loader import copy_into_table
from utils.session import MyDatabricksSession

def test_copy_into_generic_csv():
    spark = MyDatabricksSession.get_spark()

    copy_into_table(
        spark=spark,
        table_name="main.default.test_generic_copy",
        file_path="dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz",
        file_format="CSV",
        sql_template_path="sql/bootstrap/copy_into_generic.sql",
        options={"header": "true"}
    )

    df = spark.sql("SELECT * FROM main.default.test_generic_copy")
    assert df.count() > 0
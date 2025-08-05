# tests/test_dbfs_reader.py

from bootstrap.dbfs_reader import preview_dbfs_file
from utils.session import MyDatabricksSession


def test_preview_dbfs_file():
    spark = MyDatabricksSession.get_spark()  # triggers cluster spin-up
    df = preview_dbfs_file(
        file_path="/databricks-datasets/wine-quality/winequality-red.csv",
        file_format="csv",
        limit=10,
        options={"header": "true", "inferSchema": "true", "sep": ";"},
    )

    assert df.count() > 0
    assert "quality" in df.columns  # Example column, adjust as needed

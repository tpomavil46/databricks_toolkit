from bootstrap.dbfs_explorer import explore_dbfs_path

def test_explore_dbfs_structure():
    paths = explore_dbfs_path("dbfs:/databricks-datasets/ecommerce/", recursive=True)
    assert isinstance(paths, list)
    assert any("sales-historical" in p for p in paths), "Expected sales-historical dataset not found"
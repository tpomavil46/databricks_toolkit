# flake8: noqa F821
from databricks_toolkit.bronze.ingest_customer import run


run(
    spark,
    input_path="/databricks-datasets/retail-org/customers",
    output_table="demo.customers_cleaned",
)

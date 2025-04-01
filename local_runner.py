from pyspark.sql import SparkSession
from jobs.ingest_customer import run

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("LocalDev") \
        .getOrCreate()

    run(
        spark,
        input_path="data/customers.csv",
        output_table="demo.customers_cleaned"
    )
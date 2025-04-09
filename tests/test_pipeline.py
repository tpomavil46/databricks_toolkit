import pytest
from utils.session import MyDatabricksSession
from utils.logger import setup_logger
from jobs.bronze.ingest import ingest_data
from jobs.silver.transform import run as transform_data
from jobs.gold.stage import create_gold_stage
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = setup_logger("test_logger")


@pytest.fixture(scope="session")
def spark():
    return MyDatabricksSession.get_spark()


def test_bronze_ingestion_nytaxi(spark):
    input_path = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz"
    bronze_output = "main.default.nytaxi_bronze"

    logger.info(f"Starting bronze ingestion test from: {input_path}")

    try:
        df_bronze = ingest_data(spark, input_path, bronze_output)
    except Exception as e:
        pytest.fail(f"❌ Bronze ingestion failed: {e}")

    assert df_bronze is not None and df_bronze.count() > 0, "Bronze DataFrame is empty"
    logger.info(f"✅ Bronze ingestion completed: {bronze_output}")
    df_bronze.printSchema()
    df_bronze.show(5)


def test_silver_transform_nytaxi(spark):
    bronze_input = "main.default.nytaxi_bronze"
    silver_output = "main.default.nytaxi_silver"

    if not check_table_exists(spark, bronze_input):
        pytest.skip(f"❌ Bronze table not found: {bronze_input}")

    try:
        df_silver = transform_data(
            spark, bronze_input=bronze_input, silver_output=silver_output
        )
    except Exception as e:
        pytest.fail(f"❌ Silver transformation failed: {e}")

    assert df_silver is not None and df_silver.count() > 0, "Silver DataFrame is empty"
    logger.info(f"✅ Silver transform successful: {silver_output}")
    df_silver.printSchema()
    df_silver.show(5)


def test_gold_stage_nytaxi(spark):
    silver_input = "main.default.nytaxi_silver"
    gold_output = "main.default.nytaxi_gold"

    if not check_table_exists(spark, silver_input):
        pytest.skip(f"❌ Silver table not found: {silver_input}")

    create_gold_stage(spark, silver_input, gold_output, view_or_table="view")

    assert check_table_exists(
        spark, gold_output
    ), f"Gold view {gold_output} not created"
    assert check_table_exists(spark, f"{gold_output}_kpis"), "KPI table not created"


def check_table_exists(spark, table_name: str) -> bool:
    try:
        spark.sql(f"SELECT * FROM {table_name} LIMIT 1").collect()
        return True
    except Exception as e:
        logger.warning(f"⚠️ Table check failed for {table_name}: {e}")
        return False

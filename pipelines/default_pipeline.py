from databricks_toolkit.utils.config import PipelineConfig
from databricks_toolkit.bronze.ingest_customer import run as ingest_customer
from databricks_toolkit.silver.transform_orders import run as transform_orders
from databricks_toolkit.gold.generate_kpis import run as generate_kpis


def run_pipeline(config: PipelineConfig):
    print("🚀 Starting Medallion Pipeline")
    ingest_customer(config)
    transform_orders(config)
    generate_kpis(config)
    print("✅ Pipeline completed successfully.")
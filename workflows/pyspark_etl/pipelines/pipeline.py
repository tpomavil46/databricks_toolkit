# core/pipeline.py

from utils.session import DatabricksSession
from utils.io import write_df_as_table_or_path
from utils.logger import log_function_call


class MedallionPipeline:
    def __init__(
        self,
        spark=None,
        input_table=None,
        output_table=None,
        format="delta",
        bronze_output=None,
        silver_output=None,
        gold_output=None,
        **extra,
    ):
        # Use DatabricksSession to create the Spark session if not already provided
        self.spark = spark if spark else DatabricksSession.get_spark()
        self.input_table = input_table
        self.output_table = output_table
        self.format = format
        self.bronze_output = bronze_output or output_table
        self.silver_output = silver_output or output_table
        self.gold_output = gold_output
        self.extra = {
            **extra,
            "bronze_output": self.bronze_output,
            "silver_output": self.silver_output,
            "gold_output": self.gold_output,
        }

    @log_function_call
    def run_bronze(self):
        from jobs.bronze.ingest import ingest_data

        dataset = self.extra.get("dataset")
        paths = self.extra.get("input_paths")
        if isinstance(paths, str):
            paths = [p.strip() for p in paths.split(",")]

        df = ingest_data(self.spark, dataset=dataset, input_paths=paths)

        write_df_as_table_or_path(
            self.spark, df, self.bronze_output, format=self.format
        )

    @log_function_call
    def run_silver(self):
        from silver.transform_orders import run as transform_orders

        df = read_table_or_path(self.spark, self.input_table)
        df_transformed = transform_orders(df, **self.extra)
        write_df_as_table_or_path(
            self.spark, df_transformed, self.silver_output, format=self.format
        )

    @log_function_call
    def run_gold(self):
        from gold.generate_kpis import run as generate_kpis

        if not self.gold_output:
            raise ValueError("❌ gold_output is required for the gold stage")
        df = read_table_or_path(
            self.spark, self.silver_output
        )  # Read the silver output table
        # Now we explicitly pass the spark session when calling generate_kpis
        kpi_df = generate_kpis(self.spark, df, **self.extra)  # Pass spark session here
        # Pass the correct location for writing the output
        write_df_as_table_or_path(
            self.spark, kpi_df, self.gold_output, format=self.format
        )

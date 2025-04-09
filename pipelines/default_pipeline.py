# pipelines/default_pipeline.py

from core.pipeline import MedallionPipeline


def run(spark, **kwargs):
    """
    Executes the Medallion architecture pipeline:
    Bronze → Silver → Gold.

    Args:
        spark (SparkSession): The active Spark session.
        kwargs: Optional parameters:
            - input_table: Raw input file or table name.
            - bronze_path: Destination table name for bronze layer.
            - silver_path: Destination table name for silver layer.
            - gold_path: Destination base name for gold views/tables.
            - format: Storage format (e.g., delta).
            - vendor_filter: Optional vendor ID filter for gold view.
            - view_or_table: Either 'view' or 'table' for gold output.
    """
    print("🚀 Starting Medallion Pipeline")

    pipeline = MedallionPipeline(
        spark=spark,
        input_table=kwargs.get("input_table"),
        output_table=kwargs.get("output_table"),  # Optional if needed downstream
        format=kwargs.get("format", "delta"),
        bronze_output=kwargs.get("bronze_path"),
        silver_output=kwargs.get("silver_path"),
        gold_output=kwargs.get("gold_path"),
        vendor_filter=kwargs.get("vendor_filter"),        # For gold
        view_or_table=kwargs.get("view_or_table", "view") # Default to view
    )

    pipeline.run_bronze()
    pipeline.run_silver()
    pipeline.run_gold()

    print("✅ Pipeline completed successfully.")
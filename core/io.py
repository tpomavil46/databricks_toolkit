# core/io.py
def read_table(spark, table_name: str):
    print(f"📄 Reading table: {table_name}")
    return spark.table(table_name)


def write_df_as_table(df, table_name: str, format: str = "delta"):
    print(f"💾 Writing table: {table_name}")
    df.write.format(format).mode("overwrite").option("mergeSchema", "true").saveAsTable(
        table_name
    )
    print(f"✅ Table written: {table_name}")

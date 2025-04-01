def load_csv_to_df(spark, path):
    return spark.read.option("header", True).csv(path)


def write_df_as_table(df, table_name):
    df.write.mode("overwrite").saveAsTable(table_name)

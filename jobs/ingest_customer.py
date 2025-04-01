from utils.io import load_csv_to_df, write_df_as_table

def run(spark, input_path: str, output_table: str):
    df = load_csv_to_df(spark, input_path)
    write_df_as_table(df, output_table)
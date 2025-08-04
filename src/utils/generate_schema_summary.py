import pandas as pd

def generate_schema_summary(dataframes_dict: dict, output_path: str):
    rows = []
    for table_name, df in dataframes_dict.items():
        for col in df.columns:
            rows.append({
                "table_name": table_name,
                "column_name": col,
                "dtype": str(df[col].dtype)
            })
    schema_df = pd.DataFrame(rows)
    schema_df.to_csv(output_path, index=False)

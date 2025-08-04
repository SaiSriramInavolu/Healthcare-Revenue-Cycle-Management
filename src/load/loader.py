# src/load/loader.py

import logging
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class Loader:
    def __init__(self, project_id="python-sql-project-467708"):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
        self.schema_summary = []

    def get_table_id(self, table_name):
        if table_name.endswith("_cleaned"):
            dataset = "silver"
        elif table_name.startswith("dim_") or table_name.startswith("fact_"):
            dataset = "gold"
        else:
            dataset = "bronze"
        return f"{self.project_id}.{dataset}.{table_name}"

    def extract_schema(self, df: pd.DataFrame, table_id: str):
        schema = []
        for col, dtype in df.dtypes.items():
            if pd.api.types.is_datetime64_any_dtype(dtype):
                col_type = "TIMESTAMP"
            elif pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "FLOAT"
            elif pd.api.types.is_bool_dtype(dtype):
                col_type = "BOOLEAN"
            else:
                col_type = "STRING"
            schema.append({"table": table_id, "column": col, "type": col_type})
        return schema

    def load_table(self, df, table_name, partition_field=None, cluster_fields=None):
        table_id = self.get_table_id(table_name)
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_TRUNCATE"
        )

        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)

        if cluster_fields:
            job_config.clustering_fields = cluster_fields

        # Explicitly define schema for problematic columns
        if table_name == "fact_transactions":
            job_config.schema = [
                bigquery.SchemaField("transaction_date", "TIMESTAMP"),
            ]
        elif table_name == "fact_claims":
            job_config.schema = [
                bigquery.SchemaField("claim_date", "TIMESTAMP"),
            ]

        job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        logger.info(f"Loaded {len(df)} rows into {table_id}")
        self.schema_summary.extend(self.extract_schema(df, table_id))

    def save_schema_summary(self):
        if self.schema_summary:
            df_schema = pd.DataFrame(self.schema_summary)
            df_schema.to_csv("schema_summary.csv", index=False)
            logger.info("Generated schema_summary.csv")

    def run(self, data_dict: dict):
        logger.info("Starting BigQuery loading process")

        for table_name, df in data_dict.items():
            partition_field = None
            cluster_fields = None

            if table_name == "fact_transactions":
                partition_field = "transaction_date"
                cluster_fields = ["patientid"]
            elif table_name == "fact_claims":
                partition_field = "claim_date"

            self.load_table(df, table_name, partition_field, cluster_fields)

        self.save_schema_summary()
        logger.info("BigQuery loading complete")

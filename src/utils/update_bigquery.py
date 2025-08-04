import pandas as pd
from google.cloud import bigquery
import os

# Initialize BigQuery client
client = bigquery.Client()
project_id = client.project

def get_table_id(table_name):
    if table_name.endswith("_cleaned"):
        dataset = "silver"
    elif table_name.startswith("dim_") or table_name.startswith("fact_"):
        dataset = "gold"
    else:
        # This case should ideally not be reached for this script
        dataset = "bronze"
    return f"{project_id}.{dataset}.{table_name}"

def upload_to_bigquery(df, table_name, partition_field=None, cluster_fields=None):
    table_id = get_table_id(table_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE"
    )

    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(field=partition_field)

    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    # Explicitly define schema for problematic columns if needed (from loader.py)
    # This part is mostly for reference, autodetect should handle most cases
    # but we'll ensure the dataframe types are correct before upload.

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")

# Gold Layer Updates
print("Updating Gold Layer tables in BigQuery...")

# dim_patients
file_path_dim_patients = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\gold\\dim_patients.csv"
df_dim_patients = pd.read_csv(file_path_dim_patients)
upload_to_bigquery(df_dim_patients, "dim_patients")

# fact_claims
file_path_fact_claims = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\gold\\fact_claims.csv"
df_fact_claims = pd.read_csv(file_path_fact_claims)
# Convert 'claim_date' to datetime objects
df_fact_claims['claim_date'] = pd.to_datetime(df_fact_claims['claim_date'])
upload_to_bigquery(df_fact_claims, "fact_claims", partition_field="claim_date")

# fact_transactions
file_path_fact_transactions = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\gold\\fact_transactions.csv"
df_fact_transactions = pd.read_csv(file_path_fact_transactions)
# Convert 'transaction_date' to datetime objects
df_fact_transactions['transaction_date'] = pd.to_datetime(df_fact_transactions['transaction_date'])
upload_to_bigquery(df_fact_transactions, "fact_transactions", partition_field="transaction_date", cluster_fields=["unified_patient_id"])

print("Gold Layer updates complete.")

# Silver Layer Updates
print("Updating Silver Layer tables in BigQuery...")

# patients_cleaned
file_path_patients_cleaned = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\silver\\patients_cleaned.csv"
df_patients_cleaned = pd.read_csv(file_path_patients_cleaned)
upload_to_bigquery(df_patients_cleaned, "patients_cleaned")

# claims_cleaned
file_path_claims_cleaned = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\silver\\claims_cleaned.csv"
df_claims_cleaned = pd.read_csv(file_path_claims_cleaned)
# Convert 'claim_date' to datetime objects
df_claims_cleaned['claim_date'] = pd.to_datetime(df_claims_cleaned['claim_date'])
upload_to_bigquery(df_claims_cleaned, "claims_cleaned")

# transactions_cleaned
file_path_transactions_cleaned = "C:\\Users\\HELLO\\OneDrive\\Desktop\\healthcare_rcm_project3\\data\\silver\\transactions_cleaned.csv"
df_transactions_cleaned = pd.read_csv(file_path_transactions_cleaned)
# Convert 'transaction_date' to datetime objects
df_transactions_cleaned['transaction_date'] = pd.to_datetime(df_transactions_cleaned['transaction_date'])
upload_to_bigquery(df_transactions_cleaned, "transactions_cleaned")

print("Silver Layer updates complete.")

print("BigQuery update script finished.")
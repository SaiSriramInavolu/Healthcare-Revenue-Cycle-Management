import pandas as pd
import numpy as np
import logging
import re
from datetime import datetime

class Transformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transform_patients(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()

        # Rename columns to match schema
        df = df.rename(columns={
            "firstname": "first_name",
            "lastname": "last_name",
            "dob": "DOB",
            "phonenumber": "phone"
        })

        if "source_file" not in df.columns:
            self.logger.warning("Missing 'source_file' in patients data, using 'unknown' as fallback.")
            df["source_file"] = "unknown"

        df["unified_patient_id"] = df["patientid"].astype(str) + "_" + df["source_file"]

        df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce")

        df["gender"] = df["gender"].str.upper().map({
            "M": "Male", "F": "Female", "MALE": "Male", "FEMALE": "Female"
        }).fillna("Unknown")

        df["age"] = df["DOB"].apply(lambda x: int((pd.Timestamp.now() - x).days // 365.25) if pd.notnull(x) else None)

        df = df.drop_duplicates(subset=["unified_patient_id"])

        # Drop unnecessary columns
        cols_to_drop = ['id', 'f_name', 'l_name', 'm_name', 'email', 'email_valid']
        df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])

        return df

    def transform_providers(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = df.columns.str.lower()
        df = df.drop_duplicates()
        return df

    def transform_transactions(self, df: pd.DataFrame, cpt_df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Transforming transactions data")
        df = df.copy()
        cpt_df = cpt_df.copy()

        # Standardize column names
        df.columns = df.columns.str.lower()
        cpt_df.columns = cpt_df.columns.str.lower()

        # Ensure procedurecode exists and is of type string
        if 'procedurecode' in df.columns:
            df['procedurecode'] = df['procedurecode'].astype(str)
        else:
            df['procedurecode'] = 'Unknown'
        if 'procedurecode' in cpt_df.columns:
            cpt_df['procedurecode'] = cpt_df['procedurecode'].astype(str)
        else:
            cpt_df['procedurecode'] = 'Unknown'

        # Rename CPT columns to avoid conflicts
        if 'description' in cpt_df.columns:
            cpt_df = cpt_df.rename(columns={'description': 'cpt_description'})
        else:
            cpt_df['cpt_description'] = ''
        if 'category' in cpt_df.columns:
            cpt_df = cpt_df.rename(columns={'category': 'cpt_category'})
        else:
            cpt_df['cpt_category'] = ''

        # Merge CPT metadata
        df = df.merge(
            cpt_df[['procedurecode', 'cpt_description', 'cpt_category']],
            how='left',
            on='procedurecode'
        )

        # Drop cpt_description and cpt_category as they are not part of FACT_TRANSACTIONS schema
        df = df.drop(columns=['cpt_description', 'cpt_category'], errors='ignore')

        # Handle date columns
        if "transaction_date" not in df.columns and "transactiondate" in df.columns:
            df.rename(columns={'transactiondate': 'transaction_date'}, inplace=True)
        
        df['transaction_date'] = pd.to_datetime(df.get('transaction_date'), errors='coerce')

        # Type casting and filling missing values
        if 'amount' not in df.columns:
            df['amount'] = 0
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)
        if 'paidamount' not in df.columns:
            df['paidamount'] = 0
        df['paidamount'] = pd.to_numeric(df['paidamount'], errors='coerce').fillna(0)

        # Create unified_patient_id
        if 'patientid' in df.columns and 'source_file' in df.columns:
            df['unified_patient_id'] = df['patientid'].astype(str) + '_' + df['source_file'].astype(str)
        else:
            df['unified_patient_id'] = 'Unknown'

        return df

    def transform_claims(self, df: pd.DataFrame, cpt_df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Transforming claims data")
        df = df.copy()
        cpt_df = cpt_df.copy()

        # Standardize column names
        df.columns = df.columns.str.lower()
        cpt_df.columns = cpt_df.columns.str.lower()

        # Ensure procedurecode exists and is of type string
        if 'procedurecode' in df.columns:
            df['procedurecode'] = df['procedurecode'].astype(str)
        else:
            df['procedurecode'] = 'Unknown'
        if 'procedurecode' in cpt_df.columns:
            cpt_df['procedurecode'] = cpt_df['procedurecode'].astype(str)
        else:
            cpt_df['procedurecode'] = 'Unknown'

        # Rename CPT columns to avoid conflicts
        if 'description' in cpt_df.columns:
            cpt_df = cpt_df.rename(columns={'description': 'cpt_description'})
        else:
            cpt_df['cpt_description'] = ''
        if 'category' in cpt_df.columns:
            cpt_df = cpt_df.rename(columns={'category': 'cpt_category'})
        else:
            cpt_df['cpt_category'] = ''

        # Merge CPT metadata
        df = df.merge(
            cpt_df[['procedurecode', 'cpt_description', 'cpt_category']],
            how='left',
            on='procedurecode'
        )

        # Handle date columns
        if 'claim_date' not in df.columns and 'claimdate' in df.columns:
            df.rename(columns={'claimdate': 'claim_date'}, inplace=True)
        
        df['claim_date'] = pd.to_datetime(df.get('claim_date'), errors='coerce')

        # Type casting and filling missing values
        if 'amountclaimed' not in df.columns:
            df['amountclaimed'] = 0
        df['amountclaimed'] = pd.to_numeric(df['amountclaimed'], errors='coerce').fillna(0)
        if 'amountapproved' not in df.columns:
            df['amountapproved'] = 0
        df['amountapproved'] = pd.to_numeric(df['amountapproved'], errors='coerce').fillna(0)

        # Create unified_patient_id
        if 'patientid' in df.columns and 'source_file' in df.columns:
            df['unified_patient_id'] = df['patientid'].astype(str) + '_' + df['source_file'].astype(str)
        else:
            df['unified_patient_id'] = 'Unknown'

        return df

    def run(self, data_dict: dict) -> dict:
        self.logger.info("Starting transformation layer")

        patients = self.transform_patients(data_dict["patients"])
        transactions = self.transform_transactions(data_dict["transactions"], data_dict["cptcodes"])
        claims = self.transform_claims(data_dict["claims"], data_dict["cptcodes"])
        providers = self.transform_providers(data_dict["providers"])

        return {
            "patients": patients,
            "transactions": transactions,
            "claims": claims,
            "providers": providers,
            "cptcodes": data_dict["cptcodes"]
        }
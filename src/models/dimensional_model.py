import pandas as pd
import numpy as np
import logging
from datetime import datetime
from src.models.schema_definitions import DIM_PATIENTS

class DimensionalModel:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _ensure_columns(self, df: pd.DataFrame, schema: list, table_name: str) -> pd.DataFrame:
        for col in schema:
            if col not in df.columns:
                self.logger.warning(f"Missing column '{col}' in {table_name}. Will be added with default values.")
                if 'date' in col.lower() or 'dob' in col.lower():
                    df[col] = pd.NaT
                elif 'amount' in col.lower() or 'age' in col.lower() or 'id' in col.lower():
                    df[col] = 0
                elif 'valid' in col.lower() or 'is_' in col.lower():
                    df[col] = False
                else:
                    df[col] = ''
        return df

    def scd_patient(self, new_patient_df, old_patient_df=None):
        new_patient_df = new_patient_df.copy()
        new_patient_df['patient_key'] = new_patient_df['unified_patient_id'].astype('category').cat.codes

        if old_patient_df is None or old_patient_df.empty:
            new_patient_df['effective_date'] = datetime.now()
            new_patient_df['end_date'] = pd.NaT
            new_patient_df['is_current'] = True
            return new_patient_df[DIM_PATIENTS + ['patient_key', 'effective_date', 'end_date', 'is_current']]

        merged_df = pd.merge(new_patient_df, old_patient_df[old_patient_df['is_current']], on='unified_patient_id', how='left', indicator=True, suffixes=('', '_old'))
        
        new_patients = merged_df[merged_df['_merge'] == 'left_only']
        existing_patients = merged_df[merged_df['_merge'] == 'both']

        new_patients['effective_date'] = datetime.now()
        new_patients['end_date'] = pd.NaT
        new_patients['is_current'] = True

        changed_patients = existing_patients[
            (existing_patients['first_name'] != existing_patients['first_name_old']) | 
            (existing_patients['last_name'] != existing_patients['last_name_old']) | 
            (existing_patients['phone'] != existing_patients['phone_old'])
        ].copy()

        if not changed_patients.empty:
            old_patient_df.loc[old_patient_df['unified_patient_id'].isin(changed_patients['unified_patient_id']) & old_patient_df['is_current'], 'end_date'] = datetime.now()
            old_patient_df.loc[old_patient_df['unified_patient_id'].isin(changed_patients['unified_patient_id']) & old_patient_df['is_current'], 'is_current'] = False

            changed_patients['effective_date'] = datetime.now()
            changed_patients['end_date'] = pd.NaT
            changed_patients['is_current'] = True
            
            final_df = pd.concat([old_patient_df, new_patients, changed_patients], ignore_index=True)
        else:
            final_df = pd.concat([old_patient_df, new_patients], ignore_index=True)

        return final_df[DIM_PATIENTS + ['patient_key', 'effective_date', 'end_date', 'is_current']]

    def run(self, clean_data: dict) -> dict:
        self.logger.info("Building dimensional model...")

        dim_patients_scd = self.scd_patient(clean_data['patients'])
        dim_providers = self._create_dim_providers(clean_data['providers'])
        dim_procedures = self._create_dim_procedures(clean_data['cptcodes'])
        dim_date = self._create_dim_date(clean_data['transactions'], clean_data['claims'])

        fact_transactions = self._create_fact_transactions(clean_data['transactions'], dim_patients_scd, dim_providers, dim_procedures, dim_date)
        fact_claims = self._create_fact_claims(clean_data['claims'], dim_patients_scd, dim_providers, dim_procedures, dim_date)

        return {
            "dim_patients_scd": dim_patients_scd,
            "dim_providers": dim_providers,
            "dim_procedures": dim_procedures,
            "dim_date": dim_date,
            "fact_transactions": fact_transactions,
            "fact_claims": fact_claims
        }

    def _create_dim_patients(self, patients_df):
        patients_df['patient_key'] = patients_df['unified_patient_id'].astype(str).astype('category').cat.codes
        return patients_df

    def _create_dim_providers(self, providers_df):
        providers_df['provider_key'] = providers_df['providerid'].astype(str).astype('category').cat.codes
        return providers_df

    def _create_dim_procedures(self, cpt_df):
        cpt_df['procedurecode'] = cpt_df['procedurecode'].astype(str)
        cpt_df['procedure_key'] = cpt_df['procedurecode'].astype('category').cat.codes
        return cpt_df

    def _create_dim_date(self, transactions_df, claims_df):
        all_dates = pd.concat([
            pd.to_datetime(transactions_df['transaction_date'], errors='coerce'),
            pd.to_datetime(claims_df['claim_date'], errors='coerce')
        ]).dropna().unique()

        dim_date = pd.DataFrame({'date': all_dates})
        dim_date['date'] = pd.to_datetime(dim_date['date'])
        dim_date['date_key'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
        dim_date['year'] = dim_date['date'].dt.year
        dim_date['month'] = dim_date['date'].dt.month
        dim_date['day'] = dim_date['date'].dt.day
        dim_date['quarter'] = dim_date['date'].dt.quarter
        dim_date['day_of_week'] = dim_date['date'].dt.dayofweek
        return dim_date

    def _create_fact_transactions(self, transactions_df, dim_patients, dim_providers, dim_procedures, dim_date):
        fact_transactions = transactions_df.copy()

        fact_transactions['unified_patient_id'] = fact_transactions['unified_patient_id'].astype(str)
        fact_transactions['providerid'] = fact_transactions['providerid'].astype(str)
        fact_transactions['procedurecode'] = fact_transactions['procedurecode'].astype(str)

        fact_transactions = pd.merge(fact_transactions, dim_patients[['unified_patient_id', 'patient_key']], on='unified_patient_id', how='left')
        fact_transactions = pd.merge(fact_transactions, dim_providers[['providerid', 'provider_key']], on='providerid', how='left')
        fact_transactions = pd.merge(fact_transactions, dim_procedures[['procedurecode', 'procedure_key']], on='procedurecode', how='left')
        fact_transactions['transaction_date'] = pd.to_datetime(fact_transactions['transaction_date'], errors='coerce')
        fact_transactions = pd.merge(fact_transactions, dim_date[['date', 'date_key']], left_on='transaction_date', right_on='date', how='left')

        return fact_transactions

    def _create_fact_claims(self, claims_df, dim_patients, dim_providers, dim_procedures, dim_date):
        fact_claims = claims_df.copy()

        fact_claims['unified_patient_id'] = fact_claims['unified_patient_id'].astype(str)
        fact_claims['providerid'] = fact_claims['providerid'].astype(str)
        fact_claims['procedurecode'] = fact_claims['procedurecode'].astype(str)

        fact_claims = pd.merge(fact_claims, dim_patients[['unified_patient_id', 'patient_key']], on='unified_patient_id', how='left')
        fact_claims = pd.merge(fact_claims, dim_providers[['providerid', 'provider_key']], on='providerid', how='left')
        fact_claims = pd.merge(fact_claims, dim_procedures[['procedurecode', 'procedure_key']], on='procedurecode', how='left')
        fact_claims['claim_date'] = pd.to_datetime(fact_claims['claim_date'], errors='coerce')
        fact_claims = pd.merge(fact_claims, dim_date[['date', 'date_key']], left_on='claim_date', right_on='date', how='left')

        return fact_claims
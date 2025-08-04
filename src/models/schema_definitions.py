# src/models/schema_definitions.py

# Dimension: Patients
DIM_PATIENTS = [
    "unified_patient_id", "first_name", "last_name", "DOB", "gender",
    "phone", "age", "source_db"
]

# Dimension: Providers
DIM_PROVIDERS = [
    "ProviderID", "name", "specialty", "DeptID", "NPI", "source_db"
]

# Dimension: Procedures (from cptcodes.csv)
DIM_PROCEDURES = [
    "ProcedureCode", "Description", "Category"
]

# Dimension: Departments
DIM_DEPARTMENTS = [
    "DeptID", "Name"
]

# Dimension: Date
DIM_DATE = [
    "date", "year", "month", "quarter", "day_of_week"
]

# Fact: Transactions
FACT_TRANSACTIONS = [
    "TransactionID", "unified_patient_id", "ProviderID", "ProcedureCode",
    "transaction_date", "Amount", "PaidAmount", "payment_status",
    "year", "month", "day_of_week", "source_db"
]

# Fact: Claims
FACT_CLAIMS = [
    "claim_id", "unified_patient_id", "ProviderID", "claim_date",
    "amount_claimed", "amount_approved", "insurance_company",
    "claim_status", "source_file", "Description", "Category"
]

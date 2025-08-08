DIM_PATIENTS = [
    "unified_patient_id", "first_name", "last_name", "DOB", "gender",
    "phone", "age", "source_db"
]

DIM_PROVIDERS = [
    "ProviderID", "name", "specialty", "DeptID", "NPI", "source_db"
]

DIM_PROCEDURES = [
    "ProcedureCode", "Description", "Category"
]

DIM_DEPARTMENTS = [
    "DeptID", "Name"
]

DIM_DATE = [
    "date", "year", "month", "quarter", "day_of_week"
]

FACT_TRANSACTIONS = [
    "TransactionID", "unified_patient_id", "ProviderID", "ProcedureCode",
    "transaction_date", "Amount", "PaidAmount", "payment_status",
    "year", "month", "day_of_week", "source_db"
]

FACT_CLAIMS = [
    "claim_id", "unified_patient_id", "ProviderID", "claim_date",
    "amount_claimed", "amount_approved", "insurance_company",
    "claim_status", "source_file", "Description", "Category"
]
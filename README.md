Healthcare Revenue Cycle Management (RCM) – End-to-End Data Engineering Project
🔍 Overview
This project simulates a real-world healthcare analytics pipeline designed to process large-scale patient, claims, and transaction data from multiple hospital systems. It follows best practices in modern data engineering, incorporating Medallion architecture (Bronze, Silver, Gold), historical tracking via Slowly Changing Dimension (SCD) Type 2, and a robust BigQuery data warehouse for business intelligence and analytics.

🎯 Project Objectives
Consolidate data from multiple hospital sources (MySQL, CSV)

Clean, standardize, and validate all incoming data

Implement SCD Type 2 for historical patient tracking

Transform data into dimensional star schema (facts and dimensions)

Load structured datasets into BigQuery with proper partitioning/clustering

Generate schema metadata for documentation and auditing

Support downstream KPIs and Looker Studio dashboards

🛠️ Tech Stack
Category	Tools & Technologies
Languages	Python, SQL
Data Sources	MySQL (hospital_a, hospital_b), CSV (claims)
Data Lakehouse	Google BigQuery
Libraries	pandas, SQLAlchemy, google-cloud-bigquery
Architecture	Medallion (Bronze → Silver → Gold)
Data Modeling	Dimensional star schema, SCD Type 2
DevOps	Git, GitHub
Visualization	Looker Studio
Logging	Custom ETL logging via Python logger
Secrets Mgmt	dotenv (.env)

🗂️ Project Structure
graphql
Copy
Edit
healthcare_rcm_project/
│
├── config/                    # DB and GCP credential configs
├── data/
│   ├── bronze/                # Raw extracted data
│   ├── silver/                # Cleaned/standardized data
│   └── gold/                  # Final dimensional model outputs
│
├── src/
│   ├── extract/               # MySQL & CSV extractors
│   ├── transform/             # Data cleaning + standardization
│   ├── models/                # Star schema & SCD2 modeling
│   ├── load/                  # BigQuery loader + schema reporter
│   ├── utils/                 # Logger, schema generator
│
├── sql/                       # BigQuery analytics SQLs
├── .env.example               # Secrets template
├── requirements.txt
├── main.py                    # Pipeline orchestrator
└── README.md
⚙️ Pipeline Flow (Medallion Architecture)
Bronze Layer:
Extracts raw data from:

MySQL: hospital_a and hospital_b

CSV: claims.csv, cptcodes.csv

Merges data sources and stores as CSVs under /data/bronze/

Silver Layer:
Applies data transformation and quality steps:

Gender standardization, email validation

Date parsing, deduplication, phone normalization

CPT code enrichment via joins

Missing column handling with fallbacks

Saved as cleaned CSVs under /data/silver/

Gold Layer:
Converts cleaned data into star schema:

Dimension tables: dim_patients, dim_procedures, dim_providers, dim_departments, dim_date

Fact tables: fact_transactions, fact_claims

SCD Type 2: dim_patients_scd

Schema summary auto-generated (schema_summary.csv)

Output saved under /data/gold/

Load to BigQuery:

All Silver and Gold tables are loaded into BigQuery

Partitioning (e.g. by transaction_date, claim_date) and clustering (e.g. by unified_patient_id) applied

Datasets created:

bronze (optional upload)

silver (cleaned layer)

gold (fact and dimensional tables)

✅ Key Features
✔️ End-to-end ETL pipeline with Medallion architecture

✔️ Raw-to-Gold layer implementation using Python and Pandas

✔️ Robust data transformation with fallbacks, logging, and standardization

✔️ SCD Type 2 for dim_patients using surrogate keys

✔️ Dimensional modeling: fact and dimension tables for analytics

✔️ BigQuery integration with partitioning and clustering

✔️ Schema summary generated post-load (schema_summary.csv)

✔️ Business-ready metrics: Revenue, AR, Denial rates, Procedure performance

✔️ Looker Studio Dashboards (RCM KPIs, Data Quality, SCD Audits)

📊 Sample KPIs Enabled
Metric Category	Examples
Revenue	Total revenue, monthly revenue trends
Claims	Approval/denial rates, claim volume, average processing duration
Patients	Demographics, insurance coverage, patient volume
Finance	Days in A/R, collection efficiency, write-offs
Advanced	Patient Lifetime Value, Procedure Profitability, Provider Performance

🔒 Secrets & Environment Setup
All credentials (MySQL, GCP) are stored securely in a .env file

Use the .env.example file as a template

DO NOT commit the real .env file to GitHub

🚀 Getting Started
bash
Copy
Edit
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Fill in MySQL and GCP credentials

# 3. Run the ETL pipeline
python main.py
📎 Outputs
Layer	Sample Outputs
Bronze	data/bronze/patients.csv, claims.csv, ...
Silver	data/silver/patients_cleaned.csv, transactions_cleaned.csv, ...
Gold	data/gold/dim_patients.csv, fact_claims.csv, ...
Metadata	data/schema_summary.csv for table/column overview

📈 Dashboards (Looker Studio)
RCM KPI Overview

SCD Type 2 Audit Report

Data Quality Monitoring

Revenue Trends & Provider Insights

📬 Contact
For deployment support, customization, or questions, feel free to reach out.
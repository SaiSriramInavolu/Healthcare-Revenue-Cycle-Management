# main.py

import os
from src.extract.extractor import Extractor
from src.transform.transformer import Transformer
from src.load.loader import Loader
from src.utils.logger import init_logger
from src.models.dimensional_model import DimensionalModel
from src.utils.generate_schema_summary import generate_schema_summary
from src.analytics.rcm_analytics import RCMAnalytics

logger = init_logger()

def ensure_directories():
    os.makedirs("data/bronze", exist_ok=True)
    os.makedirs("data/silver", exist_ok=True)
    os.makedirs("data/gold", exist_ok=True)

def main():
    logger.info("ETL Pipeline started")
    ensure_directories()

    # Extract
    extractor = Extractor()
    data_dict = extractor.run()

    # Save Bronze Layer
    for key, df in data_dict.items():
        df.to_csv(f"data/bronze/{key}.csv", index=False)
        logger.info(f"Saved Bronze CSV: data/bronze/{key}.csv")

    # Transform
    transformer = Transformer()
    clean_data = transformer.run(data_dict)

    # Save Silver Layer
    for key, df in clean_data.items():
        cleaned_name = f"{key}_cleaned"
        df.to_csv(f"data/silver/{cleaned_name}.csv", index=False)
        logger.info(f"Saved Silver CSV: data/silver/{cleaned_name}.csv")

    # Build Star Schema (Gold Layer)
    model = DimensionalModel()
    dims_facts = model.run(clean_data)

    # Save Gold Layer
    for key, df in dims_facts.items():
        df.to_csv(f"data/gold/{key}.csv", index=False)
        logger.info(f"Saved Gold CSV: data/gold/{key}.csv")

    # Load Silver & Gold to BigQuery
    loader = Loader()

    for key, df in clean_data.items():
        cleaned_name = f"{key}_cleaned"
        loader.load_table(df, cleaned_name)

    for key, df in dims_facts.items():
        partition_field = None
        cluster_fields = None

        if key == "fact_transactions":
            partition_field = "transaction_date"
            cluster_fields = ["unified_patient_id"]
        elif key == "fact_claims":
            partition_field = "claim_date"

        loader.load_table(df, key, partition_field=partition_field, cluster_fields=cluster_fields)

    # Generate and save schema summary
    generate_schema_summary(dims_facts, output_path="data/schema_summary.csv")
    logger.info("Schema summary saved to data/schema_summary.csv")

    # Run Analytics
    analytics = RCMAnalytics()
    analytics.run_analytics()

    logger.info("ETL Pipeline completed successfully")

if __name__ == "__main__":
    main()

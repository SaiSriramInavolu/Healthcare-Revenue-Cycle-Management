import pandas as pd
import os
from sqlalchemy import create_engine
from src.utils.logger import get_logger
from config.db_config import DB_CONFIG

logger = get_logger(__name__)

class Extractor:
    def __init__(self):
        self.hospitals = ['hospital_a', 'hospital_b']
        self.use_mysql = os.getenv("USE_MYSQL", "true").lower() == "true"

    def extract_mysql(self, db):
        try:
            logger.info(f"Connecting to MySQL DB: {db}")
            engine = create_engine(DB_CONFIG[db])
            return {
                "patients": pd.read_sql("SELECT * FROM patients", engine),
                "providers": pd.read_sql("SELECT * FROM providers", engine),
                "transactions": pd.read_sql("SELECT * FROM transactions", engine),
                "encounters": pd.read_sql("SELECT * FROM encounters", engine),
                "departments": pd.read_sql("SELECT * FROM departments", engine)
            }
        except Exception as e:
            logger.error(f"MySQL extraction failed for {db}: {e}")
            return {}

    def extract_csv(self, hospital_key):
        base_path = f"data/raw/{hospital_key}/"
        try:
            return {
                "patients": pd.read_csv(os.path.join(base_path, "patients.csv")),
                "providers": pd.read_csv(os.path.join(base_path, "providers.csv")),
                "transactions": pd.read_csv(os.path.join(base_path, "transactions.csv")),
                "encounters": pd.read_csv(os.path.join(base_path, "encounters.csv")),
                "departments": pd.read_csv(os.path.join(base_path, "departments.csv"))
            }
        except Exception as e:
            logger.error(f"CSV extraction failed for {hospital_key}: {e}")
            return {}

    def extract_claims(self):
        claims_dir = "data/raw/claims/"
        all_claims = []
        for file in os.listdir(claims_dir):
            if file.endswith(".csv"):
                path = os.path.join(claims_dir, file)
                try:
                    claims = pd.read_csv(path)
                    claims['source_file'] = file
                    all_claims.append(claims)
                except Exception as e:
                    logger.warning(f"Failed to read claims file {file}: {e}")
        return pd.concat(all_claims, ignore_index=True)

    def extract_cptcodes(self):
            try:
               df = pd.read_csv("data/raw/reference/cptcodes.csv")
               df.columns = df.columns.str.strip().str.lower()
               df = df.rename(columns={
                      "procedure code category": "procedure_code_category",
                      "procedure code descriptions": "procedure_description"
                    })
               df["procedurecode"] = df.index
               return df
            except Exception as e:
                   logger.warning("Failed to load CPT codes: " + str(e))
                   return pd.DataFrame()


    def run(self):
        all_data = {"patients": [], "providers": [], "transactions": [], "encounters": [], "departments": []}

        for db_key in self.hospitals:
            logger.info(f"Extracting data for: {db_key}")
            data = self.extract_mysql(db_key) if self.use_mysql else self.extract_csv(db_key)
            for key in all_data:
                if key in data:
                    data[key]['source_db'] = db_key
                    all_data[key].append(data[key])

        merged_data = {k: pd.concat(v, ignore_index=True) for k, v in all_data.items() if v}
        merged_data["claims"] = self.extract_claims()
        merged_data["cptcodes"] = self.extract_cptcodes()

        return merged_data
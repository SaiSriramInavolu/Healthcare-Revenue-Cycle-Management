import logging
from google.cloud import bigquery

logger = logging.getLogger(__name__)

class RCMAnalytics:
    def __init__(self, project_id="python-sql-project-467708"):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id

    def _run_query(self, query: str, query_name: str):
        try:
            query_job = self.client.query(query)
            results = query_job.result()
            logger.info(f"Successfully ran query: {query_name}")
            return results.to_dataframe()
        except Exception as e:
            logger.error(f"Error running query {query_name}: {e}")
            return None

    def calculate_kpis(self):
        logger.info("Calculating RCM KPIs...")

        total_revenue_query = f"""
            SELECT SUM(Amount) as total_revenue
            FROM `{self.project_id}.gold.fact_transactions`
        """
        total_revenue = self._run_query(total_revenue_query, "Total Revenue")
        if total_revenue is not None: logger.info(f"Total Revenue: {total_revenue['total_revenue'].iloc[0]}")

        revenue_by_hospital_query = f"""
            SELECT source_db, SUM(Amount) as revenue
            FROM `{self.project_id}.gold.fact_transactions`
            GROUP BY source_db
        """
        revenue_by_hospital = self._run_query(revenue_by_hospital_query, "Revenue by Hospital")
        if revenue_by_hospital is not None: logger.info(f"Revenue by Hospital:\n{revenue_by_hospital}")

        claims_approval_rate_query = f"""
            SELECT
                COUNT(CASE WHEN claimstatus = 'Approved' THEN 1 END) * 100.0 / COUNT(*) as approval_rate
            FROM `{self.project_id}.gold.fact_claims`
        """
        claims_approval_rate = self._run_query(claims_approval_rate_query, "Claims Approval Rate")
        if claims_approval_rate is not None: logger.info(f"Claims Approval Rate: {claims_approval_rate['approval_rate'].iloc[0]:.2f}%")

        logger.info("Skipping Average Claim Processing Time due to data limitations.")

        patient_volume_query = f"""
            SELECT COUNT(DISTINCT unified_patient_id) as unique_patients
            FROM `{self.project_id}.gold.dim_patients`
        """
        patient_volume = self._run_query(patient_volume_query, "Patient Volume")
        if patient_volume is not None: logger.info(f"Unique Patient Volume: {patient_volume['unique_patients'].iloc[0]}")

        logger.info("RCM KPI calculation complete.")

    def run_analytics(self):
        self.calculate_kpis()
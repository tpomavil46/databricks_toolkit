"""
Billing monitoring utilities for Databricks toolkit.

Provides comprehensive cost monitoring through BigQuery exports,
CLI integration, and dashboard capabilities.
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from google.cloud import bigquery
import pandas as pd

# Optional logger import - handle case where it's not available
try:
    from utils.logger import log_function_call
except ImportError:
    # Fallback decorator if logger is not available
    def log_function_call(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper


class BillingMonitor:
    """Comprehensive billing monitoring for Databricks toolkit."""

    def __init__(
        self,
        project_id: str = "mydatabrickssandbox",
        billing_account_id: str = "01DAC5_0FD782_74837A",
    ):
        """
        Initialize billing monitor.

        Args:
            project_id: Google Cloud project ID
            billing_account_id: Billing account ID (with underscores, not hyphens)
        """
        self.project_id = project_id
        self.billing_account_id = billing_account_id
        self.bq_client = bigquery.Client(project=project_id)
        self.table_name = (
            f"billing_export.gcp_billing_export_resource_v1_{billing_account_id}"
        )

    @log_function_call
    def test_connection(self) -> pd.DataFrame:
        """
        Test connection and get table schema.

        Returns:
            DataFrame with sample data
        """
        query = f"""
        SELECT *
        FROM `{self.project_id}.{self.table_name}`
        LIMIT 5
        """

        return self.bq_client.query(query).to_dataframe()

    @log_function_call
    def get_monthly_costs(self, year: int = 2025, month: int = 7) -> pd.DataFrame:
        """
        Get monthly cost breakdown.

        Args:
            year: Year to query (default: 2025)
            month: Month to query 1-12 (default: 7 for July)

        Returns:
            DataFrame with monthly cost data
        """
        query = f"""
        SELECT
          service.description,
          SUM(CAST(cost AS NUMERIC)) AS Monthly_Cost,
          COUNT(DISTINCT DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific'))) AS Days_With_Costs,
          AVG(CAST(cost AS NUMERIC)) AS Average_Daily_Cost
        FROM `{self.project_id}.{self.table_name}`
        WHERE cost_type != 'tax'
          AND cost_type != 'adjustment'
          AND EXTRACT(YEAR FROM usage_start_time) = {year}
          AND EXTRACT(MONTH FROM usage_start_time) = {month}
        GROUP BY service.description
        ORDER BY Monthly_Cost DESC
        """

        return self.bq_client.query(query).to_dataframe()

    @log_function_call
    def get_databricks_costs(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get Databricks-specific costs.

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'

        Returns:
            DataFrame with Databricks cost data
        """
        query = f"""
        WITH databricks_services AS (
          SELECT 'Compute Engine' as service_name UNION ALL
          SELECT 'Cloud Storage' UNION ALL
          SELECT 'BigQuery' UNION ALL
          SELECT 'Cloud Logging' UNION ALL
          SELECT 'Cloud Monitoring' UNION ALL
          SELECT 'Cloud Network'
        ),
        cost_data AS (
          SELECT *
          FROM `{self.project_id}.{self.table_name}`
          WHERE cost_type != 'tax'
            AND cost_type != 'adjustment'
            AND usage_start_time >= '{start_date}T00:00:00 US/Pacific'
            AND usage_start_time < '{end_date}T00:00:00 US/Pacific'
            AND service.description IN (SELECT service_name FROM databricks_services)
        )
        SELECT
          DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS Day,
          service.description,
          SUM(CAST(cost AS NUMERIC)) AS Daily_Cost
        FROM cost_data
        GROUP BY Day, service.description
        ORDER BY Day DESC, Daily_Cost DESC
        """

        return self.bq_client.query(query).to_dataframe()

    @log_function_call
    def get_daily_costs(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Get detailed daily cost breakdown.

        Args:
            start_date: Start date in format 'YYYY-MM-DD'
            end_date: End date in format 'YYYY-MM-DD'

        Returns:
            DataFrame with daily cost data
        """
        query = f"""
        WITH
          spend_cud_fee_skus AS (
          SELECT
            *
          FROM
            UNNEST( ['']) AS fee_sku_id ),
          cost_data AS (
          SELECT
            *,
          IF
            (sku.id IN (
              SELECT
                *
              FROM
                spend_cud_fee_skus), cost, 0) AS spend_cud_fee_cost,
            cost - IFNULL(cost_at_effective_price_default, cost) AS spend_cud_savings,
            IFNULL(cost_at_effective_price_default, cost) - cost_at_list AS negotiated_savings,
            IFNULL( (
              SELECT
                SUM(CAST(c.amount AS NUMERIC))
              FROM
                UNNEST(credits) c
              WHERE
                c.type IN ('COMMITTED_USAGE_DISCOUNT',
                  'COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE',
                  'FEE_UTILIZATION_OFFSET')), 0) AS cud_credits,
            IFNULL( (
              SELECT
                SUM(CAST(c.amount AS NUMERIC))
              FROM
                UNNEST(credits) c
              WHERE
                c.type IN ('CREDIT_TYPE_UNSPECIFIED',
                  'PROMOTION',
                  'SUSTAINED_USAGE_DISCOUNT',
                  'DISCOUNT',
                  'FREE_TIER',
                  'SUBSCRIPTION_BENEFIT',
                  'RESELLER_MARGIN')), 0) AS other_savings
          FROM
            `{self.project_id}.{self.table_name}`
          WHERE
            cost_type != 'tax'
            AND cost_type != 'adjustment'
            AND usage_start_time >= '{start_date}T00:00:00 US/Pacific'
            AND usage_start_time < '{end_date}T00:00:00 US/Pacific' )
        SELECT
          DATE(TIMESTAMP_TRUNC(usage_start_time, Day, 'US/Pacific')) AS Day,
          service.description,
          SUM(CAST(IFNULL(cost_at_effective_price_default, cost) AS NUMERIC)) - SUM(CAST(spend_cud_fee_cost AS NUMERIC)) AS Cost,
          SUM(CAST(spend_cud_fee_cost AS NUMERIC)) + SUM(CAST(spend_cud_savings AS NUMERIC)) + SUM(CAST(cud_credits AS NUMERIC)) AS Savings_programs,
          SUM(CAST(other_savings AS NUMERIC)) AS Other_savings,
          SUM(CAST(cost AS NUMERIC)) + SUM(CAST(cud_credits AS NUMERIC)) + SUM(CAST(other_savings AS NUMERIC)) AS Subtotal
        FROM
          cost_data
        GROUP BY
          Day,
          service.description
        ORDER BY
          Day DESC,
          Subtotal DESC
        """

        return self.bq_client.query(query).to_dataframe()

    @log_function_call
    def check_cost_threshold(self, threshold: float = 100.0) -> Dict[str, Any]:
        """
        Check if current month costs exceed threshold.

        Args:
            threshold: Cost threshold in dollars

        Returns:
            Dict with cost status and alerts
        """
        now = datetime.now()
        df = self.get_monthly_costs(now.year, now.month)

        total_cost = df["Monthly_Cost"].sum()
        databricks_cost = df[
            df["description"].isin(
                [
                    "Compute Engine",
                    "Cloud Storage",
                    "BigQuery",
                    "Cloud Logging",
                    "Cloud Monitoring",
                    "Cloud Network",
                ]
            )
        ]["Monthly_Cost"].sum()

        return {
            "total_cost": total_cost,
            "databricks_cost": databricks_cost,
            "threshold_exceeded": total_cost > threshold,
            "databricks_threshold_exceeded": databricks_cost > threshold * 0.8,
            "alert": total_cost > threshold,
            "services": df.to_dict("records"),
        }

    @log_function_call
    def generate_cost_report(self, year: int, month: int) -> Dict[str, Any]:
        """
        Generate comprehensive cost report.

        Args:
            year: Year to report on
            month: Month to report on

        Returns:
            Dict with cost report data
        """
        monthly_df = self.get_monthly_costs(year, month)

        # Get date range for the month
        start_date = f"{year}-{month:02d}-01"
        if month == 12:
            end_date = f"{year + 1}-01-01"
        else:
            end_date = f"{year}-{month + 1:02d}-01"

        daily_df = self.get_daily_costs(start_date, end_date)
        databricks_df = self.get_databricks_costs(start_date, end_date)

        return {
            "month": month,
            "year": year,
            "total_cost": monthly_df["Monthly_Cost"].sum(),
            "databricks_cost": databricks_df["Daily_Cost"].sum(),
            "services": monthly_df.to_dict("records"),
            "daily_breakdown": daily_df.to_dict("records"),
            "databricks_breakdown": databricks_df.to_dict("records"),
            "cost_alerts": self.check_cost_threshold(),
        }

    @log_function_call
    def test_simple_query(self, year: int, month: int) -> pd.DataFrame:
        """
        Test a simple query to debug the GROUP BY issue.

        Args:
            year: Year to query
            month: Month to query (1-12)

        Returns:
            DataFrame with test data
        """
        query = f"""
        SELECT
          service.description,
          SUM(CAST(cost AS NUMERIC)) AS total_cost
        FROM `{self.project_id}.{self.table_name}`
        WHERE cost_type != 'tax'
          AND cost_type != 'adjustment'
          AND EXTRACT(YEAR FROM usage_start_time) = {year}
          AND EXTRACT(MONTH FROM usage_start_time) = {month}
        GROUP BY service.description
        ORDER BY total_cost DESC
        LIMIT 10
        """

        return self.bq_client.query(query).to_dataframe()

    def export_to_csv(self, df: pd.DataFrame, filename: str) -> str:
        """
        Export DataFrame to CSV.

        Args:
            df: DataFrame to export
            filename: Output filename

        Returns:
            Path to exported file
        """
        output_path = f"data/processed/{filename}"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        return output_path


class BillingCLI:
    """CLI interface for billing monitoring."""

    def __init__(self, monitor: BillingMonitor):
        self.monitor = monitor

    def run_monthly_report(
        self, year: int, month: int, output_file: Optional[str] = None
    ):
        """Run monthly cost report."""
        df = self.monitor.get_monthly_costs(year, month)

        if output_file:
            self.monitor.export_to_csv(df, output_file)
            print(f"✅ Report exported to: {output_file}")

        print(f"\n📊 Monthly Cost Report - {year}-{month:02d}")
        print("=" * 50)
        print(df.to_string(index=False))

        total_cost = df["Monthly_Cost"].sum()
        print(f"\n💰 Total Cost: ${total_cost:.2f}")

    def run_cost_check(self, threshold: float = 100.0):
        """Check current month costs against threshold."""
        status = self.monitor.check_cost_threshold(threshold)

        print(f"\n🔍 Cost Threshold Check (${threshold})")
        print("=" * 40)
        print(f"Total Cost: ${status['total_cost']:.2f}")
        print(f"Databricks Cost: ${status['databricks_cost']:.2f}")

        if status["alert"]:
            print("🚨 ALERT: Cost threshold exceeded!")
        else:
            print("✅ Costs within threshold")




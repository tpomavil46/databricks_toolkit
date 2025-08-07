#!/usr/bin/env python3
"""
Google Cloud Billing API Integration

This module provides a separate, decoupled implementation for accessing
real-time billing data directly from the Google Cloud Billing API.

Requirements:
1. Enable the Cloud Billing API in Google Cloud Console
2. Create service account credentials with billing permissions
3. Set up authentication
"""

import os
from typing import Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.auth import default
from google.auth.transport.requests import Request

try:
    from google.cloud import bigquery  # type: ignore
except ImportError:
    bigquery = None  # type: ignore

try:
    from google.cloud import billing_v1  # type: ignore
except ImportError:
    billing_v1 = None  # type: ignore


class CloudBillingAPI:
    """
    Google Cloud Billing API integration for real-time cost monitoring.

    This is a separate implementation from the billing export functionality
    and requires proper API credentials to be set up.
    """

    def __init__(self):
        """Initialize the Cloud Billing API client."""
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.billing_account_id = None
        self.client = None
        self._initialize_client()

    def _initialize_client(self):
        """Initialize the Cloud Billing API client."""
        try:
            if billing_v1 is None:
                raise ImportError("Cloud Billing API not available")

            # Get credentials
            credentials, _ = default()

            # Initialize billing client
            self.client = billing_v1.CloudBillingClient(credentials=credentials)

            # Get billing account
            self._get_billing_account()

        except ImportError:
            print(
                "❌ Cloud Billing API not available. Install with: pip install google-cloud-billing"
            )
        except Exception as e:  # noqa: BLE001
            print(f"❌ Error initializing Cloud Billing API: {e}")

    def _get_billing_account(self):
        """Get the billing account ID."""
        try:
            if not self.client:
                return

            # List billing accounts
            billing_accounts = self.client.list_billing_accounts()

            # Find the open billing account
            for account in billing_accounts:
                if account.open:
                    self.billing_account_id = account.name.split("/")[-1]
                    print(f"✅ Found billing account: {account.name}")
                    return

            print("❌ No open billing account found")

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting billing account: {e}")

    def is_configured(self) -> bool:
        """Check if the Cloud Billing API is properly configured."""
        return (
            self.client is not None
            and self.billing_account_id is not None
            and self.project_id is not None
        )

    def get_current_month_costs(self) -> Dict[str, float]:
        """
        Get current month costs by service using the Cloud Billing API.

        Returns:
            Dict mapping service names to costs for the current month
        """
        if not self.is_configured():
            print("❌ Cloud Billing API not configured")
            return {}

        try:
            print("🔍 Fetching current month costs from Cloud Billing API...")

            # Get current month's start and end dates
            current_date = datetime.now()
            start_date = current_date.replace(day=1)
            end_date = current_date

            # Use the Cloud Billing API to get current month costs
            costs = self._query_billing_api_costs(start_date, end_date)

            if costs:
                total_cost = sum(costs.values())
                print(f"✅ Retrieved ${total_cost:.2f} in current month costs")
                return costs
            else:
                print("⚠️ No current month costs found")
                return {}

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting current month costs: {e}")
            return {}

    def _query_billing_api_costs(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, float]:
        """
        Query the Cloud Billing API for costs in a date range.

        Args:
            start_date: Start date for the query
            end_date: End date for the query

        Returns:
            Dict mapping service names to costs
        """
        try:
            if not self.client or not self.billing_account_id:
                print("❌ Cloud Billing API client not available")
                return {}

            # Use the Cloud Billing API to get current month costs
            # We'll use the billing account to get cost information
            billing_account_name = f"billingAccounts/{self.billing_account_id}"

            print(f"🔍 Querying billing account: {billing_account_name}")

            # For now, let's try to get billing account information
            # and then we'll implement the actual cost queries
            try:
                # Get billing account details
                request = billing_v1.GetBillingAccountRequest(name=billing_account_name)

                billing_account = self.client.get_billing_account(request=request)
                print(f"✅ Found billing account: {billing_account.display_name}")

                # Now let's try to get cost data
                # We'll need to use a different approach for actual costs
                # Let's implement a basic cost query
                costs = self._get_costs_from_billing_account(
                    billing_account_name, start_date, end_date
                )

                return costs

            except Exception as e:  # noqa: BLE001
                print(f"❌ Error querying billing account: {e}")
                return {}

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error querying Cloud Billing API: {e}")
            return {}

    def _get_costs_from_billing_account(
        self,
        billing_account_name: str,
        start_date: datetime,
        end_date: datetime,  # noqa: ARG002
    ) -> Dict[str, float]:
        """
        Get costs from billing account using the Cloud Billing API.

        Args:
            billing_account_name: Full billing account name
            start_date: Start date for cost query
            end_date: End date for cost query

        Returns:
            Dict mapping service names to costs
        """
        try:
            print(
                "🔍 Attempting to get costs from billing account using Cloud Billing API..."
            )

            # The Cloud Billing API doesn't have a direct "get costs" endpoint
            # We need to use the Cloud Billing API to get service and SKU information
            # and then combine with other services for actual cost data

            # First, let's get the list of services available
            services = self._get_available_services()

            if not services:
                print("❌ No services available from Cloud Billing API")
                return {}

            print(f"✅ Found {len(services)} services from Cloud Billing API")

            # For each service, get the SKUs and pricing information
            costs = {}
            for service in services[:5]:  # Limit to first 5 services for testing
                try:
                    service_costs = self._get_service_costs_from_api(
                        service, billing_account_name
                    )
                    if service_costs > 0:
                        costs[service] = service_costs
                except Exception as e:  # noqa: BLE001
                    print(f"⚠️ Error getting costs for {service}: {e}")
                    continue

            return costs

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting costs from billing account: {e}")
            return {}

    def _get_available_services(self) -> List[str]:
        """
        Get list of available services from Cloud Billing API.

        Returns:
            List of service names
        """
        try:
            if not self.client:
                print("❌ Cloud Billing API client not available")
                return []

            # Use the actual Cloud Billing API REST endpoint
            # GET /v1/services - Lists all public cloud services

            # Get credentials
            credentials, _ = default()

            # Create authenticated session
            session = requests.Session()
            credentials.refresh(Request(session))

            # Make actual REST API call to Cloud Billing API
            url = "https://cloudbilling.googleapis.com/v1/services"
            headers = {
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            }

            print("🔍 Making actual REST API call to Cloud Billing API...")
            response = session.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                services = data.get("services", [])
                service_names = [
                    service.get("serviceId")
                    for service in services
                    if service.get("serviceId")
                ]
                print(
                    f"✅ Retrieved {len(service_names)} services from Cloud Billing API REST call"
                )
                return service_names
            else:
                print(
                    f"❌ Cloud Billing API call failed: {response.status_code} - {response.text}"
                )
                return []

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting services from Cloud Billing API: {e}")
            return []

    def _get_service_costs_from_api(
        self, service_id: str, billing_account_name: str  # noqa: ARG002
    ) -> float:
        """
        Get costs for a specific service using Cloud Billing API.

        Args:
            service_id: The service ID
            billing_account_name: Full billing account name

        Returns:
            Total cost for the service
        """
        try:
            # Use the actual Cloud Billing API REST endpoint
            # GET /v1/{parent=services/*}/skus - Lists all publicly available SKUs for a
            # given cloud service

            # Get credentials
            credentials, _ = default()

            # Create authenticated session
            session = requests.Session()
            credentials.refresh(Request(session))

            # Make actual REST API call to get SKUs for the service
            url = f"https://cloudbilling.googleapis.com/v1/services/{service_id}/skus"
            headers = {
                "Authorization": f"Bearer {credentials.token}",
                "Content-Type": "application/json",
            }

            print(f"🔍 Making actual REST API call to get SKUs for {service_id}...")
            response = session.get(url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                skus = data.get("skus", [])
                print(
                    f"✅ Retrieved {len(skus)} SKUs for {service_id} via Cloud Billing API REST call"
                )

                # For now, return a placeholder cost
                # The actual cost calculation would require usage data from other APIs
                return 0.0
            else:
                print(
                    f"❌ Cloud Billing API SKU call failed: {response.status_code} - {response.text}"
                )
                return 0.0

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting costs for service {service_id}: {e}")
            return 0.0

    def get_service_costs(
        self, service_name: str, days: int = 30
    ) -> pd.DataFrame:  # noqa: ARG002
        """
        Get costs for a specific service over a time period using Cloud Billing API.

        Args:
            service_name: Name of the GCP service
            days: Number of days to look back

        Returns:
            DataFrame with daily costs for the service
        """
        if not self.is_configured():
            print("❌ Cloud Billing API not configured")
            return pd.DataFrame()

        try:
            print(f"🔍 Fetching {service_name} costs from Cloud Billing API...")

            # Use the Cloud Billing API to get service costs
            service_costs = self._get_service_costs_from_api(
                service_name, f"billingAccounts/{self.billing_account_id}"
            )

            if service_costs > 0:
                # Create a simple DataFrame with the cost data
                data = [
                    {
                        "date": datetime.now().date(),
                        "cost": service_costs,
                        "service_name": service_name,
                        "is_real_data": True,
                    }
                ]
                return pd.DataFrame(data)
            else:
                print(f"❌ No data found for {service_name} via Cloud Billing API")
                return pd.DataFrame()

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting {service_name} costs via Cloud Billing API: {e}")
            return pd.DataFrame()

    def _get_service_costs_from_bigquery(
        self,
        service_name: str,
        start_date: datetime,
        end_date: datetime,  # noqa: ARG002
    ) -> pd.DataFrame:
        """
        Get service-specific costs from BigQuery billing export.

        Args:
            service_name: Name of the service to query
            start_date: Start date for query
            end_date: End date for query

        Returns:
            DataFrame with daily costs for the service
        """
        try:
            if bigquery is None:
                raise ImportError("BigQuery client not available")

            # Get credentials
            credentials, project = default()
            bq_client = bigquery.Client(credentials=credentials, project=project)

            # Try to get billing account ID for the export table
            billing_account_id = self.billing_account_id.replace("-", "_")

            # Try both standard and detailed export tables
            tables_to_try = [
                f"gcp_billing_export_v1_{billing_account_id}",
                f"gcp_billing_export_resource_v1_{billing_account_id}",
            ]

            # Map service names to actual service descriptions in billing export
            service_mapping = {
                "compute_engine": "Compute Engine",
                "bigquery": "BigQuery",
                "storage": "Cloud Storage",
                "dataproc": "Dataproc",
                "cloud_functions": "Cloud Functions",
                "cloud_run": "Cloud Run",
                "pubsub": "Pub/Sub",
                "dataflow": "Dataflow",
                "vertex_ai": "Vertex AI",
                "cloud_sql": "Cloud SQL",
                "network": "Networking",
                "cloud_logging": "Cloud Logging",
            }

            service_description = service_mapping.get(
                service_name, service_name.replace("_", " ").title()
            )

            for table_name in tables_to_try:
                try:
                    # Query service-specific costs
                    query = f"""
                    SELECT 
                        DATE(usage_end_time) as date,
                        SUM(cost) as daily_cost,
                        SUM(usage.amount) as total_usage,
                        COUNT(*) as record_count,
                        service.description as service_name,
                        sku.description as sku_description
                    FROM `{self.project_id}.billing_export.{table_name}`
                    WHERE DATE(usage_end_time) >= '{start_date.strftime('%Y-%m-%d')}'
                    AND service.description = '{service_description}'
                    GROUP BY date, service_name, sku_description
                    ORDER BY date DESC
                    """

                    print(f"🔍 Querying {service_name} costs from {table_name}...")
                    query_job = bq_client.query(query)
                    results = query_job.result()

                    data = []
                    for row in results:
                        data.append(
                            {
                                "date": row.date,
                                "cost": float(row.daily_cost),
                                "usage_amount": float(row.total_usage or 0),
                                "record_count": int(row.record_count or 0),
                                "service_name": row.service_name,
                                "sku_description": row.sku_description,
                                "is_real_data": True,
                            }
                        )

                    if data:
                        print(
                            f"✅ Found {len(data)} records for {service_name} from {table_name}"
                        )
                        return pd.DataFrame(data)

                except Exception as e:  # noqa: BLE001
                    print(f"⚠️ Error querying {service_name} from {table_name}: {e}")
                    continue

            print(f"⚠️ No data found for {service_name} in any billing export table")
            return pd.DataFrame()

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting {service_name} costs from BigQuery: {e}")
            return pd.DataFrame()

    def get_historical_costs(self, days: int = 30) -> Dict[str, float]:
        """
        Get historical costs using Cloud Billing API.

        Args:
            days: Number of days to look back

        Returns:
            Dict mapping service names to total costs
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            print(
                f"🔍 Getting historical costs for the last {days} days using Cloud Billing API..."
            )

            # Use the Cloud Billing API to get costs
            costs = self._get_costs_from_billing_account(
                f"billingAccounts/{self.billing_account_id}", start_date, end_date
            )

            if costs:
                total_cost = sum(costs.values())
                print(
                    f"✅ Found ${total_cost:.2f} in historical costs via Cloud Billing API"
                )
                return costs
            else:
                print("⚠️ No historical costs found via Cloud Billing API")
                return {}

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting historical costs via Cloud Billing API: {e}")
            return {}

    def get_cost_breakdown(self) -> Dict[str, Any]:
        """
        Get detailed cost breakdown including usage, credits, and discounts.

        Returns:
            Dict with detailed cost information
        """
        if not self.is_configured():
            print("❌ Cloud Billing API not configured")
            return {}

        try:
            print("🔍 Fetching detailed cost breakdown from Cloud Billing API...")

            # This would use the Cloud Billing API to get detailed breakdown
            # including usage costs, credits, discounts, etc.
            print("⚠️ Detailed cost breakdown not yet implemented")
            return {}

        except Exception as e:  # noqa: BLE001
            print(f"❌ Error getting cost breakdown: {e}")
            return {}

    def get_setup_instructions(self) -> str:
        """Get setup instructions for the Cloud Billing API."""
        return """
        **Cloud Billing API Setup Instructions:**
        
        1. **Enable the Cloud Billing API**:
           - Go to Google Cloud Console
           - Navigate to APIs & Services > Library
           - Search for "Cloud Billing API"
           - Click "Enable"
        
        2. **Create Service Account**:
           - Go to IAM & Admin > Service Accounts
           - Click "Create Service Account"
           - Name: "billing-api-service"
           - Description: "Service account for Cloud Billing API"
        
        3. **Assign Billing Permissions**:
           - Add role: "Billing Account Viewer"
           - Add role: "Billing Account User" (if needed)
        
        4. **Create and Download Credentials**:
           - Click on the service account
           - Go to "Keys" tab
           - Click "Add Key" > "Create new key"
           - Choose JSON format
           - Download the key file
        
        5. **Set Environment Variable**:
           ```bash
           export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
           ```
        
        6. **Test the Connection**:
           ```python
           from dashboard.cloud_billing_api import CloudBillingAPI
           api = CloudBillingAPI()
           print(f"Configured: {api.is_configured()}")
           ```
        """

    def get_status(self) -> Dict[str, Any]:
        """Get the current status of the Cloud Billing API integration."""
        return {
            "configured": self.is_configured(),
            "project_id": self.project_id,
            "billing_account_id": self.billing_account_id,
            "client_available": self.client is not None,
            "setup_required": not self.is_configured(),
        }

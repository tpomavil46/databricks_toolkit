#!/usr/bin/env python3
"""
Dashboard Configuration

Configuration system for dynamic data sources and cloud provider integrations.
"""

import os
from pathlib import Path
from typing import Dict, List, Any, Optional
import json


class DashboardConfig:
    """Configuration class for the multi-cloud analytics dashboard."""

    def __init__(self):
        self.config_file = Path(__file__).parent / "config.json"
        self.load_config()

    def load_config(self):
        """Load configuration from file or create default."""
        if self.config_file.exists():
            with open(self.config_file, "r") as f:
                self.config = json.load(f)
        else:
            self.config = self.get_default_config()
            self.save_config()

    def save_config(self):
        """Save configuration to file."""
        with open(self.config_file, "w") as f:
            json.dump(self.config, f, indent=2)

    def get_default_config(self) -> Dict[str, Any]:
        """Get default configuration."""
        return {
            "data_sources": {
                "retail": {
                    "name": "Retail Analytics",
                    "description": "E-commerce and retail data analytics",
                    "cloud_providers": ["databricks", "gcp"],
                    "metrics": ["revenue", "products", "customers", "cost"],
                    "tables": ["orders", "customers", "products", "inventory"],
                    "color_scheme": "retail",
                },
                "healthcare": {
                    "name": "Healthcare Analytics",
                    "description": "Patient and medical data analytics",
                    "cloud_providers": ["databricks", "gcp", "aws"],
                    "metrics": ["patients", "treatments", "costs", "outcomes"],
                    "tables": ["patients", "treatments", "medications", "billing"],
                    "color_scheme": "healthcare",
                },
                "finance": {
                    "name": "Financial Analytics",
                    "description": "Banking and financial data analytics",
                    "cloud_providers": ["databricks", "gcp", "azure"],
                    "metrics": ["transactions", "revenue", "risk", "compliance"],
                    "tables": ["transactions", "accounts", "risk_scores", "compliance"],
                    "color_scheme": "finance",
                },
                "manufacturing": {
                    "name": "Manufacturing Analytics",
                    "description": "Industrial and manufacturing data analytics",
                    "cloud_providers": ["databricks", "aws", "azure"],
                    "metrics": ["production", "quality", "efficiency", "cost"],
                    "tables": ["production", "quality", "machines", "maintenance"],
                    "color_scheme": "manufacturing",
                },
                "custom": {
                    "name": "Custom Analytics",
                    "description": "Custom data source configuration",
                    "cloud_providers": ["databricks", "gcp", "aws", "azure"],
                    "metrics": ["custom_metric_1", "custom_metric_2"],
                    "tables": ["custom_table_1", "custom_table_2"],
                    "color_scheme": "custom",
                },
            },
            "cloud_providers": {
                "databricks": {
                    "name": "Databricks",
                    "color": "#ff6b35",
                    "services": ["compute", "storage", "analytics", "ml"],
                    "cost_metrics": ["dbu_hours", "storage_gb", "network_gb"],
                    "api_endpoint": "https://your-workspace.cloud.databricks.com",
                    "auth_method": "token",
                },
                "gcp": {
                    "name": "Google Cloud Platform",
                    "color": "#4285f4",
                    "services": ["compute", "storage", "bigquery", "dataproc"],
                    "cost_metrics": ["compute_hours", "storage_gb", "network_gb"],
                    "api_endpoint": "https://bigquery.googleapis.com",
                    "auth_method": "service_account",
                },
                "aws": {
                    "name": "Amazon Web Services",
                    "color": "#ff9900",
                    "services": ["ec2", "s3", "glue", "emr"],
                    "cost_metrics": ["compute_hours", "storage_gb", "network_gb"],
                    "api_endpoint": "https://aws.amazon.com",
                    "auth_method": "iam",
                },
                "azure": {
                    "name": "Microsoft Azure",
                    "color": "#0078d4",
                    "services": ["compute", "storage", "synapse", "databricks"],
                    "cost_metrics": ["compute_hours", "storage_gb", "network_gb"],
                    "api_endpoint": "https://azure.microsoft.com",
                    "auth_method": "service_principal",
                },
            },
            "cost_monitoring": {
                "enabled": True,
                "refresh_interval": 300,  # 5 minutes
                "budget_alerts": True,
                "cost_breakdown": ["compute", "storage", "network", "analytics"],
                "currency": "USD",
            },
            "performance_monitoring": {
                "enabled": True,
                "metrics": ["processing_time", "data_volume", "error_rate"],
                "thresholds": {
                    "processing_time_minutes": 60,
                    "error_rate_percent": 5,
                    "cost_per_gb": 0.1,
                },
            },
            "integrations": {
                "databricks": {
                    "workspace_url": os.getenv("DATABRICKS_WORKSPACE_URL"),
                    "token": os.getenv("DATABRICKS_TOKEN"),
                    "catalog": "hive_metastore",
                    "schema": "default",
                },
                "gcp": {
                    "project_id": os.getenv("GCP_PROJECT_ID"),
                    "service_account_key": os.getenv("GCP_SERVICE_ACCOUNT_KEY"),
                    "dataset": "billing_export",
                    "location": "US",
                },
                "aws": {
                    "access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
                    "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
                    "region": os.getenv("AWS_REGION", "us-east-1"),
                    "s3_bucket": os.getenv("AWS_S3_BUCKET"),
                },
                "azure": {
                    "tenant_id": os.getenv("AZURE_TENANT_ID"),
                    "client_id": os.getenv("AZURE_CLIENT_ID"),
                    "client_secret": os.getenv("AZURE_CLIENT_SECRET"),
                    "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID"),
                },
            },
        }

    def get_data_source_config(self, source_name: str) -> Dict[str, Any]:
        """Get configuration for a specific data source."""
        return self.config["data_sources"].get(source_name, {})

    def get_cloud_provider_config(self, provider_name: str) -> Dict[str, Any]:
        """Get configuration for a specific cloud provider."""
        return self.config["cloud_providers"].get(provider_name, {})

    def get_integration_config(self, provider_name: str) -> Dict[str, Any]:
        """Get integration configuration for a cloud provider."""
        return self.config["integrations"].get(provider_name, {})

    def update_data_source(self, source_name: str, config: Dict[str, Any]):
        """Update configuration for a data source."""
        self.config["data_sources"][source_name] = config
        self.save_config()

    def update_cloud_provider(self, provider_name: str, config: Dict[str, Any]):
        """Update configuration for a cloud provider."""
        self.config["cloud_providers"][provider_name] = config
        self.save_config()

    def get_available_data_sources(self) -> List[str]:
        """Get list of available data sources."""
        return list(self.config["data_sources"].keys())

    def get_available_cloud_providers(self) -> List[str]:
        """Get list of available cloud providers."""
        return list(self.config["cloud_providers"].keys())

    def is_cost_monitoring_enabled(self) -> bool:
        """Check if cost monitoring is enabled."""
        return self.config["cost_monitoring"]["enabled"]

    def is_performance_monitoring_enabled(self) -> bool:
        """Check if performance monitoring is enabled."""
        return self.config["performance_monitoring"]["enabled"]


# Global configuration instance
config = DashboardConfig()

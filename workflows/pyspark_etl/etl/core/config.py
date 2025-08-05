"""
Configuration Management for ETL Framework

This module provides parameterized configuration management for ETL operations,
removing hardcoded values and providing environment-based configuration.
"""

import os
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
from utils.logger import log_function_call


@dataclass
class ClusterConfig:
    """Cluster configuration for Databricks operations."""

    cluster_id: str
    profile: str = "databricks"
    workspace_url: Optional[str] = None

    @classmethod
    def from_env(cls) -> "ClusterConfig":
        """Create cluster config from environment variables."""
        return cls(
            cluster_id=os.getenv("DATABRICKS_CLUSTER_ID", "5802-005055-h7vtizbe"),
            profile=os.getenv("DATABRICKS_PROFILE", "databricks"),
            workspace_url=os.getenv("DATABRICKS_WORKSPACE_URL"),
        )


@dataclass
class DatabaseConfig:
    """Database and catalog configuration."""

    catalog: str = "main"
    schema: str = "default"

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Create database config from environment variables."""
        return cls(
            catalog=os.getenv("DATABRICKS_CATALOG", "main"),
            schema=os.getenv("DATABRICKS_SCHEMA", "default"),
        )


@dataclass
class TableConfig:
    """Table naming and structure configuration."""

    project_name: str
    environment: str = "dev"
    table_prefix: Optional[str] = None
    bronze_suffix: str = "bronze"
    silver_suffix: str = "silver"
    gold_suffix: str = "gold"

    def __post_init__(self):
        """Set table prefix if not provided."""
        if self.table_prefix is None:
            self.table_prefix = f"{self.project_name}_{self.environment}"

    def get_table_name(self, layer: str, table_name: str) -> str:
        """Generate standardized table name."""
        suffix_map = {
            "bronze": self.bronze_suffix,
            "silver": self.silver_suffix,
            "gold": self.gold_suffix,
        }
        suffix = suffix_map.get(layer, layer)
        return f"{self.table_prefix}_{table_name}_{suffix}"

    @classmethod
    def from_env(cls, project_name: str) -> "TableConfig":
        """Create table config from environment variables."""
        return cls(
            project_name=project_name,
            environment=os.getenv("ENVIRONMENT", "dev"),
            table_prefix=os.getenv("TABLE_PREFIX"),
            bronze_suffix=os.getenv("BRONZE_SUFFIX", "bronze"),
            silver_suffix=os.getenv("SILVER_SUFFIX", "silver"),
            gold_suffix=os.getenv("GOLD_SUFFIX", "gold"),
        )


@dataclass
class ValidationConfig:
    """Data validation configuration."""

    null_threshold: float = 0.1
    quality_threshold: float = 0.95
    enable_validation: bool = True
    strict_mode: bool = False

    @classmethod
    def from_env(cls) -> "ValidationConfig":
        """Create validation config from environment variables."""
        return cls(
            null_threshold=float(os.getenv("NULL_THRESHOLD", "0.1")),
            quality_threshold=float(os.getenv("QUALITY_THRESHOLD", "0.95")),
            enable_validation=os.getenv("ENABLE_VALIDATION", "true").lower() == "true",
            strict_mode=os.getenv("STRICT_MODE", "false").lower() == "true",
        )


@dataclass
class PipelineConfig:
    """Complete pipeline configuration."""

    project_name: str
    cluster_config: ClusterConfig = field(default_factory=ClusterConfig.from_env)
    database_config: DatabaseConfig = field(default_factory=DatabaseConfig.from_env)
    table_config: Optional[TableConfig] = None
    validation_config: ValidationConfig = field(
        default_factory=ValidationConfig.from_env
    )
    custom_config: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Initialize table config if not provided."""
        if self.table_config is None:
            self.table_config = TableConfig.from_env(self.project_name)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PipelineConfig":
        """Create pipeline config from dictionary."""
        project_name = config_dict.get("project_name")
        if not project_name:
            raise ValueError("project_name is required in configuration")

        return cls(
            project_name=project_name,
            cluster_config=ClusterConfig(**config_dict.get("cluster", {})),
            database_config=DatabaseConfig(**config_dict.get("database", {})),
            table_config=(
                TableConfig(**config_dict.get("table", {}))
                if "table" in config_dict
                else None
            ),
            validation_config=ValidationConfig(**config_dict.get("validation", {})),
            custom_config=config_dict.get("custom", {}),
        )

    @classmethod
    def from_file(cls, config_path: str) -> "PipelineConfig":
        """Create pipeline config from JSON file."""
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            config_dict = json.load(f)

        return cls.from_dict(config_dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "project_name": self.project_name,
            "cluster": {
                "cluster_id": self.cluster_config.cluster_id,
                "profile": self.cluster_config.profile,
                "workspace_url": self.cluster_config.workspace_url,
            },
            "database": {
                "catalog": self.database_config.catalog,
                "schema": self.database_config.schema,
            },
            "table": {
                "project_name": self.table_config.project_name,
                "environment": self.table_config.environment,
                "table_prefix": self.table_config.table_prefix,
                "bronze_suffix": self.table_config.bronze_suffix,
                "silver_suffix": self.table_config.silver_suffix,
                "gold_suffix": self.table_config.gold_suffix,
            },
            "validation": {
                "null_threshold": self.validation_config.null_threshold,
                "quality_threshold": self.validation_config.quality_threshold,
                "enable_validation": self.validation_config.enable_validation,
                "strict_mode": self.validation_config.strict_mode,
            },
            "custom": self.custom_config,
        }

    def save_to_file(self, config_path: str) -> None:
        """Save config to JSON file."""
        config_path = Path(config_path)
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with open(config_path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)


@log_function_call
def create_default_config(
    project_name: str, environment: str = "dev"
) -> PipelineConfig:
    """Create a default configuration for a project."""
    config = PipelineConfig(project_name=project_name)
    config.table_config.environment = environment
    return config


@log_function_call
def load_config(project_name: str, environment: str = "dev") -> PipelineConfig:
    """Load configuration for a project and environment."""
    config_path = Path(f"config/{project_name}/{environment}.json")

    if config_path.exists():
        return PipelineConfig.from_file(str(config_path))
    else:
        # Create default config if file doesn't exist
        config = create_default_config(project_name, environment)
        config.save_to_file(str(config_path))
        return config

# Configuration Reference

Essential configuration guide for the Databricks Toolkit.

## 🔧 Environment Variables

### Required Variables
```bash
export DATABRICKS_PROFILE="your-profile"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
export GCP_PROJECT_ID="your-project-id"
```

### Optional Variables
```bash
export DATABRICKS_CATALOG="your-catalog"
export DATABRICKS_SCHEMA="your-schema"
export DEBUG="1"
export LOG_LEVEL="INFO"
```

## 📁 Configuration Files

### Environment Config (`config/environments/dev.json`)
```json
{
  "name": "development",
  "databricks": {
    "profile": "dev",
    "cluster_id": "dev-cluster-123",
    "catalog": "dev_catalog",
    "schema": "dev_schema"
  },
  "gcp": {
    "project_id": "dev-project"
  },
  "pipeline": {
    "bronze_path": "/mnt/dev/bronze",
    "silver_path": "/mnt/dev/silver",
    "gold_path": "/mnt/dev/gold"
  }
}
```

### Dashboard Config (`dashboard/config.json`)
```json
{
  "dashboard": {
    "title": "Databricks Toolkit Dashboard",
    "theme": "light",
    "auto_refresh": true
  },
  "gcp_dashboard": {
    "project_id": "your-project-id",
    "cost_threshold": 1000
  }
}
```

## 🎛️ Command-line Configuration

### SQL Workflow
```bash
python main.py sql retail --environment prod
python main.py sql retail --bronze-path /custom/bronze
```

### PySpark ETL Workflow
```bash
python main.py pyspark data_ingestion --config custom_config.json
```

### Dashboard
```bash
streamlit run dashboard/dynamic_dashboard.py --server.port 8502
```

## 🔒 Security Configuration

### Authentication
```bash
export DATABRICKS_TOKEN="your-token"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Permissions
```sql
GRANT USAGE ON CATALOG your_catalog TO your_user;
GRANT CREATE ON SCHEMA your_catalog.your_schema TO your_user;
```

## 🆘 Configuration Troubleshooting

### Common Issues
```bash
# Check configuration
echo $DATABRICKS_PROFILE
ls -la config/environments/

# Validate configuration
python -c "from utils.config import Config; Config('dev').validate()"

# Debug mode
export DEBUG="1"
python main.py sql retail --debug
```

---

**Need more help?** Check the [Troubleshooting](troubleshooting.md) guide!

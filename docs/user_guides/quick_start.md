# Quick Start Guide

Get up and running with the Databricks Toolkit in under 10 minutes!

## 🚀 5-Minute Setup

### Step 1: Install and Configure (2 minutes)

```bash
# Clone and setup
git clone <repository-url>
cd databricks_toolkit
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure Databricks
databricks configure --profile default
export DATABRICKS_PROFILE="default"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
```

### Step 2: Launch Dashboard (1 minute)

```bash
# Start the unified dashboard
streamlit run dashboard/dynamic_dashboard.py
```

Open your browser to `http://localhost:8501`

### Step 3: Run Your First Pipeline (2 minutes)

```bash
# Run a complete data pipeline
python main.py sql retail
```

That's it! You now have:
- ✅ A running dashboard with business analytics
- ✅ A complete data pipeline (Bronze → Silver → Gold)
- ✅ GCP cost monitoring capabilities

## 🎯 What Just Happened?

### Dashboard Features
- **📊 Business Analytics**: View your data tables and create charts
- **📊 Business Analytics**: Monitor data quality and pipeline performance
- **🏗️ Pipeline Builder**: Visualize and build data pipelines
- **💾 Dashboard Management**: Save and load custom configurations

### Pipeline Results
- **Bronze Layer**: Raw data ingested from source
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Business-ready aggregations and KPIs

## 🔧 Next Steps

### Explore the Dashboard
1. Click "📊 Business Analytics" in the sidebar
2. Browse available tables
3. Create your first chart
4. Explore pipeline performance and data quality metrics

### Try Different Workflows
```bash
# SQL workflow (recommended for beginners)
python main.py sql ecommerce

# PySpark ETL workflow (for advanced users)
python main.py pyspark data_ingestion
```

### Monitor Your Pipeline
```bash
# Check pipeline health
python shared/cli/monitoring/health_cli.py

# View GCP costs
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project
```

## 🆘 Troubleshooting

### Common Issues

**Dashboard won't start:**
```bash
# Check if Streamlit is installed
pip install streamlit

# Try running with debug
streamlit run dashboard/dynamic_dashboard.py --logger.level debug
```

**Pipeline fails:**
```bash
# Check cluster status
databricks clusters list

# Verify connection
python -c "from utils.session import DatabricksSession; print('Connection OK')"
```

**Import errors:**
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

## 🎯 Success Checklist

- [ ] Dashboard loads at `http://localhost:8501`
- [ ] Can see business analytics section
- [ ] Can see business analytics section
- [ ] Pipeline runs without errors
- [ ] Can view data tables in dashboard
- [ ] Can create basic charts

## 📚 What's Next?

Now that you're up and running:

1. **[Dashboard Guide](dashboard_guide.md)** - Master the unified dashboard
2. **[CLI Guide](cli_guide.md)** - Learn command-line tools
3. **[Pipeline Guide](pipeline_guide.md)** - Build custom data pipelines
4. **[Cost Monitoring Guide](cost_monitoring_guide.md)** - Optimize cloud costs

---

**Need help?** Check the [Reference](reference/) section or use the built-in help in the dashboard!

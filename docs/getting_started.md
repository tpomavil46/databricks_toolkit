# Getting Started with Databricks Toolkit

Welcome to the Databricks Toolkit! This guide will help you get up and running quickly with our comprehensive data engineering toolkit.

## 🎯 What is the Databricks Toolkit?

The Databricks Toolkit is a comprehensive solution for data engineering on Databricks that provides:

- **SQL-Driven Workflows**: SQL-first data processing with Delta Live Tables (DLT)
- **PySpark ETL Workflows**: Python-first ETL processing
- **GCP Cost Monitoring**: Real-time cloud cost tracking and optimization
- **Administrative Tools**: User, cluster, and workspace management
- **Unified Dashboard**: Business analytics and cost monitoring in one interface

## 🚀 Quick Start (5 Minutes)

### 1. Prerequisites

Before you begin, ensure you have:

- **Python 3.11+** installed
- **Databricks workspace** access
- **Databricks CLI** configured
- **Valid Databricks cluster** running

### 2. Installation

```bash
# Clone the repository
git clone <repository-url>
cd databricks_toolkit

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configuration

```bash
# Configure Databricks connection
databricks configure --profile your-profile

# Set environment variables
export DATABRICKS_PROFILE="your-profile"
export DATABRICKS_CLUSTER_ID="your-cluster-id"
```

### 4. First Run

```bash
# Launch the unified dashboard
streamlit run dashboard/dynamic_dashboard.py

# Or run a simple SQL pipeline
python main.py sql retail
```

## 📊 Choose Your Workflow

### For SQL-First Teams

If your team prefers SQL for data transformations:

```bash
# Run SQL workflow
python main.py sql retail

# Access SQL templates
python shared/sql_library/cli/sql_library_cli.py list-patterns
```

**Best for**: Teams with strong SQL skills, quick prototyping, Delta Live Tables

### For Python-First Teams

If your team prefers Python for ETL:

```bash
# Run PySpark ETL workflow
python main.py pyspark data_ingestion

# Access PySpark transformations
python workflows/pyspark_etl/run.py data_ingestion
```

**Best for**: Complex transformations, custom business logic, ML pipelines

## 🎯 Common Use Cases

### 1. Data Pipeline Development

```bash
# Create a complete Bronze → Silver → Gold pipeline
python main.py sql retail --environment dev

# Monitor pipeline execution
# Health monitoring available through admin tools
```

### 2. Cost Monitoring

```bash
# Monitor GCP costs
make billing-costs YEAR=2025 MONTH=8

# View cost dashboard
streamlit run dashboard/dynamic_dashboard.py
```

### 3. Administrative Tasks

```bash
# List workspace users
python shared/admin/cli/admin_cli.py list-users

# Monitor clusters
python shared/admin/cli/admin_cli.py list-clusters
```

## 🔧 Configuration Options

### Environment Configuration

The toolkit supports multiple environments:

```bash
# Development environment
python main.py sql retail --environment dev

# Production environment (requires additional setup)
python main.py sql retail --environment prod
```

### Custom Configuration

Create custom configurations in `config/environments/`:

```json
{
  "catalog": "your_catalog",
  "schema": "your_schema",
  "cluster_id": "your_cluster_id",
  "warehouse_id": "your_warehouse_id"
}
```

## 📚 Next Steps

Now that you're up and running, explore these resources:

1. **[User Guides](user_guides/)** - Detailed guides for specific features
2. **[API Documentation](api/)** - Complete API reference
3. **[Tutorials](tutorials/)** - Step-by-step tutorials
4. **[Reference](reference/)** - Configuration and troubleshooting

## 🆘 Getting Help

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

**Connection Problems:**
```bash
# Verify Databricks connection
databricks clusters list

# Check cluster status
databricks clusters get --cluster-id your-cluster-id
```

**Import Errors:**
```bash
# Verify virtual environment
which python
pip list | grep databricks

# Reinstall dependencies
pip install -r requirements.txt --force-reinstall
```

### Support Resources

- **Documentation**: Check the [Reference](reference/) section
- **Examples**: See [Tutorials](tutorials/) for step-by-step guides
- **CLI Help**: Use `--help` flag with any command
- **Dashboard**: Use the built-in help in the Streamlit dashboard

## 🎯 Success Checklist

After completing the setup, you should be able to:

- [ ] Dashboard loads at `http://localhost:8501`
- [ ] Can see business analytics section
- [ ] Pipeline runs without errors
- [ ] Can view data tables in dashboard
- [ ] Can create basic charts
- [ ] Can monitor GCP costs with `make billing-costs`

## 🎯 Success Metrics

You'll know you're successfully using the toolkit when you can:

- ✅ Run data pipelines with `python main.py sql <project>`
- ✅ Monitor pipeline performance and data quality
- ✅ Manage workspace resources with admin tools
- ✅ Build custom dashboards with the unified interface
- ✅ Deploy pipelines to production environments

---

**Ready to dive deeper?** Check out our [User Guides](user_guides/) for detailed walkthroughs of specific features!

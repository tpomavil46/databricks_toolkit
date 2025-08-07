# Dashboard Documentation

The unified dashboard provides comprehensive analytics and monitoring capabilities for both business data and GCP cost management.

## 🚀 Quick Start

### Launch the Dashboard

```bash
# Method 1: Direct streamlit run (recommended)
streamlit run dashboard/dynamic_dashboard.py

# Method 2: Using the launcher script
python run_dashboard.py

# Method 3: From project root
cd /path/to/databricks_toolkit
streamlit run dashboard/dynamic_dashboard.py
```

**Dashboard URL**: http://localhost:8501

## 📊 Dashboard Modes

### ☁️ GCP Dashboard Mode

Access real-time GCP cost monitoring and analytics:

**Features:**
- **BigQuery Cost Monitoring**: Query costs, data processed, usage trends
- **Cloud Storage Analytics**: Storage costs, operations, data volume
- **Dataproc Monitoring**: Cluster costs, job counts, compute hours
- **Cost Optimization**: Smart recommendations based on usage patterns
- **Real vs Sample Data**: Clear indicators for data source

**Navigation:**
- Click "☁️ GCP Dashboard" button in the sidebar
- View cost trends and breakdowns
- Get optimization recommendations

### 📊 Business Analytics Mode

Build custom analytics dashboards:

**Features:**
- **Table Discovery**: Find and explore Databricks tables
- **Chart Builder**: Create custom visualizations
- **Pipeline Builder**: Build Bronze → Silver → Gold pipelines
- **Dashboard Management**: Save and load configurations

**Navigation:**
- Click "📊 Business Analytics" button in the sidebar
- Use Chart Builder or Pipeline Builder modes

## 🎯 Dashboard Components

### Sidebar Controls

**Quick Actions:**
- **☁️ GCP Dashboard**: Switch to GCP cost monitoring
- **📊 Business Analytics**: Switch to business analytics
- **Mode Indicator**: Shows current active mode

**Dashboard Management:**
- Load saved dashboards
- Save current dashboard configuration
- Export dashboard settings

**Mode Selection:**
- **Chart Builder**: Create custom charts and visualizations
- **Pipeline Builder**: Build and visualize data pipelines

### Main Content Area

**GCP Dashboard:**
- Configuration status and metrics
- Cost overview with trends
- Service-specific analytics
- Optimization recommendations

**Business Analytics:**
- Table discovery and selection
- Chart creation tools
- Pipeline visualization
- Data preview and analysis

## 🔧 Configuration

### GCP Setup

1. **Enable BigQuery Billing Export**:
   ```bash
   # Go to Google Cloud Console > Billing
   # Enable Standard usage cost data export to BigQuery
   # Set destination dataset to 'billing_export'
   ```

2. **Set up Authentication**:
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

3. **Environment Variables** (optional):
   ```bash
   export GCP_PROJECT_ID="your-project-id"
   export GCP_SERVICE_ACCOUNT_KEY="path/to/service-account.json"
   ```

### Databricks Setup

1. **Set Environment Variables**:
   ```bash
   export DATABRICKS_WORKSPACE_URL="your-workspace-url"
   export DATABRICKS_TOKEN="your-access-token"
   ```

2. **Test Connection**:
   ```bash
   python test_dashboard_gcp.py
   ```

## 📈 GCP Cost Monitoring

### Real Data vs Sample Data

**Sample Data** (when billing export not enabled):
- Realistic cost simulations
- Clear "Sample Data" indicators
- Instructions for enabling real data

**Real Data** (when billing export enabled):
- Actual BigQuery billing data
- Real Cloud Storage costs
- Live Dataproc usage metrics

### Cost Analytics

**BigQuery Analytics:**
- Query costs and trends
- Data processed metrics
- Usage patterns analysis

**Cloud Storage Analytics:**
- Storage costs by class
- Operations and bandwidth
- Lifecycle optimization

**Dataproc Analytics:**
- Cluster costs and utilization
- Job performance metrics
- Auto-scaling recommendations

## 🏗️ Pipeline Builder

### Building Data Pipelines

1. **Select Tables**: Choose from discovered tables
2. **Define Layers**: Bronze (raw), Silver (cleaned), Gold (aggregated)
3. **Set Dependencies**: Define data flow relationships
4. **Visualize**: See the complete pipeline architecture

### Pipeline Components

**Bronze Layer:**
- Raw data ingestion
- Initial data validation
- Source system connections

**Silver Layer:**
- Data cleaning and transformation
- Business logic application
- Quality checks

**Gold Layer:**
- Aggregated metrics
- Business KPIs
- Reporting-ready data

## 💾 Dashboard Management

### Saving Dashboards

1. **Build Your Dashboard**: Create charts and pipelines
2. **Save Configuration**: Click "Save Dashboard" button
3. **Name Your Dashboard**: Provide a descriptive name
4. **Load Later**: Access saved dashboards from sidebar

### Dashboard Features

**Saved Configurations:**
- Chart layouts and settings
- Pipeline architectures
- Data source configurations

**Export/Import:**
- JSON configuration files
- Shareable dashboard setups
- Version control support

## 🧪 Testing

### Test Dashboard Functionality

```bash
# Test GCP integration
python test_gcp_dashboard.py

# Test dashboard accessibility
python test_dashboard_gcp.py

# Test CLI tools
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project-id
```

### Troubleshooting

**Common Issues:**

1. **Import Errors**:
   ```bash
   # Check Python path
   export PYTHONPATH="${PYTHONPATH}:/path/to/databricks_toolkit"
   ```

2. **GCP Authentication**:
   ```bash
   # Verify authentication
   gcloud auth list
   gcloud config get-value project
   ```

3. **Dashboard Not Loading**:
   ```bash
   # Check if dashboard is running
   curl http://localhost:8501
   ```

## 📚 Advanced Usage

### Custom Integrations

**Adding New Data Sources:**
1. Extend `cloud_integrations.py`
2. Add new integration class
3. Update dashboard configuration

**Custom Visualizations:**
1. Create new chart types
2. Add to `create_chart()` method
3. Update chart builder interface

### Performance Optimization

**For Large Datasets:**
- Use data sampling in preview
- Implement pagination
- Cache frequently accessed data

**For Real-time Monitoring:**
- Set up auto-refresh intervals
- Use streaming data sources
- Implement alerting

## 🔗 Related Documentation

- [GCP Cost Monitoring CLI](../shared/cli/monitoring/README.md)
- [SQL-Driven Workflow](../workflows/sql_driven/README.md)
- [PySpark ETL Workflow](../workflows/pyspark_etl/README.md)
- [Configuration Guide](../config/README.md)

## 🆘 Support

For issues or questions:

1. **Check the logs**: Look for error messages in the terminal
2. **Test components**: Run individual test scripts
3. **Verify configuration**: Check environment variables and authentication
4. **Review documentation**: Check related README files

## 🚀 Next Steps

1. **Enable Real GCP Data**: Set up BigQuery billing export
2. **Customize Dashboards**: Create your own analytics views
3. **Build Pipelines**: Design Bronze → Silver → Gold architectures
4. **Monitor Costs**: Track and optimize your GCP spending 
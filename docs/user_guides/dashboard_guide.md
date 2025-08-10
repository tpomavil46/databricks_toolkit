# Dashboard Guide

Master the unified dashboard for business analytics and GCP cost monitoring.

## 🎯 Overview

The unified dashboard provides a single interface for:
- **📊 Business Analytics**: Data exploration and visualization
- **📊 Business Analytics**: Data exploration and visualization
- **🏗️ Pipeline Builder**: Visual pipeline construction
- **💾 Dashboard Management**: Save and load configurations

## 🚀 Getting Started

### Launch the Dashboard

```bash
# Start the dashboard
streamlit run dashboard/dynamic_dashboard.py

# Access at http://localhost:8501
```

### Dashboard Layout

```
┌─────────────────────────────────────────────────────────┐
│                    Sidebar Navigation                    │
├─────────────────────────────────────────────────────────┤
│                                                         │
│                    Main Content Area                    │
│                                                         │
│  • Business Analytics (default)                        │
│  • Business Analytics                                  │
│  • Pipeline Builder                                    │
│  • Dashboard Management                                │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 📊 Business Analytics

### Table Discovery

1. **Browse Tables**: View all available tables in your Databricks workspace
2. **Search Tables**: Use the search bar to find specific tables
3. **Table Details**: Click on tables to see schema and sample data

### Chart Building

#### Creating Charts

1. **Select Data Source**: Choose a table from the dropdown
2. **Choose Chart Type**: Select from available chart types:
   - Bar Chart
   - Line Chart
   - Scatter Plot
   - Pie Chart
   - Heatmap
   - Table View

3. **Configure Chart**:
   ```python
   # Example chart configuration
   {
     "chart_type": "bar",
     "x_column": "category",
     "y_column": "sales",
     "title": "Sales by Category",
     "color": "blue"
   }
   ```

#### Chart Types

| Chart Type | Best For | Example Use |
|------------|----------|-------------|
| **Bar Chart** | Categorical comparisons | Sales by region |
| **Line Chart** | Time series data | Revenue over time |
| **Scatter Plot** | Correlation analysis | Price vs. demand |
| **Pie Chart** | Proportions | Market share |
| **Heatmap** | Matrix data | Correlation matrix |
| **Table** | Detailed data | Raw data view |

### Advanced Features

#### Custom Queries
```sql
-- Write custom SQL queries
SELECT 
    category,
    SUM(sales) as total_sales,
    COUNT(*) as order_count
FROM retail.silver.orders
WHERE order_date >= '2024-01-01'
GROUP BY category
ORDER BY total_sales DESC
```

#### Chart Layouts
- **Single Chart**: Focus on one visualization
- **Multi-Chart**: Compare multiple charts side-by-side
- **Dashboard Layout**: Arrange charts in a grid



## 🏗️ Pipeline Builder

### Visual Pipeline Construction

#### Building Blocks
1. **Data Sources**: Input tables and files
2. **Transformations**: Data processing steps
3. **Outputs**: Target tables and dashboards

#### Pipeline Types
- **Bronze → Silver → Gold**: Standard medallion architecture
- **Streaming**: Real-time data processing
- **Batch**: Scheduled data processing
- **Hybrid**: Combination of streaming and batch

### Pipeline Configuration

#### Bronze Layer
```sql
-- Raw data ingestion
CREATE OR REPLACE TABLE bronze.raw_data
AS SELECT * FROM source_table
```

#### Silver Layer
```sql
-- Cleaned and validated data
CREATE OR REPLACE TABLE silver.clean_data
AS SELECT 
    *,
    CASE WHEN amount > 0 THEN 'valid' ELSE 'invalid' END as status
FROM bronze.raw_data
WHERE amount IS NOT NULL
```

#### Gold Layer
```sql
-- Business-ready aggregations
CREATE OR REPLACE TABLE gold.kpis
AS SELECT 
    category,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM silver.clean_data
GROUP BY category
```

### Pipeline Monitoring

#### Health Checks
- **Data Quality**: Validate data completeness and accuracy
- **Performance**: Monitor processing times and resource usage
- **Dependencies**: Check upstream data availability

#### Alerts
- **Pipeline Failures**: Get notified of processing errors
- **Data Quality Issues**: Alert on data validation failures
- **Performance Degradation**: Monitor processing slowdowns

## 💾 Dashboard Management

### Saving Dashboards

#### Save Configuration
1. **Configure Layout**: Arrange charts and widgets
2. **Set Parameters**: Configure filters and date ranges
3. **Save Dashboard**: Save with a descriptive name

#### Dashboard Types
- **Personal**: Individual user dashboards
- **Shared**: Team-wide dashboards
- **Template**: Reusable dashboard templates

### Loading Dashboards

#### Available Dashboards
- **Default**: Pre-configured starter dashboard
- **Custom**: User-created dashboards
- **Templates**: Reusable dashboard templates

#### Dashboard Sharing
- **Export**: Share dashboard configurations
- **Import**: Load shared dashboard configurations
- **Version Control**: Track dashboard changes

## 🔧 Advanced Features

### Custom Widgets

#### Filter Widgets
```python
# Date range filter
date_range = st.date_input(
    "Select Date Range",
    value=(datetime.now() - timedelta(days=30), datetime.now())
)

# Dropdown filter
category_filter = st.selectbox(
    "Select Category",
    options=["All", "Electronics", "Clothing", "Books"]
)
```

#### Input Widgets
```python
# Text input
search_term = st.text_input("Search Tables", "")

# Number input
threshold = st.number_input("Cost Threshold", value=100.0)

# Slider
confidence_level = st.slider("Confidence Level", 0.0, 1.0, 0.95)
```

### Data Export

#### Export Formats
- **CSV**: Comma-separated values
- **JSON**: JavaScript Object Notation
- **Excel**: Microsoft Excel format
- **PDF**: Portable Document Format

#### Export Options
```python
# Export chart data
if st.button("Export Data"):
    chart_data.to_csv("chart_data.csv")
    st.success("Data exported successfully!")
```

### Real-time Updates

#### Auto-refresh
- **Dashboard**: Automatically refresh data every 5 minutes
- **Charts**: Update chart data in real-time
- **Alerts**: Show real-time notifications

#### Manual Refresh
```python
# Manual refresh button
if st.button("Refresh Data"):
    st.rerun()
```

## 🆘 Troubleshooting

### Common Issues

#### Dashboard Won't Load
```bash
# Check Streamlit installation
pip install streamlit

# Verify port availability
lsof -i :8501

# Run with debug logging
streamlit run dashboard/dynamic_dashboard.py --logger.level debug
```

#### Charts Not Displaying
1. **Check Data Source**: Verify table exists and is accessible
2. **Validate Query**: Ensure SQL query is correct
3. **Check Permissions**: Verify user has table access

#### GCP Costs Not Loading
```bash
# Verify GCP credentials
gcloud auth list

# Check billing API access
gcloud services enable billing.googleapis.com

# Test cost CLI
python shared/cli/monitoring/gcp_cost_cli.py --project-id your-project
```

### Performance Optimization

#### Dashboard Performance
- **Limit Data**: Use filters to reduce data volume
- **Cache Results**: Enable caching for expensive queries
- **Optimize Queries**: Use efficient SQL queries

#### Memory Usage
- **Close Unused Tabs**: Close dashboard tabs when not needed
- **Clear Cache**: Clear browser cache if dashboard is slow
- **Restart Dashboard**: Restart if memory usage is high

## 📚 Best Practices

### Dashboard Design
1. **Keep it Simple**: Focus on key metrics
2. **Use Consistent Colors**: Maintain color scheme
3. **Add Context**: Include titles and descriptions
4. **Test Responsiveness**: Ensure mobile compatibility

### Data Visualization
1. **Choose Right Chart**: Match chart type to data
2. **Limit Data Points**: Don't overwhelm with too much data
3. **Use Color Effectively**: Use color to highlight important data
4. **Add Interactivity**: Include filters and drill-downs

### Cost Monitoring
1. **Set Alerts**: Configure cost threshold alerts
2. **Regular Reviews**: Review costs weekly/monthly
3. **Optimize Resources**: Right-size clusters and storage
4. **Track Trends**: Monitor cost trends over time

---

**Need more help?** Check the [Reference](reference/) section or use the built-in help in the dashboard!

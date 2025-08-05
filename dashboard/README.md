# 📊 Retail Analytics Dashboard

A comprehensive Streamlit dashboard with Plotly visualizations for the Delta Live Tables retail analytics pipeline.

## 🚀 Features

### 📈 Visualizations
- **Revenue Analysis**: Bar charts showing revenue by category
- **Product Distribution**: Pie charts for product categories
- **Data Quality Metrics**: Quality score tracking
- **Time Series**: Revenue trends over time
- **Hourly Analysis**: Revenue and product patterns by hour
- **Pipeline Architecture**: Interactive diagram of the DLT pipeline

### 🎛️ Interactive Controls
- **Date Range Filter**: Select specific time periods
- **Category Filter**: Filter by product categories
- **Product Category Filter**: Filter by value tiers
- **Real-time Updates**: Live data refresh capabilities

### 📊 Key Metrics
- Total Revenue with average comparison
- Total Products with average count
- Average Price with standard deviation
- Data Quality Score percentage

## 🛠️ Installation

### Prerequisites
- Python 3.8+
- pip package manager

### Setup
1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Launch Dashboard**:
   ```bash
   # From project root
   python run_dashboard.py
   
   # Or directly with Streamlit
   streamlit run dashboard/app.py
   ```

3. **Access Dashboard**:
   - Open your browser to `http://localhost:8501`
   - The dashboard will load automatically

## 📋 Dashboard Components

### 1. Pipeline Architecture Diagram
- Visual representation of the Delta Live Tables pipeline
- Shows Bronze → Silver → Gold data flow
- Highlights key features of each layer

### 2. Revenue Analytics
- **Total Revenue by Category**: Bar chart showing revenue distribution
- **Product Distribution**: Pie chart of product categories
- **Revenue Trend**: Line chart showing daily revenue patterns
- **Hourly Analysis**: Dual-axis chart for revenue and product counts

### 3. Data Quality Metrics
- **Quality Score**: Percentage of valid records
- **Validation Metrics**: Breakdown of data quality checks
- **Category Performance**: Quality metrics by product category

### 4. Interactive Filters
- **Date Range**: Select specific time periods
- **Category Filter**: Filter by product categories
- **Product Category**: Filter by value tiers (high/medium/low)

## 🔧 Customization

### Adding New Visualizations
1. Create a new function in `app.py`:
   ```python
   def create_new_chart(df):
       # Your visualization code here
       return fig
   ```

2. Add the chart to the main layout:
   ```python
   new_chart = create_new_chart(df)
   st.plotly_chart(new_chart, use_container_width=True)
   ```

### Connecting to Real Data
1. Replace `create_sample_data()` with your data source
2. Update the data loading logic in `main()`
3. Modify filters to match your data schema

### Styling Customization
- Edit the CSS in the `st.markdown()` section
- Modify color schemes in Plotly charts
- Update layout parameters for different screen sizes

## 📊 Data Schema

The dashboard expects data with the following columns:

```python
{
    'id': int,                    # Product ID
    'name': str,                  # Product name
    'category': str,              # Product category
    'product_category': str,      # Value tier (high/medium/low)
    'total_revenue': float,       # Revenue amount
    'avg_price': float,           # Average price
    'total_products': int,        # Product count
    'high_value_products': int,   # High-value product count
    'medium_value_products': int, # Medium-value product count
    'low_value_products': int,    # Low-value product count
    'valid_date_products': int,   # Valid date count
    'valid_price_products': int,  # Valid price count
    'high_value_revenue': float,  # High-value revenue
    'medium_value_revenue': float,# Medium-value revenue
    'low_value_revenue': float,   # Low-value revenue
    'order_date': datetime,       # Order date
    'order_hour': int            # Order hour (0-23)
}
```

## 🚀 Deployment

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run dashboard
streamlit run dashboard/app.py
```

### Production Deployment
1. **Docker**:
   ```dockerfile
   FROM python:3.9-slim
   COPY requirements.txt .
   RUN pip install -r requirements.txt
   COPY dashboard/ .
   EXPOSE 8501
   CMD ["streamlit", "run", "app.py", "--server.port=8501"]
   ```

2. **Cloud Platforms**:
   - **Heroku**: Deploy with `Procfile`
   - **AWS**: Use Elastic Beanstalk or ECS
   - **GCP**: Deploy to App Engine
   - **Azure**: Use App Service

### Environment Variables
```bash
# Optional: Set custom port
export STREAMLIT_SERVER_PORT=8501

# Optional: Set custom address
export STREAMLIT_SERVER_ADDRESS=0.0.0.0
```

## 🔗 Integration with DLT Pipeline

This dashboard is designed to work with the Delta Live Tables pipeline:

1. **Bronze Layer**: Raw data ingestion with Auto Loader
2. **Silver Layer**: Data transformation with quality checks
3. **Gold Layer**: Real-time analytics and aggregations
4. **Dashboard**: Interactive visualizations and monitoring

### Pipeline Features
- **Auto Loader**: Automatic file detection and schema inference
- **Streaming Tables**: Real-time data processing
- **Materialized Views**: Pre-computed aggregations
- **Data Quality**: Built-in validation and constraints

## 📈 Performance Optimization

### For Large Datasets
1. **Data Sampling**: Use `df.sample()` for large datasets
2. **Caching**: Implement `@st.cache_data` for expensive operations
3. **Pagination**: Load data in chunks for better performance
4. **Aggregation**: Pre-compute metrics at the database level

### Memory Management
```python
# Use efficient data types
df = df.astype({
    'id': 'int32',
    'total_revenue': 'float32',
    'order_date': 'datetime64[ns]'
})

# Clear cache periodically
st.cache_data.clear()
```

## 🐛 Troubleshooting

### Common Issues
1. **Port Already in Use**:
   ```bash
   # Kill existing process
   lsof -ti:8501 | xargs kill -9
   ```

2. **Missing Dependencies**:
   ```bash
   pip install --upgrade streamlit plotly pandas numpy
   ```

3. **Data Loading Errors**:
   - Check data schema matches expected format
   - Verify all required columns are present
   - Ensure data types are correct

### Debug Mode
```bash
# Run with debug information
streamlit run dashboard/app.py --logger.level=debug
```

## 📚 Resources

- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Documentation](https://plotly.com/python/)
- [Delta Live Tables](https://docs.databricks.com/data-engineering/delta-live-tables/)
- [Databricks Jobs](https://docs.databricks.com/data-engineering/jobs/)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add your changes
4. Test the dashboard
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

---

**🎯 Perfect for Databricks Data Engineering Associate exam preparation!** 
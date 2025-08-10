# Troubleshooting Guide

Common issues and solutions for the Databricks Toolkit.

## 🚨 Quick Fixes

### Dashboard Issues
```bash
# Dashboard won't start
pip install streamlit
streamlit run dashboard/dynamic_dashboard.py --logger.level debug

# Dashboard is slow
export STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
export STREAMLIT_SERVER_MAX_MESSAGE_SIZE=200
```

### Pipeline Issues
```bash
# Pipeline fails
databricks clusters list
python -c "from utils.session import DatabricksSession; print('Connection OK')"

# Import errors
pip install -r requirements.txt --force-reinstall
```

## 🔧 Connection Issues

### Databricks Connection Problems

#### Error: "Unable to connect to Databricks"
```bash
# Check cluster status
databricks clusters list

# Verify cluster is running
databricks clusters get --cluster-id your-cluster-id

# Test connection
python -c "from utils.session import DatabricksSession; session = DatabricksSession(); print('Connected')"
```

**Solutions:**
1. **Start the cluster**: `databricks clusters start --cluster-id your-cluster-id`
2. **Check credentials**: `databricks configure --profile your-profile`
3. **Verify workspace URL**: Check `~/.databrickscfg` file

#### Error: "Authentication failed"
```bash
# Check authentication
databricks auth list

# Re-authenticate
databricks auth login

# Verify token
echo $DATABRICKS_TOKEN
```

**Solutions:**
1. **Generate new token**: Go to Databricks workspace → User Settings → Access Tokens
2. **Update credentials**: `databricks configure --profile your-profile`
3. **Check token expiration**: Tokens expire after 90 days

### GCP Connection Problems

#### Error: "GCP credentials not found"
```bash
# Check GCP authentication
gcloud auth list

# Set up application default credentials
gcloud auth application-default login

# Verify service account
echo $GOOGLE_APPLICATION_CREDENTIALS
```

**Solutions:**
1. **Login to GCP**: `gcloud auth login`
2. **Set service account**: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"`
3. **Enable APIs**: `gcloud services enable billing.googleapis.com`

## 📊 Pipeline Issues

### SQL Pipeline Failures

#### Error: "Table not found"
```sql
-- Check if table exists
SHOW TABLES IN your_catalog.your_schema;

-- Check permissions
SHOW GRANTS ON TABLE your_catalog.your_schema.your_table;
```

**Solutions:**
1. **Create table**: Run the table creation script
2. **Check schema**: Verify catalog and schema names
3. **Grant permissions**: `GRANT SELECT ON TABLE your_table TO your_user`

#### Error: "Permission denied"
```sql
-- Check user permissions
SHOW CURRENT USER();
SHOW GRANTS ON CATALOG your_catalog;
SHOW GRANTS ON SCHEMA your_catalog.your_schema;
```

**Solutions:**
1. **Grant catalog access**: `GRANT USAGE ON CATALOG your_catalog TO your_user`
2. **Grant schema access**: `GRANT USAGE ON SCHEMA your_catalog.your_schema TO your_user`
3. **Grant table access**: `GRANT SELECT ON TABLE your_table TO your_user`

### PySpark ETL Failures

#### Error: "SparkSession not initialized"
```python
# Check Spark session
from utils.session import DatabricksSession
session = DatabricksSession()
print(session.spark)
```

**Solutions:**
1. **Initialize session**: Ensure `DatabricksSession()` is called
2. **Check cluster**: Verify cluster is running
3. **Restart kernel**: Restart Jupyter/notebook kernel

#### Error: "OutOfMemoryError"
```python
# Check memory usage
session.spark.conf.get("spark.driver.memory")
session.spark.conf.get("spark.executor.memory")
```

**Solutions:**
1. **Increase memory**: Set `spark.driver.memory=4g`
2. **Repartition data**: Use `.repartition()` to reduce partition size
3. **Optimize queries**: Use efficient transformations

## ☁️ GCP Cost Monitoring Issues

### Cost Data Not Loading

#### Error: "Billing API not enabled"
```bash
# Enable billing API
gcloud services enable billing.googleapis.com

# Check API status
gcloud services list --enabled | grep billing
```

**Solutions:**
1. **Enable API**: `gcloud services enable billing.googleapis.com`
2. **Check permissions**: Ensure user has billing viewer role
3. **Verify project**: Check project ID is correct

#### Error: "No billing data found"
```bash
# Check billing account
gcloud billing accounts list

# Verify project billing
gcloud billing projects describe your-project-id
```

**Solutions:**
1. **Link billing account**: `gcloud billing projects link your-project-id --billing-account=your-billing-account`
2. **Check date range**: Ensure date range has billing data
3. **Wait for data**: Billing data can take 24-48 hours to appear

## 🛠️ Development Issues

### Import Errors

#### Error: "ModuleNotFoundError"
```bash
# Check Python path
python -c "import sys; print(sys.path)"

# Check virtual environment
which python
pip list | grep databricks
```

**Solutions:**
1. **Activate virtual environment**: `source .venv/bin/activate`
2. **Install dependencies**: `pip install -r requirements.txt`
3. **Check PYTHONPATH**: `export PYTHONPATH="${PYTHONPATH}:/path/to/project"`

#### Error: "Version conflicts"
```bash
# Check package versions
pip list

# Update packages
pip install --upgrade databricks-sdk
pip install --upgrade pyspark
```

**Solutions:**
1. **Clean install**: `pip uninstall -r requirements.txt && pip install -r requirements.txt`
2. **Use specific versions**: Pin versions in `requirements.txt`
3. **Create new environment**: `python -m venv .venv_new`

### Testing Issues

#### Error: "Tests failing"
```bash
# Run tests with verbose output
python -m pytest tests/ -v

# Run specific test
python -m pytest tests/test_pipeline.py::test_pipeline_placeholder -v

# Check test environment
python -c "import pytest; print(pytest.__version__)"
```

**Solutions:**
1. **Install test dependencies**: `pip install pytest pytest-cov`
2. **Check test data**: Ensure test data files exist
3. **Mock external services**: Use mocks for external dependencies

## 📱 Dashboard Issues

### Performance Problems

#### Dashboard is slow
```bash
# Check memory usage
ps aux | grep streamlit

# Monitor CPU usage
top -p $(pgrep streamlit)
```

**Solutions:**
1. **Reduce data size**: Use filters to limit data
2. **Enable caching**: Use `@st.cache_data` decorator
3. **Optimize queries**: Use efficient SQL queries
4. **Restart dashboard**: Kill and restart Streamlit process

#### Charts not displaying
```python
# Check data source
print(df.head())
print(df.shape)

# Verify chart configuration
print(chart_config)
```

**Solutions:**
1. **Check data**: Ensure data frame is not empty
2. **Validate columns**: Verify column names exist
3. **Check data types**: Ensure appropriate data types for chart
4. **Clear cache**: Use `st.cache_data.clear()`

## 🔒 Security Issues

### Permission Denied

#### Error: "Access denied to workspace"
```bash
# Check user permissions
python shared/admin/cli/admin_cli.py get-user --user-name your-username

# Verify workspace access
python shared/admin/cli/admin_cli.py workspace-info
```

**Solutions:**
1. **Contact admin**: Request workspace access
2. **Check groups**: Verify user is in correct groups
3. **Verify token**: Ensure token has correct permissions

#### Error: "GCP permissions insufficient"
```bash
# Check GCP roles
gcloud projects get-iam-policy your-project-id

# Add billing viewer role
gcloud projects add-iam-policy-binding your-project-id \
  --member="user:your-user@company.com" \
  --role="roles/billing.viewer"
```

**Solutions:**
1. **Add billing viewer**: Grant billing viewer role
2. **Add data viewer**: Grant BigQuery data viewer role
3. **Contact admin**: Request necessary permissions

## 📊 Data Quality Issues

### Data Validation Failures

#### Error: "Data quality check failed"
```sql
-- Check data quality
SELECT COUNT(*) as total_rows,
       COUNT(CASE WHEN column_name IS NULL THEN 1 END) as null_count
FROM your_table;
```

**Solutions:**
1. **Clean data**: Remove or fix invalid records
2. **Update constraints**: Adjust data quality rules
3. **Investigate source**: Check upstream data quality

#### Error: "Schema mismatch"
```python
# Check schema
df.printSchema()

# Compare schemas
expected_schema = StructType([...])
actual_schema = df.schema
```

**Solutions:**
1. **Update schema**: Modify table schema
2. **Transform data**: Convert data types
3. **Recreate table**: Drop and recreate with correct schema

## 🚀 Performance Issues

### Slow Pipeline Execution

#### Pipeline taking too long
```python
# Check execution plan
df.explain()

# Monitor resource usage
session.spark.conf.get("spark.sql.adaptive.enabled")
```

**Solutions:**
1. **Optimize queries**: Use efficient SQL
2. **Increase resources**: Use larger cluster
3. **Partition data**: Use appropriate partitioning
4. **Cache data**: Use `.cache()` for repeated operations

#### Memory issues
```python
# Check memory usage
session.spark.conf.get("spark.driver.memory")
session.spark.conf.get("spark.executor.memory")
```

**Solutions:**
1. **Increase memory**: Set higher memory limits
2. **Repartition**: Reduce partition size
3. **Persist data**: Use `.persist()` for intermediate results
4. **Clean up**: Remove unused data frames

## 🆘 Getting Help

### Debug Mode

#### Enable debug logging
```bash
# Set debug environment variable
export DEBUG="1"
export LOG_LEVEL="DEBUG"

# Run with debug output
python main.py sql retail --debug
```

#### Collect diagnostic information
```bash
# System information
python -c "import sys; print(sys.version)"
python -c "import platform; print(platform.platform())"

# Package versions
pip freeze > requirements_installed.txt

# Databricks information
databricks --version
databricks clusters list
```

### Support Resources

1. **Documentation**: Check the [User Guides](user_guides/) section
2. **Examples**: See [Tutorials](tutorials/) for step-by-step guides
3. **CLI Help**: Use `--help` flag with any command
4. **Logs**: Check application logs for detailed error messages

### Common Commands

#### Health Check
```bash
# Health monitoring available through admin tools
python -c "from shared.admin.core.admin_client import AdminClient; print('Admin tools available')"
```

#### Reset Environment
```bash
# Clear cache
rm -rf ~/.cache/streamlit
rm -rf .streamlit/

# Reset configuration
rm -f ~/.databrickscfg
databricks configure --profile default

# Reinstall dependencies
pip uninstall -r requirements.txt
pip install -r requirements.txt
```

---

**Still having issues?** Check the logs, enable debug mode, and collect diagnostic information before seeking additional help!

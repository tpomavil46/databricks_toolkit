# Monitoring CLI Tools

This directory contains CLI tools for monitoring various aspects of the data platform, including costs, health, and performance.

## GCP Cost Monitoring

The GCP cost monitoring tools use **BigQuery billing export** to provide real cost data from Google Cloud Platform.

### Prerequisites

1. **Enable BigQuery Billing Export**:
   - Go to [Google Cloud Console > Billing](https://console.cloud.google.com/billing)
   - Select your billing account
   - Click "Export" in the left sidebar
   - Enable **Standard usage cost data export** to BigQuery
   - Set destination dataset to `billing_export` in your project
   - Wait a few hours for data to populate

2. **Set up Authentication**:
   ```bash
   gcloud auth application-default login
   gcloud config set project YOUR_PROJECT_ID
   ```

3. **Install Dependencies**:
   ```bash
   pip install google-cloud-bigquery google-auth
   ```

### Usage

#### Basic Cost Analysis

```bash
# Get current cost breakdown for the last 30 days
python shared/cli/monitoring/gcp_cost_cli.py --project-id mydatabrickssandbox

# Get cost breakdown for the last 7 days
python shared/cli/monitoring/gcp_cost_cli.py --project-id mydatabrickssandbox --days 7

# Get cost trends over time
python shared/cli/monitoring/gcp_cost_cli.py --project-id mydatabrickssandbox --trends

# Export results as JSON
python shared/cli/monitoring/gcp_cost_cli.py --project-id mydatabrickssandbox --output json
```

#### Example Output

```
============================================================
🔍 GCP COST ANALYSIS REPORT
============================================================
📊 Project: mydatabrickssandbox
📅 Period: 30 days
💰 Total Cost: $156.78
🕒 Timestamp: 2025-08-07T09:24:20.125994
✅ Real Data: True

📈 SERVICE BREAKDOWN:
----------------------------------------
🔹 BigQuery: $89.45
   Description: BigQuery Analysis
🔹 Cloud Storage: $45.23
   Description: Standard Storage
🔹 Compute Engine: $22.10
   Description: N1 Predefined Instance Core running in Virginia

💡 RECOMMENDATIONS:
----------------------------------------
• Consider reviewing BigQuery query optimization
• Consider lifecycle policies for Cloud Storage
• Review Compute Engine usage and consider committed use discounts
• Optimize BigQuery queries and consider slot reservations
============================================================
```

### BigQuery Table Structure

The tools expect the following BigQuery tables from billing export:

- **Standard usage cost table**: `gcp_billing_export_v1_<BILLING-ACCOUNT-ID>`
- **Detailed usage cost table**: `gcp_billing_export_resource_v1_<BILLING-ACCOUNT-ID>`

### Dashboard Integration

The dashboard automatically uses the same BigQuery billing export data:

1. **BigQuery Usage**: Shows query costs, bytes processed, and usage trends
2. **Cloud Storage**: Shows storage costs, operations, and bucket usage
3. **Dataproc**: Shows cluster costs, job counts, and compute hours

### Troubleshooting

#### "No billing export tables found"

This means the BigQuery billing export is not enabled or data hasn't populated yet.

**Solution**:
1. Go to [Google Cloud Console > Billing](https://console.cloud.google.com/billing)
2. Enable **Standard usage cost data export** to BigQuery
3. Wait 2-4 hours for data to populate
4. Verify tables exist: `bq ls billing_export`

#### "No billing account found"

This means the billing account ID couldn't be extracted from the table names.

**Solution**:
1. Check that billing export is enabled
2. Verify table naming follows the pattern: `gcp_billing_export_v1_<ACCOUNT-ID>`
3. Ensure you have proper BigQuery permissions

#### Authentication Errors

**Solution**:
```bash
# Set up application default credentials
gcloud auth application-default login

# Set the project
gcloud config set project YOUR_PROJECT_ID

# Verify authentication
gcloud auth list
```

### Sample Data Fallback

When billing export is not available, the tools provide realistic sample data for testing:

- **BigQuery**: Simulated query costs and usage
- **Cloud Storage**: Simulated storage costs and operations
- **Dataproc**: Simulated cluster costs and job counts

This allows development and testing even without real billing data.

### Cost Optimization Recommendations

The tools automatically generate recommendations based on cost patterns:

- **High BigQuery costs**: Suggests query optimization and slot reservations
- **High Storage costs**: Suggests lifecycle policies and storage class optimization
- **High Compute costs**: Suggests committed use discounts and auto-scaling

### Integration with Dashboard

The dashboard uses the same `GoogleCloudIntegration` class that:

1. **Detects billing export availability**
2. **Falls back to sample data** when export is not available
3. **Provides real-time cost monitoring** when export is enabled
4. **Shows cost trends and recommendations** in the UI

### Environment Variables

Set these environment variables for the dashboard:

```bash
export GCP_PROJECT_ID="your-project-id"
export GCP_SERVICE_ACCOUNT_KEY="path/to/service-account.json"  # Optional
```

### Testing

Run the test script to verify everything works:

```bash
python test_gcp_integration_updated.py
```

This will test both the CLI tools and dashboard integration with proper error handling and fallback to sample data.

## Other Monitoring Tools

### Health Monitoring

```bash
python shared/cli/monitoring/health_cli.py --help
```

### Performance Monitoring

```bash
python shared/cli/monitoring/performance_cli.py --help
```

### Cost Monitoring (General)

```bash
python shared/cli/monitoring/cost_cli.py --help
```

## References

- [Google Cloud Billing Export Documentation](https://cloud.google.com/billing/docs/how-to/export-data-bigquery)
- [BigQuery Billing Examples](https://cloud.google.com/billing/docs/how-to/bq-examples)
- [Visualize Costs with Looker Studio](https://cloud.google.com/billing/docs/how-to/visualize-data)

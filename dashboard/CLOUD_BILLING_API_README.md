# Google Cloud Billing API Integration

This module provides a separate, decoupled implementation for accessing real-time billing data directly from the Google Cloud Billing API.

## 🎯 Purpose

The Cloud Billing API provides **real-time access** to your Google Cloud billing data, unlike the billing export which has delays. This allows for:

- **Real-time cost monitoring**
- **Current month data** (not just historical)
- **Detailed cost breakdowns**
- **Service-specific cost analysis**

## 📋 Prerequisites

1. **Google Cloud Project** with billing enabled
2. **Billing Account** with proper permissions
3. **Service Account** with billing permissions
4. **Cloud Billing API** enabled

## 🔧 Setup Instructions

### Step 1: Enable the Cloud Billing API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to **APIs & Services** > **Library**
3. Search for **"Cloud Billing API"**
4. Click **"Enable"**

### Step 2: Create Service Account

1. Go to **IAM & Admin** > **Service Accounts**
2. Click **"Create Service Account"**
3. Fill in the details:
   - **Name**: `billing-api-service`
   - **Description**: `Service account for Cloud Billing API`
4. Click **"Create and Continue"**

### Step 3: Assign Billing Permissions

1. In the **"Grant this service account access to project"** step:
2. Add these roles:
   - **Billing Account Viewer**
   - **Billing Account User** (if you need to modify billing)
3. Click **"Continue"**
4. Click **"Done"**

### Step 4: Create and Download Credentials

1. Click on your newly created service account
2. Go to the **"Keys"** tab
3. Click **"Add Key"** > **"Create new key"**
4. Choose **JSON** format
5. Click **"Create"**
6. The key file will download automatically

### Step 5: Set Environment Variable

```bash
# Set the environment variable to point to your key file
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

# Or add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
echo 'export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"' >> ~/.bashrc
source ~/.bashrc
```

### Step 6: Install Required Dependencies

```bash
pip install google-cloud-billing
```

## 🧪 Testing the Setup

Run the test script to verify your setup:

```bash
python test_cloud_billing_api.py
```

Expected output for successful setup:
```
🧪 Testing Cloud Billing API Integration
==================================================
✅ Configured: True
✅ Project ID: your-project-id
✅ Billing Account ID: your-billing-account-id
✅ Client Available: True
✅ Setup Required: False
```

## 📊 Usage

### Basic Usage

```python
from dashboard.cloud_billing_api import CloudBillingAPI

# Initialize the API
api = CloudBillingAPI()

# Check if configured
if api.is_configured():
    # Get current month costs
    costs = api.get_current_month_costs()
    print(f"Current month total: ${sum(costs.values()):.2f}")
else:
    print("API not configured")
```

### Available Methods

- `get_current_month_costs()` - Get current month costs by service
- `get_service_costs(service_name, days)` - Get costs for a specific service
- `get_cost_breakdown()` - Get detailed cost breakdown
- `get_status()` - Get API configuration status

## 🔍 Integration with Dashboard

The Cloud Billing API is **completely separate** from the billing export functionality. You can:

1. **Use both simultaneously** - Compare real-time vs. historical data
2. **Use either independently** - Choose your preferred data source
3. **Switch between them** - Use real-time for monitoring, export for analysis

## 🚨 Troubleshooting

### Common Issues

1. **"API not enabled"**
   - Enable the Cloud Billing API in Google Cloud Console

2. **"Permission denied"**
   - Ensure service account has "Billing Account Viewer" role

3. **"No billing account found"**
   - Check that your project has billing enabled
   - Verify billing account permissions

4. **"Credentials not found"**
   - Set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   - Verify the JSON key file path is correct

### Debug Commands

```bash
# Check if credentials are set
echo $GOOGLE_APPLICATION_CREDENTIALS

# Test gcloud authentication
gcloud auth application-default print-access-token

# Check billing account
gcloud billing accounts list
```

## 📈 Next Steps

Once the Cloud Billing API is set up, you can:

1. **Implement real-time cost monitoring**
2. **Add cost alerts and notifications**
3. **Create detailed cost breakdowns**
4. **Integrate with the dashboard**

## 🔗 Resources

- [Cloud Billing API Documentation](https://cloud.google.com/billing/docs/reference/rest)
- [Service Account Best Practices](https://cloud.google.com/iam/docs/service-accounts)
- [Billing Permissions](https://cloud.google.com/billing/docs/how-to/billing-access)

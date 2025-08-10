# Google Cloud Billing API Commands Reference

This document tracks working commands for the Google Cloud Billing API integration with the Databricks toolkit.

## Authentication Setup

```bash
# Authenticate with OAuth 2.0 (opens browser)
gcloud auth login

# Get fresh access token
TOKEN=$(gcloud auth print-access-token)
```

## Working API Commands

### 1. List All Billing Accounts
```bash
curl -X GET "https://cloudbilling.googleapis.com/v1/billingAccounts" \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**
- `billingAccounts/01010C-65238F-33E9F0` (closed)
- `billingAccounts/01DAC5-0FD782-74837A` (open - "PomavilleGCPBilling")

### 2. Get Specific Billing Account Details
```bash
curl -X GET "https://cloudbilling.googleapis.com/v1/billingAccounts/01DAC5-0FD782-74837A" \
  -H "Authorization: Bearer $TOKEN"
```

### 3. Get Billing Info for Project
```bash
curl -X GET "https://cloudbilling.googleapis.com/v1/projects/mydatabrickssandbox/billingInfo" \
  -H "Authorization: Bearer $TOKEN"
```

### 4. Associate Billing Account with Project
```bash
curl -X PUT "https://cloudbilling.googleapis.com/v1/projects/mydatabrickssandbox/billingInfo" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "billingAccountName": "billingAccounts/01DAC5-0FD782-74837A"
  }'
```

### 5. Disable Billing on Project (Set to Empty)
```bash
curl -X PUT "https://cloudbilling.googleapis.com/v1/projects/mydatabrickssandbox/billingInfo" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "billingAccountName": ""
  }'
```

## WORKING COST DATA - BigQuery Export

### 6. CLI Commands (WORKING)
```bash
# Get monthly costs
make billing-costs YEAR=2025 MONTH=7

# Check cost threshold
make billing-check THRESHOLD=100

# Generate comprehensive report
make billing-report YEAR=2025 MONTH=7



### 7. Make Commands (WORKING)
```bash
# Get monthly costs
make billing-costs YEAR=2025 MONTH=7

# Check cost threshold
make billing-check THRESHOLD=100

# Generate report
make billing-report YEAR=2025 MONTH=7



### 8. Python Integration (WORKING)
```python
from utils.billing_monitor import BillingMonitor

monitor = BillingMonitor()
costs = monitor.get_monthly_costs(2025, 7)
status = monitor.check_cost_threshold(100.0)
```

## Project Details
- **Project ID**: `mydatabrickssandbox`
- **Primary Billing Account**: `01DAC5-0FD782-74837A`
- **BigQuery Table**: `billing_export.gcp_billing_export_resource_v1_01DAC5_0FD782_74837A`
- **Authentication**: OAuth 2.0 via gcloud

## Current Status
✅ **Billing Export**: Set up and working  
✅ **Cost Data**: Available for July 2025  
✅ **CLI Tools**: All commands working  
✅ **Python API**: Full integration working  
✅ **Pipeline Integration**: Cost checking before expensive operations  

## Sample Results (July 2025)
- **Total Cost**: $0.38
- **Compute Engine**: $0.12 (Databricks clusters)
- **Networking**: $0.26
- **Cloud Logging**: $0.00

## Notes
- All commands require fresh token: `TOKEN=$(gcloud auth print-access-token)`
- Token expires, so refresh before each session
- OAuth 2.0 authentication required (not service account)
- BigQuery export takes 24-48 hours to populate with new data
- Current month data may not be available immediately

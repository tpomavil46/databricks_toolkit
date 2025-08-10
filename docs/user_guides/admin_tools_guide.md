# Admin Tools Guide

Complete guide to the administrative tools for Databricks workspace management.

## 🎯 Overview

Comprehensive administrative tools for managing Databricks workspace resources including user management, cluster management, security monitoring, and workspace administration.

## 🏗️ Architecture

The administrative tools are organized into specialized managers:

- **AdminClient**: Unified client for all administrative operations
- **UserManager**: User creation, modification, deletion, and health monitoring
- **ClusterManager**: Cluster management, monitoring, and optimization
- **SecurityManager**: Security monitoring, access control, and compliance
- **WorkspaceManager**: Workspace monitoring, health checks, and administration

## 🚀 Quick Start

### 1. Basic Usage

```python
from shared.admin.core.admin_client import AdminClient

# Initialize admin client
admin = AdminClient(profile="databricks")

# Get workspace information
info = admin.get_workspace_info()
print(f"Workspace URL: {info['workspace_url']}")

# Run health check
health = admin.run_health_check()

# Get usage summary
admin.print_usage_summary()
```

### 2. User Management

```python
# List all users
users = admin.users.list_users()

# Create a new user
result = admin.users.create_user(
    user_name="john.doe",
    display_name="John Doe",
    email="john.doe@company.com",
    groups=["developers", "data-scientists"]
)

# Add user to group
admin.users.add_user_to_group("john.doe", "admins")

# Get user health metrics
health = admin.users.get_user_health()
```

### 3. Cluster Management

```python
# List all clusters
clusters = admin.clusters.list_clusters()

# Create a new cluster
result = admin.clusters.create_cluster(
    cluster_name="development-cluster",
    spark_version="13.3.x-scala2.12",
    node_type_id="i3.xlarge",
    num_workers=2,
    autotermination_minutes=60
)

# Start/stop cluster
admin.clusters.start_cluster("cluster-id")
admin.clusters.stop_cluster("cluster-id")

# Get cluster health
health = admin.clusters.get_cluster_health()
```

### 4. Security Management

```python
# Get security summary
summary = admin.security.get_security_summary()

# Run security audit
audit = admin.security.run_security_audit()

# Get security health
health = admin.security.get_security_health()
```

### 5. Workspace Management

```python
# Get workspace information
info = admin.workspace.get_workspace_info()

# List workspace objects
objects = admin.workspace.list_workspace_objects("/")
```

## 👥 User Management

### User Operations

#### List and Search Users
```python
# List all users
users = admin.users.list_users()

# Get specific user
user = admin.users.get_user("john.doe")

# Search users
results = admin.users.search_users("john")
```

#### Create and Modify Users
```python
# Create new user
result = admin.users.create_user(
    user_name="jane.doe",
    display_name="Jane Doe",
    email="jane.doe@company.com",
    groups=["developers"]
)

# Update user
admin.users.update_user("jane.doe", display_name="Jane Smith")

# Deactivate user
admin.users.deactivate_user("john.doe")
```

#### Group Management
```python
# List groups
groups = admin.users.list_groups()

# Add user to group
admin.users.add_user_to_group("jane.doe", "admins")

# Remove user from group
admin.users.remove_user_from_group("jane.doe", "developers")

# Create group
admin.users.create_group("new-team")
```

### User Health Monitoring

```python
# Get user health metrics
health = admin.users.get_user_health()

# Monitor user activity
activity = admin.users.get_user_activity("john.doe")

# Check user permissions
permissions = admin.users.get_user_permissions("john.doe")
```

## 🖥️ Cluster Management

### Cluster Operations

#### List and Monitor Clusters
```python
# List all clusters
clusters = admin.clusters.list_clusters()

# Get cluster details
cluster = admin.clusters.get_cluster("cluster-123")

# Monitor cluster status
status = admin.clusters.get_cluster_status("cluster-123")
```

#### Create and Configure Clusters
```python
# Create development cluster
dev_cluster = admin.clusters.create_cluster(
    cluster_name="dev-cluster",
    spark_version="13.3.x-scala2.12",
    node_type_id="i3.xlarge",
    num_workers=2,
    autotermination_minutes=60
)

# Create production cluster
prod_cluster = admin.clusters.create_cluster(
    cluster_name="prod-cluster",
    spark_version="13.3.x-scala2.12",
    node_type_id="i3.2xlarge",
    num_workers=4,
    autotermination_minutes=0
)
```

#### Cluster Lifecycle Management
```python
# Start cluster
admin.clusters.start_cluster("cluster-123")

# Stop cluster
admin.clusters.stop_cluster("cluster-123")

# Restart cluster
admin.clusters.restart_cluster("cluster-123")

# Delete cluster
admin.clusters.delete_cluster("cluster-123")
```

### Cluster Health Monitoring

```python
# Get cluster health
health = admin.clusters.get_cluster_health("cluster-123")

# Monitor performance
performance = admin.clusters.get_cluster_performance("cluster-123")

# Check resource utilization
utilization = admin.clusters.get_resource_utilization("cluster-123")
```

## 🔒 Security Management

### Security Monitoring

#### Security Overview
```python
# Get security summary
summary = admin.security.get_security_summary()

# Run security audit
audit = admin.security.run_security_audit()

# Get security health
health = admin.security.get_security_health()
```

#### Access Control
```python
# Check user access
access = admin.security.check_user_access("john.doe", "table_name")

# Review permissions
permissions = admin.security.review_permissions()

# Audit access logs
logs = admin.security.get_access_logs()
```

#### Compliance Monitoring
```python
# Check compliance status
compliance = admin.security.check_compliance()

# Generate compliance report
report = admin.security.generate_compliance_report()

# Monitor policy violations
violations = admin.security.get_policy_violations()
```

## 🏢 Workspace Management

### Workspace Operations

#### Workspace Information
```python
# Get workspace info
info = admin.workspace.get_workspace_info()

# Get workspace health
health = admin.workspace.get_workspace_health()

# Get workspace statistics
stats = admin.workspace.get_workspace_stats()
```

#### Object Management
```python
# List workspace objects
objects = admin.workspace.list_workspace_objects("/")

# Search objects
results = admin.workspace.search_objects("query")

# Get object details
details = admin.workspace.get_object_details("/path/to/object")
```

#### Workspace Administration
```python
# Monitor workspace usage
usage = admin.workspace.monitor_usage()

# Check workspace capacity
capacity = admin.workspace.check_capacity()

# Optimize workspace
optimization = admin.workspace.optimize_workspace()
```

## 🛠️ CLI Usage

### Basic Commands

```bash
# List users
python shared/admin/cli/admin_cli.py list-users

# List clusters
python shared/admin/cli/admin_cli.py list-clusters

# Get workspace info
python shared/admin/cli/admin_cli.py workspace-info

# Run health check
python shared/admin/cli/admin_cli.py health-check
```

### User Management Commands

```bash
# Create user
python shared/admin/cli/admin_cli.py create-user \
  --user-name john.doe \
  --email john.doe@company.com \
  --groups developers

# Add user to group
python shared/admin/cli/admin_cli.py add-user-to-group \
  --user-name john.doe \
  --group admins

# Get user details
python shared/admin/cli/admin_cli.py get-user \
  --user-name john.doe
```

### Cluster Management Commands

```bash
# Create cluster
python shared/admin/cli/admin_cli.py create-cluster \
  --name dev-cluster \
  --spark-version 13.3.x-scala2.12 \
  --node-type i3.xlarge \
  --workers 2

# Start cluster
python shared/admin/cli/admin_cli.py start-cluster \
  --cluster-id cluster-123

# Get cluster health
python shared/admin/cli/admin_cli.py cluster-health \
  --cluster-id cluster-123
```

## 📊 Monitoring and Reporting

### Health Monitoring

```python
# Run comprehensive health check
health = admin.run_health_check()

# Get system health
system_health = admin.get_system_health()

# Monitor specific components
cluster_health = admin.clusters.get_cluster_health()
user_health = admin.users.get_user_health()
security_health = admin.security.get_security_health()
```

### Usage Reporting

```python
# Get usage summary
usage = admin.get_usage_summary()

# Generate detailed report
report = admin.generate_usage_report()

# Export usage data
admin.export_usage_data("usage_report.csv")
```

### Performance Monitoring

```python
# Monitor cluster performance
performance = admin.clusters.monitor_performance()

# Track user activity
activity = admin.users.track_activity()

# Monitor workspace performance
workspace_perf = admin.workspace.monitor_performance()
```

## 🔧 Configuration

### Environment Setup

```bash
# Set Databricks profile
export DATABRICKS_PROFILE="admin"

# Set workspace URL
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Set authentication token
export DATABRICKS_TOKEN="your-admin-token"
```

### Configuration Files

```python
# admin_config.json
{
    "default_profile": "admin",
    "workspace_url": "https://your-workspace.cloud.databricks.com",
    "monitoring": {
        "enable_health_checks": true,
        "health_check_interval": 3600,
        "enable_alerts": true
    },
    "security": {
        "enable_audit_logging": true,
        "compliance_checks": true,
        "policy_enforcement": true
    }
}
```

## 📚 Best Practices

### 1. User Management
- Use descriptive usernames and display names
- Assign users to appropriate groups
- Regularly review user access and permissions
- Implement least-privilege access

### 2. Cluster Management
- Use appropriate cluster sizes for workloads
- Set auto-termination for development clusters
- Monitor cluster performance and costs
- Implement cluster naming conventions

### 3. Security
- Regularly audit user access and permissions
- Monitor for security violations
- Implement strong authentication
- Keep security policies up to date

### 4. Monitoring
- Set up regular health checks
- Monitor resource utilization
- Track user activity patterns
- Generate regular reports

## 🔧 Troubleshooting

### Common Issues

1. **Authentication Errors**
   ```
   Error: Authentication failed
   ```
   **Solution**: Check Databricks token and permissions

2. **Permission Denied**
   ```
   Error: Permission denied for operation
   ```
   **Solution**: Verify admin privileges and workspace access

3. **Cluster Issues**
   ```
   Error: Cannot start cluster
   ```
   **Solution**: Check cluster configuration and resource availability

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Debug admin operations
admin = AdminClient(profile="admin", debug=True)
```

## 🎯 Next Steps

1. **Implement automated monitoring** and alerting
2. **Add advanced security features** like SSO integration
3. **Create custom reports** and dashboards
4. **Integrate with external monitoring tools**

---

**For more information, see the [Getting Started](getting_started.md) guide or [CLI Guide](cli_guide.md).**

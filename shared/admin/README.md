# Administrative Tools

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
from admin.core.admin_client import AdminClient

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

# Get workspace statistics
stats = admin.workspace.get_workspace_stats()

# Run workspace audit
audit = admin.workspace.run_workspace_audit()
```

## 🖥️ CLI Usage

The administrative tools include a comprehensive CLI for easy command-line access:

### Basic Commands

```bash
# Get workspace information
python admin/cli/admin_cli.py workspace-info

# Run comprehensive health check
python admin/cli/admin_cli.py health-check

# Get usage summary
python admin/cli/admin_cli.py usage-summary

# Run security audit
python admin/cli/admin_cli.py security-audit

# Run workspace audit
python admin/cli/admin_cli.py workspace-audit
```

### User Management Commands

```bash
# List all users
python admin/cli/admin_cli.py list-users

# Create a new user
python admin/cli/admin_cli.py create-user \
  --username john.doe \
  --display-name "John Doe" \
  --email john.doe@company.com \
  --groups developers data-scientists

# Get user summary
python admin/cli/admin_cli.py user-summary
```

### Cluster Management Commands

```bash
# List all clusters
python admin/cli/admin_cli.py list-clusters

# Create a new cluster
python admin/cli/admin_cli.py create-cluster \
  --name "development-cluster" \
  --workers 2 \
  --node-type i3.xlarge \
  --spark-version "13.3.x-scala2.12" \
  --autotermination 60

# Get cluster summary
python admin/cli/admin_cli.py cluster-summary
```

### Summary Commands

```bash
# Get workspace summary
python admin/cli/admin_cli.py workspace-summary

# Get security summary
python admin/cli/admin_cli.py security-summary
```

## 📊 Health Monitoring

### System Health Check

The administrative tools provide comprehensive health monitoring:

```python
# Run full system health check
health = admin.get_system_health()

# Health components:
# - Workspace health
# - Cluster health  
# - User health
# - Security health
# - Overall health score
```

### Health Metrics

Each component provides detailed health metrics:

- **Workspace Health**: Accessibility, functionality, user access
- **Cluster Health**: Running ratio, error states, autotermination coverage
- **User Health**: Active ratio, email coverage, user count
- **Security Health**: Admin access, authentication, workspace access

### Health Scoring

Health scores are calculated on a 0-100 scale:

- **80-100**: Healthy (green)
- **60-79**: Warning (yellow)  
- **0-59**: Critical (red)

## 🔒 Security Features

### Security Auditing

Comprehensive security audits check:

- **Admin Access**: Verify admin capabilities
- **User Authentication**: Check current user status
- **Workspace Access**: Test workspace permissions
- **Unity Catalog**: Verify Unity Catalog availability

### Access Control

The tools provide granular access control:

- **User Management**: Create, update, delete users
- **Group Management**: Add/remove users from groups
- **Cluster Management**: Create, start, stop clusters
- **Workspace Management**: Monitor workspace access

## 📈 Usage Monitoring

### Usage Statistics

Track workspace usage with detailed metrics:

- **Cluster Usage**: Total clusters, running clusters, utilization
- **User Usage**: Total users, active users, inactive users
- **Workspace Usage**: Object counts, path depth, organization

### Usage Reports

Generate comprehensive usage reports:

```python
# Get usage summary
usage = admin.get_usage_summary()

# Print formatted summary
admin.print_usage_summary()
```

## 🛠️ Configuration

### Profile Configuration

The tools use Databricks profiles from `.databrickscfg`:

```bash
# Use specific profile
python admin/cli/admin_cli.py workspace-info --profile production

# Default profile (databricks)
python admin/cli/admin_cli.py workspace-info
```

### Environment Variables

Configure behavior with environment variables:

```bash
# Set default profile
export DATABRICKS_PROFILE=production

# Set logging level
export LOG_LEVEL=INFO
```

## 🔧 Advanced Usage

### Custom Health Checks

Create custom health check functions:

```python
def custom_health_check(admin_client):
    """Custom health check implementation."""
    # Get workspace health
    workspace_health = admin_client.workspace.get_workspace_health()
    
    # Get cluster health
    cluster_health = admin_client.clusters.get_cluster_health()
    
    # Custom logic
    if workspace_health['health_score'] > 80 and cluster_health['health_score'] > 80:
        return {'status': 'healthy', 'score': 95}
    else:
        return {'status': 'warning', 'score': 65}
```

### Batch Operations

Perform batch operations on multiple resources:

```python
# Batch user creation
users_to_create = [
    {'username': 'user1', 'display_name': 'User 1', 'email': 'user1@company.com'},
    {'username': 'user2', 'display_name': 'User 2', 'email': 'user2@company.com'}
]

for user_data in users_to_create:
    result = admin.users.create_user(**user_data)
    print(f"Created user: {result['message']}")

# Batch cluster management
clusters = admin.clusters.list_clusters()
for cluster in clusters:
    if cluster.state == 'RUNNING' and cluster.autotermination_minutes > 120:
        print(f"Cluster {cluster.cluster_name} has long autotermination")
```

## 🚨 Error Handling

### Graceful Error Handling

All operations include comprehensive error handling:

```python
try:
    result = admin.users.create_user(...)
    if result['success']:
        print("✅ User created successfully")
    else:
        print(f"❌ Failed: {result['error']}")
except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")
```

### Error Recovery

The tools provide error recovery mechanisms:

- **Retry Logic**: Automatic retries for transient failures
- **Fallback Options**: Alternative approaches when primary methods fail
- **Detailed Logging**: Comprehensive error logging for debugging

## 📋 Best Practices

### Security Best Practices

1. **Use Admin Profiles**: Use dedicated admin profiles for administrative operations
2. **Audit Regularly**: Run security audits regularly to maintain compliance
3. **Monitor Access**: Track user access and cluster usage patterns
4. **Principle of Least Privilege**: Grant minimum necessary permissions

### Operational Best Practices

1. **Health Monitoring**: Set up regular health checks
2. **Usage Tracking**: Monitor resource usage to optimize costs
3. **Documentation**: Document administrative procedures and policies
4. **Backup Procedures**: Implement backup and recovery procedures

### Performance Best Practices

1. **Batch Operations**: Use batch operations for large-scale changes
2. **Caching**: Cache frequently accessed data
3. **Async Operations**: Use async operations for long-running tasks
4. **Resource Limits**: Set appropriate resource limits

## 🔄 Integration

### Integration with ETL Framework

The administrative tools integrate seamlessly with the ETL framework:

```python
from etl.core.config import create_default_config
from admin.core.admin_client import AdminClient

# Create ETL configuration
config = create_default_config("project", "dev")

# Initialize admin client
admin = AdminClient()

# Check system health before ETL operations
health = admin.get_system_health()
if health['overall']['status'] == 'healthy':
    # Proceed with ETL operations
    pass
else:
    print("⚠️  System health issues detected")
```

### Integration with CLI Tools

The administrative tools complement existing CLI tools:

```bash
# Use admin tools with existing CLI
python admin/cli/admin_cli.py health-check
python cli/dbfs_cli.py --path dbfs:/databricks-datasets/
python cli/analyze_dataset.py --path dbfs:/databricks-datasets/retail-org/customers/
```

## 📚 API Reference

### AdminClient

Main administrative client providing unified access to all administrative tools.

**Methods:**
- `get_workspace_info()`: Get comprehensive workspace information
- `get_system_health()`: Get system health metrics
- `run_health_check()`: Run comprehensive health check
- `get_usage_summary()`: Get workspace usage summary
- `print_usage_summary()`: Print formatted usage summary

### UserManager

User management operations for Databricks workspace.

**Methods:**
- `list_users()`: List all users
- `get_user(user_name)`: Get specific user
- `create_user(...)`: Create new user
- `update_user(...)`: Update user information
- `delete_user(user_name)`: Delete user
- `add_user_to_group(...)`: Add user to group
- `remove_user_from_group(...)`: Remove user from group
- `get_user_health()`: Get user health metrics
- `print_user_summary()`: Print user summary

### ClusterManager

Cluster management operations for Databricks workspace.

**Methods:**
- `list_clusters()`: List all clusters
- `get_cluster(cluster_id)`: Get specific cluster
- `create_cluster(...)`: Create new cluster
- `start_cluster(cluster_id)`: Start cluster
- `stop_cluster(cluster_id)`: Stop cluster
- `delete_cluster(cluster_id)`: Delete cluster
- `get_cluster_health()`: Get cluster health metrics
- `print_cluster_summary()`: Print cluster summary
- `get_cluster_usage_stats()`: Get cluster usage statistics

### SecurityManager

Security management operations for Databricks workspace.

**Methods:**
- `get_security_health()`: Get security health metrics
- `get_security_summary()`: Get security configuration summary
- `print_security_summary()`: Print security summary
- `run_security_audit()`: Run comprehensive security audit

### WorkspaceManager

Workspace management operations for Databricks workspace.

**Methods:**
- `get_workspace_health()`: Get workspace health metrics
- `get_workspace_stats()`: Get workspace statistics
- `list_workspace_objects(path)`: List workspace objects
- `get_workspace_info()`: Get comprehensive workspace information
- `print_workspace_summary()`: Print workspace summary
- `run_workspace_audit()`: Run comprehensive workspace audit

## 🎯 Next Steps

The administrative tools provide a solid foundation for Databricks workspace management. Future enhancements could include:

1. **Advanced Monitoring**: Integration with monitoring systems
2. **Automated Remediation**: Automatic fixing of common issues
3. **Compliance Reporting**: Detailed compliance and audit reports
4. **Cost Optimization**: Automated cost optimization recommendations
5. **Integration APIs**: REST APIs for external system integration

## 📞 Support

For questions, issues, or contributions:

1. **Documentation**: Check this README and inline documentation
2. **Examples**: Review the example usage patterns
3. **CLI Help**: Use `python admin/cli/admin_cli.py --help`
4. **Health Checks**: Run health checks to diagnose issues
5. **Logs**: Check application logs for detailed error information 
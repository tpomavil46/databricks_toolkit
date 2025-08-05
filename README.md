# Databricks Toolkit

A comprehensive toolkit for Databricks development with support for both SQL-driven and PySpark ETL workflows.

## 🏗️ Architecture

The toolkit is organized into clear, separate workflows to make navigation and understanding easy:

```
databricks_toolkit/
├── workflows/                    # 🎯 CLEAR WORKFLOW SEPARATION
│   ├── sql_driven/             # SQL-First Workflow
│   │   ├── README.md           # SQL workflow documentation
│   │   ├── run.py              # SQL workflow runner
│   │   ├── pipelines/          # SQL-driven pipelines
│   │   ├── sql/                # SQL templates and queries
│   │   ├── config/             # SQL workflow configuration
│   │   └── examples/           # SQL workflow examples
│   │
│   └── pyspark_etl/            # PySpark ETL Workflow
│       ├── README.md           # PySpark workflow documentation
│       ├── run.py              # PySpark workflow runner
│       ├── pipelines/          # PySpark ETL pipelines
│       ├── transformations/    # PySpark transformations
│       ├── config/             # PySpark workflow configuration
│       └── examples/           # PySpark workflow examples
│
├── shared/                      # 🔧 SHARED COMPONENTS
│   ├── cli/                    # Command-line tools
│   ├── admin/                  # Administrative tools
│   ├── utils/                  # Shared utilities
│   ├── sql_library/            # SQL library components
│   └── bootstrap/              # Bootstrap tools
│
├── config/                      # ⚙️ GLOBAL CONFIGURATION
│   ├── environments/           # Environment configs
│   ├── jobs/                   # Job configurations
│   └── templates/              # Configuration templates
│
├── tests/                       # 🧪 COMPREHENSIVE TESTING
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   └── ci/                     # CI-specific tests
│
├── tools/                       # 🛠️ UTILITY TOOLS
│   ├── create_test_data.py     # Test data generation
│   ├── check_tables.py         # Table inspection
│   ├── find_datasets.py        # Dataset discovery
│   └── test_sql_library.py    # SQL library testing
│
└── docs/                       # 📚 DOCUMENTATION
    ├── getting_started.md
    ├── workflows/
    └── examples/
```

## 🚀 Quick Start

### Choose Your Workflow

**SQL-Driven Workflow** - For SQL-first development:
```bash
# Run SQL workflow for retail project
python main.py sql retail

# Run with specific environment
python main.py sql ecommerce --environment prod
```

**PySpark ETL Workflow** - For Python-first development:
```bash
# Run PySpark ETL workflow
python main.py pyspark data_ingestion

# Run with specific environment
python main.py pyspark transformation --environment staging
```

**List Available Workflows**:
```bash
python main.py list
```

## 🛠️ Commands Reference

### Development & Testing

| Command | Description |
|---------|-------------|
| `make test` | Run unit tests using pytest |
| `make test-integration` | Run integration tests |
| `make lint` | Run flake8 for code linting |
| `make format` | Run black to auto-format Python files |
| `make sync` | Sync GitHub → Databricks Repos |

### Local Execution

| Command | Description |
|---------|-------------|
| `python main.py sql <project>` | Run SQL workflow locally |
| `python main.py pyspark <pipeline>` | Run PySpark ETL workflow locally |
| `python tools/create_test_data.py` | Generate test data |
| `python tools/check_tables.py` | Check available tables |
| `python tools/find_datasets.py` | Find available datasets |

### Deployment & Remote Execution

| Command | Description |
|---------|-------------|
| `make deploy-ingest` | Deploy ingest job to Databricks |
| `make run-ingest-remote JOB_ID=<id>` | Run ingest job remotely |
| `make deploy-transform` | Deploy transform job to Databricks |
| `make run-transform-remote JOB_ID=<id>` | Run transform job remotely |
| `make deploy-pipeline` | Deploy multi-task pipeline |
| `make run-pipeline-remote JOB_ID=<id>` | Run pipeline remotely |
| `make clean-remote JOB_ID=<id>` | Delete remote job |

### Job Management

| Command | Description |
|---------|-------------|
| `make generate-job JOB=<job_name>` | Generate job JSON spec |
| `databricks jobs list` | List all jobs in workspace |
| `databricks runs list --job-id <id>` | List runs for a job |
| `databricks runs get --run-id <id>` | Get run status |

### Databricks CLI Commands

| Command | Description |
|---------|-------------|
| `databricks clusters list` | List available clusters |
| `databricks fs ls dbfs:/...` | List files in DBFS |
| `databricks repos update --path "/Repos/..."` | Update repository |

## 📊 Workflow Comparison

| Feature | SQL-Driven | PySpark ETL |
|---------|------------|--------------|
| **Primary Language** | SQL | Python |
| **Best For** | SQL-first teams | Python-first teams |
| **Complexity** | Simple to moderate | Moderate to complex |
| **Performance** | Optimized SQL engine | Custom optimizations |
| **Maintenance** | SQL files | Python classes |
| **Extensibility** | SQL templates | Python inheritance |

## 🔧 Key Features

### ✅ **Clear Separation**
- **SQL-Driven Workflow**: SQL-first data processing
- **PySpark ETL Workflow**: Python-first ETL processing
- **Shared Components**: Reusable across both workflows

### ✅ **Easy Navigation**
- Clear entry points for each workflow
- Logical file organization
- Comprehensive documentation

### ✅ **Maintainable**
- Loose coupling between workflows
- Shared components reduce duplication
- Clear configuration management

### ✅ **Scalable**
- Easy to add new workflows
- Consistent structure across components
- Clear extension points

## 🛠️ Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd databricks_toolkit
```

2. **Set up virtual environment**:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**:
```bash
pip install -r requirements.txt
```

4. **Configure Databricks connection**:
```bash
# Set up your .databrickscfg file
databricks configure --profile your-profile
```

## 📋 Prerequisites

- Python 3.11+
- Databricks workspace access
- Databricks Connect configured
- Valid Databricks cluster

## 🔄 Usage Examples

### SQL-Driven Workflow

**Run a complete SQL pipeline**:
```bash
python main.py sql retail
```

**Run with custom environment**:
```bash
python main.py sql ecommerce --environment prod
```

**Direct workflow access**:
```bash
python workflows/sql_driven/run.py retail --environment staging
```

### PySpark ETL Workflow

**Run a PySpark ETL pipeline**:
```bash
python main.py pyspark data_ingestion
```

**Run with custom environment**:
```bash
python main.py pyspark transformation --environment prod
```

**Direct workflow access**:
```bash
python workflows/pyspark_etl/run.py data_ingestion --environment dev
```

### Shared Components

**CLI Tools**:
```bash
# List DBFS contents
python -m shared.cli.dbfs_cli list /path/to/data

# Execute SQL file
python -m shared.cli.query_file execute sql/bronze/retail/ingest_customers.sql
```

**Admin Tools**:
```bash
# List users
python -m shared.admin.admin_cli list-users

# Monitor clusters
python -m shared.admin.admin_cli list-clusters
```

**SQL Library**:
```bash
# List SQL patterns
python -m shared.sql_library.cli.sql_library_cli list-patterns

# Generate SQL library
python -m shared.sql_library.cli.sql_library_cli create-function-library
```

## 🧪 Testing

**Run CI tests** (recommended for development):
```bash
make test-ci
```

**Run simple tests** (no external dependencies):
```bash
make test-simple
```

**Run all tests** (including integration tests):
```bash
make test
```

**Run linting**:
```bash
make lint
```

## 📚 Documentation

- **[SQL-Driven Workflow](workflows/sql_driven/README.md)** - Complete SQL workflow guide
- **[PySpark ETL Workflow](workflows/pyspark_etl/README.md)** - Complete PySpark ETL guide
- **[CI Setup Guide](CI_SETUP.md)** - CI/CD configuration and testing
- **Commands Reference** - See the Commands Reference section above
- **[Roadmap](ROADMAP.md)** - Project roadmap and future enhancements

## 🔧 Configuration

### Environment Variables

```bash
# Required
export DATABRICKS_PROFILE="your-profile"
export DATABRICKS_CLUSTER_ID="your-cluster-id"

# Optional
export DATABRICKS_CATALOG="your-catalog"
export DATABRICKS_SCHEMA="your-schema"
```

### Configuration Files

- **Environment configs**: `config/environments/`
- **Job configs**: `config/jobs/`
- **Pipeline configs**: `workflows/*/config/`

## 🏗️ Development

### Project Structure

The toolkit follows a clear, organized structure:

1. **Workflows** - Separate SQL and PySpark ETL workflows
2. **Shared Components** - Reusable tools and utilities
3. **Configuration** - Environment and job configurations
4. **Testing** - Comprehensive test suites
5. **Documentation** - Complete guides and references

### Adding New Workflows

1. Create new workflow directory in `workflows/`
2. Add workflow runner (`run.py`)
3. Create workflow documentation (`README.md`)
4. Add configuration files
5. Update main entry point (`main.py`)

### Best Practices

1. **Use appropriate workflow** for your use case
2. **Follow naming conventions** for files and directories
3. **Add tests** for new functionality
4. **Update documentation** when adding features
5. **Use shared components** when possible

## 🤝 Contributing

1. **Choose your workflow** (SQL or PySpark ETL)
2. **Follow the structure** of existing components
3. **Add tests** for new functionality
4. **Update documentation** for changes
5. **Use shared components** when possible

## 📈 Roadmap

The Databricks Toolkit has completed its core development phases and is now production-ready. For detailed information about completed work and future enhancements, see the **[Roadmap](ROADMAP.md)**.

### ✅ **Completed Phases**
- **Phase 1**: Core Workflows (SQL-driven and PySpark ETL)
- **Phase 2**: SQL Library Framework
- **Phase 3**: Administrative Tools
- **Phase 4**: Enhanced CLI Toolkit
- **Phase 5**: Testing & Quality Assurance

### 🚀 **Current Status**
- **Production Ready** with comprehensive testing
- **Enterprise Features** with administrative tools
- **Professional CLI** with monitoring capabilities
- **Complete Documentation** with usage examples

### 🎯 **Future Enhancements**
- **Phase 6**: Advanced Features (ML, Streaming, Data Quality)
- **Phase 7**: Enterprise Features (Multi-tenant, Security, Performance)
- **Phase 8**: Developer Experience (IDE Integration, Training)

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Check Python path and virtual environment
2. **Connection Issues**: Verify Databricks credentials and cluster status
3. **Permission Errors**: Check workspace permissions and cluster access
4. **Performance Issues**: Monitor Spark UI and optimize configurations

### Getting Help

1. Check the [documentation](docs/)
2. Review [troubleshooting guides](docs/troubleshooting.md)
3. Run tests to identify issues
4. Check logs for detailed error messages

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**For detailed workflow documentation, see:**
- [SQL-Driven Workflow](workflows/sql_driven/README.md)
- [PySpark ETL Workflow](workflows/pyspark_etl/README.md)

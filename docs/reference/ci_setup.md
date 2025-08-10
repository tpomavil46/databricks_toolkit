# CI/CD Setup Guide

## Overview

This document explains how the CI/CD pipeline is configured and how to handle different types of tests.

## Test Strategy

### **1. CI Tests (`tests/run_ci_tests.py`)**
- **Purpose**: Run in GitHub Actions CI environment
- **Scope**: Unit tests, basic functionality tests, SQL library tests
- **Dependencies**: Minimal - no external connections required
- **Expected Result**: All tests should pass

**Tests Included:**
- ✅ Basic file operations
- ✅ CSV operations  
- ✅ Environment variable handling
- ✅ SQL library patterns, functions, templates, quality checks
- ✅ Logger utility functionality
- ⏭️ Schema normalizer (skipped - requires Databricks Connect)

### **2. Integration Tests (`tests/integration/test_suite.py`)**
- **Purpose**: Full end-to-end testing with Databricks
- **Scope**: Complete workflow testing, connection testing
- **Dependencies**: Databricks cluster, credentials, network access
- **Expected Result**: May fail in CI without proper Databricks setup

**Tests Included:**
- CLI tools (DBFS, dataset analysis, bronze ingestion)
- Admin tools (user, cluster, security management)
- ETL tools (pipeline initialization, data validation)
- Core tools (SQL pipeline executor, SQL-driven pipeline)
- Bootstrap tools (DBFS explorer)
- Utility tools (logger, schema normalizer)
- End-to-end workflows

### **3. Simple Tests (`tests/run_simple_tests.py`)**
- **Purpose**: Local development testing
- **Scope**: Basic functionality without external dependencies
- **Dependencies**: None
- **Expected Result**: All tests should pass

## CI Configuration

### GitHub Actions Workflow (`.github/workflows/ci.yml`)

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install flake8
      
      - name: Run CI tests
        run: |
          python tests/run_ci_tests.py
      
      - name: Run lint (continue on error)
        run: |
          make lint || echo "⚠️ Lint errors found"
        continue-on-error: true
```

### Key Features:
- ✅ **CI Tests**: Run comprehensive unit tests
- ✅ **Lint Check**: Style checking (non-blocking)
- ✅ **Error Handling**: Graceful handling of expected failures
- ✅ **Clear Feedback**: Detailed test summaries

## Local Development

### Available Commands:

```bash
# Run CI tests (recommended for development)
make test-ci

# Run simple tests (no dependencies)
make test-simple

# Run all tests (including integration tests)
make test

# Run integration tests only
make test-integration

# Run linting
make lint

# Run formatting
make format
```

### Test Results:

**CI Tests (13 tests):**
- ✅ 12 passing
- ⏭️ 1 skipped (schema normalizer - requires Databricks Connect)
- ❌ 0 failures

**Integration Tests (31 tests):**
- ✅ 22 passing
- ❌ 9 failing (expected - require Databricks setup)

## Troubleshooting

### CI Failures

**If CI tests fail:**
1. Check the test output for specific error messages
2. Verify that all required modules are properly imported
3. Ensure test data files exist if referenced
4. Check for syntax errors in test files

**If integration tests fail:**
1. Verify Databricks credentials are configured
2. Check cluster availability and connectivity
3. Ensure required datasets exist in DBFS
4. Review network connectivity to Databricks workspace

### Common Issues

**Import Errors:**
- Ensure `PYTHONPATH` is set correctly
- Check that all required packages are installed
- Verify module structure and `__init__.py` files

**Connection Timeouts:**
- Check Databricks cluster status
- Verify network connectivity
- Review authentication credentials

**Lint Errors:**
- These are style issues and don't affect functionality
- Can be fixed gradually over time
- CI continues even with lint errors

## Future Improvements

### **Phase 1: Enhanced CI Testing**
- [ ] Add more unit tests for core functionality
- [ ] Create mock-based integration tests
- [ ] Add performance benchmarks
- [ ] Implement test coverage reporting

### **Phase 2: Full Integration Testing**
- [ ] Configure Databricks credentials in GitHub Secrets
- [ ] Set up dedicated test cluster for CI
- [ ] Add automated data setup/teardown
- [ ] Implement parallel test execution

### **Phase 3: Advanced CI Features**
- [ ] Add test result caching
- [ ] Implement test parallelization
- [ ] Add performance regression testing
- [ ] Create test result dashboards

## Best Practices

### **For Developers:**
1. **Always run CI tests locally** before pushing
2. **Use `make test-ci`** for quick validation
3. **Fix lint errors** when convenient (not blocking)
4. **Add unit tests** for new functionality
5. **Document test requirements** for integration tests

### **For CI/CD:**
1. **CI tests must pass** for merge approval
2. **Integration tests** are optional but recommended
3. **Lint errors** are warnings, not blockers
4. **Test coverage** should increase over time
5. **Performance regressions** should be caught early

## Support

For CI/CD issues:
1. Check this document first
2. Review test output for specific errors
3. Verify environment setup matches requirements
4. Contact the development team for complex issues

---

**Last Updated**: 2024-12-31  
**Maintainer**: Databricks Toolkit Team 
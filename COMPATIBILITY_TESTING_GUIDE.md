# Compatibility Testing Guide

## Overview

Mock-spark includes a comprehensive compatibility testing system that verifies the package works correctly across multiple Python and PySpark version combinations using Docker.

## Quick Start

### Prerequisites

1. **Docker Desktop** - Install from https://www.docker.com/products/docker-desktop
2. **5-10 GB disk space** - For Docker images
3. **30-60 minutes** - For full test run

### Validate Setup

Before running tests, validate your setup:

```bash
python tests/compatibility_matrix/validate_setup.py
```

### Run All Tests

```bash
./run_compatibility_tests.sh
```

This will:
1. Build Docker images for each Python/PySpark combination
2. Run critical tests in isolated containers
3. Generate `COMPATIBILITY_REPORT.md` with results

## Test Coverage

### Version Matrix

Tests 20 combinations:
- **Python**: 3.9, 3.10, 3.11, 3.12, 3.13
- **PySpark**: 3.2.4, 3.3.4, 3.4.3, 3.5.1
- **Java**: 11 (for PySpark 3.2-3.4), 17 (for PySpark 3.5+)

### Tests Run

Each combination runs:
1. ✓ Package import verification
2. ✓ Basic DataFrame operations
3. ✓ Data type handling
4. ✓ SQL operations
5. ✓ PySpark compatibility

## Usage Examples

### Run Full Matrix

```bash
# Interactive mode (with confirmation)
./run_compatibility_tests.sh

# Direct execution
python tests/compatibility_matrix/run_matrix_tests.py
```

### Test Single Combination

```bash
# Test Python 3.10 + PySpark 3.3.4
./tests/compatibility_matrix/test_single_combination.sh 3.10 3.3.4

# Test Python 3.11 + PySpark 3.5.1
./tests/compatibility_matrix/test_single_combination.sh 3.11 3.5.1
```

### Custom Python/PySpark Versions

Edit `tests/compatibility_matrix/run_matrix_tests.py`:

```python
def get_pyspark_versions(self):
    return [
        ("3.2.4", "11"),
        ("3.3.4", "11"),
        # Add your versions here
    ]

def get_python_versions(self):
    return ["3.9", "3.10", "3.11", "3.12", "3.13"]
```

## Output

### COMPATIBILITY_REPORT.md

Generated report includes:

1. **Summary Statistics**
   - Total combinations tested
   - Pass/fail counts
   - Test duration

2. **Compatibility Matrix Table**
   - Visual grid showing pass/fail for each combination
   - Easy to spot compatibility issues

3. **Detailed Results**
   - Individual results for each combination
   - Error messages for failures
   - Test duration per combination

### Example Report

```markdown
# Mock-Spark Compatibility Matrix

**Generated:** 2024-01-15 10:30:00
**Total test time:** 45.2 minutes

## Summary
- **Total combinations tested:** 20
- **Passed:** 18 ✓
- **Failed:** 2 ✗

## Compatibility Matrix

| Python | PySpark 3.2 | PySpark 3.3 | PySpark 3.4 | PySpark 3.5 |
|--------|-------------|-------------|-------------|-------------|
| 3.9    | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✓ Pass      |
| 3.10   | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✗ Fail      |
| 3.11   | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✓ Pass      |
| 3.12   | ✓ Pass      | ✓ Pass      | ✓ Pass      | ✓ Pass      |
| 3.13   | ✓ Pass      | ✓ Pass      | ✗ Fail      | ✗ Fail      |

## Detailed Results

### Python 3.10 + PySpark 3.5.1 (Java 17)
**Status:** ✗ FAIL
**Duration:** 45.23s
**Error:** 
```
ImportError: cannot import name 'DataFrame' from 'pyspark.sql'
```

...
```

## Troubleshooting

### Docker Not Running

**Error:** `docker: command not found`

**Solution:**
1. Install Docker Desktop
2. Start Docker Desktop application
3. Verify: `docker info`

### Out of Disk Space

**Error:** `no space left on device`

**Solution:**
```bash
# Clean up old Docker images
docker system prune -a

# Check disk usage
docker system df
```

### Test Timeout

**Error:** Tests hang or timeout

**Solution:**
1. Increase timeout in `run_matrix_tests.py`:
   ```python
   timeout=600,  # 10 minutes
   ```
2. Check Docker resources (CPU/memory allocation)

### Specific Combination Fails

**Solution:**
1. Check detailed error in `COMPATIBILITY_REPORT.md`
2. Run single combination for debugging:
   ```bash
   ./tests/compatibility_matrix/test_single_combination.sh 3.10 3.3.4
   ```
3. Some combinations may be inherently incompatible

### Permission Denied

**Error:** `Permission denied`

**Solution:**
```bash
chmod +x run_compatibility_tests.sh
chmod +x tests/compatibility_matrix/test_single_combination.sh
chmod +x tests/compatibility_matrix/run_matrix_tests.py
```

## Advanced Usage

### Parallel Testing (Future Enhancement)

Currently tests run sequentially. To add parallel execution:

```python
# In run_matrix_tests.py
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(self.run_tests, ...) for ...]
```

### CI/CD Integration

Add to GitHub Actions workflow:

```yaml
name: Compatibility Tests

on: [push, pull_request]

jobs:
  compatibility:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Run compatibility tests
        run: |
          python tests/compatibility_matrix/run_matrix_tests.py
      - name: Upload report
        uses: actions/upload-artifact@v2
        with:
          name: compatibility-report
          path: COMPATIBILITY_REPORT.md
```

### Custom Test Selection

Modify `tests/compatibility_matrix/test_runner.sh` to run different tests:

```bash
# Add more tests
python -m pytest tests/unit/test_window_functions.py -v
python -m pytest tests/compatibility/test_delta_compat.py -v
```

## Performance Tips

1. **Use SSD** - Faster Docker image builds
2. **Allocate Resources** - Give Docker 4+ GB RAM and 2+ CPUs
3. **Run Overnight** - Full matrix takes 30-60 minutes
4. **Test Subset** - Edit versions list to test fewer combinations

## Maintenance

### Adding New Python Version

1. Edit `get_python_versions()` in `run_matrix_tests.py`
2. Add version to list: `return ["3.9", ..., "3.14"]`

### Adding New PySpark Version

1. Edit `get_pyspark_versions()` in `run_matrix_tests.py`
2. Add tuple: `("3.6.0", "17")`  # (version, java_version)

### Updating Test Coverage

1. Edit `tests/compatibility_matrix/test_runner.sh`
2. Add/remove test files as needed

## Related Documentation

- [tests/compatibility_matrix/README.md](tests/compatibility_matrix/README.md) - Detailed documentation
- [tests/compatibility_matrix/QUICK_START.md](tests/compatibility_matrix/QUICK_START.md) - Quick reference
- [COMPATIBILITY_TESTING_SETUP.md](COMPATIBILITY_TESTING_SETUP.md) - Implementation details

## Support

For issues or questions:
- Check troubleshooting section above
- Review detailed error in `COMPATIBILITY_REPORT.md`
- Open issue on GitHub with test output


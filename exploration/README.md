# Exploration Scripts

This directory contains scripts that run against **real PySpark 3.2.4 + Delta Lake 2.0.2** to document actual Spark behavior.

## Purpose

Before implementing features in mock-spark, we run exploration scripts to:
1. Understand exactly how real PySpark behaves
2. Document API signatures, return types, and edge cases
3. Capture actual error messages and exceptions
4. Identify schema transformations and data changes

## Environment

Use the dedicated Python 3.8 environment:

```bash
source venv_exploration_py38/bin/activate
python exploration/{script_name}.py > exploration/outputs/{script_name}_output.txt
```

## Scripts

- `delta_basic_write.py` - Basic Delta format write operations
- `delta_schema_evolution.py` - Schema evolution with mergeSchema
- `delta_merge.py` - MERGE INTO operations
- `delta_time_travel.py` - Time travel and versioning
- `complex_columns.py` - Complex column expressions
- `datetime_functions.py` - DateTime transformation functions
- `multi_schema.py` - Cross-schema operations

## Output

Outputs are saved to `exploration/outputs/` and are NOT tracked in git.
They serve as the specification for implementing features in mock-spark.


"""
Examples demonstrating inferSchema parity issues between PySpark and Sparkless.

This file demonstrates the behavior differences between:
- PySpark's inferSchema (default: False)
- Polars' infer_schema (default: True)
- Current Sparkless behavior

These examples should be used to validate parity fixes.
"""

# Example 1: CSV Reading Default Behavior
# ========================================


def example_csv_default_behavior():
    """
    PySpark: inferSchema=False by default (all columns read as strings)
    Polars: infer_schema=True by default (automatically infers types)

    Expected PySpark behavior:
    - All columns are StringType when inferSchema is not specified

    Current Sparkless issue:
    - Polars backend may infer types even when inferSchema is not set
    """
    print("=" * 60)
    print("Example 1: CSV Reading Default Behavior")
    print("=" * 60)

    # Create a sample CSV file for testing
    csv_content = """name,age,salary,active
Alice,25,50000.5,true
Bob,30,60000,false
Charlie,35,70000.75,true"""

    with open("/tmp/test_data.csv", "w") as f:
        f.write(csv_content)

    print("\n--- PySpark Expected Behavior ---")
    print("spark.read.option('header', True).csv('data.csv')")
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: string (nullable = true)      # String, not integer!")
    print("   |-- salary: string (nullable = true)   # String, not double!")
    print("   |-- active: string (nullable = true)   # String, not boolean!")
    print("\nNote: All columns are StringType because inferSchema=False by default")

    print("\n--- Current Sparkless Behavior (Issue) ---")
    print("spark.read.option('header', True).csv('data.csv')")
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: long (nullable = true)        # ❌ Inferred as integer!")
    print("   |-- salary: double (nullable = true)   # ❌ Inferred as double!")
    print("   |-- active: boolean (nullable = true)   # ❌ Inferred as boolean!")
    print("\nIssue: Polars defaults to infer_schema=True, causing type inference")
    print("even when PySpark would keep everything as strings.")


# Example 2: CSV Reading with Explicit inferSchema=False
# =======================================================


def example_csv_explicit_false():
    """
    When inferSchema=False is explicitly set, both should read all columns as strings.

    Current issue: Sparkless may not properly pass infer_schema=False to Polars.
    """
    print("\n" + "=" * 60)
    print("Example 2: CSV Reading with Explicit inferSchema=False")
    print("=" * 60)

    print("\n--- PySpark Expected Behavior ---")
    print(
        "spark.read.option('header', True).option('inferSchema', False).csv('data.csv')"
    )
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: string (nullable = true)")
    print("   |-- salary: string (nullable = true)")
    print("   |-- active: string (nullable = true)")
    print("\nAll columns are StringType (explicit inferSchema=False)")

    print("\n--- Current Sparkless Behavior (Issue) ---")
    print(
        "spark.read.option('header', True).option('inferSchema', False).csv('data.csv')"
    )
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: long (nullable = true)        # ❌ Still inferred!")
    print("   |-- salary: double (nullable = true)   # ❌ Still inferred!")
    print("   |-- active: boolean (nullable = true)  # ❌ Still inferred!")
    print("\nIssue: infer_schema=False may not be properly passed to Polars scan_csv()")


# Example 3: CSV Reading with Explicit inferSchema=True
# =======================================================


def example_csv_explicit_true():
    """
    When inferSchema=True is explicitly set, both should infer types.
    However, the inferred types should match PySpark's behavior.
    """
    print("\n" + "=" * 60)
    print("Example 3: CSV Reading with Explicit inferSchema=True")
    print("=" * 60)

    print("\n--- PySpark Expected Behavior ---")
    print(
        "spark.read.option('header', True).option('inferSchema', True).csv('data.csv')"
    )
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: long (nullable = true)        # Inferred as LongType")
    print("   |-- salary: double (nullable = true)   # Inferred as DoubleType")
    print("   |-- active: boolean (nullable = true)   # Inferred as BooleanType")

    print("\n--- Sparkless Expected Behavior (After Fix) ---")
    print(
        "spark.read.option('header', True).option('inferSchema', True).csv('data.csv')"
    )
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: long (nullable = true)        # Should match PySpark")
    print("   |-- salary: double (nullable = true)   # Should match PySpark")
    print("   |-- active: boolean (nullable = true)   # Should match PySpark")


# Example 4: Type Inference Differences - Mixed Int/Float
# ========================================================


def example_mixed_int_float():
    """
    PySpark promotes mixed int/float columns to DoubleType.
    Polars may handle this differently.
    """
    print("\n" + "=" * 60)
    print("Example 4: Type Inference - Mixed Int/Float")
    print("=" * 60)

    data = [  # noqa: F841
        {"id": 1, "value": 1.5},
        {"id": 2, "value": 2},  # Integer value
        {"id": 3, "value": 3.7},
    ]

    print("\nData:")
    print("  [{'id': 1, 'value': 1.5},")
    print("   {'id': 2, 'value': 2},      # Integer value")
    print("   {'id': 3, 'value': 3.7}]")

    print("\n--- PySpark Expected Behavior ---")
    print("spark.createDataFrame(data)")
    print("Schema:")
    print("  root")
    print("   |-- id: long (nullable = true)")
    print("   |-- value: double (nullable = true)   # Promoted to DoubleType")
    print("\nNote: PySpark promotes mixed int/float to DoubleType")

    print("\n--- Polars Behavior (Potential Issue) ---")
    print("Polars may infer value as Float64, which should map to DoubleType")
    print("But we need to ensure the promotion logic matches PySpark")


# Example 5: Type Inference - Date/Timestamp Strings
# ===================================================


def example_date_timestamp_inference():
    """
    PySpark and Polars may differ in how they detect date/timestamp strings.
    """
    print("\n" + "=" * 60)
    print("Example 5: Type Inference - Date/Timestamp Strings")
    print("=" * 60)

    data = [  # noqa: F841
        {"date_col": "2024-01-15", "timestamp_col": "2024-01-15 10:30:00"},
        {"date_col": "2024-02-20", "timestamp_col": "2024-02-20 14:45:30"},
    ]

    print("\nData:")
    print("  [{'date_col': '2024-01-15', 'timestamp_col': '2024-01-15 10:30:00'},")
    print("   {'date_col': '2024-02-20', 'timestamp_col': '2024-02-20 14:45:30'}]")

    print("\n--- PySpark Expected Behavior ---")
    print("spark.createDataFrame(data)")
    print("Schema:")
    print("  root")
    print("   |-- date_col: date (nullable = true)        # Detected as DateType")
    print(
        "   |-- timestamp_col: timestamp (nullable = true) # Detected as TimestampType"
    )

    print("\n--- Sparkless Expected Behavior (After Fix) ---")
    print("Should match PySpark's date/timestamp detection patterns")
    print(
        "Current SchemaInferenceEngine has _is_date_string() and _is_timestamp_string()"
    )
    print("methods that should be used consistently")


# Example 6: DataFrame Creation from Dict Data
# =============================================


def example_dataframe_from_dict():
    """
    When creating DataFrames from dict data, Polars automatically infers schema.
    We need to ensure this matches PySpark's inference behavior.
    """
    print("\n" + "=" * 60)
    print("Example 6: DataFrame Creation from Dict Data")
    print("=" * 60)

    data = [  # noqa: F841
        {"name": "Alice", "age": 25, "score": 95.5},
        {"name": "Bob", "age": 30, "score": 87},
    ]

    print("\nData:")
    print("  [{'name': 'Alice', 'age': 25, 'score': 95.5},")
    print("   {'name': 'Bob', 'age': 30, 'score': 87}]")

    print("\n--- PySpark Expected Behavior ---")
    print("spark.createDataFrame(data)")
    print("Schema:")
    print("  root")
    print("   |-- name: string (nullable = true)")
    print("   |-- age: long (nullable = true)        # Python int → LongType")
    print("   |-- score: double (nullable = true)    # Mixed int/float → DoubleType")

    print("\n--- Current Sparkless Behavior (Potential Issue) ---")
    print("In materializer.py line 138: pl.DataFrame(data)")
    print("Polars infers types automatically, but we need to ensure:")
    print("  1. Polars types are correctly mapped to Sparkless types")
    print("  2. Mixed int/float promotion matches PySpark")
    print("  3. Type inference uses SchemaInferenceEngine for consistency")


# Example 7: Null Handling in Type Inference
# ==========================================


def example_null_handling():
    """
    PySpark raises ValueError if all values for a column are null.
    """
    print("\n" + "=" * 60)
    print("Example 7: Null Handling in Type Inference")
    print("=" * 60)

    data = [  # noqa: F841
        {"name": "Alice", "age": 25, "unknown": None},
        {"name": "Bob", "age": 30, "unknown": None},
    ]

    print("\nData:")
    print("  [{'name': 'Alice', 'age': 25, 'unknown': None},")
    print("   {'name': 'Bob', 'age': 30, 'unknown': None}]")

    print("\n--- PySpark Expected Behavior ---")
    print("spark.createDataFrame(data)")
    print("Raises: ValueError('Some of types cannot be determined after inferring')")
    print("\nNote: PySpark cannot infer type for 'unknown' column (all nulls)")

    print("\n--- Sparkless Expected Behavior (After Fix) ---")
    print("Should match PySpark: raise ValueError for all-null columns")
    print("Current SchemaInferenceEngine.infer_from_data() already implements this")


if __name__ == "__main__":
    """
    Run all examples to demonstrate inferSchema parity issues.
    """
    example_csv_default_behavior()
    example_csv_explicit_false()
    example_csv_explicit_true()
    example_mixed_int_float()
    example_date_timestamp_inference()
    example_dataframe_from_dict()
    example_null_handling()

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print("\nKey Issues Identified:")
    print("1. CSV reading defaults to infer_schema=True in Polars (should be False)")
    print("2. infer_schema=False may not be properly passed to Polars scan_csv()")
    print("3. Type inference needs to match PySpark's promotion rules")
    print("4. Date/timestamp string detection should be consistent")
    print("5. DataFrame creation from dict should use consistent inference logic")
    print("\nFiles to Fix:")
    print("- sparkless/dataframe/reader.py: _extract_csv_options() method")
    print("- sparkless/backend/polars/materializer.py: pl.DataFrame() usage")
    print("- Ensure type mapping uses polars_dtype_to_mock_type() consistently")

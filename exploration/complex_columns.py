"""
Explore how real PySpark handles complex column expressions.

Run with: python exploration/complex_columns.py > exploration/outputs/complex_columns_output.txt 2>&1
"""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import tempfile
import shutil

# Create temp directory
temp_dir = tempfile.mkdtemp(prefix="complex_col_")
print(f"Using temp directory: {temp_dir}\n")

try:
    # Create Spark session
    spark = (
        SparkSession.builder.appName("ComplexColumnsExploration")
        .config("spark.sql.warehouse.dir", f"{temp_dir}/spark-warehouse")
        .master("local[*]")
        .getOrCreate()
    )

    print("=" * 80)
    print("TEST 1: Complex AND expressions")
    print("=" * 80)

    data = [
        {"user_id": "u1", "action": "click"},
        {"user_id": None, "action": "view"},
        {"user_id": "u3", "action": None},
        {"user_id": None, "action": None},
    ]
    df = spark.createDataFrame(data)

    print("\nOriginal data:")
    df.show()

    # Create complex AND expression
    expr_and = F.col("user_id").isNotNull() & F.col("action").isNotNull()
    print(f"\nExpression type: {type(expr_and)}")
    print(f"Expression: {expr_and}")

    # Use with filter
    print("\nFilter with complex AND:")
    df.filter(expr_and).show()

    # Use with withColumn
    print("\nwithColumn with complex AND:")
    df_with_col = df.withColumn("is_valid", expr_and)
    df_with_col.show()
    df_with_col.printSchema()

    # Check values
    rows = df_with_col.collect()
    print("\nValidation results:")
    for i, row in enumerate(rows):
        print(
            f"  Row {i}: user_id={row['user_id']}, action={row['action']}, is_valid={row['is_valid']}"
        )

    print("\n" + "=" * 80)
    print("TEST 2: Complex OR expressions")
    print("=" * 80)

    expr_or = F.col("user_id").isNull() | F.col("action").isNull()

    print("\nwithColumn with complex OR:")
    df_or = df.withColumn("has_null", expr_or)
    df_or.show()

    print("\n" + "=" * 80)
    print("TEST 3: Nested complex expressions")
    print("=" * 80)

    # (col1 & col2) | col3
    data2 = [
        {"a": True, "b": True, "c": False},
        {"a": True, "b": False, "c": False},
        {"a": False, "b": False, "c": True},
        {"a": False, "b": False, "c": False},
    ]
    df2 = spark.createDataFrame(data2)

    print("\nOriginal data:")
    df2.show()

    nested_expr = (F.col("a") & F.col("b")) | F.col("c")
    df2_nested = df2.withColumn("result", nested_expr)

    print("\nAfter (a AND b) OR c:")
    df2_nested.show()

    print("\n" + "=" * 80)
    print("TEST 4: alias() vs label() on complex expressions")
    print("=" * 80)

    # Check if alias works
    print("\nUsing alias():")
    expr_alias = expr_and.alias("validation_check")
    df_alias = df.select("*", expr_alias)
    df_alias.show()
    print(f"Column name: {df_alias.columns[-1]}")

    # Check if label exists (deprecated in newer Spark)
    print("\nChecking label() method:")
    try:
        # Check if method exists
        if hasattr(expr_and, "label"):
            expr_label = expr_and.label("validation_check")
            print(f"label() exists: {expr_label is not None}")
        else:
            print("label() method not found on Column object")
            print("Note: label() may be deprecated, use alias() instead")
    except Exception as e:
        print(f"label() error: {type(e).__name__}: {e}")
        print("Note: label() is deprecated/removed, use alias() instead")

    print("\n" + "=" * 80)
    print("TEST 5: Complex expressions in SQL")
    print("=" * 80)

    df.createOrReplaceTempView("test_data")

    sql_result = spark.sql(
        """
        SELECT 
            user_id,
            action,
            (user_id IS NOT NULL AND action IS NOT NULL) as is_valid
        FROM test_data
    """
    )

    print("\nSQL equivalent:")
    sql_result.show()

    print("\n" + "=" * 80)
    print("TEST 6: Multiple complex conditions combined")
    print("=" * 80)

    data3 = [
        {"id": 1, "val1": 10, "val2": 20, "status": "active"},
        {"id": 2, "val1": 5, "val2": 3, "status": "inactive"},
        {"id": 3, "val1": None, "val2": 15, "status": "active"},
    ]
    df3 = spark.createDataFrame(data3)

    print("\nOriginal data:")
    df3.show()

    # Complex: (val1 > val2) AND status == 'active' AND val1 IS NOT NULL
    complex_cond = (
        (F.col("val1") > F.col("val2")) & (F.col("status") == "active") & F.col("val1").isNotNull()
    )

    df3_complex = df3.withColumn("complex_check", complex_cond)
    print("\nWith complex condition:")
    df3_complex.show()

    print("\n" + "=" * 80)
    print("SUMMARY: Key Findings")
    print("=" * 80)
    print("1. Complex AND expressions: col1.isNotNull() & col2.isNotNull()")
    print("2. Complex OR expressions: col1.isNull() | col2.isNull()")
    print("3. Nested expressions: (col1 & col2) | col3")
    print("4. alias() works on complex expressions")
    print("5. label() is deprecated - use alias() instead")
    print("6. Complex expressions work in withColumn")
    print("7. Can combine comparison, boolean, and null check operators")

finally:
    spark.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)
    print(f"\nCleaned up temp directory: {temp_dir}")

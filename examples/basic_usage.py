#!/usr/bin/env python3
"""
Basic Usage Example for Sparkless

Demonstrates core Sparkless functionality with practical examples.

Status: 515 tests passing (100%) | Production Ready | Version 2.0.0
Features: 100% Zero Raw SQL | Database Agnostic | Pure SQLAlchemy
"""

import os
import sys

# Allow running this script directly without installing the package
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from sparkless.sql import SparkSession, functions as F, Window


def main() -> None:
    """Demonstrate basic Sparkless usage."""
    # Default to fast mode unless explicitly requested to run full demo
    if os.environ.get("MOCK_SPARK_EXAMPLES_FULL") != "1":
        print("🚀 Sparkless - Basic Usage Example (fast mode)")
        spark = SparkSession("BasicExampleFast")
        df = spark.createDataFrame([{"id": 1, "name": "Alice", "salary": 80000}])
        _ = df.select(
            "name", F.round(F.col("salary") / 1000, 1).alias("salary_k")
        ).collect()
        spark.stop()
        return
    print("🚀 Sparkless - Basic Usage Example")
    print("=" * 60)

    # 1. Create Session
    print("\n1️⃣  Creating Sparkless Session...")
    spark = SparkSession("BasicExample")
    print(f"   ✓ Session created: {spark.app_name}")

    # 2. Create DataFrame
    print("\n2️⃣  Creating DataFrame...")
    data = [
        {"id": 1, "name": "Alice", "dept": "Engineering", "salary": 80000},
        {"id": 2, "name": "Bob", "dept": "Sales", "salary": 75000},
        {"id": 3, "name": "Charlie", "dept": "Engineering", "salary": 90000},
        {"id": 4, "name": "Diana", "dept": "Marketing", "salary": 70000},
        {"id": 5, "name": "Eve", "dept": "Sales", "salary": 85000},
    ]
    df = spark.createDataFrame(data)
    print(f"   ✓ Created DataFrame: {df.count()} rows, {len(df.columns)} columns")

    print("\n   Schema:")
    df.printSchema()

    print("\n   Data:")
    df.show()

    # 3. Filtering
    print("\n3️⃣  Filtering...")
    high_earners = df.filter(F.col("salary") > 75000)
    print(f"   ✓ Employees earning > $75k: {high_earners.count()}")
    high_earners.show()

    # 4. Column Operations
    print("\n4️⃣  Column Operations...")
    result = df.select(
        "name",
        "salary",
        F.round(F.col("salary") / 1000, 1).alias("salary_k"),
        F.upper(F.col("name")).alias("upper_name"),
    )
    print("   ✓ Applied transformations:")
    result.show()

    # 5. Aggregations
    print("\n5️⃣  Aggregations...")
    dept_stats = (
        df.groupBy("dept")
        .agg(
            F.count("*").alias("count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
        )
        .orderBy(F.desc("avg_salary"))
    )
    print("   ✓ Department statistics:")
    dept_stats.show()

    # 6. Window Functions
    print("\n6️⃣  Window Functions...")
    window_spec = Window.partitionBy("dept").orderBy("salary")
    ranked = df.select(
        "name", "dept", "salary", F.row_number().over(window_spec).alias("rank")
    )
    print("   ✓ Salary rankings by department:")
    ranked.show()

    # 7. Joins
    print("\n7️⃣  Joins...")
    dept_data = [
        {"dept": "Engineering", "location": "San Francisco"},
        {"dept": "Sales", "location": "New York"},
        {"dept": "Marketing", "location": "Boston"},
    ]
    dept_df = spark.createDataFrame(dept_data)

    joined = df.join(dept_df, "dept").select("name", "dept", "salary", "location")
    print("   ✓ Joined with department locations:")
    joined.show()

    # 8. SQL Queries
    print("\n8️⃣  SQL Queries...")
    df.createOrReplaceTempView("employees")
    # Simple SQL query
    sql_result = spark.sql(
        "SELECT name, dept, salary FROM employees WHERE salary > 80000"
    )
    print("   ✓ SQL query result (salary > 80k):")
    sql_result.show()

    # 9. Lazy Evaluation Demo
    print("\n9️⃣  Lazy Evaluation...")
    # Transformations are queued (not executed)
    lazy_result = (
        df.filter(F.col("salary") > 70000)
        .select("name", "salary")
        .orderBy(F.desc("salary"))
    )
    print("   ✓ Transformations queued (not executed yet)")

    # Action triggers execution
    top_earners = lazy_result.collect()
    print(f"   ✓ Action executed: {len(top_earners)} results")
    for row in top_earners:
        print(f"     - {row['name']}: ${row['salary']:,}")

    # 10. Cleanup
    print("\n🔟 Cleanup...")
    spark.stop()
    print("   ✓ Session stopped")

    print("\n✨ Example completed successfully!")
    print("\n💡 Key Takeaways:")
    print("   • Sparkless provides drop-in PySpark replacement")
    print("   • No JVM required - 10x faster tests")
    print("   • Full API compatibility with lazy evaluation")
    print("   • Perfect for unit testing and CI/CD")


if __name__ == "__main__":
    main()

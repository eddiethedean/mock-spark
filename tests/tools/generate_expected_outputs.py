#!/usr/bin/env python3
"""
Generate expected outputs from PySpark for compatibility testing.

This script runs test operations against real PySpark and captures
the outputs to JSON files for later comparison with mock-spark.

Usage:
    python tests/tools/generate_expected_outputs.py --all
    python tests/tools/generate_expected_outputs.py --category dataframe_operations
    python tests/tools/generate_expected_outputs.py --pyspark-version 3.5
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import *
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Error: PySpark not available. Install with: pip install pyspark")
    sys.exit(1)


class ExpectedOutputGenerator:
    """Generates expected outputs from PySpark for test comparison."""
    
    def __init__(self, output_dir: Optional[str] = None, pyspark_version: str = "3.5"):
        """Initialize the generator."""
        if output_dir is None:
            output_dir = project_root / "tests" / "expected_outputs"
        self.output_dir = Path(output_dir)
        self.pyspark_version = pyspark_version
        self.spark: Optional[SparkSession] = None
        
        # Create output directory structure
        self._create_directory_structure()
    
    def _create_directory_structure(self):
        """Create the expected outputs directory structure."""
        categories = [
            "dataframe_operations",
            "functions", 
            "sql_operations",
            "delta_operations",
            "window_operations",
            "joins",
            "arrays",
            "maps",
            "datetime",
            "aggregations",
            "null_handling",
            "set_operations",
            "chained_operations",
            "pyspark_30_features",
            "pyspark_31_features", 
            "pyspark_32_features",
            "pyspark_33_features",
            "pyspark_34_features",
            "pyspark_35_features"
        ]
        
        for category in categories:
            (self.output_dir / category).mkdir(parents=True, exist_ok=True)
    
    def start_spark_session(self):
        """Start a PySpark session for generating outputs."""
        if self.spark is not None:
            return
        
        # Set Java options for compatibility
        os.environ.setdefault("JAVA_TOOL_OPTIONS", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
        )
        
        # Create temporary warehouse directory
        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
        
        self.spark = (
            SparkSession.builder
            .appName("ExpectedOutputGenerator")
            .master("local[1]")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.execution.arrow.pyspark.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "1g")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .getOrCreate()
        )
        
        # Set log level to reduce noise
        self.spark.sparkContext.setLogLevel("ERROR")
    
    def stop_spark_session(self):
        """Stop the PySpark session."""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
    
    def generate_dataframe_operations(self):
        """Generate expected outputs for DataFrame operations."""
        self.start_spark_session()
        
        # Test data
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "department": "IT"},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "department": "HR"},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "department": "IT"},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0, "department": "Finance"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Test cases
        test_cases = [
            ("basic_select", lambda: df.select("id", "name", "age")),
            ("select_with_alias", lambda: df.select(df.id.alias("user_id"), df.name.alias("full_name"))),
            ("filter_operations", lambda: df.filter(df.age > 30)),
            ("filter_with_boolean", lambda: df.filter(df.salary > 60000)),
            ("with_column", lambda: df.withColumn("bonus", df.salary * 0.1)),
            ("drop_column", lambda: df.drop("department")),
            ("distinct", lambda: df.select("department").distinct()),
            ("order_by", lambda: df.orderBy("salary")),
            ("order_by_desc", lambda: df.orderBy(df.salary.desc())),
            ("limit", lambda: df.limit(2)),
            ("column_access", lambda: df.select(df["id"], df["name"], df["salary"])),
            ("group_by", lambda: df.groupBy("department").count()),
            ("aggregation", lambda: df.groupBy("department").agg(F.avg("salary").alias("avg_salary"), F.count("id").alias("count"))),
        ]
        
        for test_name, operation in test_cases:
            try:
                result_df = operation()
                self._save_expected_output("dataframe_operations", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")
    
    def generate_functions(self):
        """Generate expected outputs for function operations."""
        self.start_spark_session()
        
        # Test data with various types
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True, "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": False, "email": "bob@test.com"},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "active": True, "email": "charlie@company.org"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # String functions
        # Create additional test data with leading/trailing spaces
        string_test_data = [
            {"id": 1, "name": "  Alice  ", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "  Charlie", "age": 35},
        ]
        df_string = self.spark.createDataFrame(string_test_data)
        
        string_tests = [
            ("upper", lambda: df.select(F.upper(df.name))),
            ("lower", lambda: df.select(F.lower(df.name))),
            ("length", lambda: df.select(F.length(df.name))),
            ("substring", lambda: df.select(F.substring(df.name, 1, 3))),
            ("concat", lambda: df.select(F.concat(df.name, F.lit(" - "), df.email))),
            ("split", lambda: df.select(F.split(df.email, "@"))),
            ("regexp_extract", lambda: df.select(F.regexp_extract(df.email, r"@(.+)", 1))),
            ("trim", lambda: df_string.select(F.trim(df_string.name))),
            ("ltrim", lambda: df_string.select(F.ltrim(df_string.name))),
            ("rtrim", lambda: df_string.select(F.rtrim(df_string.name))),
            ("lpad", lambda: df_string.select(F.lpad(df_string.name, 10, " "))),
            ("rpad", lambda: df_string.select(F.rpad(df_string.name, 10, " "))),
            ("like", lambda: df_string.select(df_string.name.like("%a%"))),
            ("rlike", lambda: df_string.select(df_string.name.rlike("^[A-Z]"))),
        ]
        
        for test_name, operation in string_tests:
            # Use appropriate dataframe based on test
            if test_name in ["trim", "ltrim", "rtrim", "lpad", "rpad", "like", "rlike"]:
                test_data_used = string_test_data
            else:
                test_data_used = test_data
            try:
                result_df = operation()
                self._save_expected_output("functions", f"string_{test_name}", test_data_used, result_df)
                print(f"✓ Generated string_{test_name}")
            except Exception as e:
                print(f"✗ Failed string_{test_name}: {e}")
        
        # Math functions
        # Create additional test data for math functions
        math_test_data = [
            {"id": 1, "a": 5, "b": 3, "c": 7, "angle": 0.785, "value": 4.5},
            {"id": 2, "a": 2, "b": 8, "c": 1, "angle": 1.57, "value": 3.2},
            {"id": 3, "a": 9, "b": 4, "c": 6, "angle": 0.0, "value": 7.8},
        ]
        df_math = self.spark.createDataFrame(math_test_data)
        
        math_tests = [
            ("abs", lambda: df.select(F.abs(df.salary))),
            ("round", lambda: df.select(F.round(df.salary, -3))),
            ("sqrt", lambda: df.select(F.sqrt(df.salary))),
            ("pow", lambda: df.select(F.pow(df.age, 2))),
            ("log", lambda: df.select(F.log(df.salary))),
            ("exp", lambda: df.select(F.exp(F.lit(1)))),
            ("sin", lambda: df_math.select(F.sin(df_math.angle))),
            ("cos", lambda: df_math.select(F.cos(df_math.angle))),
            ("tan", lambda: df_math.select(F.tan(df_math.angle))),
            ("ceil", lambda: df_math.select(F.ceil(df_math.value))),
            ("floor", lambda: df_math.select(F.floor(df_math.value))),
            ("greatest", lambda: df_math.select(F.greatest(df_math.a, df_math.b, df_math.c))),
            ("least", lambda: df_math.select(F.least(df_math.a, df_math.b, df_math.c))),
        ]
        
        for test_name, operation in math_tests:
            # Use appropriate dataframe based on test
            if test_name in ["sin", "cos", "tan", "ceil", "floor", "greatest", "least"]:
                math_test_data_used = math_test_data
            else:
                math_test_data_used = test_data
            try:
                result_df = operation()
                self._save_expected_output("functions", f"math_{test_name}", math_test_data_used, result_df)
                print(f"✓ Generated math_{test_name}")
            except Exception as e:
                print(f"✗ Failed math_{test_name}: {e}")
        
        # Aggregate functions
        agg_tests = [
            ("sum", lambda: df.groupBy("active").agg(F.sum("salary"))),
            ("avg", lambda: df.groupBy("active").agg(F.avg("salary"))),
            ("count", lambda: df.groupBy("active").agg(F.count("id"))),
            ("max", lambda: df.groupBy("active").agg(F.max("salary"))),
            ("min", lambda: df.groupBy("active").agg(F.min("salary"))),
        ]
        
        for test_name, operation in agg_tests:
            try:
                result_df = operation()
                self._save_expected_output("functions", f"agg_{test_name}", test_data, result_df)
                print(f"✓ Generated agg_{test_name}")
            except Exception as e:
                print(f"✗ Failed agg_{test_name}: {e}")
        
        # Conditional functions
        conditional_test_data = [
            {"id": 1, "age": 25, "salary": 50000, "col1": None, "col2": "backup", "col3": "default"},
            {"id": 2, "age": 35, "salary": None, "col1": "primary", "col2": None, "col3": "default"},
            {"id": 3, "age": 45, "salary": 70000, "col1": "primary", "col2": "backup", "col3": None},
        ]
        df_conditional = self.spark.createDataFrame(conditional_test_data)
        
        conditional_tests = [
            ("when_otherwise", lambda: df_conditional.select(F.when(df_conditional.age > 30, "Senior").otherwise("Junior").alias("level"))),
            ("coalesce", lambda: df_conditional.select(F.coalesce(df_conditional.col1, df_conditional.col2, df_conditional.col3).alias("first_non_null"))),
        ]
        
        for test_name, operation in conditional_tests:
            try:
                result_df = operation()
                self._save_expected_output("functions", f"conditional_{test_name}", conditional_test_data, result_df)
                print(f"✓ Generated conditional_{test_name}")
            except Exception as e:
                print(f"✗ Failed conditional_{test_name}: {e}")
    
    def generate_window_operations(self):
        """Generate expected outputs for window operations."""
        self.start_spark_session()
        
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000},
            {"id": 4, "name": "David", "department": "IT", "salary": 55000},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Window functions
        from pyspark.sql.window import Window
        
        window_spec = Window.partitionBy("department").orderBy("salary")
        
        window_tests = [
            ("row_number", lambda: df.withColumn("row_num", F.row_number().over(window_spec))),
            ("rank", lambda: df.withColumn("rank", F.rank().over(window_spec))),
            ("dense_rank", lambda: df.withColumn("dense_rank", F.dense_rank().over(window_spec))),
            ("lag", lambda: df.withColumn("prev_salary", F.lag("salary", 1).over(window_spec))),
            ("lead", lambda: df.withColumn("next_salary", F.lead("salary", 1).over(window_spec))),
            ("sum_over_window", lambda: df.withColumn("dept_total", F.sum("salary").over(Window.partitionBy("department")))),
        ]
        
        for test_name, operation in window_tests:
            try:
                result_df = operation()
                self._save_expected_output("window_operations", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")
    
    def generate_sql_operations(self):
        """Generate expected outputs for SQL operations."""
        self.start_spark_session()
        
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0},
        ]
        
        df = self.spark.createDataFrame(test_data)
        df.createOrReplaceTempView("employees")
        
        sql_tests = [
            ("basic_select", "SELECT id, name, age FROM employees"),
            ("filtered_select", "SELECT * FROM employees WHERE age > 30"),
            ("aggregation", "SELECT AVG(salary) as avg_salary FROM employees"),
            ("group_by", "SELECT COUNT(*) as count FROM employees GROUP BY (age > 30)"),
        ]
        
        for test_name, sql_query in sql_tests:
            try:
                result_df = self.spark.sql(sql_query)
                self._save_expected_output("sql_operations", test_name, test_data, result_df, sql_query=sql_query)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_join_outputs(self):
        """Generate expected outputs for join operations."""
        self.start_spark_session()
        
        # Test data for joins
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = self.spark.createDataFrame(employees_data)
        dept_df = self.spark.createDataFrame(departments_data)
        
        # Join operations
        join_tests = [
            ("inner_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner")),
            ("left_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left")),
            ("right_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right")),
            ("outer_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer")),
            ("cross_join", lambda: emp_df.crossJoin(dept_df)),
            ("semi_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi")),
            ("anti_join", lambda: emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti")),
        ]
        
        for test_name, operation in join_tests:
            try:
                result_df = operation()
                self._save_expected_output("joins", test_name, employees_data + departments_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_array_outputs(self):
        """Generate expected outputs for array operations."""
        self.start_spark_session()
        
        # Test data with arrays
        test_data = [
            {"id": 1, "name": "Alice", "scores": [85, 90, 78], "tags": ["python", "data"]},
            {"id": 2, "name": "Bob", "scores": [92, 88, 95], "tags": ["java", "backend"]},
            {"id": 3, "name": "Charlie", "scores": [76, 82, 89], "tags": ["python", "ml"]},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Array functions
        array_tests = [
            ("array_contains", lambda: df.select(F.array_contains(df.scores, 90))),
            ("array_position", lambda: df.select(F.array_position(df.scores, 90))),
            ("size", lambda: df.select(F.size(df.scores))),
            ("element_at", lambda: df.select(F.element_at(df.scores, 2))),
            ("array_append", lambda: df.select(F.array_union(df.scores, F.array(F.lit(100))))),
            ("array_remove", lambda: df.select(F.array_remove(df.scores, 90))),
            ("array_distinct", lambda: df.select(F.array_distinct(df.tags))),
            ("explode", lambda: df.select(df.name, F.explode(df.scores).alias("score"))),
            ("explode_outer", lambda: df.select(df.name, F.explode_outer(df.scores).alias("score"))),
        ]
        
        for test_name, operation in array_tests:
            try:
                result_df = operation()
                self._save_expected_output("arrays", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_datetime_outputs(self):
        """Generate expected outputs for date/time operations."""
        self.start_spark_session()
        
        # Test data with dates
        test_data = [
            {"id": 1, "name": "Alice", "hire_date": "2020-01-15", "birth_date": "1990-05-20"},
            {"id": 2, "name": "Bob", "hire_date": "2019-03-10", "birth_date": "1985-12-03"},
            {"id": 3, "name": "Charlie", "hire_date": "2021-07-22", "birth_date": "1992-08-14"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Date/time functions
        datetime_tests = [
            ("year", lambda: df.select(F.year(df.hire_date))),
            ("month", lambda: df.select(F.month(df.hire_date))),
            ("dayofmonth", lambda: df.select(F.dayofmonth(df.hire_date))),
            ("dayofweek", lambda: df.select(F.dayofweek(df.hire_date))),
            ("dayofyear", lambda: df.select(F.dayofyear(df.hire_date))),
            ("weekofyear", lambda: df.select(F.weekofyear(df.hire_date))),
            ("quarter", lambda: df.select(F.quarter(df.hire_date))),
            ("date_add", lambda: df.select(F.date_add(df.hire_date, 30))),
            ("date_sub", lambda: df.select(F.date_sub(df.hire_date, 30))),
            ("months_between", lambda: df.select(F.months_between(df.hire_date, df.birth_date))),
            ("date_format", lambda: df.select(F.date_format(df.hire_date, "yyyy-MM"))),
            ("to_date", lambda: df.select(F.to_date(df.hire_date))),
            ("current_date", lambda: df.select(F.current_date())),
            ("current_timestamp", lambda: df.select(F.current_timestamp())),
        ]
        
        for test_name, operation in datetime_tests:
            try:
                result_df = operation()
                self._save_expected_output("datetime", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_window_outputs(self):
        """Generate expected outputs for window functions."""
        self.start_spark_session()
        
        test_data = [
            {"id": 1, "name": "Alice", "department": "IT", "salary": 50000, "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "department": "HR", "salary": 60000, "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "department": "IT", "salary": 70000, "hire_date": "2021-07-22"},
            {"id": 4, "name": "David", "department": "IT", "salary": 55000, "hire_date": "2020-11-05"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        from pyspark.sql.window import Window
        
        # Window specifications
        dept_window = Window.partitionBy("department").orderBy("salary")
        salary_window = Window.partitionBy("department")
        
        # Window functions
        window_tests = [
            ("row_number", lambda: df.withColumn("row_num", F.row_number().over(dept_window))),
            ("rank", lambda: df.withColumn("rank", F.rank().over(dept_window))),
            ("dense_rank", lambda: df.withColumn("dense_rank", F.dense_rank().over(dept_window))),
            ("percent_rank", lambda: df.withColumn("percent_rank", F.percent_rank().over(dept_window))),
            ("ntile", lambda: df.withColumn("ntile", F.ntile(2).over(dept_window))),
            ("lag", lambda: df.withColumn("prev_salary", F.lag("salary", 1).over(dept_window))),
            ("lead", lambda: df.withColumn("next_salary", F.lead("salary", 1).over(dept_window))),
            ("first", lambda: df.withColumn("first_salary", F.first("salary").over(salary_window))),
            ("last", lambda: df.withColumn("last_salary", F.last("salary").over(salary_window))),
            ("sum_over_window", lambda: df.withColumn("dept_total", F.sum("salary").over(salary_window))),
            ("avg_over_window", lambda: df.withColumn("dept_avg", F.avg("salary").over(salary_window))),
            ("count_over_window", lambda: df.withColumn("dept_count", F.count("salary").over(salary_window))),
        ]
        
        for test_name, operation in window_tests:
            try:
                result_df = operation()
                self._save_expected_output("windows", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_null_handling_outputs(self):
        """Generate expected outputs for null handling operations."""
        self.start_spark_session()
        
        # Test data with nulls
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "department": "IT"},
            {"id": 2, "name": None, "age": 30, "salary": None, "department": "HR"},
            {"id": 3, "name": "Charlie", "age": None, "salary": 70000.0, "department": None},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0, "department": "Finance"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Null handling functions
        null_tests = [
            ("isnull", lambda: df.select(F.isnull(df.name))),
            ("isnotnull", lambda: df.select(~F.isnull(df.name))),
            ("coalesce", lambda: df.select(F.coalesce(df.salary, F.lit(0)))),
            ("when_otherwise", lambda: df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary))),
            ("nvl", lambda: df.select(F.coalesce(df.salary, F.lit(0)))),
            ("nvl2", lambda: df.select(F.when(df.salary.isNull(), 0).otherwise(df.salary * 1.1))),
            ("nullif", lambda: df.select(F.when(df.age == 30, None).otherwise(df.age))),
        ]
        
        for test_name, operation in null_tests:
            try:
                result_df = operation()
                self._save_expected_output("null_handling", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_set_operations_outputs(self):
        """Generate expected outputs for set operations."""
        self.start_spark_session()
        
        # Test data for set operations
        df1_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        
        df2_data = [
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
            {"id": 4, "name": "David", "age": 40},
        ]
        
        df1 = self.spark.createDataFrame(df1_data)
        df2 = self.spark.createDataFrame(df2_data)
        
        # Set operations
        set_tests = [
            ("union", lambda: df1.union(df2)),
            ("union_all", lambda: df1.unionAll(df2)),
            ("intersect", lambda: df1.intersect(df2)),
            ("except", lambda: df1.exceptAll(df2)),
            ("subtract", lambda: df1.subtract(df2)),
        ]
        
        for test_name, operation in set_tests:
            try:
                result_df = operation()
                self._save_expected_output("set_operations", test_name, df1_data + df2_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def _generate_chained_operations_outputs(self):
        """Generate expected outputs for chained operations."""
        self.start_spark_session()
        
        # Test data for complex scenarios
        test_data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "department": "IT", "hire_date": "2020-01-15"},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "department": "HR", "hire_date": "2019-03-10"},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "department": "IT", "hire_date": "2021-07-22"},
            {"id": 4, "name": "David", "age": 40, "salary": 80000.0, "department": "Finance", "hire_date": "2018-11-05"},
            {"id": 5, "name": "Eve", "age": 28, "salary": 55000.0, "department": "IT", "hire_date": "2022-02-14"},
        ]
        
        df = self.spark.createDataFrame(test_data)
        
        # Chained operations
        chained_tests = [
            ("filter_select_groupby_agg", lambda: df.filter(df.age > 30).select("name", "salary", "department").groupBy("department").agg(F.avg("salary").alias("avg_salary"))),
            ("withcolumn_filter_orderby", lambda: df.withColumn("bonus", df.salary * 0.1).filter((df.salary * 0.1) > 5000).orderBy((df.salary * 0.1).desc())),
            ("select_expr_groupby_agg_orderby", lambda: df.selectExpr("name", "salary", "department", "CASE WHEN age > 30 THEN 'Senior' ELSE 'Junior' END as level").groupBy("level").agg(F.count("name").alias("count"), F.avg("salary").alias("avg_salary")).orderBy("avg_salary")),
            ("complex_string_operations", lambda: df.select(F.upper(df.name).alias("name_upper"), F.concat(df.name, F.lit(" ("), df.department, F.lit(")")).alias("name_dept"), F.length(df.name).alias("name_length")).filter(F.length(df.name) > 4)),
        ]
        
        for test_name, operation in chained_tests:
            try:
                result_df = operation()
                self._save_expected_output("chained_operations", test_name, test_data, result_df)
                print(f"✓ Generated {test_name}")
            except Exception as e:
                print(f"✗ Failed {test_name}: {e}")

    def generate_joins(self):
        """Generate expected outputs for join operations."""
        self._generate_join_outputs()

    def generate_arrays(self):
        """Generate expected outputs for array operations."""
        self._generate_array_outputs()

    def generate_datetime(self):
        """Generate expected outputs for date/time operations."""
        self._generate_datetime_outputs()

    def generate_windows(self):
        """Generate expected outputs for window functions."""
        self._generate_window_outputs()

    def generate_null_handling(self):
        """Generate expected outputs for null handling operations."""
        self._generate_null_handling_outputs()

    def generate_set_operations(self):
        """Generate expected outputs for set operations."""
        self._generate_set_operations_outputs()

    def generate_chained_operations(self):
        """Generate expected outputs for chained operations."""
        self._generate_chained_operations_outputs()
    
    def _save_expected_output(
        self, 
        category: str, 
        test_name: str, 
        input_data: List[Dict[str, Any]], 
        result_df: Any,
        sql_query: Optional[str] = None
    ):
        """Save expected output to JSON file."""
        try:
            # Collect results
            rows = result_df.collect()
            schema = result_df.schema
            
            # Convert rows to dictionaries
            data = []
            for row in rows:
                if hasattr(row, "asDict"):
                    try:
                        data.append(row.asDict(recursive=True))
                    except TypeError:
                        data.append(row.asDict())
                else:
                    data.append(row.asDict() if hasattr(row, "asDict") else dict(row))
            
            # Convert schema to dictionary
            schema_dict = {
                "field_count": len(schema.fields),
                "field_names": [field.name for field in schema.fields],
                "field_types": [field.dataType.typeName() for field in schema.fields],
                "fields": [
                    {
                        "name": field.name,
                        "type": field.dataType.typeName(),
                        "nullable": field.nullable
                    }
                    for field in schema.fields
                ]
            }
            
            # Create output structure
            output = {
                "test_id": test_name,
                "pyspark_version": self.pyspark_version,
                "generated_at": datetime.now().isoformat(),
                "input_data": input_data,
                "operation": sql_query or f"DataFrame operation: {test_name}",
                "expected_output": {
                    "schema": schema_dict,
                    "data": data,
                    "row_count": len(data)
                }
            }
            
            # Save to file
            output_file = self.output_dir / category / f"{test_name}.json"
            with open(output_file, 'w') as f:
                json.dump(output, f, indent=2, default=str)
                
        except Exception as e:
            print(f"Error saving {test_name}: {e}")
    
    def generate_all(self):
        """Generate all expected outputs."""
        print("Generating expected outputs from PySpark...")
        print(f"PySpark version: {self.pyspark_version}")
        print(f"Output directory: {self.output_dir}")
        print()
        
        try:
            # Core operations
            self.generate_dataframe_operations()
            self.generate_functions()
            self.generate_window_operations()
            self.generate_sql_operations()
            
            # New comprehensive categories
            self.generate_joins()
            self.generate_arrays()
            self.generate_datetime()
            self.generate_windows()
            self.generate_null_handling()
            self.generate_set_operations()
            self.generate_chained_operations()
            
            # Save metadata
            categories = [
                "dataframe_operations", "functions", "window_operations", "sql_operations",
                "joins", "arrays", "datetime", "windows", "null_handling", 
                "set_operations", "chained_operations"
            ]
            
            metadata = {
                "generated_at": datetime.now().isoformat(),
                "pyspark_version": self.pyspark_version,
                "categories": categories,
                "total_tests": sum(len(list((self.output_dir / cat).glob("*.json"))) for cat in categories)
            }
            
            metadata_file = self.output_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            print(f"\n✓ Generated {metadata['total_tests']} expected outputs")
            print(f"✓ Metadata saved to {metadata_file}")
            
        finally:
            self.stop_spark_session()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate expected outputs from PySpark")
    parser.add_argument("--all", action="store_true", help="Generate all expected outputs")
    parser.add_argument("--category", help="Generate outputs for specific category")
    parser.add_argument("--pyspark-version", default="3.5", help="PySpark version to use")
    parser.add_argument("--output-dir", help="Output directory for expected outputs")
    
    args = parser.parse_args()
    
    if not PYSPARK_AVAILABLE:
        print("Error: PySpark not available. Install with: pip install pyspark")
        sys.exit(1)
    
    generator = ExpectedOutputGenerator(
        output_dir=args.output_dir,
        pyspark_version=args.pyspark_version
    )
    
    if args.all:
        generator.generate_all()
    elif args.category:
        # Generate specific category
        category_methods = {
            "dataframe_operations": generator.generate_dataframe_operations,
            "functions": generator.generate_functions,
            "window_operations": generator.generate_window_operations,
            "sql_operations": generator.generate_sql_operations,
            "joins": generator.generate_joins,
            "arrays": generator.generate_arrays,
            "datetime": generator.generate_datetime,
            "windows": generator.generate_windows,
            "null_handling": generator.generate_null_handling,
            "set_operations": generator.generate_set_operations,
            "chained_operations": generator.generate_chained_operations,
        }
        
        if args.category in category_methods:
            category_methods[args.category]()
        else:
            print(f"Unknown category: {args.category}")
            print(f"Available categories: {', '.join(category_methods.keys())}")
            sys.exit(1)
    else:
        print("Please specify --all or --category")
        print(f"Available categories: {', '.join(['dataframe_operations', 'functions', 'window_operations', 'sql_operations', 'joins', 'arrays', 'datetime', 'windows', 'null_handling', 'set_operations', 'chained_operations'])}")
        sys.exit(1)


if __name__ == "__main__":
    main()

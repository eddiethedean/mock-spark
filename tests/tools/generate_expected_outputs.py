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
        string_tests = [
            ("upper", lambda: df.select(F.upper(df.name))),
            ("lower", lambda: df.select(F.lower(df.name))),
            ("length", lambda: df.select(F.length(df.name))),
            ("substring", lambda: df.select(F.substring(df.name, 1, 3))),
            ("concat", lambda: df.select(F.concat(df.name, F.lit(" - "), df.email))),
            ("split", lambda: df.select(F.split(df.email, "@"))),
            ("regexp_extract", lambda: df.select(F.regexp_extract(df.email, r"@(.+)", 1))),
        ]
        
        for test_name, operation in string_tests:
            try:
                result_df = operation()
                self._save_expected_output("functions", f"string_{test_name}", test_data, result_df)
                print(f"✓ Generated string_{test_name}")
            except Exception as e:
                print(f"✗ Failed string_{test_name}: {e}")
        
        # Math functions
        math_tests = [
            ("abs", lambda: df.select(F.abs(df.salary))),
            ("round", lambda: df.select(F.round(df.salary, -3))),
            ("sqrt", lambda: df.select(F.sqrt(df.salary))),
            ("pow", lambda: df.select(F.pow(df.age, 2))),
            ("log", lambda: df.select(F.log(df.salary))),
            ("exp", lambda: df.select(F.exp(F.lit(1)))),
        ]
        
        for test_name, operation in math_tests:
            try:
                result_df = operation()
                self._save_expected_output("functions", f"math_{test_name}", test_data, result_df)
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
            self.generate_dataframe_operations()
            self.generate_functions()
            self.generate_window_operations()
            self.generate_sql_operations()
            
            # Save metadata
            metadata = {
                "generated_at": datetime.now().isoformat(),
                "pyspark_version": self.pyspark_version,
                "categories": [
                    "dataframe_operations",
                    "functions",
                    "window_operations", 
                    "sql_operations"
                ],
                "total_tests": sum(len(list((self.output_dir / cat).glob("*.json"))) for cat in [
                    "dataframe_operations", "functions", "window_operations", "sql_operations"
                ])
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
        if args.category == "dataframe_operations":
            generator.generate_dataframe_operations()
        elif args.category == "functions":
            generator.generate_functions()
        elif args.category == "window_operations":
            generator.generate_window_operations()
        elif args.category == "sql_operations":
            generator.generate_sql_operations()
        else:
            print(f"Unknown category: {args.category}")
            sys.exit(1)
    else:
        print("Please specify --all or --category")
        sys.exit(1)


if __name__ == "__main__":
    main()

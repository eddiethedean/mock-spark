"""
Test factories for Mock Spark.

This module provides factory patterns for creating complex test scenarios
and comprehensive test data with various configurations.
"""

from typing import Dict, List, Any
from mock_spark import MockSparkSession
from mock_spark.core.interfaces.dataframe import IDataFrame
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from .generators import DataGenerator, RealisticDataGenerator, EdgeCaseDataGenerator


class DataFrameTestFactory:
    """Factory for creating comprehensive DataFrame test scenarios."""

    @staticmethod
    def create_performance_test_dataframe(
        session: MockSparkSession, num_rows: int = 10000, num_columns: int = 10
    ) -> IDataFrame:
        """Create a large DataFrame for performance testing."""
        data = []
        for i in range(num_rows):
            row = {"id": i}
            for j in range(num_columns - 1):  # -1 because id is already added
                row[f"col_{j}"] = DataGenerator.generate_string(10)
            data.append(row)

        # Create schema
        fields = [MockStructField("id", IntegerType(), True)]
        for j in range(num_columns - 1):
            fields.append(MockStructField(f"col_{j}", StringType(), True))

        schema = MockStructType(fields)
        return session.createDataFrame(data, schema)

    @staticmethod
    def create_stress_test_dataframe(session: MockSparkSession) -> IDataFrame:
        """Create a DataFrame with stress test data (edge cases, nulls, etc.)."""
        data = [
            # Normal data
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "active": True},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "active": False},
            # Edge cases
            {"id": 0, "name": "", "age": 0, "salary": 0.0, "active": False},  # Zero values
            {
                "id": -1,
                "name": "Negative",
                "age": -1,
                "salary": -1000.0,
                "active": True,
            },  # Negative values
            # Null values
            {"id": 3, "name": None, "age": 35, "salary": 70000.0, "active": True},
            {"id": 4, "name": "Charlie", "age": None, "salary": 80000.0, "active": False},
            {"id": 5, "name": "David", "age": 40, "salary": None, "active": None},
            # Large values
            {"id": 999999, "name": "x" * 1000, "age": 999, "salary": 999999.99, "active": True},
            # Unicode data
            {"id": 6, "name": "测试", "age": 28, "salary": 55000.0, "active": True},
            {"id": 7, "name": "café", "age": 32, "salary": 65000.0, "active": False},
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("salary", DoubleType(), True),
                MockStructField("active", BooleanType(), True),
            ]
        )

        return session.createDataFrame(data, schema)

    @staticmethod
    def create_join_test_dataframes(session: MockSparkSession) -> Dict[str, IDataFrame]:
        """Create DataFrames for testing join operations."""
        # Left DataFrame
        left_data = [
            {"id": 1, "name": "Alice", "dept_id": 10},
            {"id": 2, "name": "Bob", "dept_id": 20},
            {"id": 3, "name": "Charlie", "dept_id": 10},
            {"id": 4, "name": "David", "dept_id": 30},
        ]

        left_schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("dept_id", IntegerType(), True),
            ]
        )

        # Right DataFrame
        right_data = [
            {"dept_id": 10, "dept_name": "Engineering", "budget": 1000000.0},
            {"dept_id": 20, "dept_name": "Marketing", "budget": 500000.0},
            {"dept_id": 40, "dept_name": "Sales", "budget": 750000.0},  # No matching employee
        ]

        right_schema = MockStructType(
            [
                MockStructField("dept_id", IntegerType(), True),
                MockStructField("dept_name", StringType(), True),
                MockStructField("budget", DoubleType(), True),
            ]
        )

        return {
            "employees": session.createDataFrame(left_data, left_schema),
            "departments": session.createDataFrame(right_data, right_schema),
        }

    @staticmethod
    def create_window_test_dataframe(session: MockSparkSession) -> IDataFrame:
        """Create a DataFrame for testing window functions."""
        data = [
            {
                "id": 1,
                "name": "Alice",
                "department": "Engineering",
                "salary": 80000.0,
                "hire_date": "2020-01-15",
            },
            {
                "id": 2,
                "name": "Bob",
                "department": "Engineering",
                "salary": 90000.0,
                "hire_date": "2019-06-10",
            },
            {
                "id": 3,
                "name": "Charlie",
                "department": "Marketing",
                "salary": 70000.0,
                "hire_date": "2021-03-20",
            },
            {
                "id": 4,
                "name": "David",
                "department": "Engineering",
                "salary": 85000.0,
                "hire_date": "2020-11-05",
            },
            {
                "id": 5,
                "name": "Eve",
                "department": "Marketing",
                "salary": 75000.0,
                "hire_date": "2021-08-12",
            },
            {
                "id": 6,
                "name": "Frank",
                "department": "Sales",
                "salary": 65000.0,
                "hire_date": "2022-01-30",
            },
        ]

        schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("name", StringType(), True),
                MockStructField("department", StringType(), True),
                MockStructField("salary", DoubleType(), True),
                MockStructField("hire_date", StringType(), True),
            ]
        )

        return session.createDataFrame(data, schema)


class SessionTestFactory:
    """Factory for creating comprehensive session test scenarios."""

    @staticmethod
    def create_session_with_tables(
        session: MockSparkSession, table_configs: Dict[str, Dict[str, Any]]
    ) -> MockSparkSession:
        """Create a session with pre-populated tables."""
        for table_name, config in table_configs.items():
            data = config.get("data", [])
            schema = config.get("schema")

            if schema is None and data:
                # Auto-generate schema from data
                first_row = data[0]
                fields = []
                for key, value in first_row.items():
                    if isinstance(value, int):
                        field_type = IntegerType()
                    elif isinstance(value, float):
                        field_type = DoubleType()
                    elif isinstance(value, bool):
                        field_type = BooleanType()
                    else:
                        field_type = StringType()
                    fields.append(MockStructField(key, field_type, True))
                schema = MockStructType(fields)

            if schema:
                df = session.createDataFrame(data, schema)
                df.createOrReplaceTempView(table_name)

        return session

    @staticmethod
    def create_session_with_configs(
        session: MockSparkSession, configs: Dict[str, str]
    ) -> MockSparkSession:
        """Create a session with specific configurations."""
        for key, value in configs.items():
            session.conf.set(key, value)
        return session

    @staticmethod
    def create_session_with_storage_data(
        session: MockSparkSession, storage_data: Dict[str, List[Dict[str, Any]]]
    ) -> MockSparkSession:
        """Create a session with data in storage."""
        for table_name, data in storage_data.items():
            if data:
                # Create schema from first row
                first_row = data[0]
                fields = []
                for key, value in first_row.items():
                    if isinstance(value, int):
                        field_type = IntegerType()
                    elif isinstance(value, float):
                        field_type = DoubleType()
                    elif isinstance(value, bool):
                        field_type = BooleanType()
                    else:
                        field_type = StringType()
                    fields.append(MockStructField(key, field_type, True))

                schema = MockStructType(fields)
                session.storage.create_table("default", table_name, schema)
                session.storage.insert_data("default", table_name, data)

        return session


class FunctionTestFactory:
    """Factory for creating comprehensive function test scenarios."""

    @staticmethod
    def create_arithmetic_test_expressions() -> List[Dict[str, Any]]:
        """Create arithmetic expression test cases."""
        return [
            {"left": 10, "operator": "+", "right": 5, "expected": 15},
            {"left": 10, "operator": "-", "right": 3, "expected": 7},
            {"left": 4, "operator": "*", "right": 6, "expected": 24},
            {"left": 15, "operator": "/", "right": 3, "expected": 5.0},
            {"left": 17, "operator": "%", "right": 5, "expected": 2},
        ]

    @staticmethod
    def create_comparison_test_expressions() -> List[Dict[str, Any]]:
        """Create comparison expression test cases."""
        return [
            {"left": 10, "operator": "==", "right": 10, "expected": True},
            {"left": 10, "operator": "!=", "right": 5, "expected": True},
            {"left": 10, "operator": ">", "right": 5, "expected": True},
            {"left": 10, "operator": ">=", "right": 10, "expected": True},
            {"left": 5, "operator": "<", "right": 10, "expected": True},
            {"left": 5, "operator": "<=", "right": 5, "expected": True},
        ]

    @staticmethod
    def create_string_function_test_cases() -> List[Dict[str, Any]]:
        """Create string function test cases."""
        return [
            {"function": "upper", "input": "hello", "expected": "HELLO"},
            {"function": "lower", "input": "WORLD", "expected": "world"},
            {"function": "length", "input": "test", "expected": 4},
            {"function": "trim", "input": "  spaces  ", "expected": "spaces"},
        ]

    @staticmethod
    def create_aggregate_function_test_cases() -> List[Dict[str, Any]]:
        """Create aggregate function test cases."""
        return [
            {"function": "count", "data": [1, 2, 3, 4, 5], "expected": 5},
            {"function": "sum", "data": [1, 2, 3, 4, 5], "expected": 15},
            {"function": "avg", "data": [1, 2, 3, 4, 5], "expected": 3.0},
            {"function": "max", "data": [1, 2, 3, 4, 5], "expected": 5},
            {"function": "min", "data": [1, 2, 3, 4, 5], "expected": 1},
        ]


class IntegrationTestFactory:
    """Factory for creating integration test scenarios."""

    @staticmethod
    def create_e2e_data_pipeline_test(session: MockSparkSession) -> Dict[str, Any]:
        """Create an end-to-end data pipeline test scenario."""
        # Source data
        source_data = RealisticDataGenerator.generate_person_data(100)
        source_schema = MockStructType(
            [
                MockStructField("id", IntegerType(), True),
                MockStructField("first_name", StringType(), True),
                MockStructField("last_name", StringType(), True),
                MockStructField("age", IntegerType(), True),
                MockStructField("department", StringType(), True),
                MockStructField("salary", DoubleType(), True),
                MockStructField("active", BooleanType(), True),
                MockStructField("hire_date", StringType(), True),
            ]
        )

        source_df = session.createDataFrame(source_data, source_schema)
        source_df.createOrReplaceTempView("employees")

        # Expected transformations
        transformations = [
            "SELECT * FROM employees WHERE active = true",
            "SELECT department, COUNT(*) as employee_count FROM employees GROUP BY department",
            "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department",
            "SELECT *, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as salary_rank FROM employees",
        ]

        return {
            "source_data": source_df,
            "transformations": transformations,
            "expected_tables": ["employees"],
        }

    @staticmethod
    def create_performance_benchmark_test(session: MockSparkSession) -> Dict[str, Any]:
        """Create a performance benchmark test scenario."""
        # Large dataset
        large_df = DataFrameTestFactory.create_performance_test_dataframe(session, 50000, 20)

        # Performance test operations
        operations = [
            lambda df: df.select("id", "col_0", "col_1"),
            lambda df: df.filter(df["id"] > 25000),
            lambda df: df.groupBy("col_0").count(),
            lambda df: df.orderBy("id"),
            lambda df: df.limit(1000),
        ]

        return {
            "test_dataframe": large_df,
            "operations": operations,
            "expected_performance": {"max_duration_seconds": 5.0, "max_memory_mb": 100.0},
        }

    @staticmethod
    def create_error_handling_test_scenarios() -> List[Dict[str, Any]]:
        """Create error handling test scenarios."""
        return [
            {
                "name": "column_not_found",
                "operation": lambda df: df.select("nonexistent_column"),
                "expected_error": "AnalysisException",
            },
            {
                "name": "invalid_arithmetic",
                "operation": lambda df: df.select(df["id"] + "string"),
                "expected_error": "AnalysisException",
            },
            {
                "name": "division_by_zero",
                "operation": lambda df: df.select(df["id"] / 0),
                "expected_error": "QueryExecutionException",
            },
            {
                "name": "invalid_filter",
                "operation": lambda df: df.filter("invalid_sql"),
                "expected_error": "AnalysisException",
            },
        ]


# Convenience functions for easy use
def create_comprehensive_test_session() -> MockSparkSession:
    """Create a session with comprehensive test data."""
    session = MockSessionFactory.create_default_session()

    # Add various test tables
    table_configs = {
        "employees": {
            "data": RealisticDataGenerator.generate_person_data(50),
            "schema": None,  # Auto-generate
        },
        "departments": {
            "data": [
                {"id": 1, "name": "Engineering", "budget": 1000000.0},
                {"id": 2, "name": "Marketing", "budget": 500000.0},
                {"id": 3, "name": "Sales", "budget": 750000.0},
            ],
            "schema": None,
        },
        "stress_test": {"data": EdgeCaseDataGenerator.generate_boundary_values(), "schema": None},
    }

    return SessionTestFactory.create_session_with_tables(session, table_configs)


def create_benchmark_test_suite(session: MockSparkSession) -> Dict[str, Any]:
    """Create a comprehensive benchmark test suite."""
    return {
        "performance_test": IntegrationTestFactory.create_performance_benchmark_test(session),
        "e2e_pipeline": IntegrationTestFactory.create_e2e_data_pipeline_test(session),
        "error_scenarios": IntegrationTestFactory.create_error_handling_test_scenarios(),
        "function_tests": {
            "arithmetic": FunctionTestFactory.create_arithmetic_test_expressions(),
            "comparison": FunctionTestFactory.create_comparison_test_expressions(),
            "string": FunctionTestFactory.create_string_function_test_cases(),
            "aggregate": FunctionTestFactory.create_aggregate_function_test_cases(),
        },
    }

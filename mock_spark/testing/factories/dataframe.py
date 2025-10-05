"""
DataFrame test factory for Mock Spark.

This module provides the DataFrameTestFactory class for creating
comprehensive DataFrame test scenarios and data.
"""

from typing import Dict
from ...core.interfaces.dataframe import IDataFrame
from ...session import MockSparkSession
from ...spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from ..generators import DataGenerator


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
            {
                "id": 0,
                "name": "",
                "age": 0,
                "salary": 0.0,
                "active": False,
            },  # Zero values
            {
                "id": -1,
                "name": "Negative",
                "age": -1,
                "salary": -1000.0,
                "active": True,
            },  # Negative values
            # Null values
            {"id": 3, "name": None, "age": 35, "salary": 70000.0, "active": True},
            {
                "id": 4,
                "name": "Charlie",
                "age": None,
                "salary": 80000.0,
                "active": False,
            },
            {"id": 5, "name": "David", "age": 40, "salary": None, "active": None},
            # Large values
            {
                "id": 999999,
                "name": "x" * 1000,
                "age": 999,
                "salary": 999999.99,
                "active": True,
            },
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
            {
                "dept_id": 40,
                "dept_name": "Sales",
                "budget": 750000.0,
            },  # No matching employee
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

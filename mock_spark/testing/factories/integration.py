"""
Integration test factory for Mock Spark.

This module provides the IntegrationTestFactory class for creating
comprehensive integration test scenarios and end-to-end tests.
"""

from typing import Dict, Any, List
from ...session import MockSparkSession
from ...spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
)
from .dataframe import DataFrameTestFactory
from ..generators import RealisticDataGenerator


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
        source_df.createGlobalTempView("employees")

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
            "expected_performance": {
                "max_duration_seconds": 5.0,
                "max_memory_mb": 100.0,
            },
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

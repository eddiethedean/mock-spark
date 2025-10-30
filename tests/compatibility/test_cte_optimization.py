"""
Compatibility tests for CTE optimization.

This module tests that complex CTE chains with multiple operations work correctly
without falling back to table-per-operation, especially with datetime parsing.
"""

import pytest
import warnings
from mock_spark import MockSparkSession, F


@pytest.mark.compatibility
class TestCTEOptimizationCompatibility:
    """Test CTE optimization compatibility."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        session = MockSparkSession("cte_optimization_test")
        yield session
        session.stop()

    def test_complex_cte_chain_with_datetime(self, spark):
        """Test complex CTE chain with datetime parsing operations."""
        test_data = [
            {"id": 1, "timestamp_str": "2025-10-29T10:30:45.123456", "value": 100},
            {"id": 2, "timestamp_str": "2025-10-29T14:20:30", "value": 200},
        ]

        df = spark.createDataFrame(test_data)

        # Complex chain: filter -> withColumn (datetime) -> withColumn (calculation) -> select
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # Fail on CTE fallback warnings
            try:
                result = (
                    df.filter(F.col("value") > 50)
                    .withColumn(
                        "parsed_timestamp",
                        F.to_timestamp(
                            F.col("timestamp_str"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                        ),
                    )
                    .withColumn("doubled", F.col("value") * 2)
                    .select("id", "parsed_timestamp", "doubled")
                )

                rows = result.collect()
                assert len(rows) == 2
                # Verify results are correct
                assert all(row.doubled is not None for row in rows)
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_multiple_withcolumn_operations_in_cte(self, spark):
        """Test multiple withColumn operations in CTE chain."""
        test_data = [
            {"name": "Alice", "age": 25, "salary": 50000},
            {"name": "Bob", "age": 30, "salary": 60000},
        ]

        df = spark.createDataFrame(test_data)

        # Chain of 5+ operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn("age_plus_1", F.col("age") + 1)
                    .withColumn("age_plus_2", F.col("age_plus_1") + 1)
                    .withColumn("bonus", F.col("salary") * 0.1)
                    .withColumn("total", F.col("salary") + F.col("bonus"))
                    .select("name", "age_plus_2", "total")
                )

                rows = result.collect()
                assert len(rows) == 2
                assert rows[0].age_plus_2 == 27
                assert rows[0].total == 55000.0
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_with_string_operations(self, spark):
        """Test CTE chain with string concatenation operations."""
        test_data = [
            {"first": "John", "last": "Doe"},
            {"first": "Jane", "last": "Smith"},
        ]

        df = spark.createDataFrame(test_data)

        # Chain with string operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn(
                        "full_name", F.concat_ws(" ", F.col("first"), F.col("last"))
                    )
                    .withColumn("greeting", F.lit("Hello ") + F.col("full_name"))
                    .withColumn("upper_greeting", F.upper(F.col("greeting")))
                    .select("full_name", "upper_greeting")
                )

                rows = result.collect()
                assert len(rows) == 2
                assert rows[0].full_name == "John Doe"
                assert rows[0].upper_greeting == "HELLO JOHN DOE"
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

    def test_cte_with_boolean_expressions(self, spark):
        """Test CTE chain with boolean expressions."""
        test_data = [
            {"value": 25, "min": 10, "max": 50},
            {"value": 5, "min": 10, "max": 50},
        ]

        df = spark.createDataFrame(test_data)

        # Chain with boolean operations
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            try:
                result = (
                    df.withColumn(
                        "is_valid",
                        (F.col("value") >= F.col("min"))
                        & (F.col("value") <= F.col("max")),
                    )
                    .withColumn(
                        "is_extreme",
                        (F.col("value") < F.col("min"))
                        | (F.col("value") > F.col("max")),
                    )
                    .filter(F.col("is_valid") == F.lit(True))
                    .select("value", "is_valid")
                )

                rows = result.collect()
                assert len(rows) == 1  # Only valid value should pass filter
                assert rows[0].is_valid is True
            except UserWarning as e:
                if "CTE optimization failed" in str(e):
                    pytest.fail(f"CTE optimization should succeed but fell back: {e}")
                raise

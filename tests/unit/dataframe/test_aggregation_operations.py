"""
Unit tests for aggregation operations.
"""

import pytest
from sparkless.dataframe.operations.aggregation_operations import (
    AggregationOperationsStatic,
)
from sparkless.spark_types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
)
from sparkless.core.exceptions.analysis import ColumnNotFoundException


@pytest.mark.unit
class TestAggregationOperations:
    """Test AggregationOperationsStatic methods."""

    def test_describe_all_numeric_columns(self):
        """Test describe with no columns specified describes all numeric columns."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("score", DoubleType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        data = [
            {"id": 1, "name": "Alice", "score": 85.5, "age": 25},
            {"id": 2, "name": "Bob", "score": 90.0, "age": 30},
            {"id": 3, "name": "Charlie", "score": 75.5, "age": 28},
        ]

        result_data, result_schema = AggregationOperationsStatic.describe(
            data, schema, []
        )

        assert len(result_data) == 3  # id, score, age
        assert all("summary" in row for row in result_data)
        assert any(row["summary"] == "id" for row in result_data)
        assert any(row["summary"] == "score" for row in result_data)
        assert any(row["summary"] == "age" for row in result_data)

    def test_describe_specific_columns(self):
        """Test describe with specific columns."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"id": 1, "score": 85.5},
            {"id": 2, "score": 90.0},
            {"id": 3, "score": 75.5},
        ]

        result_data, result_schema = AggregationOperationsStatic.describe(
            data, schema, ["score"]
        )

        assert len(result_data) == 1
        assert result_data[0]["summary"] == "score"
        assert result_data[0]["count"] == "3"

    def test_describe_no_numeric_columns(self):
        """Test describe when no numeric columns exist."""
        schema = StructType(
            [
                StructField("name", StringType(), True),
            ]
        )
        data = [{"name": "Alice"}, {"name": "Bob"}]

        result_data, result_schema = AggregationOperationsStatic.describe(
            data, schema, []
        )

        assert result_data == []
        assert result_schema == schema

    def test_describe_column_not_found(self):
        """Test describe raises error for non-existent column."""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
            ]
        )
        data = [{"id": 1}]

        with pytest.raises(ColumnNotFoundException):
            AggregationOperationsStatic.describe(data, schema, ["nonexistent"])

    def test_describe_with_nulls(self):
        """Test describe handles null values correctly."""
        schema = StructType(
            [
                StructField("score", DoubleType(), True),
            ]
        )
        data = [
            {"score": 85.5},
            {"score": None},
            {"score": 90.0},
        ]

        result_data, _ = AggregationOperationsStatic.describe(data, schema, ["score"])

        assert len(result_data) == 1
        assert result_data[0]["count"] == "2"  # Only non-null values counted

    def test_describe_empty_data(self):
        """Test describe with empty data."""
        schema = StructType(
            [
                StructField("score", DoubleType(), True),
            ]
        )
        data = []

        result_data, _ = AggregationOperationsStatic.describe(data, schema, ["score"])

        assert len(result_data) == 1
        assert result_data[0]["count"] == "0"
        assert result_data[0]["mean"] == "NaN"

    def test_describe_statistics_calculation(self):
        """Test that describe calculates correct statistics."""
        schema = StructType(
            [
                StructField("value", IntegerType(), False),
            ]
        )
        data = [
            {"value": 1},
            {"value": 2},
            {"value": 3},
            {"value": 4},
            {"value": 5},
        ]

        result_data, _ = AggregationOperationsStatic.describe(data, schema, ["value"])

        assert len(result_data) == 1
        assert result_data[0]["count"] == "5"
        assert result_data[0]["min"] == "1"
        assert result_data[0]["max"] == "5"
        # Mean should be 3.0
        assert float(result_data[0]["mean"]) == 3.0

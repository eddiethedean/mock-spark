"""Tests for AggregationOperations class."""

import pytest
from mock_spark.dataframe.operations.aggregation_operations import AggregationOperations
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
)
from mock_spark import MockSparkSession


class TestAggregationOperations:
    """Test cases for AggregationOperations."""

    def setup_method(self):
        """Set up test data."""
        self.spark = MockSparkSession("test")

        # Create test DataFrame with mixed data types
        self.data = [
            {"id": 1, "name": "Alice", "age": 25, "salary": 50000.0, "bonus": 5000},
            {"id": 2, "name": "Bob", "age": 30, "salary": 60000.0, "bonus": 6000},
            {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0, "bonus": 7000},
            {"id": 4, "name": "David", "age": 28, "salary": 55000.0, "bonus": 5500},
            {"id": 5, "name": "Eve", "age": 32, "salary": 65000.0, "bonus": 6500},
        ]
        self.schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("bonus", IntegerType()),
            ]
        )
        self.df = self.spark.createDataFrame(self.data, self.schema)

    def test_describe_all_numeric_columns(self):
        """Test describe with all numeric columns."""
        result_data, result_schema = AggregationOperations.describe(
            self.data, self.schema, []
        )

        # Should have 4 numeric columns (id, age, salary, bonus)
        assert len(result_data) == 4

        # Check schema
        assert len(result_schema.fields) == 6
        field_names = [field.name for field in result_schema.fields]
        expected_fields = ["summary", "count", "mean", "stddev", "min", "max"]
        assert set(field_names) == set(expected_fields)

        # Check that all numeric columns are included
        summary_values = [row["summary"] for row in result_data]
        expected_columns = ["id", "age", "salary", "bonus"]
        assert set(summary_values) == set(expected_columns)

    def test_describe_specific_columns(self):
        """Test describe with specific columns."""
        result_data, result_schema = AggregationOperations.describe(
            self.data, self.schema, ["age", "salary"]
        )

        # Should have 2 columns
        assert len(result_data) == 2

        # Check that only specified columns are included
        summary_values = [row["summary"] for row in result_data]
        assert set(summary_values) == {"age", "salary"}

    def test_describe_with_no_numeric_columns(self):
        """Test describe with no numeric columns."""
        # Create DataFrame with only string columns
        string_data = [{"name": "Alice"}, {"name": "Bob"}]
        string_schema = MockStructType([MockStructField("name", StringType())])

        result_data, result_schema = AggregationOperations.describe(
            string_data, string_schema, []
        )

        # Should return empty result
        assert len(result_data) == 0

    def test_describe_with_empty_data(self):
        """Test describe with empty data."""
        result_data, result_schema = AggregationOperations.describe(
            [], self.schema, ["age"]
        )

        # Should have 1 row with NaN values
        assert len(result_data) == 1
        row = result_data[0]
        assert row["summary"] == "age"
        assert row["count"] == "0"
        assert row["mean"] == "NaN"
        assert row["stddev"] == "NaN"
        assert row["min"] == "NaN"
        assert row["max"] == "NaN"

    def test_summary_default_stats(self):
        """Test summary with default statistics."""
        result_data, result_schema = AggregationOperations.summary(
            self.data, self.schema, []
        )

        # Should have 4 numeric columns
        assert len(result_data) == 4

        # Check schema includes default stats
        field_names = [field.name for field in result_schema.fields]
        expected_fields = [
            "summary",
            "count",
            "mean",
            "stddev",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
        ]
        assert set(field_names) == set(expected_fields)

    def test_summary_custom_stats(self):
        """Test summary with custom statistics."""
        custom_stats = ["count", "mean", "min", "max"]
        result_data, result_schema = AggregationOperations.summary(
            self.data, self.schema, custom_stats
        )

        # Should have 4 numeric columns
        assert len(result_data) == 4

        # Check schema includes only custom stats
        field_names = [field.name for field in result_schema.fields]
        expected_fields = ["summary"] + custom_stats
        assert set(field_names) == set(expected_fields)

    def test_compute_basic_stats(self):
        """Test compute_basic_stats method."""
        values = [1, 2, 3, 4, 5]
        stats = AggregationOperations.compute_basic_stats(values)

        assert stats["count"] == "5"
        assert stats["mean"] == "3"
        assert stats["min"] == "1"
        assert stats["max"] == "5"
        assert stats["stddev"] == "1.5811"  # Approximate

    def test_compute_basic_stats_empty(self):
        """Test compute_basic_stats with empty values."""
        stats = AggregationOperations.compute_basic_stats([])

        assert stats["count"] == "0"
        assert stats["mean"] == "NaN"
        assert stats["stddev"] == "NaN"
        assert stats["min"] == "NaN"
        assert stats["max"] == "NaN"

    def test_compute_extended_stats(self):
        """Test compute_extended_stats method."""
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        stats = ["count", "mean", "min", "max", "25%", "50%", "75%"]
        result = AggregationOperations.compute_extended_stats(values, stats)

        assert result["count"] == "10"
        assert result["mean"] == "5.5"
        assert result["min"] == "1"
        assert result["max"] == "10"
        assert result["50%"] == "5.5"  # Median

    def test_compute_extended_stats_empty(self):
        """Test compute_extended_stats with empty values."""
        stats = ["count", "mean", "min", "max"]
        result = AggregationOperations.compute_extended_stats([], stats)

        for stat in stats:
            assert result[stat] == "NaN"

    def test_get_numeric_columns(self):
        """Test get_numeric_columns method."""
        numeric_cols = AggregationOperations.get_numeric_columns(self.schema)

        expected_cols = ["id", "age", "salary", "bonus"]
        assert set(numeric_cols) == set(expected_cols)

    def test_get_numeric_columns_no_numeric(self):
        """Test get_numeric_columns with no numeric columns."""
        string_schema = MockStructType([MockStructField("name", StringType())])
        numeric_cols = AggregationOperations.get_numeric_columns(string_schema)

        assert len(numeric_cols) == 0

    def test_extract_numeric_values(self):
        """Test extract_numeric_values method."""
        values = AggregationOperations.extract_numeric_values(self.data, "age")

        expected_values = [25, 30, 35, 28, 32]
        assert values == expected_values

    def test_extract_numeric_values_with_nulls(self):
        """Test extract_numeric_values with null values."""
        data_with_nulls = [
            {"age": 25},
            {"age": None},
            {"age": 30},
            {"age": "invalid"},  # Non-numeric string
        ]

        values = AggregationOperations.extract_numeric_values(data_with_nulls, "age")

        expected_values = [25, 30]
        assert values == expected_values

    def test_describe_with_mixed_data_types(self):
        """Test describe with mixed data types including non-numeric values."""
        mixed_data = [
            {"age": 25, "score": 85.5},
            {"age": None, "score": 90.0},
            {"age": 30, "score": "invalid"},  # Non-numeric
            {"age": 35, "score": 95.5},
        ]
        mixed_schema = MockStructType(
            [
                MockStructField("age", IntegerType()),
                MockStructField("score", DoubleType()),
            ]
        )

        result_data, result_schema = AggregationOperations.describe(
            mixed_data, mixed_schema, ["age", "score"]
        )

        # Should have 2 rows
        assert len(result_data) == 2

        # Check age statistics (should handle None values)
        age_row = next(row for row in result_data if row["summary"] == "age")
        assert age_row["count"] == "3"  # Only 3 valid values

        # Check score statistics (should handle non-numeric values)
        score_row = next(row for row in result_data if row["summary"] == "score")
        assert score_row["count"] == "3"  # Only 3 valid values

    def test_summary_with_single_value(self):
        """Test summary with single value (edge case for stddev)."""
        single_value_data = [{"age": 25}]
        single_value_schema = MockStructType([MockStructField("age", IntegerType())])

        result_data, result_schema = AggregationOperations.summary(
            single_value_data, single_value_schema, ["count", "mean", "stddev"]
        )

        assert len(result_data) == 1
        row = result_data[0]
        assert row["count"] == "1"
        assert row["mean"] == "25"
        assert row["stddev"] == "0.0"  # Standard deviation of single value is 0

    def test_describe_column_not_found(self):
        """Test describe with non-existent column."""
        with pytest.raises(Exception):  # Should raise ColumnNotFoundException
            AggregationOperations.describe(
                self.data, self.schema, ["nonexistent_column"]
            )

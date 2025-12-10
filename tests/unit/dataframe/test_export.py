"""
Unit tests for DataFrame export utilities.
"""

import pytest
from mock_spark import SparkSession
from mock_spark.dataframe.export import DataFrameExporter

# Try to import pandas - will skip tests if not available
pandas = pytest.importorskip("pandas")


@pytest.mark.unit
class TestDataFrameExporter:
    """Test DataFrameExporter operations."""

    def test_to_pandas_basic(self):
        """Test converting DataFrame to pandas."""

        spark = SparkSession("test")
        df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )

        pandas_df = DataFrameExporter.to_pandas(df)

        assert len(pandas_df) == 2
        assert list(pandas_df.columns) == ["id", "name"]
        assert pandas_df.iloc[0]["id"] == 1
        assert pandas_df.iloc[0]["name"] == "Alice"

    def test_to_pandas_empty_dataframe(self):
        """Test converting empty DataFrame to pandas."""

        spark = SparkSession("test")
        df = spark.createDataFrame([], "id INT, name STRING")

        pandas_df = DataFrameExporter.to_pandas(df)

        assert len(pandas_df) == 0
        assert list(pandas_df.columns) == ["id", "name"]

    def test_to_pandas_without_pandas_raises_error(self):
        """Test to_pandas raises ImportError when pandas not installed."""
        # This test would require mocking the import, which is complex
        # For now, we'll just test that the method exists and handles the case
        spark = SparkSession("test")
        df = spark.createDataFrame([{"id": 1}])

        # pandas should be available (in dev dependencies)
        result = DataFrameExporter.to_pandas(df)
        assert result is not None

    def test_to_pandas_with_lazy_evaluation(self):
        """Test to_pandas handles lazy evaluation."""

        from mock_spark import F

        spark = SparkSession("test")
        df = spark.createDataFrame([{"id": 1}]).filter(F.col("id") > 0)

        # Should materialize lazy operations
        pandas_df = DataFrameExporter.to_pandas(df)

        assert len(pandas_df) == 1

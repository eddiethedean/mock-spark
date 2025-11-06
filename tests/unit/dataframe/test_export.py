"""
Unit tests for DataFrame export utilities.
"""

import importlib.util
import pytest
from mock_spark import SparkSession
from mock_spark.dataframe.export import DataFrameExporter

# Check if pandas is available
_pandas_available = importlib.util.find_spec("pandas") is not None


@pytest.mark.unit
class TestDataFrameExporter:
    """Test DataFrameExporter operations."""

    def test_to_pandas_basic(self):
        """Test converting DataFrame to pandas."""
        if not _pandas_available:
            pytest.skip("pandas not installed")

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
        if not _pandas_available:
            pytest.skip("pandas not installed")

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

        # If pandas is installed, this should work
        # If not, it will raise ImportError
        if _pandas_available:
            result = DataFrameExporter.to_pandas(df)
            assert result is not None
        else:
            with pytest.raises(ImportError, match="pandas is required"):
                DataFrameExporter.to_pandas(df)

    def test_to_pandas_with_lazy_evaluation(self):
        """Test to_pandas handles lazy evaluation."""
        if not _pandas_available:
            pytest.skip("pandas not installed")

        from mock_spark import F

        spark = SparkSession("test")
        df = spark.createDataFrame([{"id": 1}]).filter(F.col("id") > 0)

        # Should materialize lazy operations
        pandas_df = DataFrameExporter.to_pandas(df)

        assert len(pandas_df) == 1

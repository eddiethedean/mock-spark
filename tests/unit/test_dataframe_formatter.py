"""Tests for DataFrameFormatter class."""

from mock_spark.dataframe.display.formatter import DataFrameFormatter
from mock_spark.spark_types import MockRow


class TestDataFrameFormatter:
    """Test cases for DataFrameFormatter."""

    def test_format_row(self):
        """Test formatting a single row."""
        row = MockRow({"name": "John", "age": 30, "city": "New York"})
        columns = ["name", "age", "city"]
        result = DataFrameFormatter.format_row(row, columns)
        assert result == '["John", 30, "New York"]'

    def test_format_row_with_null(self):
        """Test formatting a row with null values."""
        row = MockRow({"name": "John", "age": None, "city": "New York"})
        columns = ["name", "age", "city"]
        result = DataFrameFormatter.format_row(row, columns)
        assert result == '["John", null, "New York"]'

    def test_format_rows(self):
        """Test formatting multiple rows."""
        rows = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Jane", "age": 25}),
        ]
        columns = ["name", "age"]
        result = DataFrameFormatter.format_rows(rows, columns)
        assert result == ['["John", 30]', '["Jane", 25]']

    def test_format_rows_with_limit(self):
        """Test formatting rows with a limit."""
        rows = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Jane", "age": 25}),
            MockRow({"name": "Bob", "age": 35}),
        ]
        columns = ["name", "age"]
        result = DataFrameFormatter.format_rows(rows, columns, limit=2)
        assert len(result) == 2
        assert result == ['["John", 30]', '["Jane", 25]']

    def test_format_schema(self):
        """Test formatting schema information."""
        columns = ["name", "age", "city"]
        types = ["string", "long", "string"]
        result = DataFrameFormatter.format_schema(columns, types)
        expected = "root\n name: string\n age: long\n city: string"
        assert result == expected

    def test_truncate_string(self):
        """Test string truncation."""
        long_string = "This is a very long string that should be truncated"
        result = DataFrameFormatter.truncate_string(long_string, max_length=20)
        assert result == "This is a very lo..."
        assert len(result) == 20

    def test_truncate_string_no_truncation_needed(self):
        """Test string truncation when not needed."""
        short_string = "Short"
        result = DataFrameFormatter.truncate_string(short_string, max_length=20)
        assert result == "Short"

    def test_format_value_for_display(self):
        """Test formatting values for display."""
        assert DataFrameFormatter.format_value_for_display(None) == "null"
        assert DataFrameFormatter.format_value_for_display("Hello") == "Hello"
        assert DataFrameFormatter.format_value_for_display(42) == "42"
        assert DataFrameFormatter.format_value_for_display(True) == "True"

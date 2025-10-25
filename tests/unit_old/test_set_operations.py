"""Tests for SetOperations class."""

from mock_spark.dataframe.operations.set_operations import SetOperations
from mock_spark.spark_types import MockRow


class TestSetOperations:
    """Test cases for SetOperations."""

    def test_distinct_rows(self):
        """Test removing duplicate rows."""
        rows = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Jane", "age": 25}),
            MockRow({"name": "John", "age": 30}),  # Duplicate
            MockRow({"name": "Bob", "age": 35}),
        ]
        result = SetOperations.distinct_rows(rows)
        assert len(result) == 3
        assert result[0].name == "John"
        assert result[1].name == "Jane"
        assert result[2].name == "Bob"

    def test_union_rows(self):
        """Test union of two row lists."""
        rows1 = [MockRow({"name": "John", "age": 30})]
        rows2 = [MockRow({"name": "Jane", "age": 25})]
        result = SetOperations.union_rows(rows1, rows2)
        assert len(result) == 2
        assert result[0].name == "John"
        assert result[1].name == "Jane"

    def test_intersect_rows(self):
        """Test intersection of two row lists."""
        rows1 = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Jane", "age": 25}),
        ]
        rows2 = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Bob", "age": 35}),
        ]
        result = SetOperations.intersect_rows(rows1, rows2)
        assert len(result) == 1
        assert result[0].name == "John"
        assert result[0].age == 30

    def test_except_rows(self):
        """Test finding rows in first list that are not in second list."""
        rows1 = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Jane", "age": 25}),
        ]
        rows2 = [
            MockRow({"name": "John", "age": 30}),
            MockRow({"name": "Bob", "age": 35}),
        ]
        result = SetOperations.except_rows(rows1, rows2)
        assert len(result) == 1
        assert result[0].name == "Jane"
        assert result[0].age == 25

    def test_rows_equal(self):
        """Test checking if two rows are equal."""
        row1 = MockRow({"name": "John", "age": 30})
        row2 = MockRow({"name": "John", "age": 30})
        row3 = MockRow({"name": "Jane", "age": 25})

        assert SetOperations.rows_equal(row1, row2) is True
        assert SetOperations.rows_equal(row1, row3) is False

    def test_rows_equal_different_columns(self):
        """Test checking equality of rows with different columns."""
        row1 = MockRow({"name": "John", "age": 30})
        row2 = MockRow({"name": "John", "city": "NYC"})

        assert SetOperations.rows_equal(row1, row2) is False

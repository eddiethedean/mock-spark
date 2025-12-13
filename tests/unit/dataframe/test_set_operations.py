"""
Unit tests for set operations.
"""

import pytest
from sparkless.dataframe.operations.set_operations import SetOperations
from sparkless.spark_types import Row


@pytest.mark.unit
class TestSetOperations:
    """Test SetOperations methods."""

    def test_distinct_rows_removes_duplicates(self):
        """Test distinct_rows removes duplicate rows."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 2, "name": "Bob"})
        row3 = Row({"id": 1, "name": "Alice"})  # Duplicate

        result = SetOperations.distinct_rows([row1, row2, row3])

        assert len(result) == 2
        assert row1 in result
        assert row2 in result

    def test_distinct_rows_with_dict_values(self):
        """Test distinct_rows handles dict values correctly."""
        # Create rows with same dict content but different dict objects
        # The implementation converts dicts to tuples, so same content should be recognized
        metadata1 = {"key": "value"}
        metadata2 = {"key": "value"}  # Same content
        row1 = Row({"id": 1, "metadata": metadata1})
        row2 = Row({"id": 1, "metadata": metadata2})

        result = SetOperations.distinct_rows([row1, row2])

        # Should recognize as duplicates based on content (converted to tuples)
        assert len(result) == 1

    def test_distinct_rows_with_list_values(self):
        """Test distinct_rows handles list values correctly."""
        # Create rows with same list content but different list objects
        # The implementation converts lists to tuples, so same content should be recognized
        tags1 = ["a", "b"]
        tags2 = ["a", "b"]  # Same content
        row1 = Row({"id": 1, "tags": tags1})
        row2 = Row({"id": 1, "tags": tags2})

        result = SetOperations.distinct_rows([row1, row2])

        # Should recognize as duplicates based on content (converted to tuples)
        assert len(result) == 1

    def test_union_rows_combines_lists(self):
        """Test union_rows combines two row lists."""
        rows1 = [Row({"id": 1}), Row({"id": 2})]
        rows2 = [Row({"id": 3}), Row({"id": 4})]

        result = SetOperations.union_rows(rows1, rows2)

        assert len(result) == 4
        assert all(r in result for r in rows1 + rows2)

    def test_union_combines_dataframes(self):
        """Test union combines two DataFrames."""
        from sparkless import SparkSession

        spark = SparkSession("test")
        df1 = spark.createDataFrame([{"id": 1}, {"id": 2}])
        df2 = spark.createDataFrame([{"id": 3}, {"id": 4}])

        result_data, result_schema = SetOperations.union(
            df1.data, df1.schema, df2.data, df2.schema, df1.storage
        )

        assert len(result_data) == 4
        # Convert Row objects to dicts for comparison
        ids = []
        for row in result_data:
            if isinstance(row, dict):
                ids.append(row.get("id"))
            elif hasattr(row, "data"):
                # Row object - access via data attribute
                if isinstance(row.data, dict):
                    ids.append(row.data.get("id"))
                else:
                    ids.append(getattr(row, "id", None))
            elif hasattr(row, "__dict__"):
                # Row object - access via __dict__
                row_dict = row.__dict__
                ids.append(row_dict.get("id"))
            else:
                # Try direct attribute access
                ids.append(getattr(row, "id", None))
        # Filter out None values and check
        ids = [i for i in ids if i is not None]
        assert set(ids) == {1, 2, 3, 4}

    def test_intersect_rows_finds_common_rows(self):
        """Test intersect_rows finds common rows."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 2, "name": "Bob"})
        row3 = Row({"id": 1, "name": "Alice"})

        rows1 = [row1, row2]
        rows2 = [row3]

        result = SetOperations.intersect_rows(rows1, rows2)

        assert len(result) == 1
        assert row1 in result

    def test_intersect_rows_no_common(self):
        """Test intersect_rows returns empty when no common rows."""
        rows1 = [Row({"id": 1})]
        rows2 = [Row({"id": 2})]

        result = SetOperations.intersect_rows(rows1, rows2)

        assert len(result) == 0

    def test_except_rows_finds_difference(self):
        """Test except_rows finds rows in first but not second."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 2, "name": "Bob"})
        row3 = Row({"id": 1, "name": "Alice"})

        rows1 = [row1, row2]
        rows2 = [row3]

        result = SetOperations.except_rows(rows1, rows2)

        assert len(result) == 1
        assert row2 in result
        assert row1 not in result

    def test_except_rows_all_removed(self):
        """Test except_rows when all rows are removed."""
        row1 = Row({"id": 1})
        rows1 = [row1]
        rows2 = [row1]

        result = SetOperations.except_rows(rows1, rows2)

        assert len(result) == 0

    def test_rows_equal_true(self):
        """Test rows_equal returns True for equal rows."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 1, "name": "Alice"})

        assert SetOperations.rows_equal(row1, row2) is True

    def test_rows_equal_false_different_values(self):
        """Test rows_equal returns False for different values."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 1, "name": "Bob"})

        assert SetOperations.rows_equal(row1, row2) is False

    def test_rows_equal_false_different_keys(self):
        """Test rows_equal returns False for different keys."""
        row1 = Row({"id": 1, "name": "Alice"})
        row2 = Row({"id": 1, "score": 85})

        assert SetOperations.rows_equal(row1, row2) is False

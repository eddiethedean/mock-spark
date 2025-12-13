"""
Unit tests for join operations.
"""

import pytest
from sparkless.dataframe.operations.join_operations import JoinOperationsStatic
from sparkless import SparkSession


@pytest.mark.unit
class TestJoinOperations:
    """Test JoinOperationsStatic methods."""

    def test_cross_join_basic(self):
        """Test basic cross join."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame([{"id": 1}, {"id": 2}])
        right_df = spark.createDataFrame([{"name": "a"}, {"name": "b"}])

        result_data, result_schema = JoinOperationsStatic.cross_join(left_df, right_df)

        assert len(result_data) == 4  # 2 x 2
        assert len(result_schema.fields) == 2
        assert any(f.name == "id" for f in result_schema.fields)
        assert any(f.name == "name" for f in result_schema.fields)

    def test_cross_join_with_name_conflict(self):
        """Test cross join handles column name conflicts."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame([{"id": 1, "value": "left"}])
        right_df = spark.createDataFrame([{"id": 2, "value": "right"}])

        result_data, result_schema = JoinOperationsStatic.cross_join(left_df, right_df)

        # Right column should be renamed
        field_names = [f.name for f in result_schema.fields]
        assert "id" in field_names
        assert "value" in field_names
        assert any("value_right" in name for name in field_names)

    def test_inner_join_basic(self):
        """Test basic inner join."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )
        right_df = spark.createDataFrame(
            [
                {"id": 1, "score": 85},
                {"id": 2, "score": 90},
            ]
        )

        result_data, result_schema = JoinOperationsStatic.inner_join(
            left_df, right_df, ["id"]
        )

        assert len(result_data) == 2
        assert all("name" in row for row in result_data)
        assert all("score" in row for row in result_data)

    def test_inner_join_no_matches(self):
        """Test inner join with no matching rows."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame([{"id": 1, "name": "Alice"}])
        right_df = spark.createDataFrame([{"id": 2, "score": 85}])

        result_data, _ = JoinOperationsStatic.inner_join(left_df, right_df, ["id"])

        assert len(result_data) == 0

    def test_left_join_basic(self):
        """Test basic left join."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )
        right_df = spark.createDataFrame(
            [
                {"id": 1, "score": 85},
            ]
        )

        result_data, _ = JoinOperationsStatic.left_join(left_df, right_df, ["id"])

        assert len(result_data) == 2
        # First row should have score
        assert result_data[0]["score"] == 85
        # Second row should have None for score
        assert result_data[1]["score"] is None

    def test_right_join_basic(self):
        """Test basic right join."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
            ]
        )
        right_df = spark.createDataFrame(
            [
                {"id": 1, "score": 85},
                {"id": 2, "score": 90},
            ]
        )

        result_data, _ = JoinOperationsStatic.right_join(left_df, right_df, ["id"])

        assert len(result_data) == 2
        # First row should have name
        assert result_data[0]["name"] == "Alice"
        # Second row should have None for name
        assert result_data[1]["name"] is None

    def test_outer_join_basic(self):
        """Test basic outer join."""
        spark = SparkSession("test")
        left_df = spark.createDataFrame(
            [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )
        right_df = spark.createDataFrame(
            [
                {"id": 2, "score": 90},
                {"id": 3, "score": 95},
            ]
        )

        result_data, _ = JoinOperationsStatic.outer_join(left_df, right_df, ["id"])

        assert len(result_data) == 3
        # All rows should be present
        ids = {row["id"] for row in result_data}
        assert ids == {1, 2, 3}

"""Tests for JoinOperations class."""

from mock_spark.dataframe.operations.join_operations import JoinOperations
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
)
from mock_spark import MockSparkSession


class TestJoinOperations:
    """Test cases for JoinOperations."""

    def setup_method(self):
        """Set up test data."""
        self.spark = MockSparkSession("test")

        # Create test DataFrames
        self.left_data = [
            {"id": 1, "name": "Alice", "age": 25},
            {"id": 2, "name": "Bob", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35},
        ]
        self.left_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )
        self.left_df = self.spark.createDataFrame(self.left_data, self.left_schema)

        self.right_data = [
            {"id": 1, "department": "Engineering", "salary": 80000},
            {"id": 2, "department": "Marketing", "salary": 70000},
            {"id": 4, "department": "Sales", "salary": 60000},
        ]
        self.right_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("department", StringType()),
                MockStructField("salary", IntegerType()),
            ]
        )
        self.right_df = self.spark.createDataFrame(self.right_data, self.right_schema)

    def test_cross_join(self):
        """Test cross join operation."""
        result_data, result_schema = JoinOperations.cross_join(
            self.left_df, self.right_df
        )

        # Should have 3 * 3 = 9 rows
        assert len(result_data) == 9

        # Should have all fields from both DataFrames
        assert len(result_schema.fields) == 6  # 3 from left + 3 from right

        # Check that all combinations are present
        combinations = set()
        for row in result_data:
            left_id = row["id"]
            right_id = row["id_right"] if "id_right" in row else row["id"]
            combinations.add((left_id, right_id))

        expected_combinations = {
            (1, 1),
            (1, 2),
            (1, 4),
            (2, 1),
            (2, 2),
            (2, 4),
            (3, 1),
            (3, 2),
            (3, 4),
        }
        assert combinations == expected_combinations

    def test_inner_join(self):
        """Test inner join operation."""
        result_data, result_schema = JoinOperations.inner_join(
            self.left_df, self.right_df, ["id"]
        )

        # Should have 2 rows (id 1 and 2 match)
        assert len(result_data) == 2

        # Check that only matching rows are included
        ids = [row["id"] for row in result_data]
        assert set(ids) == {1, 2}

        # Check that data is properly combined
        for row in result_data:
            assert "name" in row
            assert "department" in row
            assert "salary" in row

    def test_left_join(self):
        """Test left join operation."""
        result_data, result_schema = JoinOperations.left_join(
            self.left_df, self.right_df, ["id"]
        )

        # Should have 3 rows (all from left)
        assert len(result_data) == 3

        # Check that all left rows are present
        left_ids = [row["id"] for row in result_data]
        assert set(left_ids) == {1, 2, 3}

        # Check that unmatched rows have null values for right columns
        for row in result_data:
            if row["id"] == 3:  # Charlie has no match
                assert row["department"] is None
                assert row["salary"] is None
            else:  # Alice and Bob have matches
                assert row["department"] is not None
                assert row["salary"] is not None

    def test_right_join(self):
        """Test right join operation."""
        result_data, result_schema = JoinOperations.right_join(
            self.left_df, self.right_df, ["id"]
        )

        # Should have 3 rows (all from right)
        assert len(result_data) == 3

        # Check that all right rows are present
        right_ids = [row["id"] for row in result_data]
        assert set(right_ids) == {1, 2, 4}

        # Check that unmatched rows have null values for left columns
        for row in result_data:
            if row["id"] == 4:  # Sales department has no match
                assert row["name"] is None
                assert row["age"] is None
            else:  # Engineering and Marketing have matches
                assert row["name"] is not None
                assert row["age"] is not None

    def test_outer_join(self):
        """Test outer join operation."""
        result_data, result_schema = JoinOperations.outer_join(
            self.left_df, self.right_df, ["id"]
        )

        # Should have 4 rows (3 from left + 1 from right, with 2 matches)
        assert len(result_data) == 4

        # Check that all unique IDs are present
        all_ids = [row["id"] for row in result_data]
        assert set(all_ids) == {1, 2, 3, 4}

        # Check that unmatched rows have appropriate null values
        for row in result_data:
            if row["id"] == 3:  # Charlie from left, no match
                assert row["department"] is None
                assert row["salary"] is None
            elif row["id"] == 4:  # Sales from right, no match
                assert row["name"] is None
                assert row["age"] is None
            else:  # Matched rows
                assert row["name"] is not None
                assert row["department"] is not None

    def test_join_with_duplicate_column_names(self):
        """Test join with duplicate column names."""
        # Create DataFrames with overlapping column names
        left_data = [{"id": 1, "value": "left_value"}]
        left_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", StringType()),
            ]
        )
        left_df = self.spark.createDataFrame(left_data, left_schema)

        right_data = [{"id": 1, "value": "right_value"}]
        right_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("value", StringType()),
            ]
        )
        right_df = self.spark.createDataFrame(right_data, right_schema)

        result_data, result_schema = JoinOperations.inner_join(
            left_df, right_df, ["id"]
        )

        # Should handle column name conflicts
        assert len(result_data) == 1
        row = result_data[0]
        assert "value" in row  # Left value
        assert "right_value" in row  # Right value with prefix

    def test_infer_join_schema(self):
        """Test join schema inference."""
        join_params = (self.right_df, ["id"], "inner")
        result_schema = JoinOperations.infer_join_schema(self.left_df, join_params)

        # Should have all fields from both schemas
        field_names = [field.name for field in result_schema.fields]
        expected_names = ["id", "name", "age", "department", "salary"]
        assert set(field_names) == set(expected_names)

    def test_join_with_empty_dataframes(self):
        """Test join with empty DataFrames."""
        empty_data = []
        empty_schema = MockStructType([])
        empty_df = self.spark.createDataFrame(empty_data, empty_schema)

        # Cross join with empty DataFrame should return empty result
        result_data, result_schema = JoinOperations.cross_join(empty_df, self.right_df)
        assert len(result_data) == 0

        # Inner join with empty DataFrame should return empty result
        result_data, result_schema = JoinOperations.inner_join(
            empty_df, self.right_df, ["id"]
        )
        assert len(result_data) == 0

    def test_join_with_single_column(self):
        """Test join with single column."""
        result_data, result_schema = JoinOperations.inner_join(
            self.left_df, self.right_df, ["id"]
        )

        # Should work with single column join
        assert len(result_data) == 2
        for row in result_data:
            assert row["id"] in [1, 2]  # Only matching IDs

    def test_join_with_multiple_columns(self):
        """Test join with multiple columns."""
        # Create DataFrames with multiple join columns
        left_data = [{"id": 1, "dept": "A", "name": "Alice"}]
        left_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("dept", StringType()),
                MockStructField("name", StringType()),
            ]
        )
        left_df = self.spark.createDataFrame(left_data, left_schema)

        right_data = [{"id": 1, "dept": "A", "salary": 80000}]
        right_schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("dept", StringType()),
                MockStructField("salary", IntegerType()),
            ]
        )
        right_df = self.spark.createDataFrame(right_data, right_schema)

        result_data, result_schema = JoinOperations.inner_join(
            left_df, right_df, ["id", "dept"]
        )

        # Should match on both columns
        assert len(result_data) == 1
        row = result_data[0]
        assert row["id"] == 1
        assert row["dept"] == "A"
        assert row["name"] == "Alice"
        assert row["salary"] == 80000

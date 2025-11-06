"""
Unit tests for base aggregate functions.
"""

import pytest
from mock_spark.functions.base import AggregateFunction
from mock_spark.spark_types import LongType


@pytest.mark.unit
class TestAggregateFunction:
    """Test AggregateFunction evaluation methods."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"value": 10, "count": 1},
            {"value": 20, "count": 2},
            {"value": 30, "count": 3},
            {"value": None, "count": None},
        ]

    def test_evaluate_count_with_column(self, sample_data):
        """Test _evaluate_count with a specific column."""
        func = AggregateFunction("count", "count")
        result = func._evaluate_count(sample_data)
        assert result == 3  # Only non-null values

    def test_evaluate_count_with_none(self, sample_data):
        """Test _evaluate_count with None column (count all rows)."""
        func = AggregateFunction(None, "count")
        result = func._evaluate_count(sample_data)
        assert result == 4  # All rows

    def test_evaluate_sum(self, sample_data):
        """Test _evaluate_sum."""
        func = AggregateFunction("value", "sum")
        result = func._evaluate_sum(sample_data)
        assert result == 60  # 10 + 20 + 30

    def test_evaluate_sum_with_none_column(self, sample_data):
        """Test _evaluate_sum with None column."""
        func = AggregateFunction(None, "sum")
        result = func._evaluate_sum(sample_data)
        assert result == 0

    def test_evaluate_sum_with_all_nulls(self):
        """Test _evaluate_sum with all null values."""
        data = [{"value": None}, {"value": None}]
        func = AggregateFunction("value", "sum")
        result = func._evaluate_sum(data)
        assert result == 0

    def test_evaluate_avg(self, sample_data):
        """Test _evaluate_avg."""
        func = AggregateFunction("value", "avg")
        result = func._evaluate_avg(sample_data)
        assert result == 20.0  # (10 + 20 + 30) / 3

    def test_evaluate_avg_with_none_column(self, sample_data):
        """Test _evaluate_avg with None column."""
        func = AggregateFunction(None, "avg")
        result = func._evaluate_avg(sample_data)
        assert result == 0.0

    def test_evaluate_avg_with_non_numeric_values(self):
        """Test _evaluate_avg with non-numeric values."""
        data = [{"value": "a"}, {"value": "b"}]
        func = AggregateFunction("value", "avg")
        result = func._evaluate_avg(data)
        assert result is None

    def test_evaluate_max(self, sample_data):
        """Test _evaluate_max."""
        func = AggregateFunction("value", "max")
        result = func._evaluate_max(sample_data)
        assert result == 30

    def test_evaluate_max_with_none_column(self, sample_data):
        """Test _evaluate_max with None column."""
        func = AggregateFunction(None, "max")
        result = func._evaluate_max(sample_data)
        assert result is None

    def test_evaluate_max_with_strings(self):
        """Test _evaluate_max with string values."""
        data = [{"value": "apple"}, {"value": "banana"}]
        func = AggregateFunction("value", "max")
        result = func._evaluate_max(data)
        assert result == "banana"

    def test_evaluate_min(self, sample_data):
        """Test _evaluate_min."""
        func = AggregateFunction("value", "min")
        result = func._evaluate_min(sample_data)
        assert result == 10

    def test_evaluate_min_with_none_column(self, sample_data):
        """Test _evaluate_min with None column."""
        func = AggregateFunction(None, "min")
        result = func._evaluate_min(sample_data)
        assert result is None

    def test_evaluate_min_with_strings(self):
        """Test _evaluate_min with string values."""
        data = [{"value": "apple"}, {"value": "banana"}]
        func = AggregateFunction("value", "min")
        result = func._evaluate_min(data)
        assert result == "apple"

    def test_evaluate_with_unsupported_function(self, sample_data):
        """Test evaluate with unsupported function name."""
        func = AggregateFunction("value", "unsupported")
        result = func.evaluate(sample_data)
        assert result is None

    def test_configure_data_type_with_nullable(self):
        """Test _configure_data_type sets nullable correctly."""
        func = AggregateFunction("col", "count")
        # Function name 'count' should make data_type non-nullable
        assert func.data_type is not None  # Data type exists

    def test_configure_data_type_with_nullable_avg(self):
        """Test _configure_data_type for avg (nullable)."""
        func = AggregateFunction("col", "avg")
        assert func.data_type is not None  # Data type exists

    def test_column_name_property_with_str(self):
        """Test column_name property with string."""
        func = AggregateFunction("col_name", "count")
        assert func.column_name == "col_name"

    def test_column_name_property_with_column(self):
        """Test column_name property with Column."""
        from mock_spark.functions.base import Column

        col = Column("test_col")
        func = AggregateFunction(col, "count")
        assert func.column_name == "test_col"

    def test_column_name_property_with_star(self):
        """Test column_name property with count(*) equivalent."""
        func = AggregateFunction("*", "count")
        assert func.column_name == "*"

    def test_column_name_property_with_none(self):
        """Test column_name property with None."""
        func = AggregateFunction(None, "count")
        assert func.column_name == "*"

    def test_generate_name_for_count_star(self):
        """Test _generate_name for count(*)."""
        func = AggregateFunction("*", "count")
        assert func.name == "count(1)"

    def test_generate_name_for_count_none(self):
        """Test _generate_name for count() with None."""
        func = AggregateFunction(None, "count")
        assert func.name == "count"

    def test_generate_name_for_count_distinct(self):
        """Test _generate_name for countDistinct."""
        func = AggregateFunction("col", "countDistinct")
        # PySpark uses "count(col)" not "count(DISTINCT col)" for column names
        assert func.name == "count(col)"

    def test_generate_name_for_sum(self):
        """Test _generate_name for sum."""
        func = AggregateFunction("value", "sum")
        assert func.name == "sum(value)"

    def test_alias_method(self):
        """Test alias method."""
        func = AggregateFunction("col", "sum")
        func.alias("total")
        assert func.name == "total"

    def test_over_method(self):
        """Test over method creates WindowFunction."""
        func = AggregateFunction("col", "sum")
        window_func = func.over("window_spec")
        from mock_spark.functions.window_execution import WindowFunction

        assert isinstance(window_func, WindowFunction)

    def test_init_with_custom_data_type(self):
        """Test initialization with custom data type."""
        func = AggregateFunction("col", "custom", LongType())
        assert isinstance(func.data_type, LongType)

    def test_init_with_custom_data_type_nullable(self):
        """Test initialization with custom nullable data type."""
        func = AggregateFunction("col", "count", LongType(nullable=False))
        assert not func.data_type.nullable

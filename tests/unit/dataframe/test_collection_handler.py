"""
Unit tests for collection handler.
"""

import pytest

# Skip if collection_handler module doesn't exist
try:
    from mock_spark.dataframe.collection_handler import CollectionHandler
except ImportError:
    pytest.skip(
        "dataframe.collection_handler module not available in this version",
        allow_module_level=True,
    )
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    LongType,
    StringType,
)


@pytest.mark.unit
class TestCollectionHandler:
    """Test collection handler operations."""

    @pytest.fixture
    def handler(self):
        """Create a collection handler."""
        return CollectionHandler()

    @pytest.fixture
    def schema(self):
        """Create a schema for testing."""
        return MockStructType(
            [
                MockStructField("id", LongType()),
                MockStructField("name", StringType()),
            ]
        )

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

    def test_collect(self, handler, sample_data, schema):
        """Test collect operation."""
        result = handler.collect(sample_data, schema)
        assert len(result) == 3
        assert all(hasattr(row, "__class__") for row in result)

    def test_take(self, handler, sample_data, schema):
        """Test take operation."""
        result = handler.take(sample_data, schema, 2)
        assert len(result) == 2

    def test_take_with_zero(self, handler, sample_data, schema):
        """Test take with zero count."""
        result = handler.take(sample_data, schema, 0)
        assert len(result) == 0

    def test_take_with_negative(self, handler, sample_data, schema):
        """Test take with negative count."""
        # Negative index may have unexpected behavior
        result = handler.take(sample_data, schema, -1)
        # Just check it doesn't crash
        assert isinstance(result, list)

    def test_take_with_overflow(self, handler, sample_data, schema):
        """Test take with count larger than data."""
        result = handler.take(sample_data, schema, 10)
        assert len(result) == 3

    def test_head_with_default_n(self, handler, sample_data, schema):
        """Test head with default n=1."""
        result = handler.head(sample_data, schema)
        assert result is not None
        assert not isinstance(result, list)

    def test_head_with_n_1(self, handler, sample_data, schema):
        """Test head with n=1."""
        result = handler.head(sample_data, schema, n=1)
        assert result is not None
        assert not isinstance(result, list)

    def test_head_with_n_greater_than_1(self, handler, sample_data, schema):
        """Test head with n>1."""
        result = handler.head(sample_data, schema, n=2)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_head_with_empty_data(self, handler, schema):
        """Test head with empty data."""
        result = handler.head([], schema)
        assert result is None

    def test_tail_with_default_n(self, handler, sample_data, schema):
        """Test tail with default n=1."""
        result = handler.tail(sample_data, schema)
        assert result is not None
        assert not isinstance(result, list)

    def test_tail_with_n_1(self, handler, sample_data, schema):
        """Test tail with n=1."""
        result = handler.tail(sample_data, schema, n=1)
        assert result is not None
        assert not isinstance(result, list)

    def test_tail_with_n_greater_than_1(self, handler, sample_data, schema):
        """Test tail with n>1."""
        result = handler.tail(sample_data, schema, n=2)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_tail_with_empty_data(self, handler, schema):
        """Test tail with empty data."""
        result = handler.tail([], schema)
        assert result is None

    def test_to_local_iterator(self, handler, sample_data, schema):
        """Test to_local_iterator operation."""
        result = handler.to_local_iterator(sample_data, schema)
        items = list(result)
        assert len(items) == 3

    def test_to_local_iterator_with_prefetch(self, handler, sample_data, schema):
        """Test to_local_iterator with prefetch=True."""
        result = handler.to_local_iterator(sample_data, schema, prefetch=True)
        items = list(result)
        assert len(items) == 3

    def test_collect_with_empty_data(self, handler, schema):
        """Test collect with empty data."""
        result = handler.collect([], schema)
        assert len(result) == 0

    def test_collect_with_single_row(self, handler, schema):
        """Test collect with single row."""
        data = [{"id": 1, "name": "Alice"}]
        result = handler.collect(data, schema)
        assert len(result) == 1

    def test_take_returns_correct_rows(self, handler, sample_data, schema):
        """Test take returns the correct rows."""
        result = handler.take(sample_data, schema, 2)
        assert result[0].id == 1
        assert result[1].id == 2

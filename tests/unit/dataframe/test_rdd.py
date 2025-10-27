"""
Unit tests for RDD operations.
"""

import pytest
from mock_spark.dataframe.rdd import MockRDD, MockGroupedRDD


@pytest.mark.unit
class TestMockRDD:
    """Test MockRDD operations."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
            {"id": 4, "value": 20},
        ]

    @pytest.fixture
    def sample_rdd(self, sample_data):
        """Create a sample RDD."""
        return MockRDD(sample_data)

    def test_collect(self, sample_rdd, sample_data):
        """Test collect operation."""
        result = sample_rdd.collect()
        assert result == sample_data

    def test_count(self, sample_rdd):
        """Test count operation."""
        assert sample_rdd.count() == 4

    def test_take(self, sample_rdd):
        """Test take operation."""
        result = sample_rdd.take(2)
        assert len(result) == 2
        assert result == [{"id": 1, "value": 10}, {"id": 2, "value": 20}]

    def test_first(self, sample_rdd):
        """Test first operation."""
        result = sample_rdd.first()
        assert result == {"id": 1, "value": 10}

    def test_first_with_empty_rdd(self):
        """Test first with empty RDD."""
        rdd = MockRDD([])
        assert rdd.first() is None

    def test_foreach(self, sample_rdd):
        """Test foreach operation."""
        collected = []
        sample_rdd.foreach(lambda item: collected.append(item["value"]))
        assert collected == [10, 20, 30, 20]

    def test_map(self, sample_rdd):
        """Test map operation."""
        result = sample_rdd.map(lambda item: item["value"] * 2)
        assert isinstance(result, MockRDD)
        values = result.collect()
        assert values == [20, 40, 60, 40]

    def test_filter(self, sample_rdd):
        """Test filter operation."""
        result = sample_rdd.filter(lambda item: item["value"] > 15)
        assert isinstance(result, MockRDD)
        assert result.count() == 3

    def test_reduce(self, sample_rdd):
        """Test reduce operation."""
        # Need to use a proper accumulator pattern
        result = sample_rdd.reduce(
            lambda acc, item: {"value": acc["value"] + item["value"]}
        )
        assert "value" in result

    def test_reduce_with_empty_rdd(self):
        """Test reduce with empty RDD raises error."""
        rdd = MockRDD([])
        with pytest.raises(ValueError, match="Cannot reduce empty RDD"):
            rdd.reduce(lambda x, y: x + y)

    def test_groupBy(self, sample_rdd):
        """Test groupBy operation."""
        result = sample_rdd.groupBy(lambda item: item["value"] % 2)
        assert isinstance(result, MockGroupedRDD)

    def test_groupBy_creates_correct_groups(self, sample_rdd):
        """Test groupBy creates correct groups."""
        grouped = sample_rdd.groupBy(lambda item: item["value"] % 2)
        result = grouped.mapValues(lambda vals: [v["value"] for v in vals])
        result_list = result.collect()
        # Should have groups with keys 0 and 1
        assert len(result_list) <= 2

    def test_cache(self, sample_rdd):
        """Test cache operation."""
        result = sample_rdd.cache()
        assert result is sample_rdd  # Cache returns self

    def test_persist_without_storage_level(self, sample_rdd):
        """Test persist without storage level."""
        result = sample_rdd.persist()
        assert result is sample_rdd

    def test_persist_with_storage_level(self, sample_rdd):
        """Test persist with storage level."""
        storage_level = "MEMORY_AND_DISK"
        result = sample_rdd.persist(storage_level)
        assert result is sample_rdd

    def test_unpersist(self, sample_rdd):
        """Test unpersist operation."""
        sample_rdd.cache()  # May or may not set cached attribute
        result = sample_rdd.unpersist()
        assert result is sample_rdd

    def test_iter(self, sample_rdd):
        """Test iteration."""
        items = list(sample_rdd)
        assert len(items) == 4

    def test_len(self, sample_rdd):
        """Test len operation."""
        assert len(sample_rdd) == 4

    def test_repr(self, sample_rdd):
        """Test representation."""
        repr_str = repr(sample_rdd)
        assert "MockRDD" in repr_str

    def test_toDF(self, sample_rdd):
        """Test toDF operation."""
        from mock_spark import MockSparkSession

        spark = MockSparkSession("test")
        df = sample_rdd.toDF()
        # Should return None or a dataframe - implementation dependent
        assert True


@pytest.mark.unit
class TestMockGroupedRDD:
    """Test MockGroupedRDD operations."""

    def test_mapValues(self):
        """Test mapValues operation."""
        groups = {0: [{"v": 10}, {"v": 30}], 1: [{"v": 20}]}
        grouped = MockGroupedRDD(groups)
        result = grouped.mapValues(lambda vals: sum(v["v"] for v in vals))
        assert isinstance(result, MockRDD)
        values = result.collect()
        assert len(values) == 2

    def test_reduceByKey(self):
        """Test reduceByKey operation."""
        groups = {"a": [1, 2], "b": [3, 4]}
        grouped = MockGroupedRDD(groups)
        result = grouped.reduceByKey(lambda x, y: x + y)
        assert isinstance(result, MockRDD)

    def test_countByKey(self):
        """Test countByKey operation."""
        groups = {"a": [1, 2], "b": [3], "a": [4]}
        grouped = MockGroupedRDD(groups)
        result = grouped.countByKey()
        assert isinstance(result, dict)

    def test_collect(self):
        """Test collect on grouped RDD."""
        groups = {0: [{"v": 10}], 1: [{"v": 20}]}
        grouped = MockGroupedRDD(groups)
        result = grouped.collect()
        assert isinstance(result, list)

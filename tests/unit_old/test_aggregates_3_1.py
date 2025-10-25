"""Unit tests for aggregate functions from PySpark 3.1."""

from mock_spark import MockSparkSession, F


class TestBooleanAggregates:
    """Test boolean aggregate functions (PySpark 3.1)."""

    def setup_method(self):
        """Setup test session."""
        self.spark = MockSparkSession("test")

    def teardown_method(self):
        """Cleanup session."""
        self.spark.stop()

    def test_bool_and(self):
        """Test bool_and - aggregate AND."""
        data = [
            {"group": "A", "flag": True},
            {"group": "A", "flag": True},
            {"group": "B", "flag": True},
            {"group": "B", "flag": False},
        ]
        df = self.spark.createDataFrame(data)

        result = (
            df.groupBy("group")
            .agg(F.bool_and(F.col("flag")).alias("all_true"))
            .collect()
        )

        # Group A: all True
        group_a = [r for r in result if r["group"] == "A"][0]
        assert group_a["all_true"]

        # Group B: has False
        group_b = [r for r in result if r["group"] == "B"][0]
        assert not group_b["all_true"]

    def test_bool_or(self):
        """Test bool_or - aggregate OR."""
        data = [
            {"group": "A", "flag": False},
            {"group": "A", "flag": False},
            {"group": "B", "flag": False},
            {"group": "B", "flag": True},
        ]
        df = self.spark.createDataFrame(data)

        result = (
            df.groupBy("group")
            .agg(F.bool_or(F.col("flag")).alias("any_true"))
            .collect()
        )

        # Group A: all False
        group_a = [r for r in result if r["group"] == "A"][0]
        assert not group_a["any_true"]

        # Group B: has True
        group_b = [r for r in result if r["group"] == "B"][0]
        assert group_b["any_true"]

    def test_every_alias(self):
        """Test every - alias for bool_and."""
        data = [{"flag": True}, {"flag": True}, {"flag": True}]
        df = self.spark.createDataFrame(data)

        result = df.agg(F.every(F.col("flag")).alias("result")).collect()
        assert result[0]["result"]

    def test_some_alias(self):
        """Test some - alias for bool_or."""
        data = [{"flag": False}, {"flag": False}, {"flag": True}]
        df = self.spark.createDataFrame(data)

        result = df.agg(F.some(F.col("flag")).alias("result")).collect()
        assert result[0]["result"]


class TestAdvancedAggregates:
    """Test advanced aggregate functions (PySpark 3.1)."""

    def setup_method(self):
        """Setup test session."""
        self.spark = MockSparkSession("test")

    def teardown_method(self):
        """Cleanup session."""
        self.spark.stop()

    def test_max_by(self):
        """Test max_by - value associated with maximum."""
        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92},
        ]
        df = self.spark.createDataFrame(data)

        result = df.agg(
            F.max_by(F.col("name"), F.col("score")).alias("top_scorer")
        ).collect()

        assert result[0]["top_scorer"] == "Alice"

    def test_min_by(self):
        """Test min_by - value associated with minimum."""
        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92},
        ]
        df = self.spark.createDataFrame(data)

        result = df.agg(
            F.min_by(F.col("name"), F.col("score")).alias("lowest_scorer")
        ).collect()

        assert result[0]["lowest_scorer"] == "Bob"

    def test_count_if(self):
        """Test count_if - conditional counting."""
        data = [{"value": 10}, {"value": 25}, {"value": 30}, {"value": 15}]
        df = self.spark.createDataFrame(data)

        result = df.agg(
            F.count_if(F.col("value") > 20).alias("count_above_20")
        ).collect()

        assert result[0]["count_above_20"] == 2

    def test_any_value(self):
        """Test any_value - return any non-null value."""
        data = [
            {"group": "A", "value": 10},
            {"group": "A", "value": 20},
            {"group": "B", "value": 30},
        ]
        df = self.spark.createDataFrame(data)

        result = (
            df.groupBy("group")
            .agg(F.any_value(F.col("value")).alias("some_value"))
            .collect()
        )

        # Should return some value from each group (non-deterministic)
        group_a = [r for r in result if r["group"] == "A"][0]
        assert group_a["some_value"] in [10, 20]

        group_b = [r for r in result if r["group"] == "B"][0]
        assert group_b["some_value"] == 30

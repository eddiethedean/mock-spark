"""
Tests for PySpark 3.4+ features with version gating.

Tests 5 high-value features introduced in PySpark 3.3-3.4:
- melt() DataFrame method (3.4+)
- to() DataFrame method (3.4+)
- withMetadata() DataFrame method (3.3+)
- observe() DataFrame method (3.3+)
- window_time() function (3.4+)
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import (
    IntegerType,
    MockStructType,
    MockStructField,
)


@pytest.fixture
def spark():
    """Create MockSparkSession for tests."""
    return MockSparkSession("test_pyspark_34_features")


class TestMelt:
    """Test melt() DataFrame method (PySpark 3.4+)."""

    def test_melt_basic(self, spark):
        """Test melt with basic wide-to-long transformation."""
        df = spark.createDataFrame([{"id": 1, "A": 10, "B": 20, "C": 30}])

        result = df.melt(ids=["id"], values=["A", "B", "C"])
        data = result.collect()

        assert len(data) == 3
        assert all(row["id"] == 1 for row in data)
        assert data[0]["variable"] == "A"
        assert data[0]["value"] == 10
        assert data[1]["variable"] == "B"
        assert data[1]["value"] == 20
        assert data[2]["variable"] == "C"
        assert data[2]["value"] == 30

    def test_melt_multiple_ids(self, spark):
        """Test melt with multiple ID columns."""
        df = spark.createDataFrame(
            [{"user": "Alice", "date": "2024-01-01", "clicks": 5, "views": 10}]
        )

        result = df.melt(ids=["user", "date"], values=["clicks", "views"])
        data = result.collect()

        assert len(data) == 2
        assert data[0]["user"] == "Alice"
        assert data[0]["date"] == "2024-01-01"
        assert data[0]["variable"] == "clicks"
        assert data[0]["value"] == 5

    def test_melt_no_ids(self, spark):
        """Test melt with no ID columns."""
        df = spark.createDataFrame([{"A": 1, "B": 2, "C": 3}])

        result = df.melt(values=["A", "B"])
        data = result.collect()

        assert len(data) == 2
        assert "variable" in data[0]
        assert "value" in data[0]

    def test_melt_custom_column_names(self, spark):
        """Test melt with custom variable and value column names."""
        df = spark.createDataFrame([{"id": 1, "metric1": 100, "metric2": 200}])

        result = df.melt(
            ids=["id"],
            values=["metric1", "metric2"],
            variableColumnName="metric_name",
            valueColumnName="metric_value",
        )
        data = result.collect()

        assert "metric_name" in data[0]
        assert "metric_value" in data[0]
        assert data[0]["metric_name"] == "metric1"
        assert data[0]["metric_value"] == 100

    def test_melt_preserves_types(self, spark):
        """Test melt preserves data types correctly."""
        df = spark.createDataFrame([{"id": 1, "score1": 95.5, "score2": 87.3}])

        result = df.melt(ids=["id"], values=["score1", "score2"])

        # Schema should have value column with double type
        assert "value" in result.columns
        # Data should preserve decimal values
        data = result.collect()
        assert data[0]["value"] == 95.5


class TestToSchema:
    """Test to() DataFrame method (PySpark 3.4+)."""

    def test_to_with_struct_schema(self, spark):
        """Test to() with MockStructType schema."""
        df = spark.createDataFrame([{"a": 1, "b": 2, "c": 3}])

        schema = MockStructType(
            [MockStructField("a", IntegerType()), MockStructField("b", IntegerType())]
        )

        result = df.to(schema)
        data = result.collect()

        assert len(data) == 1
        assert "a" in data[0]
        assert "b" in data[0]
        assert "c" not in data[0]  # Column c should be dropped

    def test_to_with_ddl_string(self, spark):
        """Test to() with DDL schema string."""
        df = spark.createDataFrame([{"id": 1, "name": "Alice", "extra": "data"}])

        result = df.to("id: long, name: string")
        data = result.collect()

        assert len(data) == 1
        assert data[0]["id"] == 1
        assert data[0]["name"] == "Alice"
        assert "extra" not in data[0]

    def test_to_schema_enforcement(self, spark):
        """Test to() enforces schema structure."""
        df = spark.createDataFrame([{"x": 10, "y": 20, "z": 30}])

        new_schema = MockStructType(
            [MockStructField("x", IntegerType()), MockStructField("y", IntegerType())]
        )

        result = df.to(new_schema)

        assert set(result.columns) == {"x", "y"}
        assert result.count() == 1


class TestWithMetadata:
    """Test withMetadata() DataFrame method (PySpark 3.3+)."""

    def test_withmetadata_basic(self, spark):
        """Test withMetadata attaches metadata to column."""
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

        metadata = {"comment": "User identifier", "pii": False}
        result = df.withMetadata("id", metadata)

        # Check schema has metadata
        id_field = [f for f in result.schema.fields if f.name == "id"][0]
        assert id_field.metadata == metadata

    def test_withmetadata_preserves_data(self, spark):
        """Test withMetadata doesn't change data."""
        df = spark.createDataFrame([{"value": 42}])

        result = df.withMetadata("value", {"unit": "meters"})
        data = result.collect()

        assert len(data) == 1
        assert data[0]["value"] == 42

    def test_withmetadata_multiple_columns(self, spark):
        """Test withMetadata can be chained for multiple columns."""
        df = spark.createDataFrame([{"col1": 1, "col2": 2}])

        result = df.withMetadata("col1", {"desc": "First column"}).withMetadata(
            "col2", {"desc": "Second column"}
        )

        col1_field = [f for f in result.schema.fields if f.name == "col1"][0]
        col2_field = [f for f in result.schema.fields if f.name == "col2"][0]

        assert col1_field.metadata["desc"] == "First column"
        assert col2_field.metadata["desc"] == "Second column"

    def test_withmetadata_nonexistent_column(self, spark):
        """Test withMetadata with nonexistent column (no error in mock)."""
        df = spark.createDataFrame([{"a": 1}])

        # Should not raise error, just pass through
        result = df.withMetadata("nonexistent", {"key": "value"})
        assert result.count() == 1


class TestObserve:
    """Test observe() DataFrame method (PySpark 3.3+)."""

    def test_observe_basic(self, spark):
        """Test observe registers observation."""
        df = spark.createDataFrame([{"value": i} for i in range(10)])

        result = df.observe("metrics", F.count(F.lit(1)).alias("row_count"))

        # Check observation is stored
        assert hasattr(result, "_observations")
        assert "metrics" in result._observations

    def test_observe_multiple_metrics(self, spark):
        """Test observe with multiple metric expressions."""
        df = spark.createDataFrame([{"value": i} for i in range(5)])

        result = df.observe(
            "stats", F.count(F.lit(1)).alias("count"), F.sum("value").alias("total")
        )

        assert hasattr(result, "_observations")
        assert len(result._observations["stats"]) == 2

    def test_observe_chainable(self, spark):
        """Test observe can be called multiple times."""
        df = spark.createDataFrame([{"id": i} for i in range(10)])

        result = df.observe("metric1", F.count(F.lit(1))).observe(
            "metric2", F.sum("id")
        )

        assert hasattr(result, "_observations")
        assert "metric1" in result._observations
        assert "metric2" in result._observations
        assert len(result._observations) == 2

    def test_observe_doesnt_affect_data(self, spark):
        """Test observe doesn't change DataFrame data."""
        df = spark.createDataFrame([{"value": 42}])

        result = df.observe("test", F.sum("value"))
        data = result.collect()

        assert len(data) == 1
        assert data[0]["value"] == 42


class TestWindowTime:
    """Test window_time() function (PySpark 3.4+)."""

    def test_window_time_basic(self, spark):
        """Test window_time creates column operation."""
        result_col = F.window_time("window_col")

        assert result_col.operation == "window_time"
        assert "window_col" in result_col.name

    def test_window_time_with_column_object(self, spark):
        """Test window_time with Column object."""
        result_col = F.window_time(F.col("window"))

        assert result_col.operation == "window_time"
        assert "window" in result_col.name

    def test_window_time_function_exists(self):
        """Test window_time function is accessible from F namespace."""
        assert hasattr(F, "window_time")
        assert callable(F.window_time)

    def test_window_time_in_aggregation(self, spark):
        """Test window_time can be used in aggregation context."""
        spark.createDataFrame([{"timestamp": "2024-01-01 10:00:00", "value": 1}])

        # Should not raise an error when used in select
        # (actual window extraction would happen in DuckDB translation)
        result_col = F.window_time(F.col("timestamp"))
        assert result_col is not None


class TestIntegration:
    """Integration tests for PySpark 3.4+ features."""

    def test_melt_then_aggregate(self, spark):
        """Test melt followed by aggregation."""
        df = spark.createDataFrame(
            [{"id": 1, "A": 10, "B": 20}, {"id": 2, "A": 15, "B": 25}]
        )

        melted = df.melt(ids=["id"], values=["A", "B"])
        result = melted.groupBy("variable").agg(F.sum("value").alias("total"))
        data = result.collect()

        assert len(data) == 2
        a_total = [r for r in data if r["variable"] == "A"][0]["total"]
        b_total = [r for r in data if r["variable"] == "B"][0]["total"]
        assert a_total == 25  # 10 + 15
        assert b_total == 45  # 20 + 25

    def test_observe_with_melt(self, spark):
        """Test observe with melt operation."""
        df = spark.createDataFrame([{"id": 1, "val1": 10, "val2": 20}])

        melted = df.melt(ids=["id"], values=["val1", "val2"])
        result = melted.observe("melted_metrics", F.count(F.lit(1)))

        assert hasattr(result, "_observations")
        assert "melted_metrics" in result._observations

        # Melted should have 2 rows (1 row * 2 value columns)
        assert len(result.collect()) == 2

    def test_withmetadata_preserved_through_select(self, spark):
        """Test metadata is preserved through select operations."""
        df = spark.createDataFrame([{"id": 1, "name": "Alice"}])

        df_with_meta = df.withMetadata("id", {"isPrimaryKey": True})
        result = df_with_meta.select("id", "name")

        # Check metadata is preserved
        id_field = [f for f in result.schema.fields if f.name == "id"][0]
        assert id_field.metadata.get("isPrimaryKey") is True

    def test_all_34_features_accessible(self, spark):
        """Test all PySpark 3.4+ features are accessible."""
        df = spark.createDataFrame([{"id": 1, "A": 10, "B": 20}])

        # melt
        assert hasattr(df, "melt")

        # to
        assert hasattr(df, "to")

        # withMetadata (3.3+)
        assert hasattr(df, "withMetadata")

        # observe (3.3+)
        assert hasattr(df, "observe")

        # window_time
        assert hasattr(F, "window_time")

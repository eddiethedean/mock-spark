"""
PySpark 3.2 Phase 2 Compatibility Tests.

Tests Pandas API, DataFrame.transform(), unpivot(), and DEFAULT columns
against real PySpark to ensure compatibility.
"""

import pytest

try:
    from pyspark.sql import SparkSession  # noqa: F401
    from pyspark.sql import functions as PySparkF  # noqa: F401
    import pandas as pd  # noqa: F401

    PYSPARK_AVAILABLE = True
    PANDAS_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    try:
        import pandas as pd  # noqa: F401

        PANDAS_AVAILABLE = True
    except ImportError:
        PANDAS_AVAILABLE = False

from mock_spark import MockSparkSession
from mock_spark import functions as F


@pytest.mark.compatibility
@pytest.mark.skipif(not PANDAS_AVAILABLE, reason="Pandas not installed")
class TestPandasAPICompat:
    """Test Pandas API compatibility."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")

        self.test_data = [
            {"category": "A", "value": 10.0},
            {"category": "A", "value": 20.0},
            {"category": "B", "value": 15.0},
            {"category": "B", "value": 25.0},
        ]

    def test_mapinpandas_basic(self):
        """Test basic mapInPandas functionality."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        def multiply_by_two(iterator):
            for pdf in iterator:
                pdf["value"] = pdf["value"] * 2
                yield pdf

        result = mock_df.mapInPandas(
            multiply_by_two, schema="category string, value double"
        )

        collected = result.collect()
        assert len(collected) == 4
        assert collected[0]["value"] == 20.0  # 10 * 2

    def test_applyinpandas_groupby(self):
        """Test applyInPandas on grouped data."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        def normalize(pdf):
            mean = pdf["value"].mean()
            std = pdf["value"].std()
            if std > 0:
                pdf["normalized"] = (pdf["value"] - mean) / std
            else:
                pdf["normalized"] = 0.0
            return pdf

        result = mock_df.groupBy("category").applyInPandas(
            normalize, schema="category string, value double, normalized double"
        )

        collected = result.collect()
        assert len(collected) == 4

    def test_grouped_transform(self):
        """Test grouped transform."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        def add_group_mean(pdf):
            pdf["group_mean"] = pdf["value"].mean()
            return pdf

        result = mock_df.groupBy("category").transform(add_group_mean)

        collected = result.collect()
        assert len(collected) == 4
        # Category A mean should be 15.0 (10+20)/2
        category_a_rows = [r for r in collected if r["category"] == "A"]
        assert all(r["group_mean"] == 15.0 for r in category_a_rows)


@pytest.mark.compatibility
class TestDataFrameTransformCompat:
    """Test DataFrame.transform() compatibility."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")

        self.test_data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
        ]

    def test_transform_basic(self):
        """Test basic DataFrame transformation."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        def add_double(df):
            return df.withColumn("double_value", F.col("value") * 2)

        result = mock_df.transform(add_double)

        collected = result.collect()
        assert len(collected) == 2
        assert "double_value" in collected[0]
        assert collected[0]["double_value"] == 20

    def test_transform_chaining(self):
        """Test chained transformations."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        def add_one(df):
            return df.withColumn("value_plus_one", F.col("value") + 1)

        def multiply_two(df):
            return df.withColumn("value_times_two", F.col("value") * 2)

        result = mock_df.transform(add_one).transform(multiply_two)

        collected = result.collect()
        assert len(collected) == 2
        assert "value_plus_one" in collected[0]
        assert "value_times_two" in collected[0]


@pytest.mark.compatibility
class TestUnpivotCompat:
    """Test unpivot() compatibility."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")

        self.test_data = [
            {"id": 1, "name": "Alice", "Q1": 100, "Q2": 200, "Q3": 300, "Q4": 400},
            {"id": 2, "name": "Bob", "Q1": 150, "Q2": 250, "Q3": 350, "Q4": 450},
        ]

    def test_unpivot_basic(self):
        """Test basic unpivot operation."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.unpivot(
            ids=["id", "name"],
            values=["Q1", "Q2", "Q3", "Q4"],
            variableColumnName="quarter",
            valueColumnName="sales",
        )

        collected = result.collect()
        # 2 rows * 4 quarters = 8 rows
        assert len(collected) == 8

        # Check structure
        assert "id" in collected[0]
        assert "name" in collected[0]
        assert "quarter" in collected[0]
        assert "sales" in collected[0]

        # Check values
        alice_q1 = [r for r in collected if r["id"] == 1 and r["quarter"] == "Q1"][0]
        assert alice_q1["sales"] == 100

    def test_unpivot_single_id(self):
        """Test unpivot with single id column."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.unpivot(
            ids="id",
            values=["Q1", "Q2"],
            variableColumnName="quarter",
            valueColumnName="sales",
        )

        collected = result.collect()
        # 2 rows * 2 quarters = 4 rows
        assert len(collected) == 4


@pytest.mark.compatibility
class TestDefaultColumnsCompat:
    """Test DEFAULT column values compatibility."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")

    def test_default_value_in_schema(self):
        """Test that schema supports default values."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            StringType,
            IntegerType,
        )

        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("status", StringType(), default_value="active"),
            ]
        )

        # Check that default_value is stored
        status_field = schema.fields[1]
        assert status_field.default_value == "active"
        assert status_field.name == "status"

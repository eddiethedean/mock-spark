"""
Tests for final core features to achieve 100% PySpark 3.0-3.5 coverage.

Tests 3 critical features available in all PySpark versions:
- PandasUDFType enum
- to_str() function
- mapInPandas() DataFrame method
"""

import pytest
from mock_spark import MockSparkSession, F
from mock_spark.spark_types import IntegerType, StringType
from mock_spark.functions import PandasUDFType


@pytest.fixture
def spark():
    """Create MockSparkSession for tests."""
    return MockSparkSession("test_final_core_features")


class TestPandasUDFType:
    """Test PandasUDFType enum class."""

    def test_pandasudftype_scalar(self):
        """Test SCALAR Pandas UDF type."""
        assert PandasUDFType.SCALAR == 200

    def test_pandasudftype_grouped_map(self):
        """Test GROUPED_MAP Pandas UDF type."""
        assert PandasUDFType.GROUPED_MAP == 201

    def test_pandasudftype_grouped_agg(self):
        """Test GROUPED_AGG Pandas UDF type."""
        assert PandasUDFType.GROUPED_AGG == 202

    def test_pandasudftype_scalar_iter(self):
        """Test SCALAR_ITER Pandas UDF type."""
        assert PandasUDFType.SCALAR_ITER == 203

    def test_pandasudftype_map_iter(self):
        """Test MAP_ITER Pandas UDF type."""
        assert PandasUDFType.MAP_ITER == 204

    def test_pandasudftype_usage_with_pandas_udf(self, spark):
        """Test PandasUDFType can be used with pandas_udf function."""

        # Create a pandas UDF with explicit type
        @F.pandas_udf(IntegerType(), functionType=PandasUDFType.SCALAR)
        def double_value(s):
            return s * 2

        from mock_spark.functions import UserDefinedFunction

        assert isinstance(double_value, UserDefinedFunction)
        assert double_value.evalType == "PANDAS"

    def test_pandasudftype_all_types_defined(self):
        """Test all PandasUDFType constants are defined."""
        assert hasattr(PandasUDFType, "SCALAR")
        assert hasattr(PandasUDFType, "GROUPED_MAP")
        assert hasattr(PandasUDFType, "GROUPED_AGG")
        assert hasattr(PandasUDFType, "SCALAR_ITER")
        assert hasattr(PandasUDFType, "MAP_ITER")


class TestToStrFunction:
    """Test to_str function."""

    def test_to_str_with_column_name(self, spark):
        """Test to_str with string column name."""
        result_col = F.to_str("value")

        assert result_col.operation == "to_str"
        assert "value" in result_col.name

    def test_to_str_with_column_object(self, spark):
        """Test to_str with Column object."""
        result_col = F.to_str(F.col("number"))

        assert result_col.operation == "to_str"
        assert "number" in result_col.name

    def test_to_str_function_exists(self):
        """Test to_str function is accessible from F namespace."""
        assert hasattr(F, "to_str")
        assert callable(F.to_str)

    def test_to_str_in_select(self, spark):
        """Test to_str can be used in select statement."""
        df = spark.createDataFrame([{"id": 1, "value": 42}])

        # Should not raise an error
        result = df.select(F.to_str("value").alias("value_str"))
        assert "value_str" in result.columns


class TestMapInPandas:
    """Test mapInPandas DataFrame method."""

    def test_mapinpandas_basic_transformation(self, spark):
        """Test mapInPandas with basic transformation."""
        pytest.importorskip("pandas")

        df = spark.createDataFrame(
            [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
        )

        def double_values(iterator):
            for pdf in iterator:
                pdf["value"] = pdf["value"] * 2
                yield pdf

        result = df.mapInPandas(double_values, "id: long, value: long")
        data = result.collect()

        assert len(data) == 3
        assert data[0]["value"] == 20
        assert data[1]["value"] == 40
        assert data[2]["value"] == 60

    def test_mapinpandas_with_schema_string(self, spark):
        """Test mapInPandas with DDL schema string."""
        pytest.importorskip("pandas")

        df = spark.createDataFrame([{"x": 1}, {"x": 2}])

        def add_column(iterator):
            for pdf in iterator:
                pdf["y"] = pdf["x"] + 100
                yield pdf

        result = df.mapInPandas(add_column, "x: int, y: int")
        data = result.collect()

        assert len(data) == 2
        assert data[0]["y"] == 101
        assert data[1]["y"] == 102

    def test_mapinpandas_with_struct_schema(self, spark):
        """Test mapInPandas with MockStructType schema."""
        pytest.importorskip("pandas")

        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType

        df = spark.createDataFrame([{"a": 1}])

        schema = MockStructType(
            [MockStructField("a", IntegerType()), MockStructField("b", IntegerType())]
        )

        def add_field(iterator):
            for pdf in iterator:
                pdf["b"] = 999
                yield pdf

        result = df.mapInPandas(add_field, schema)
        data = result.collect()

        assert data[0]["b"] == 999

    def test_mapinpandas_iterator_pattern(self, spark):
        """Test mapInPandas with proper iterator pattern."""
        pytest.importorskip("pandas")

        df = spark.createDataFrame([{"id": i} for i in range(5)])

        def process_partition(iterator):
            """Process each partition and yield results."""
            for pdf in iterator:
                # Add a computed column
                pdf["doubled"] = pdf["id"] * 2
                yield pdf

        result = df.mapInPandas(process_partition, "id: int, doubled: int")
        data = result.collect()

        assert len(data) == 5
        assert all("doubled" in row for row in data)
        assert data[2]["doubled"] == 4

    def test_mapinpandas_complex_transformation(self, spark):
        """Test mapInPandas with complex pandas operations."""
        pytest.importorskip("pandas")

        df = spark.createDataFrame(
            [
                {"category": "A", "value": 10},
                {"category": "A", "value": 20},
                {"category": "B", "value": 30},
            ]
        )

        def aggregate_partition(iterator):
            """Aggregate within partition using pandas."""
            for pdf in iterator:
                # Group and aggregate using pandas
                result = pdf.groupby("category")["value"].sum().reset_index()
                yield result

        result = df.mapInPandas(aggregate_partition, "category: string, value: long")
        data = result.collect()

        assert len(data) == 2
        # Find category A and B
        cat_a = [r for r in data if r["category"] == "A"][0]
        cat_b = [r for r in data if r["category"] == "B"][0]
        assert cat_a["value"] == 30
        assert cat_b["value"] == 30

    def test_mapinpandas_without_pandas_raises_error(self, spark, monkeypatch):
        """Test mapInPandas raises error when pandas not installed."""
        df = spark.createDataFrame([{"id": 1}])

        # Mock pandas import failure
        import sys

        pandas_module = sys.modules.get("pandas")
        if pandas_module:
            monkeypatch.setitem(sys.modules, "pandas", None)

        def transform(iterator):
            for pdf in iterator:
                yield pdf

        with pytest.raises(ImportError, match="pandas is required for mapInPandas"):
            df.mapInPandas(transform, "id: int")

    def test_mapinpandas_empty_dataframe(self, spark):
        """Test mapInPandas with empty DataFrame."""
        pytest.importorskip("pandas")

        df = spark.createDataFrame([])

        def transform(iterator):
            for pdf in iterator:
                yield pdf

        result = df.mapInPandas(transform, "id: int, value: int")
        data = result.collect()

        assert len(data) == 0


class TestIntegration:
    """Integration tests combining new features."""

    def test_to_str_accessible(self, spark):
        """Test to_str function is accessible and returns correct type."""
        df = spark.createDataFrame([{"id": 1, "value": 100}])

        # Apply to_str - should not raise an error
        result_col = F.to_str("value")
        assert result_col.operation == "to_str"

        # Can be used in select
        df_with_str = df.select(F.to_str("value").alias("value_str"), "id")
        assert "value_str" in df_with_str.columns
        assert df_with_str.count() == 1

    def test_pandas_udf_with_pandasudftype(self, spark):
        """Test creating pandas_udf with PandasUDFType constant."""

        # Should not raise an error
        @F.pandas_udf(StringType(), functionType=PandasUDFType.GROUPED_AGG)
        def concat_strings(s):
            return s.str.cat(sep=",")

        from mock_spark.functions import UserDefinedFunction

        assert isinstance(concat_strings, UserDefinedFunction)

"""
Unit tests for UDF (User-Defined Function) classes.
"""

import pytest
from mock_spark import SparkSession
from mock_spark.functions.udf import UserDefinedFunction, UserDefinedTableFunction
from mock_spark.spark_types import StringType, StructType, StructField


@pytest.mark.unit
class TestUserDefinedFunction:
    """Test UserDefinedFunction class."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

    def test_udf_basic(self, spark, sample_data):
        """Test basic UDF creation and usage."""

        def upper_case(s):
            return s.upper() if s else None

        udf_func = UserDefinedFunction(upper_case, StringType())
        df = spark.createDataFrame(sample_data)

        result = df.select(udf_func("name").alias("upper_name"))
        assert result is not None
        assert len(result.columns) == 1

    def test_udf_as_nondeterministic(self, spark, sample_data):
        """Test marking UDF as nondeterministic."""

        def random_func(s):
            return s

        udf_func = UserDefinedFunction(random_func, StringType())
        udf_func.asNondeterministic()

        assert udf_func._deterministic is False


@pytest.mark.unit
class TestUserDefinedTableFunction:
    """Test UserDefinedTableFunction class (PySpark 3.5+)."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data for testing."""
        return [
            {"id": 1, "text": "hello"},
            {"id": 2, "text": "world"},
        ]

    def test_table_udf_basic(self, spark, sample_data):
        """Test basic UserDefinedTableFunction creation."""

        def split_string(s):
            """Split string into characters, returning list of rows."""
            if s is None:
                return []
            return [(char,) for char in s]

        return_schema = StructType([StructField("char", StringType(), True)])

        table_udf = UserDefinedTableFunction(split_string, return_schema)
        df = spark.createDataFrame(sample_data)

        # Test that the function can be called
        result = df.select(table_udf("text").alias("chars"))
        assert result is not None
        assert len(result.columns) == 1

    def test_table_udf_with_name(self, spark, sample_data):
        """Test UserDefinedTableFunction with custom name."""

        def split_string(s):
            return [(char,) for char in (s or "")]

        return_schema = StructType([StructField("char", StringType(), True)])

        table_udf = UserDefinedTableFunction(
            split_string, return_schema, name="split_chars"
        )
        df = spark.createDataFrame(sample_data)

        result = df.select(table_udf("text").alias("chars"))
        assert result is not None

    def test_table_udf_multiple_columns(self, spark):
        """Test UserDefinedTableFunction with multiple input columns."""

        def combine_strings(s1, s2):
            """Combine two strings into pairs."""
            if s1 is None or s2 is None:
                return []
            return [(s1 + s2,)]

        return_schema = StructType([StructField("combined", StringType(), True)])

        table_udf = UserDefinedTableFunction(combine_strings, return_schema)
        test_data = [
            {"id": 1, "first": "hello", "second": "world"},
        ]
        df = spark.createDataFrame(test_data)

        result = df.select(table_udf("first", "second").alias("combined"))
        assert result is not None

    def test_table_udf_empty_input(self, spark):
        """Test UserDefinedTableFunction with empty input."""

        def empty_func(s):
            return []

        return_schema = StructType([StructField("result", StringType(), True)])

        table_udf = UserDefinedTableFunction(empty_func, return_schema)
        test_data = [{"id": 1, "text": ""}]
        df = spark.createDataFrame(test_data)

        result = df.select(table_udf("text").alias("result"))
        assert result is not None

    def test_table_udf_no_columns_error(self, spark, sample_data):
        """Test that UserDefinedTableFunction raises error with no columns."""

        def func():
            return []

        return_schema = StructType([StructField("result", StringType(), True)])

        table_udf = UserDefinedTableFunction(func, return_schema)
        df = spark.createDataFrame(sample_data)

        with pytest.raises(
            ValueError, match="Table UDF requires at least one column argument"
        ):
            df.select(table_udf().alias("result"))

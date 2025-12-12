"""
Tests for PySpark namespace imports - verifying drop-in replacement compatibility.

This module tests that all PySpark imports work correctly with mock-spark,
enabling code written for PySpark to work without any import changes.
"""

import pytest
from typing import Any


@pytest.mark.compatibility
class TestPySparkNamespaceImports:
    """Test PySpark namespace imports work correctly."""

    def test_pyspark_sql_imports(self):
        """Test that pyspark.sql imports work correctly."""
        # Test core classes
        from pyspark.sql import SparkSession, DataFrame, functions as F
        from pyspark.sql import Column, Row, Window

        # Verify imports work
        assert SparkSession is not None
        assert DataFrame is not None
        assert F is not None
        assert Column is not None
        assert Row is not None
        assert Window is not None

    def test_pyspark_sql_types_imports(self):
        """Test that pyspark.sql.types imports work correctly."""
        from pyspark.sql.types import (
            StringType,
            IntegerType,
            LongType,
            DoubleType,
            BooleanType,
            DateType,
            TimestampType,
            DecimalType,
            ArrayType,
            MapType,
            StructType,
            StructField,
            Row,
        )

        # Verify all types are importable
        assert StringType is not None
        assert IntegerType is not None
        assert LongType is not None
        assert DoubleType is not None
        assert BooleanType is not None
        assert DateType is not None
        assert TimestampType is not None
        assert DecimalType is not None
        assert ArrayType is not None
        assert MapType is not None
        assert StructType is not None
        assert StructField is not None
        assert Row is not None

    def test_pyspark_sql_functions_imports(self):
        """Test that pyspark.sql.functions imports work correctly."""
        from pyspark.sql.functions import col, lit, upper, lower, sum, count, max, min

        # Verify functions are importable
        assert col is not None
        assert lit is not None
        assert upper is not None
        assert lower is not None
        assert sum is not None
        assert count is not None
        assert max is not None
        assert min is not None

    def test_pyspark_sql_utils_imports(self):
        """Test that pyspark.sql.utils exception imports work correctly."""
        from pyspark.sql.utils import (
            AnalysisException,
            IllegalArgumentException,
            ParseException,
            QueryExecutionException,
        )

        # Verify exceptions are importable
        assert AnalysisException is not None
        assert IllegalArgumentException is not None
        assert ParseException is not None
        assert QueryExecutionException is not None

    def test_pyspark_sql_functions_module_level(self):
        """Test that pyspark.sql.functions works as a module (not just F namespace)."""
        from pyspark.sql import functions

        # Verify functions module has expected attributes
        assert hasattr(functions, "col")
        assert hasattr(functions, "lit")
        assert hasattr(functions, "upper")
        assert hasattr(functions, "sum")
        assert hasattr(functions, "F")

    def test_pyspark_namespace_usage(self):
        """Test that pyspark namespace can be used to create a session and DataFrame."""
        from pyspark.sql import SparkSession, functions as F
        from pyspark.sql.types import StringType, IntegerType, StructType, StructField

        # Create session using pyspark namespace
        spark = SparkSession("test_app")
        try:
            # Create DataFrame
            data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
            df = spark.createDataFrame(data)

            # Use functions from pyspark namespace
            result = df.select(F.upper(F.col("name")), F.col("age") * 2)

            # Verify it works
            rows = result.collect()
            assert len(rows) == 2
            assert rows[0][0] == "ALICE"  # upper(name)
            assert rows[0][1] == 50  # age * 2
        finally:
            spark.stop()

    def test_pyspark_get_active_session(self):
        """Test that SparkSession.getActiveSession() works from pyspark namespace."""
        from pyspark.sql import SparkSession

        # Initially no active session
        assert SparkSession.getActiveSession() is None

        # Create session
        spark = SparkSession("test_app")
        try:
            # Should return active session
            active = SparkSession.getActiveSession()
            assert active is not None
            assert active is spark
        finally:
            spark.stop()

        # After stop, should be None again
        assert SparkSession.getActiveSession() is None

    def test_pyspark_catalog_create_database(self):
        """Test that catalog.createDatabase() works with pyspark namespace."""
        from pyspark.sql import SparkSession

        spark = SparkSession("test_app")
        try:
            # Create database using catalog (should use SQL internally)
            spark.catalog.createDatabase("test_db", ignoreIfExists=True)

            # Verify database was created
            databases = spark.catalog.listDatabases()
            db_names = [db.name for db in databases]
            assert "test_db" in db_names
        finally:
            spark.stop()

    def test_pyspark_exception_handling(self):
        """Test that exceptions from pyspark.sql.utils work correctly."""
        from pyspark.sql import SparkSession
        from pyspark.sql.utils import AnalysisException

        spark = SparkSession("test_app")
        try:
            # Test AnalysisException with catalog operation
            with pytest.raises(AnalysisException):
                # Try to access non-existent database
                spark.catalog.setCurrentDatabase("non_existent_database")
        finally:
            spark.stop()

    def test_pyspark_all_functions_importable(self):
        """Test that all common functions are importable from pyspark.sql.functions."""
        from pyspark.sql.functions import (
            col, lit, when, coalesce, isnull, isnotnull,
            upper, lower, length, trim, substring, concat,
            sum, avg, count, max, min, stddev,
            to_date, to_timestamp, current_date, current_timestamp,
            array, array_contains, explode, size,
            struct, get_json_object, json_tuple,
        )

        # Verify all are callable
        assert callable(col)
        assert callable(lit)
        assert callable(upper)
        assert callable(sum)
        assert callable(to_date)

    def test_pyspark_all_types_importable(self):
        """Test that all common types are importable from pyspark.sql.types."""
        from pyspark.sql.types import (
            DataType,
            StringType, IntegerType, LongType, DoubleType, FloatType,
            BooleanType, DateType, TimestampType, DecimalType,
            ArrayType, MapType, StructType, StructField,
            BinaryType, NullType, ShortType, ByteType,
            Row,
        )

        # Verify all can be instantiated
        assert StringType() is not None
        assert IntegerType() is not None
        assert StructType([]) is not None
        assert ArrayType(StringType()) is not None

    def test_pyspark_all_exceptions_importable(self):
        """Test that all exceptions are importable from pyspark.sql.utils."""
        from pyspark.sql.utils import (
            AnalysisException,
            IllegalArgumentException,
            ParseException,
            QueryExecutionException,
            SparkUpgradeException,
            StreamingQueryException,
            TempTableAlreadyExistsException,
            UnsupportedOperationException,
        )

        # Verify all can be raised
        with pytest.raises(AnalysisException):
            raise AnalysisException("test")
        with pytest.raises(IllegalArgumentException):
            raise IllegalArgumentException("test")

    def test_pyspark_functions_module_attributes(self):
        """Test that pyspark.sql.functions module has all expected attributes."""
        from pyspark.sql import functions

        # Test F namespace
        assert hasattr(functions, "F")
        assert hasattr(functions.F, "col")
        assert hasattr(functions.F, "lit")
        assert hasattr(functions.F, "upper")

        # Test module-level functions
        assert hasattr(functions, "col")
        assert hasattr(functions, "lit")
        assert hasattr(functions, "upper")
        assert hasattr(functions, "sum")
        assert hasattr(functions, "count")

    def test_pyspark_imports_work_in_functions(self):
        """Test that pyspark imports work when used in actual functions."""
        def process_data():
            from pyspark.sql import SparkSession, functions as F
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType

            spark = SparkSession("function_test")
            try:
                schema = StructType([
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                ])
                data = [{"name": "Alice", "age": 25}]
                df = spark.createDataFrame(data, schema)
                result = df.select(F.upper(F.col("name"))).collect()
                return result[0][0]
            finally:
                spark.stop()

        result = process_data()
        assert result == "ALICE"

    def test_pyspark_imports_work_in_classes(self):
        """Test that pyspark imports work when used in class methods."""
        try:
            from pyspark.sql import SparkSession, functions as F

            class DataProcessor:
                def __init__(self):
                    self.spark = SparkSession("class_test")

                def process(self, data):
                    df = self.spark.createDataFrame(data)
                    return df.select(F.col("value") * 2).collect()

                def cleanup(self):
                    self.spark.stop()

            processor = DataProcessor()
            try:
                result = processor.process([{"value": 10}])
                assert result[0][0] == 20
            finally:
                processor.cleanup()
        except ImportError:
            pytest.skip("Real PySpark takes precedence, cannot test mock-spark namespace")

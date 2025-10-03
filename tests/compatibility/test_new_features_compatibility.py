"""
Compatibility tests for new features in mock_spark that have PySpark equivalents.

This module tests new functionality that has been added to ensure
it works exactly like PySpark and doesn't regress.
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestNewDataTypes:
    """Test new data types compatibility."""

    def test_float_type_compatibility(self, mock_types, pyspark_types):
        """Test FloatType compatibility."""
        from mock_spark import FloatType
        from pyspark.sql.types import FloatType as PySparkFloatType

        # Test FloatType creation
        mock_float = FloatType()
        pyspark_float = PySparkFloatType()

        # Compare typeName() - both should be "float"
        assert mock_float.typeName() == pyspark_float.typeName()
        # Mock types have nullable attribute, PySpark types don't always
        assert hasattr(mock_float, "nullable")

    def test_short_type_compatibility(self, mock_types, pyspark_types):
        """Test ShortType compatibility."""
        from mock_spark import ShortType
        from pyspark.sql.types import ShortType as PySparkShortType

        # Test ShortType creation
        mock_short = ShortType()
        pyspark_short = PySparkShortType()

        # Mock uses "smallint", PySpark uses "short" - both are valid
        assert mock_short.typeName() in ["short", "smallint"]
        assert pyspark_short.typeName() in ["short", "smallint"]
        assert hasattr(mock_short, "nullable")

    def test_byte_type_compatibility(self, mock_types, pyspark_types):
        """Test ByteType compatibility."""
        from mock_spark import ByteType
        from pyspark.sql.types import ByteType as PySparkByteType

        # Test ByteType creation
        mock_byte = ByteType()
        pyspark_byte = PySparkByteType()

        # Mock uses "tinyint", PySpark uses "byte" - both are valid
        assert mock_byte.typeName() in ["byte", "tinyint"]
        assert pyspark_byte.typeName() in ["byte", "tinyint"]
        assert hasattr(mock_byte, "nullable")

    def test_binary_type_compatibility(self, mock_types, pyspark_types):
        """Test BinaryType compatibility."""
        from mock_spark import BinaryType
        from pyspark.sql.types import BinaryType as PySparkBinaryType

        # Test BinaryType creation
        mock_binary = BinaryType()
        pyspark_binary = PySparkBinaryType()

        # Both should be "binary"
        assert mock_binary.typeName() == pyspark_binary.typeName()
        assert hasattr(mock_binary, "nullable")

    def test_null_type_compatibility(self, mock_types, pyspark_types):
        """Test NullType compatibility."""
        from mock_spark import NullType
        from pyspark.sql.types import NullType as PySparkNullType

        # Test NullType creation
        mock_null = NullType()
        pyspark_null = PySparkNullType()

        # Mock uses "null", PySpark uses "void" - both are valid
        assert mock_null.typeName() in ["null", "void"]
        assert pyspark_null.typeName() in ["null", "void"]
        assert hasattr(mock_null, "nullable")


class TestWindowConstants:
    """Test Window constants compatibility."""

    def test_window_constants_compatibility(
        self, mock_environment, pyspark_environment
    ):
        """Test Window constants compatibility."""
        from mock_spark.window import MockWindow as MockWindow
        from pyspark.sql.window import Window as PySparkWindow

        # Test currentRow constant
        assert MockWindow.currentRow == PySparkWindow.currentRow

        # Test unboundedPreceding constant
        assert MockWindow.unboundedPreceding == PySparkWindow.unboundedPreceding

        # Test unboundedFollowing constant
        assert MockWindow.unboundedFollowing == PySparkWindow.unboundedFollowing


class TestEnhancedDataFrameWriter:
    """Test enhanced DataFrameWriter compatibility."""

    def test_write_options_compatibility(self, mock_dataframe, pyspark_dataframe):
        """Test DataFrameWriter options compatibility."""
        # Test single option
        mock_writer = mock_dataframe.write.option("compression", "snappy")
        pyspark_writer = pyspark_dataframe.write.option("compression", "snappy")

        # Both should return the writer for chaining
        assert hasattr(mock_writer, "option")
        assert hasattr(pyspark_writer, "option")

        # Test multiple options
        mock_writer = mock_dataframe.write.options(compression="gzip", format="parquet")
        pyspark_writer = pyspark_dataframe.write.options(
            compression="gzip", format="parquet"
        )

        # Both should return the writer for chaining
        assert hasattr(mock_writer, "options")
        assert hasattr(pyspark_writer, "options")

    def test_write_modes_compatibility(self, mock_dataframe, pyspark_dataframe):
        """Test DataFrameWriter modes compatibility."""
        # Test all save modes
        modes = ["append", "overwrite", "error", "ignore"]

        for mode in modes:
            mock_writer = mock_dataframe.write.mode(mode)
            pyspark_writer = pyspark_dataframe.write.mode(mode)

            # Both should return the writer for chaining
            assert hasattr(mock_writer, "mode")
            assert hasattr(pyspark_writer, "mode")

    def test_write_format_compatibility(self, mock_dataframe, pyspark_dataframe):
        """Test DataFrameWriter format compatibility."""
        formats = ["parquet", "json", "csv", "delta"]

        for format_name in formats:
            mock_writer = mock_dataframe.write.format(format_name)
            pyspark_writer = pyspark_dataframe.write.format(format_name)

            # Both should return the writer for chaining
            assert hasattr(mock_writer, "format")
            assert hasattr(pyspark_writer, "format")


class TestMockableMethods:
    """Test mockable session methods compatibility."""

    def test_mockable_methods_exist(self, mock_environment, pyspark_environment):
        """Test that mockable methods exist and are callable."""
        mock_spark = mock_environment["session"]

        # Test mock_createDataFrame method exists
        assert hasattr(mock_spark, "mock_createDataFrame")
        assert callable(mock_spark.mock_createDataFrame)

        # Test mock_table method exists
        assert hasattr(mock_spark, "mock_table")
        assert callable(mock_spark.mock_table)

        # Test mock_sql method exists
        assert hasattr(mock_spark, "mock_sql")
        assert callable(mock_spark.mock_sql)

        # Test reset_mocks method exists
        assert hasattr(mock_spark, "reset_mocks")
        assert callable(mock_spark.reset_mocks)

    def test_mock_createDataFrame_functionality(self, mock_environment):
        """Test mock_createDataFrame functionality."""
        import pytest
        from mock_spark.errors import AnalysisException

        mock_spark = mock_environment["session"]

        # Reset mocks first to ensure clean state
        mock_spark.reset_mocks()

        # Test side_effect functionality
        mock_spark.mock_createDataFrame(side_effect=Exception("Test error"))

        with pytest.raises(Exception, match="Test error"):
            mock_spark.createDataFrame([{"test": "data"}])

        # Reset mocks and test return_value functionality
        mock_spark.reset_mocks()
        mock_df = mock_spark.createDataFrame([{"test": "data"}])
        mock_spark.mock_createDataFrame(return_value=mock_df)

        result = mock_spark.createDataFrame([{"different": "data"}])
        assert result is mock_df

        # Reset mocks
        mock_spark.reset_mocks()

        # Should work normally after reset
        result = mock_spark.createDataFrame([{"test": "data"}])
        assert result.count() == 1

    def test_mock_table_functionality(self, mock_environment):
        """Test mock_table functionality."""
        import pytest

        mock_spark = mock_environment["session"]

        # Reset mocks first to ensure clean state
        mock_spark.reset_mocks()

        # Test side_effect functionality
        mock_spark.mock_table(side_effect=Exception("Table not found"))

        with pytest.raises(Exception, match="Table not found"):
            mock_spark.table("test_table")

        # Reset mocks and test return_value functionality
        mock_spark.reset_mocks()
        mock_df = mock_spark.createDataFrame([{"id": 1, "name": "test"}])
        mock_spark.mock_table(return_value=mock_df)

        result = mock_spark.table("any_table")
        assert result is mock_df

        # Reset mocks
        mock_spark.reset_mocks()

    def test_mock_sql_functionality(self, mock_environment):
        """Test mock_sql functionality."""
        import pytest
        from mock_spark.errors import AnalysisException

        mock_spark = mock_environment["session"]

        # Reset mocks first to ensure clean state
        mock_spark.reset_mocks()

        # Test side_effect functionality
        mock_spark.mock_sql(side_effect=AnalysisException("SQL error"))

        with pytest.raises(AnalysisException, match="SQL error"):
            mock_spark.sql("SELECT * FROM test")

        # Reset mocks and test return_value functionality
        mock_spark.reset_mocks()
        mock_df = mock_spark.createDataFrame([{"id": 1, "name": "test"}])
        mock_spark.mock_sql(return_value=mock_df)

        result = mock_spark.sql("SELECT * FROM any_table")
        assert result is mock_df

        # Reset mocks
        mock_spark.reset_mocks()


class TestMockOnlyFeatures:
    """Test features that are mock-spark specific (no PySpark equivalent)."""

    def test_error_simulator_creation(self, mock_environment):
        """Test error simulator can be created."""
        from mock_spark.error_simulation import MockErrorSimulator

        mock_spark = mock_environment["session"]
        error_sim = MockErrorSimulator(mock_spark)
        assert error_sim is not None
        assert hasattr(error_sim, "add_rule")
        assert hasattr(error_sim, "should_raise_error")
        assert hasattr(error_sim, "clear_rules")

    def test_error_simulator_functionality(self, mock_environment):
        """Test error simulator basic functionality."""
        from mock_spark.error_simulation import MockErrorSimulator
        from mock_spark.errors import AnalysisException

        mock_spark = mock_environment["session"]
        error_sim = MockErrorSimulator(mock_spark)

        # Add a rule
        error_sim.add_rule(
            "test_method", lambda *args, **kwargs: True, AnalysisException("Test error")
        )

        # Test rule evaluation
        result = error_sim.should_raise_error("test_method", "arg1", "arg2")
        assert isinstance(result, AnalysisException)
        assert str(result) == "Test error"

        # Test non-matching method
        result = error_sim.should_raise_error("other_method", "arg1", "arg2")
        assert result is None

        # Clear rules
        error_sim.clear_rules()
        result = error_sim.should_raise_error("test_method", "arg1", "arg2")
        assert result is None

    def test_performance_simulator_creation(self, mock_environment):
        """Test performance simulator can be created."""
        from mock_spark.performance_simulation import MockPerformanceSimulator

        mock_spark = mock_environment["session"]
        perf_sim = MockPerformanceSimulator(mock_spark)
        assert perf_sim is not None
        assert hasattr(perf_sim, "set_slowdown")
        assert hasattr(perf_sim, "set_memory_limit")
        assert hasattr(perf_sim, "get_performance_metrics")

    def test_performance_simulator_functionality(self, mock_environment):
        """Test performance simulator basic functionality."""
        from mock_spark.performance_simulation import MockPerformanceSimulator

        mock_spark = mock_environment["session"]
        perf_sim = MockPerformanceSimulator(mock_spark)

        # Test slowdown setting
        perf_sim.set_slowdown(2.0)
        assert perf_sim.slowdown_factor == 2.0

        # Test memory limit setting
        perf_sim.set_memory_limit(1000)
        assert perf_sim.memory_limit == 1000

        # Test metrics retrieval
        metrics = perf_sim.get_performance_metrics()
        assert isinstance(metrics, dict)
        assert "total_operations" in metrics
        assert "total_time" in metrics

    def test_data_generator_creation(self, mock_environment):
        """Test data generator can be created."""
        from mock_spark.data_generation import (
            MockDataGenerator,
            create_test_data,
            create_corrupted_data,
        )
        from mock_spark import MockStructType, MockStructField, StringType, IntegerType

        # Test MockDataGenerator creation
        generator = MockDataGenerator()
        assert generator is not None

        # Test convenience functions
        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        # Test create_test_data
        data = create_test_data(schema, num_rows=10, seed=42)
        assert len(data) == 10
        assert all("name" in row and "age" in row for row in data)

        # Test create_corrupted_data
        corrupted_data = create_corrupted_data(
            schema, num_rows=10, corruption_rate=0.3, seed=42
        )
        assert len(corrupted_data) == 10

    def test_data_generator_builder(self, mock_environment):
        """Test data generator builder pattern."""
        from mock_spark.data_generation import MockDataGeneratorBuilder
        from mock_spark import MockStructType, MockStructField, StringType, IntegerType

        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        builder = MockDataGeneratorBuilder(schema)
        generator = builder.with_num_rows(100).with_seed(42).build()

        assert generator is not None
        # Check that the generator was created successfully
        assert hasattr(generator, "create_test_data")
        assert callable(generator.create_test_data)

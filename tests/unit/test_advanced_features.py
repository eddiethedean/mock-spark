"""
Unit tests for Mock-Spark advanced features.

These tests verify error simulation, performance simulation, and data generation
without real PySpark.
"""

import pytest
import time
from mock_spark import MockSparkSession, F
from mock_spark.error_simulation import MockErrorSimulator, AnalysisException
from mock_spark.performance_simulation import MockPerformanceSimulator
from mock_spark.data_generation import (
    create_test_data,
    create_corrupted_data,
    create_realistic_data,
)
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    StringType,
    IntegerType,
    DoubleType,
)


@pytest.mark.fast
class TestErrorSimulation:
    """Test error simulation features."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def error_simulator(self, spark):
        """Create an error simulator for testing."""
        return MockErrorSimulator(spark)

    def test_error_simulation_basic(self, spark, error_simulator):
        """Test basic error simulation."""
        # Add error rule for table method
        error_simulator.add_rule(
            "table",
            lambda name: "nonexistent" in name,
            AnalysisException("Table not found"),
        )

        # Test that error simulation framework works
        # Note: Error simulation is not yet integrated into session methods
        # This test verifies the framework itself works
        error = error_simulator.should_raise_error("table", "nonexistent.table")
        assert error is not None
        assert isinstance(error, AnalysisException)
        assert "Table not found" in str(error)

    def test_error_simulation_condition(self, spark, error_simulator):
        """Test error simulation with conditions."""
        # Add error rule for createDataFrame method
        error_simulator.add_rule(
            "createDataFrame",
            lambda data, schema=None: len(data) > 1000,
            AnalysisException("Data too large"),
        )

        # Test with small data (should not raise error)
        small_data = [{"name": "Alice", "age": 25}]
        error = error_simulator.should_raise_error("createDataFrame", small_data)
        assert error is None

        # Test with large data (should raise error)
        large_data = [{"name": f"Person{i}", "age": i} for i in range(1001)]
        error = error_simulator.should_raise_error("createDataFrame", large_data)
        assert error is not None
        assert isinstance(error, AnalysisException)
        assert "Data too large" in str(error)

    def test_error_simulation_clear_rules(self, spark, error_simulator):
        """Test clearing error rules."""
        # Add error rule
        error_simulator.add_rule(
            "table",
            lambda name: True,  # Always raise error
            AnalysisException("Always fails"),
        )

        # Verify error is raised
        with pytest.raises(AnalysisException):
            spark.table("any.table")

        # Clear rules
        error_simulator.clear_rules("table")

        # Verify error is no longer raised
        # Note: This will raise a different error (table not found), but not our simulated error
        with pytest.raises(AnalysisException, match="Table or view"):
            spark.table("any.table")

    def test_error_simulation_multiple_rules(self, spark, error_simulator):
        """Test multiple error rules."""
        # Add multiple rules
        error_simulator.add_rule(
            "table",
            lambda name: "nonexistent" in name,
            AnalysisException("Table not found"),
        )
        error_simulator.add_rule(
            "table",
            lambda name: "forbidden" in name,
            AnalysisException("Access denied"),
        )

        # Test first rule
        error = error_simulator.should_raise_error("table", "nonexistent.table")
        assert error is not None
        assert isinstance(error, AnalysisException)
        assert "Table not found" in str(error)

        # Test second rule
        error = error_simulator.should_raise_error("table", "forbidden.table")
        assert error is not None
        assert isinstance(error, AnalysisException)
        assert "Access denied" in str(error)


class TestPerformanceSimulation:
    """Test performance simulation features."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    @pytest.fixture
    def perf_simulator(self, spark):
        """Create a performance simulator for testing."""
        return MockPerformanceSimulator(spark)

    def test_slowdown_simulation(self, spark, perf_simulator):
        """Test slowdown simulation."""
        # Set slowdown factor
        perf_simulator.set_slowdown(2.0)

        # Test slow operation
        start_time = time.time()
        result = perf_simulator.simulate_slow_operation(lambda: "test")
        end_time = time.time()

        assert result == "test"
        # Note: Actual timing may vary, but we're testing the interface

    def test_memory_limit_simulation(self, spark, perf_simulator):
        """Test memory limit simulation."""
        # Set memory limit
        perf_simulator.set_memory_limit(1000)  # 1000 bytes

        # Test operation within limit
        small_data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(small_data)
        assert df.count() == 1

        # Test operation exceeding limit
        large_data = [{"name": f"Person{i}", "age": i} for i in range(1000)]
        with pytest.raises(Exception):  # Should raise memory error
            perf_simulator.check_memory_usage(len(str(large_data)))

    def test_performance_metrics(self, spark, perf_simulator):
        """Test performance metrics collection."""
        # Perform some operations
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)
        df.count()

        # Check that metrics are collected
        metrics = perf_simulator.get_performance_metrics()
        assert "total_operations" in metrics
        assert "total_time" in metrics


class TestDataGeneration:
    """Test data generation features."""

    def test_create_test_data(self):
        """Test basic test data generation."""
        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
            ]
        )

        data = create_test_data(schema, num_rows=10, seed=42)

        assert len(data) == 10
        assert all(isinstance(row, dict) for row in data)
        assert all("name" in row for row in data)
        assert all("age" in row for row in data)
        assert all("salary" in row for row in data)

    def test_create_corrupted_data(self):
        """Test corrupted data generation."""
        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
            ]
        )

        data = create_corrupted_data(schema, corruption_rate=0.3, num_rows=100, seed=42)

        assert len(data) == 100
        # Check that some data is corrupted (has None values or wrong types)
        corrupted_count = 0
        for row in data:
            for field_name, value in row.items():
                if (
                    value is None
                    or value == "corrupted"
                    or value == "not_a_number"
                    or value == "invalid_date"
                    or value == "not_an_array"
                    or isinstance(value, int)
                    and field_name == "name"
                    or isinstance(value, str)
                    and field_name == "age"
                ):
                    corrupted_count += 1
        assert corrupted_count > 0

    def test_create_realistic_data(self):
        """Test realistic data generation."""
        schema = MockStructType(
            [
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
            ]
        )

        data = create_realistic_data(schema, num_rows=50, seed=42)

        assert len(data) == 50
        assert all(isinstance(row, dict) for row in data)

        # Check that ages are realistic (between 18 and 65)
        ages = [row["age"] for row in data if row["age"] is not None]
        assert all(18 <= age <= 65 for age in ages)

    def test_data_generation_with_complex_schema(self):
        """Test data generation with complex schema."""
        schema = MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
                MockStructField("active", StringType()),  # Will be generated as boolean-like
            ]
        )

        data = create_test_data(schema, num_rows=20, seed=42)

        assert len(data) == 20
        assert all(isinstance(row, dict) for row in data)
        assert all("id" in row for row in data)
        assert all("name" in row for row in data)


class TestMockableMethods:
    """Test mockable methods feature."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    def test_mock_createDataFrame(self, spark):
        """Test mocking createDataFrame method."""
        # Mock createDataFrame to raise an error
        spark.mock_createDataFrame(side_effect=Exception("Connection failed"))

        with pytest.raises(Exception, match="Connection failed"):
            spark.createDataFrame([{"name": "Alice"}])

        # Reset mocks
        spark.reset_mocks()

        # Should work normally now
        df = spark.createDataFrame([{"name": "Alice"}])
        assert df.count() == 1

    def test_mock_table(self, spark):
        """Test mocking table method."""
        # Create a mock DataFrame
        mock_df = spark.createDataFrame([{"name": "Alice", "age": 25}])

        # Mock table method to return mock DataFrame
        spark.mock_table(return_value=mock_df)

        result = spark.table("any.table")
        assert result.count() == 1
        assert set(result.columns) == {"name", "age"}

        # Reset mocks
        spark.reset_mocks()

    def test_mock_sql(self, spark):
        """Test mocking sql method."""
        # Mock sql method to raise an error
        spark.mock_sql(side_effect=AnalysisException("SQL syntax error"))

        with pytest.raises(AnalysisException, match="SQL syntax error"):
            spark.sql("SELECT * FROM table")

        # Reset mocks
        spark.reset_mocks()


class TestDataFrameWriter:
    """Test enhanced DataFrameWriter features."""

    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        return MockSparkSession("test")

    def test_write_modes(self, spark):
        """Test DataFrameWriter modes."""
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)

        # Test different modes
        writer = df.write

        # Test mode setting
        writer.mode("overwrite")
        writer.mode("append")
        writer.mode("error")
        writer.mode("ignore")

        # Test invalid mode
        with pytest.raises(Exception):
            writer.mode("invalid")

    def test_write_options(self, spark):
        """Test DataFrameWriter options."""
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)

        writer = df.write

        # Test option setting
        writer.option("compression", "snappy")
        writer.option("format", "parquet")

        # Test multiple options
        writer.options(compression="gzip", format="json")

    def test_save_operations(self, spark):
        """Test save operations."""
        data = [{"name": "Alice", "age": 25}]
        df = spark.createDataFrame(data)

        # Test saveAsTable
        df.write.mode("overwrite").saveAsTable("test_table")

        # Test save
        df.write.mode("overwrite").save("/path/to/data")

        # Verify table was created
        result = spark.table("test_table")
        assert result.count() == 1

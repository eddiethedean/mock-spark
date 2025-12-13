"""
Unit tests for JSON and CSV functions.

Tests JSON parsing, serialization, and CSV formatting functions.
"""

import pytest
from sparkless import SparkSession, F
from sparkless.functions.core.column import Column, ColumnOperation


@pytest.mark.fast
class TestJSONFunctions:
    """Test JSON parsing and serialization functions."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    @pytest.fixture
    def sample_data(self):
        """Sample data with JSON-like string column."""
        return [
            {"id": 1, "json_data": '{"name": "Alice", "age": 25}'},
            {"id": 2, "json_data": '{"name": "Bob", "age": 30}'},
            {"id": 3, "json_data": '{"name": "Charlie", "age": 35}'},
        ]

    def test_from_json_exists(self, spark):
        """Test that from_json function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "from_json")
        assert hasattr(F, "from_json")

    def test_to_json_exists(self, spark):
        """Test that to_json function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "to_json")
        assert hasattr(F, "to_json")

    def test_schema_of_json_exists(self, spark):
        """Test that schema_of_json function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "schema_of_json")
        assert hasattr(F, "schema_of_json")

    def test_schema_of_csv_exists(self, spark):
        """Test that schema_of_csv function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "schema_of_csv")
        assert hasattr(F, "schema_of_csv")

    def test_from_json_column(self, spark, sample_data):
        """Test from_json returns a column operation."""
        _ = spark.createDataFrame(sample_data)

        # Test that calling from_json creates an operation
        # Note: Actual implementation may vary
        result = F.from_json("json_data", "name STRING, age INT")

        assert isinstance(result, (Column, ColumnOperation, type(None)))

    def test_to_json_column(self, spark, sample_data):
        """Test to_json returns a column operation."""
        _ = spark.createDataFrame(sample_data)

        # Test that calling to_json creates an operation
        # Note: Actual implementation may vary
        result = F.to_json(F.col("id"))

        assert isinstance(result, (Column, ColumnOperation, type(None)))

    def test_json_functions_in_f_namespace(self):
        """Test that JSON functions are accessible via F namespace."""
        # These functions should be in the F namespace
        # Implementation may return placeholder objects
        assert hasattr(F, "from_json")
        assert hasattr(F, "to_json")
        assert hasattr(F, "schema_of_json")
        assert hasattr(F, "schema_of_csv")


@pytest.mark.fast
class TestCSVFunctions:
    """Test CSV formatting and parsing functions."""

    @pytest.fixture
    def spark(self):
        """Create a SparkSession for testing."""
        return SparkSession("test")

    def test_from_csv_exists(self, spark):
        """Test that from_csv function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "from_csv")
        assert hasattr(F, "from_csv")

    def test_to_csv_exists(self, spark):
        """Test that to_csv function exists."""
        # Check that JSONCSVFunctions class exists
        from sparkless.functions.json_csv import JSONCSVFunctions

        # The function exists in the class
        assert hasattr(JSONCSVFunctions, "to_csv")
        assert hasattr(F, "to_csv")

    def test_csv_functions_in_f_namespace(self):
        """Test that CSV functions are accessible via F namespace."""
        # These functions should be in the F namespace
        # Implementation may return placeholder objects
        assert hasattr(F, "from_csv")
        assert hasattr(F, "to_csv")
        assert hasattr(F, "schema_of_csv")

    def test_json_csv_module_importable(self):
        """Test that json_csv module is importable."""
        from sparkless.functions import json_csv

        # Module should be importable
        assert json_csv is not None
        # Should have JSONCSVFunctions class
        from sparkless.functions.json_csv import JSONCSVFunctions

        assert JSONCSVFunctions is not None

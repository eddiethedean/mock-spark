"""
Compatibility tests for function operations using expected outputs.

This module validates that mock-spark functions produce the same results
as PySpark by comparing against pre-generated expected outputs.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


class TestFunctionsCompatibility:
    """Test function operation compatibility against expected PySpark outputs."""
    
    @pytest.fixture
    def spark(self):
        """Create a MockSparkSession for testing."""
        from mock_spark import MockSparkSession
        session = MockSparkSession("functions_test")
        yield session
        session.stop()
    
    def test_string_upper(self, spark):
        """Test upper function against expected outputs."""
        expected = load_expected_output("functions", "string_upper")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.upper())
        
        assert_dataframes_equal(result, expected)
    
    def test_string_lower(self, spark):
        """Test lower function against expected outputs."""
        expected = load_expected_output("functions", "string_lower")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.lower())
        
        assert_dataframes_equal(result, expected)
    
    def test_string_length(self, spark):
        """Test length function against expected outputs."""
        expected = load_expected_output("functions", "string_length")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.length())
        
        assert_dataframes_equal(result, expected)
    
    def test_string_substring(self, spark):
        """Test substring function against expected outputs."""
        expected = load_expected_output("functions", "string_substring")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.substring(1, 3))
        
        assert_dataframes_equal(result, expected)
    
    def test_string_concat(self, spark):
        """Test concat function against expected outputs."""
        expected = load_expected_output("functions", "string_concat")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.name.concat(" - ").concat(df.email))
        
        assert_dataframes_equal(result, expected)
    
    def test_string_split(self, spark):
        """Test split function against expected outputs."""
        expected = load_expected_output("functions", "string_split")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.email.split("@"))
        
        assert_dataframes_equal(result, expected)
    
    def test_string_regexp_extract(self, spark):
        """Test regexp_extract function against expected outputs."""
        expected = load_expected_output("functions", "string_regexp_extract")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.email.regexp_extract(r"@(.+)", 1))
        
        assert_dataframes_equal(result, expected)
    
    def test_math_abs(self, spark):
        """Test abs function against expected outputs."""
        expected = load_expected_output("functions", "math_abs")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.salary.abs())
        
        assert_dataframes_equal(result, expected)
    
    def test_math_round(self, spark):
        """Test round function against expected outputs."""
        expected = load_expected_output("functions", "math_round")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.salary.round(-3))
        
        assert_dataframes_equal(result, expected)
    
    def test_math_sqrt(self, spark):
        """Test sqrt function against expected outputs."""
        expected = load_expected_output("functions", "math_sqrt")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.salary.sqrt())
        
        assert_dataframes_equal(result, expected)
    
    def test_math_pow(self, spark):
        """Test pow function against expected outputs."""
        expected = load_expected_output("functions", "math_pow")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.age.pow(2))
        
        assert_dataframes_equal(result, expected)
    
    def test_math_log(self, spark):
        """Test log function against expected outputs."""
        expected = load_expected_output("functions", "math_log")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(df.salary.log())
        
        assert_dataframes_equal(result, expected)
    
    def test_math_exp(self, spark):
        """Test exp function against expected outputs."""
        expected = load_expected_output("functions", "math_exp")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(1.0.exp())
        
        assert_dataframes_equal(result, expected)
    
    def test_agg_sum(self, spark):
        """Test sum aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_sum")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(df.salary.sum())
        
        assert_dataframes_equal(result, expected)
    
    def test_agg_avg(self, spark):
        """Test avg aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_avg")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(df.salary.avg())
        
        assert_dataframes_equal(result, expected)
    
    def test_agg_count(self, spark):
        """Test count aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_count")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(df.id.count())
        
        assert_dataframes_equal(result, expected)
    
    def test_agg_max(self, spark):
        """Test max aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_max")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(df.salary.max())
        
        assert_dataframes_equal(result, expected)
    
    def test_agg_min(self, spark):
        """Test min aggregation against expected outputs."""
        expected = load_expected_output("functions", "agg_min")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("active").agg(df.salary.min())
        
        assert_dataframes_equal(result, expected)

"""
API parity tests for string operations.

Tests string functions between MockSpark and PySpark
to ensure identical behavior and results.
"""

import pytest
from tests.api_parity.conftest import ParityTestBase, compare_dataframes


class TestStringOperations(ParityTestBase):
    """Test string operations for API parity."""

    def test_upper_function(self, mock_spark, pyspark_spark, string_data):
        """Test upper string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.upper("text").alias("upper_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.upper("text").alias("upper_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_lower_function(self, mock_spark, pyspark_spark, string_data):
        """Test lower string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.lower("text").alias("lower_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.lower("text").alias("lower_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_length_function(self, mock_spark, pyspark_spark, string_data):
        """Test length string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.length("text").alias("text_length"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.length("text").alias("text_length"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_trim_function(self, mock_spark, pyspark_spark):
        """Test trim string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        data = [
            {"id": 1, "text": "  Hello World  "},
            {"id": 2, "text": "\tMock Spark\n"},
            {"id": 3, "text": "  API Parity  "},
        ]
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select("text", MockF.trim("text").alias("trimmed_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select("text", PySparkF.trim("text").alias("trimmed_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_ltrim_function(self, mock_spark, pyspark_spark):
        """Test ltrim string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        data = [
            {"id": 1, "text": "  Hello World"},
            {"id": 2, "text": "\tMock Spark"},
            {"id": 3, "text": "  API Parity"},
        ]
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select("text", MockF.ltrim("text").alias("ltrimmed_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select("text", PySparkF.ltrim("text").alias("ltrimmed_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_rtrim_function(self, mock_spark, pyspark_spark):
        """Test rtrim string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        data = [
            {"id": 1, "text": "Hello World  "},
            {"id": 2, "text": "Mock Spark\n"},
            {"id": 3, "text": "API Parity  "},
        ]
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select("text", MockF.rtrim("text").alias("rtrimmed_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select("text", PySparkF.rtrim("text").alias("rtrimmed_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_substring_function(self, mock_spark, pyspark_spark, string_data):
        """Test substring string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.substring("text", 1, 5).alias("substring_1_5"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.substring("text", 1, 5).alias("substring_1_5"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_concat_function(self, mock_spark, pyspark_spark, string_data):
        """Test concat string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", "email", MockF.concat("text", " - ", "email").alias("concatenated"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", "email", PySparkF.concat("text", " - ", "email").alias("concatenated"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_concat_ws_function(self, mock_spark, pyspark_spark, string_data):
        """Test concat_ws string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", "email", MockF.concat_ws(" - ", "text", "email").alias("concatenated_ws"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", "email", PySparkF.concat_ws(" - ", "text", "email").alias("concatenated_ws"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_split_function(self, mock_spark, pyspark_spark, string_data):
        """Test split string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.split("text", " ").alias("split_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.split("text", " ").alias("split_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_regexp_replace_function(self, mock_spark, pyspark_spark, string_data):
        """Test regexp_replace string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.regexp_replace("text", "o", "0").alias("replaced_text"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.regexp_replace("text", "o", "0").alias("replaced_text"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_regexp_extract_function(self, mock_spark, pyspark_spark, string_data):
        """Test regexp_extract string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select("text", MockF.regexp_extract("text", r"(\w+)", 1).alias("extracted_word"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select("text", PySparkF.regexp_extract("text", r"(\w+)", 1).alias("extracted_word"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_like_function(self, mock_spark, pyspark_spark, string_data):
        """Test like string function."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(mock_df.text.like("%World%"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(pyspark_df.text.like("%World%"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_rlike_function(self, mock_spark, pyspark_spark, string_data):
        """Test rlike string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(MockF.rlike("text", r"^[A-Z]"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(PySparkF.rlike("text", r"^[A-Z]"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_startswith_function(self, mock_spark, pyspark_spark, string_data):
        """Test startswith string function."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(mock_df.text.startswith("Hello"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(pyspark_df.text.startswith("Hello"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_endswith_function(self, mock_spark, pyspark_spark, string_data):
        """Test endswith string function."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(mock_df.text.endswith("World"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(pyspark_df.text.endswith("World"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_contains_function(self, mock_spark, pyspark_spark, string_data):
        """Test contains string function."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(mock_df.text.contains("Mock"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(pyspark_df.text.contains("Mock"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_isin_function(self, mock_spark, pyspark_spark, string_data):
        """Test isin string function."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.filter(mock_df.text.isin(["Hello World", "Mock Spark"]))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.filter(pyspark_df.text.isin(["Hello World", "Mock Spark"]))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_when_otherwise_function(self, mock_spark, pyspark_spark, string_data):
        """Test when/otherwise string function."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select(
            "text",
            MockF.when(MockF.length("text") > 10, "Long").otherwise("Short").alias("text_length_category")
        )
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select(
            "text",
            PySparkF.when(PySparkF.length("text") > 10, "Long").otherwise("Short").alias("text_length_category")
        )
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_coalesce_function(self, mock_spark, pyspark_spark):
        """Test coalesce function with null values."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        data = [
            {"id": 1, "text1": "Hello", "text2": None, "text3": "World"},
            {"id": 2, "text1": None, "text2": "Mock", "text3": None},
            {"id": 3, "text1": None, "text2": None, "text3": "Spark"},
        ]
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select("id", MockF.coalesce("text1", "text2", "text3").alias("coalesced"))
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data)
        pyspark_result = pyspark_df.select("id", PySparkF.coalesce("text1", "text2", "text3").alias("coalesced"))
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_complex_string_operations(self, mock_spark, pyspark_spark, string_data):
        """Test complex string operations combining multiple functions."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF
        
        # MockSpark
        mock_df = mock_spark.createDataFrame(string_data)
        mock_result = mock_df.select(
            "text",
            MockF.upper("text").alias("upper_text"),
            MockF.length("text").alias("text_length"),
            MockF.substring("text", 1, 5).alias("first_5_chars"),
            MockF.concat("text", " - ", "email").alias("full_info")
        )
        
        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(string_data)
        pyspark_result = pyspark_df.select(
            "text",
            PySparkF.upper("text").alias("upper_text"),
            PySparkF.length("text").alias("text_length"),
            PySparkF.substring("text", 1, 5).alias("first_5_chars"),
            PySparkF.concat("text", " - ", "email").alias("full_info")
        )
        
        # Compare
        compare_dataframes(mock_result, pyspark_result)

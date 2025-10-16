"""Compatibility tests for PySpark 3.0 and 3.1 features.

These tests verify that mock-spark's implementation of PySpark 3.0/3.1 functions
behaves identically to real PySpark.
"""

import pytest
import math
from decimal import Decimal

# Skip all tests if PySpark is not installed
pytest.importorskip("pyspark")

from pyspark.sql import SparkSession as RealSparkSession
from pyspark.sql import functions as F_real
from mock_spark import MockSparkSession
from mock_spark import functions as F_mock


# Helper to check if function exists in PySpark
def pyspark_has_function(func_name):
    """Check if a function exists in the installed PySpark version."""
    return hasattr(F_real, func_name)


@pytest.fixture(scope="module")
def real_spark():
    """Create real PySpark session."""
    spark = RealSparkSession.builder \
        .appName("compatibility-test-3.0-3.1") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def mock_spark():
    """Create mock Spark session."""
    spark = MockSparkSession("compatibility-test-3.0-3.1")
    yield spark
    spark.stop()


class TestHyperbolicMathCompat:
    """Test hyperbolic math functions (PySpark 3.0) match real PySpark."""
    
    def test_acosh_compat(self, real_spark, mock_spark):
        """Test acosh produces identical results."""
        data = [{"id": 1, "value": 1.5}, {"id": 2, "value": 2.0}, {"id": 3, "value": 10.0}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.col("id"),
            F_real.acosh(F_real.col("value")).alias("result")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.col("id"),
            F_mock.acosh(F_mock.col("value")).alias("result")
        ).collect()
        
        # Compare results
        for i in range(len(data)):
            assert real_result[i]["id"] == mock_result[i]["id"]
            assert abs(real_result[i]["result"] - mock_result[i]["result"]) < 0.0001
    
    def test_asinh_compat(self, real_spark, mock_spark):
        """Test asinh produces identical results."""
        data = [{"id": 1, "value": 0.5}, {"id": 2, "value": 1.0}, {"id": 3, "value": 5.0}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.col("id"),
            F_real.asinh(F_real.col("value")).alias("result")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.col("id"),
            F_mock.asinh(F_mock.col("value")).alias("result")
        ).collect()
        
        # Compare results
        for i in range(len(data)):
            assert real_result[i]["id"] == mock_result[i]["id"]
            assert abs(real_result[i]["result"] - mock_result[i]["result"]) < 0.0001
    
    def test_atanh_compat(self, real_spark, mock_spark):
        """Test atanh produces identical results."""
        data = [{"id": 1, "value": 0.2}, {"id": 2, "value": 0.5}, {"id": 3, "value": 0.9}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.col("id"),
            F_real.atanh(F_real.col("value")).alias("result")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.col("id"),
            F_mock.atanh(F_mock.col("value")).alias("result")
        ).collect()
        
        # Compare results
        for i in range(len(data)):
            assert real_result[i]["id"] == mock_result[i]["id"]
            assert abs(real_result[i]["result"] - mock_result[i]["result"]) < 0.0001


class TestUtilityFunctionsCompat:
    """Test utility functions match real PySpark."""
    
    @pytest.mark.skipif(not pyspark_has_function('make_date'), 
                        reason="make_date() added in PySpark 3.3")
    def test_make_date_compat(self, real_spark, mock_spark):
        """Test make_date produces identical results (PySpark 3.3+)."""
        data = [
            {"id": 1, "year": 2024, "month": 3, "day": 15},
            {"id": 2, "year": 2023, "month": 12, "day": 31},
            {"id": 3, "year": 2025, "month": 1, "day": 1}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.col("id"),
            F_real.make_date(F_real.col("year"), F_real.col("month"), F_real.col("day")).alias("date")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.col("id"),
            F_mock.make_date(F_mock.col("year"), F_mock.col("month"), F_mock.col("day")).alias("date")
        ).collect()
        
        # Compare results
        for i in range(len(data)):
            assert real_result[i]["id"] == mock_result[i]["id"]
            assert str(real_result[i]["date"]) == str(mock_result[i]["date"])
    
    @pytest.mark.skipif(not pyspark_has_function('version'), 
                        reason="version() added in PySpark 3.5")
    def test_version_compat(self, real_spark, mock_spark):
        """Test version returns a non-empty string (PySpark 3.5+)."""
        # Real PySpark
        real_df = real_spark.createDataFrame([{"id": 1}])
        real_result = real_df.select(F_real.version().alias("version")).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame([{"id": 1}])
        mock_result = mock_df.select(F_mock.version().alias("version")).collect()
        
        # Both should return non-empty version strings
        assert real_result[0]["version"] is not None
        assert mock_result[0]["version"] is not None
        assert isinstance(real_result[0]["version"], str)
        assert isinstance(mock_result[0]["version"], str)
        assert len(real_result[0]["version"]) > 0
        assert len(mock_result[0]["version"]) > 0
        # Note: Actual version strings will differ (PySpark vs mock-spark)


class TestBooleanAggregatesCompat:
    """Test boolean aggregate functions (PySpark 3.5) match real PySpark."""
    
    @pytest.mark.skipif(not pyspark_has_function('bool_and'), 
                        reason="bool_and() added in PySpark 3.5")
    def test_bool_and_compat(self, real_spark, mock_spark):
        """Test bool_and produces identical results (PySpark 3.5+)."""
        data = [
            {"group": "A", "flag": True},
            {"group": "A", "flag": True},
            {"group": "B", "flag": True},
            {"group": "B", "flag": False}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.groupBy("group").agg(
            F_real.bool_and(F_real.col("flag")).alias("all_true")
        ).orderBy("group").collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.groupBy("group").agg(
            F_mock.bool_and(F_mock.col("flag")).alias("all_true")
        ).collect()
        mock_result = sorted(mock_result, key=lambda r: r["group"])
        
        # Compare results
        for i in range(len(real_result)):
            assert real_result[i]["group"] == mock_result[i]["group"]
            assert real_result[i]["all_true"] == mock_result[i]["all_true"]
    
    @pytest.mark.skipif(not pyspark_has_function('bool_or'), 
                        reason="bool_or() added in PySpark 3.5")
    def test_bool_or_compat(self, real_spark, mock_spark):
        """Test bool_or produces identical results (PySpark 3.5+)."""
        data = [
            {"group": "A", "flag": False},
            {"group": "A", "flag": False},
            {"group": "B", "flag": False},
            {"group": "B", "flag": True}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.groupBy("group").agg(
            F_real.bool_or(F_real.col("flag")).alias("any_true")
        ).orderBy("group").collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.groupBy("group").agg(
            F_mock.bool_or(F_mock.col("flag")).alias("any_true")
        ).collect()
        mock_result = sorted(mock_result, key=lambda r: r["group"])
        
        # Compare results
        for i in range(len(real_result)):
            assert real_result[i]["group"] == mock_result[i]["group"]
            assert real_result[i]["any_true"] == mock_result[i]["any_true"]
    
    @pytest.mark.skipif(not pyspark_has_function('every'), 
                        reason="every() added in PySpark 3.5")
    def test_every_alias_compat(self, real_spark, mock_spark):
        """Test every (alias for bool_and) produces identical results (PySpark 3.5+)."""
        data = [{"flag": True}, {"flag": True}, {"flag": False}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(F_real.every(F_real.col("flag")).alias("result")).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(F_mock.every(F_mock.col("flag")).alias("result")).collect()
        
        # Compare results
        assert real_result[0]["result"] == mock_result[0]["result"]
    
    @pytest.mark.skipif(not pyspark_has_function('some'), 
                        reason="some() added in PySpark 3.5")
    def test_some_alias_compat(self, real_spark, mock_spark):
        """Test some (alias for bool_or) produces identical results (PySpark 3.5+)."""
        data = [{"flag": False}, {"flag": False}, {"flag": True}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(F_real.some(F_real.col("flag")).alias("result")).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(F_mock.some(F_mock.col("flag")).alias("result")).collect()
        
        # Compare results
        assert real_result[0]["result"] == mock_result[0]["result"]


class TestAdvancedAggregatesCompat:
    """Test advanced aggregate functions (PySpark 3.3 & 3.5) match real PySpark."""
    
    @pytest.mark.skipif(not pyspark_has_function('max_by'), 
                        reason="max_by() added in PySpark 3.3")
    def test_max_by_compat(self, real_spark, mock_spark):
        """Test max_by produces identical results (PySpark 3.3+)."""
        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(
            F_real.max_by(F_real.col("name"), F_real.col("score")).alias("top_scorer")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(
            F_mock.max_by(F_mock.col("name"), F_mock.col("score")).alias("top_scorer")
        ).collect()
        
        # Compare results
        assert real_result[0]["top_scorer"] == mock_result[0]["top_scorer"]
    
    @pytest.mark.skipif(not pyspark_has_function('min_by'), 
                        reason="min_by() added in PySpark 3.3")
    def test_min_by_compat(self, real_spark, mock_spark):
        """Test min_by produces identical results (PySpark 3.3+)."""
        data = [
            {"name": "Alice", "score": 95},
            {"name": "Bob", "score": 87},
            {"name": "Charlie", "score": 92}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(
            F_real.min_by(F_real.col("name"), F_real.col("score")).alias("lowest_scorer")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(
            F_mock.min_by(F_mock.col("name"), F_mock.col("score")).alias("lowest_scorer")
        ).collect()
        
        # Compare results
        assert real_result[0]["lowest_scorer"] == mock_result[0]["lowest_scorer"]
    
    @pytest.mark.skipif(not pyspark_has_function('count_if'), 
                        reason="count_if() added in PySpark 3.5")
    def test_count_if_compat(self, real_spark, mock_spark):
        """Test count_if produces identical results (PySpark 3.5+)."""
        data = [
            {"value": 10},
            {"value": 25},
            {"value": 30},
            {"value": 15}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(
            F_real.count_if(F_real.col("value") > 20).alias("count_above_20")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(
            F_mock.count_if(F_mock.col("value") > 20).alias("count_above_20")
        ).collect()
        
        # Compare results
        assert real_result[0]["count_above_20"] == mock_result[0]["count_above_20"]
    
    @pytest.mark.skipif(not pyspark_has_function('any_value'), 
                        reason="any_value() added in PySpark 3.5")
    def test_any_value_compat(self, real_spark, mock_spark):
        """Test any_value returns a value from the dataset (PySpark 3.5+, non-deterministic)."""
        data = [
            {"group": "A", "value": 10},
            {"group": "A", "value": 20},
            {"group": "B", "value": 30}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.groupBy("group").agg(
            F_real.any_value(F_real.col("value")).alias("some_value")
        ).orderBy("group").collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.groupBy("group").agg(
            F_mock.any_value(F_mock.col("value")).alias("some_value")
        ).collect()
        mock_result = sorted(mock_result, key=lambda r: r["group"])
        
        # Compare - values should be from the dataset (non-deterministic selection)
        for i in range(len(real_result)):
            assert real_result[i]["group"] == mock_result[i]["group"]
            # Both should return a value that exists in the group
            group_values = [d["value"] for d in data if d["group"] == real_result[i]["group"]]
            assert real_result[i]["some_value"] in group_values
            assert mock_result[i]["some_value"] in group_values


class TestGroupedAggregatesCompat:
    """Test aggregate functions work correctly with groupBy."""
    
    @pytest.mark.skipif(not pyspark_has_function('max_by'), 
                        reason="max_by() added in PySpark 3.3")
    def test_max_by_grouped_compat(self, real_spark, mock_spark):
        """Test max_by with groupBy produces identical results (PySpark 3.3+)."""
        data = [
            {"dept": "IT", "name": "Alice", "salary": 95000},
            {"dept": "IT", "name": "Bob", "salary": 87000},
            {"dept": "HR", "name": "Charlie", "salary": 65000},
            {"dept": "HR", "name": "Diana", "salary": 72000}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.groupBy("dept").agg(
            F_real.max_by(F_real.col("name"), F_real.col("salary")).alias("top_earner")
        ).orderBy("dept").collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.groupBy("dept").agg(
            F_mock.max_by(F_mock.col("name"), F_mock.col("salary")).alias("top_earner")
        ).collect()
        mock_result = sorted(mock_result, key=lambda r: r["dept"])
        
        # Compare results
        for i in range(len(real_result)):
            assert real_result[i]["dept"] == mock_result[i]["dept"]
            assert real_result[i]["top_earner"] == mock_result[i]["top_earner"]
    
    @pytest.mark.skipif(not pyspark_has_function('count_if'), 
                        reason="count_if() added in PySpark 3.5")
    def test_count_if_grouped_compat(self, real_spark, mock_spark):
        """Test count_if with groupBy produces identical results (PySpark 3.5+)."""
        data = [
            {"category": "A", "value": 10},
            {"category": "A", "value": 25},
            {"category": "A", "value": 30},
            {"category": "B", "value": 15},
            {"category": "B", "value": 5}
        ]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.groupBy("category").agg(
            F_real.count_if(F_real.col("value") > 20).alias("count_above_20")
        ).orderBy("category").collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.groupBy("category").agg(
            F_mock.count_if(F_mock.col("value") > 20).alias("count_above_20")
        ).collect()
        mock_result = sorted(mock_result, key=lambda r: r["category"])
        
        # Compare results
        for i in range(len(real_result)):
            assert real_result[i]["category"] == mock_result[i]["category"]
            assert real_result[i]["count_above_20"] == mock_result[i]["count_above_20"]


class TestEdgeCasesCompat:
    """Test edge cases and boundary conditions."""
    
    def test_acosh_minimum_value(self, real_spark, mock_spark):
        """Test acosh(1.0) = 0.0 (minimum valid input)."""
        data = [{"value": 1.0}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(F_real.acosh(F_real.col("value")).alias("result")).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(F_mock.acosh(F_mock.col("value")).alias("result")).collect()
        
        # Both should return 0.0
        assert abs(real_result[0]["result"]) < 0.0001
        assert abs(mock_result[0]["result"]) < 0.0001
    
    @pytest.mark.skipif(not pyspark_has_function('bool_and'), 
                        reason="bool_and() added in PySpark 3.5")
    def test_bool_and_empty_group(self, real_spark, mock_spark):
        """Test bool_and with all false values (PySpark 3.5+)."""
        data = [{"flag": False}, {"flag": False}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(F_real.bool_and(F_real.col("flag")).alias("result")).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(F_mock.bool_and(F_mock.col("flag")).alias("result")).collect()
        
        # Both should return False
        assert real_result[0]["result"] == False
        assert mock_result[0]["result"] == False
    
    @pytest.mark.skipif(not pyspark_has_function('count_if'), 
                        reason="count_if() added in PySpark 3.5")
    def test_count_if_no_matches(self, real_spark, mock_spark):
        """Test count_if when no rows match condition (PySpark 3.5+)."""
        data = [{"value": 5}, {"value": 10}, {"value": 15}]
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.agg(
            F_real.count_if(F_real.col("value") > 100).alias("count")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.agg(
            F_mock.count_if(F_mock.col("value") > 100).alias("count")
        ).collect()
        
        # Both should return 0
        assert real_result[0]["count"] == 0
        assert mock_result[0]["count"] == 0
    
    @pytest.mark.skipif(not pyspark_has_function('make_date'), 
                        reason="make_date() added in PySpark 3.3")
    def test_make_date_leap_year(self, real_spark, mock_spark):
        """Test make_date with leap year date (PySpark 3.3+)."""
        data = [{"year": 2024, "month": 2, "day": 29}]  # Leap year
        
        # Real PySpark
        real_df = real_spark.createDataFrame(data)
        real_result = real_df.select(
            F_real.make_date(F_real.col("year"), F_real.col("month"), F_real.col("day")).alias("date")
        ).collect()
        
        # Mock Spark
        mock_df = mock_spark.createDataFrame(data)
        mock_result = mock_df.select(
            F_mock.make_date(F_mock.col("year"), F_mock.col("month"), F_mock.col("day")).alias("date")
        ).collect()
        
        # Both should handle leap year correctly
        assert str(real_result[0]["date"]) == "2024-02-29"
        assert str(mock_result[0]["date"]) == "2024-02-29"


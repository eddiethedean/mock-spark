"""
Tests for newly implemented critical functions and methods.

This test file covers the new implementations from the critical functions plan:
- DataFrame method aliases (where, sort, toDF, groupby, drop_duplicates, unionAll, subtract, alias, withColumns)
- Statistical functions (median, mode, percentile)
- String functions (char_length, character_length)
- DateTime functions (weekday, extract)
- Conditional functions (ifnull, nullif)
- Array functions (array_agg, cardinality)
- Math functions (cot, csc, sec, e, pi, ln)
- Bitwise functions (bit_and, bit_or, bit_xor)
- Hash functions (xxhash64)
- Advanced DataFrame methods (approxQuantile, cov, crosstab, freqItems, hint, intersectAll, isEmpty, sampleBy, withColumnsRenamed)
"""

import pytest
from mock_spark import MockSparkSession
from mock_spark.functions import F


@pytest.fixture
def spark():
    """Create a mock spark session for testing."""
    return MockSparkSession("test_new_functions")


class TestDataFrameMethodAliases:
    """Test DataFrame method aliases."""

    def test_where_alias(self, spark):
        """Test where() as alias for filter()."""
        df = spark.createDataFrame([{"age": 25}, {"age": 30}, {"age": 35}])
        result = df.where(F.col("age") > 27)
        assert result.count() == 2

    def test_sort_alias(self, spark):
        """Test sort() as alias for orderBy()."""
        df = spark.createDataFrame([{"age": 30}, {"age": 25}, {"age": 35}])
        result = df.sort("age")
        data = result.collect()
        assert data[0]["age"] == 25
        assert data[2]["age"] == 35

    def test_toDF(self, spark):
        """Test toDF() to rename columns."""
        df = spark.createDataFrame([{"a": 1, "b": 2}])
        result = df.toDF("x", "y")
        assert "x" in result.columns
        assert "y" in result.columns
        assert "a" not in result.columns

    def test_groupby_lowercase(self, spark):
        """Test groupby() as lowercase alias."""
        df = spark.createDataFrame([{"dept": "IT", "count": 1}, {"dept": "IT", "count": 2}])
        result = df.groupby("dept").count()
        assert result.count() == 1

    def test_drop_duplicates_alias(self, spark):
        """Test drop_duplicates() alias."""
        df = spark.createDataFrame([{"a": 1}, {"a": 1}, {"a": 2}])
        result = df.drop_duplicates()
        assert result.count() == 2

    def test_unionAll_deprecated(self, spark):
        """Test unionAll() deprecated alias."""
        df1 = spark.createDataFrame([{"a": 1}])
        df2 = spark.createDataFrame([{"a": 2}])
        with pytest.warns(FutureWarning, match="unionAll is deprecated"):
            result = df1.unionAll(df2)
        assert result.count() == 2

    def test_subtract(self, spark):
        """Test subtract() set difference."""
        df1 = spark.createDataFrame([{"a": 1}, {"a": 2}, {"a": 3}])
        df2 = spark.createDataFrame([{"a": 2}])
        result = df1.subtract(df2)
        assert result.count() == 2
        values = [r["a"] for r in result.collect()]
        assert 2 not in values

    def test_alias(self, spark):
        """Test alias() for DataFrame."""
        df = spark.createDataFrame([{"a": 1}])
        result = df.alias("df1")
        assert hasattr(result, "_alias")
        assert result._alias == "df1"

    def test_withColumns(self, spark):
        """Test withColumns() to add multiple columns."""
        df = spark.createDataFrame([{"a": 1}])
        result = df.withColumns({
            "b": F.col("a") * 2,
            "c": F.col("a") * 3
        })
        assert "b" in result.columns
        assert "c" in result.columns
        row = result.collect()[0]
        assert row["b"] == 2
        assert row["c"] == 3


class TestAdvancedDataFrameMethods:
    """Test advanced DataFrame methods."""

    def test_approxQuantile(self, spark):
        """Test approxQuantile()."""
        df = spark.createDataFrame([{"value": i} for i in range(1, 101)])
        quantiles = df.approxQuantile("value", [0.25, 0.5, 0.75], 0.0)
        assert len(quantiles) == 3
        assert 20 < quantiles[0] < 30  # Q1
        assert 45 < quantiles[1] < 55  # Median
        assert 70 < quantiles[2] < 80  # Q3

    def test_cov(self, spark):
        """Test cov() covariance calculation."""
        df = spark.createDataFrame([
            {"x": 1, "y": 2},
            {"x": 2, "y": 4},
            {"x": 3, "y": 6}
        ])
        cov_value = df.cov("x", "y")
        assert cov_value > 0  # Positive covariance for positive correlation

    def test_crosstab(self, spark):
        """Test crosstab() cross-tabulation."""
        df = spark.createDataFrame([
            {"age": "young", "gender": "M"},
            {"age": "young", "gender": "F"},
            {"age": "old", "gender": "M"}
        ])
        result = df.crosstab("age", "gender")
        assert result.count() == 2  # Two age groups

    def test_freqItems(self, spark):
        """Test freqItems() frequent items detection."""
        df = spark.createDataFrame([{"a": i % 3} for i in range(100)])
        result = df.freqItems(["a"], 0.2)
        assert result.count() == 1

    def test_hint(self, spark):
        """Test hint() optimization hints (no-op)."""
        df = spark.createDataFrame([{"a": 1}])
        result = df.hint("broadcast")
        assert result.count() == 1  # Should return same DataFrame

    def test_intersectAll(self, spark):
        """Test intersectAll() with duplicates."""
        df1 = spark.createDataFrame([{"a": 1}, {"a": 1}, {"a": 2}])
        df2 = spark.createDataFrame([{"a": 1}, {"a": 1}, {"a": 1}])
        result = df1.intersectAll(df2)
        assert result.count() == 2  # Minimum of 2 and 3 occurrences of 1

    def test_isEmpty(self, spark):
        """Test isEmpty() check."""
        df_empty = spark.createDataFrame([])
        df_not_empty = spark.createDataFrame([{"a": 1}])
        assert df_empty.isEmpty() == True
        assert df_not_empty.isEmpty() == False

    def test_sampleBy(self, spark):
        """Test sampleBy() stratified sampling."""
        df = spark.createDataFrame([{"group": "A"} for _ in range(100)] + [{"group": "B"} for _ in range(100)])
        result = df.sampleBy("group", {"A": 0.5, "B": 0.5}, seed=42)
        # With seed, should be deterministic
        count = result.count()
        assert 50 < count < 150  # Should sample around 100 total

    def test_withColumnsRenamed(self, spark):
        """Test withColumnsRenamed() multiple column rename."""
        df = spark.createDataFrame([{"a": 1, "b": 2, "c": 3}])
        result = df.withColumnsRenamed({"a": "x", "b": "y"})
        assert "x" in result.columns
        assert "y" in result.columns
        assert "c" in result.columns
        assert "a" not in result.columns


class TestStatisticalFunctions:
    """Test statistical aggregate functions."""

    def test_median(self, spark):
        """Test median() aggregate function."""
        df = spark.createDataFrame([{"value": i} for i in [1, 2, 3, 4, 5]])
        # Note: median needs to be handled in aggregation, this just tests the function exists
        median_func = F.median("value")
        assert median_func.function_name == "median"

    def test_mode(self, spark):
        """Test mode() aggregate function."""
        df = spark.createDataFrame([{"value": 1}, {"value": 1}, {"value": 2}])
        mode_func = F.mode("value")
        assert mode_func.function_name == "mode"

    def test_percentile(self, spark):
        """Test percentile() aggregate function."""
        df = spark.createDataFrame([{"value": i} for i in range(1, 11)])
        percentile_func = F.percentile("value", 0.5)
        assert percentile_func.function_name == "percentile"
        assert percentile_func.percentage == 0.5


class TestStringFunctions:
    """Test string functions."""

    def test_char_length(self, spark):
        """Test char_length() as length alias."""
        df = spark.createDataFrame([{"text": "hello"}])
        result = df.select(F.char_length("text").alias("len"))
        # Note: This tests the function is created properly
        assert "len" in result.columns

    def test_character_length(self, spark):
        """Test character_length() as length alias."""
        df = spark.createDataFrame([{"text": "world"}])
        result = df.select(F.character_length("text").alias("len"))
        assert "len" in result.columns

    def test_xxhash64(self, spark):
        """Test xxhash64() hash function."""
        df = spark.createDataFrame([{"text": "test"}])
        result = df.select(F.xxhash64("text").alias("hash"))
        assert "hash" in result.columns


class TestDateTimeFunctions:
    """Test datetime functions."""

    def test_weekday(self, spark):
        """Test weekday() function."""
        df = spark.createDataFrame([{"date": "2024-01-15"}])  # Monday
        result = df.select(F.weekday(F.col("date")).alias("weekday"))
        assert "weekday" in result.columns

    def test_extract(self, spark):
        """Test extract() function."""
        df = spark.createDataFrame([{"date": "2024-01-15"}])
        result = df.select(F.extract("YEAR", "date").alias("year"))
        assert "year" in result.columns


class TestConditionalFunctions:
    """Test conditional/null functions."""

    def test_ifnull(self, spark):
        """Test ifnull() function."""
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType
        schema = MockStructType([
            MockStructField("a", IntegerType(), nullable=True),
            MockStructField("b", IntegerType(), nullable=False)
        ])
        df = spark.createDataFrame([{"a": None, "b": 10}], schema=schema)
        result = df.select(F.ifnull("a", "b").alias("result"))
        assert "result" in result.columns

    def test_nullif(self, spark):
        """Test nullif() function."""
        df = spark.createDataFrame([{"a": 5, "b": 5}])
        result = df.select(F.nullif("a", "b").alias("result"))
        assert "result" in result.columns


class TestArrayFunctions:
    """Test array functions."""

    def test_array_agg(self, spark):
        """Test array_agg() aggregate function."""
        df = spark.createDataFrame([{"dept": "IT", "name": "Alice"}, {"dept": "IT", "name": "Bob"}])
        agg_func = F.array_agg("name")
        assert agg_func.function_name == "array_agg"

    def test_cardinality(self, spark):
        """Test cardinality() function."""
        df = spark.createDataFrame([{"arr": [1, 2, 3, 4, 5]}])
        result = df.select(F.cardinality("arr").alias("size"))
        assert "size" in result.columns


class TestMathFunctions:
    """Test math functions."""

    def test_cot(self, spark):
        """Test cot() cotangent function."""
        df = spark.createDataFrame([{"angle": 1.57}])
        result = df.select(F.cot("angle").alias("cot_val"))
        assert "cot_val" in result.columns

    def test_csc(self, spark):
        """Test csc() cosecant function."""
        df = spark.createDataFrame([{"angle": 1.57}])
        result = df.select(F.csc("angle").alias("csc_val"))
        assert "csc_val" in result.columns

    def test_sec(self, spark):
        """Test sec() secant function."""
        df = spark.createDataFrame([{"angle": 0.5}])
        result = df.select(F.sec("angle").alias("sec_val"))
        assert "sec_val" in result.columns

    def test_e_constant(self, spark):
        """Test e() Euler's number constant."""
        df = spark.createDataFrame([{"dummy": 1}])
        result = df.select(F.e().alias("euler"))
        assert "euler" in result.columns

    def test_pi_constant(self, spark):
        """Test pi() constant."""
        df = spark.createDataFrame([{"dummy": 1}])
        result = df.select(F.pi().alias("pi_val"))
        assert "pi_val" in result.columns

    def test_ln(self, spark):
        """Test ln() natural logarithm."""
        df = spark.createDataFrame([{"value": 2.718}])
        result = df.select(F.ln("value").alias("ln_val"))
        assert "ln_val" in result.columns


class TestBitwiseFunctions:
    """Test bitwise aggregate functions."""

    def test_bit_and(self, spark):
        """Test bit_and() aggregate function."""
        df = spark.createDataFrame([{"dept": "IT", "flags": 7}, {"dept": "IT", "flags": 3}])
        agg_func = F.bit_and("flags")
        assert agg_func.function_name == "bit_and"

    def test_bit_or(self, spark):
        """Test bit_or() aggregate function."""
        df = spark.createDataFrame([{"dept": "IT", "flags": 1}, {"dept": "IT", "flags": 2}])
        agg_func = F.bit_or("flags")
        assert agg_func.function_name == "bit_or"

    def test_bit_xor(self, spark):
        """Test bit_xor() aggregate function."""
        df = spark.createDataFrame([{"dept": "IT", "flags": 5}, {"dept": "IT", "flags": 3}])
        agg_func = F.bit_xor("flags")
        assert agg_func.function_name == "bit_xor"


class TestIntegration:
    """Integration tests combining multiple new features."""

    def test_multiple_aliases_chain(self, spark):
        """Test chaining multiple alias methods."""
        df = spark.createDataFrame([{"a": 30}, {"a": 25}, {"a": 35}])
        result = df.where(F.col("a") > 27).sort("a")
        data = result.collect()
        assert len(data) == 2
        assert data[0]["a"] == 30
        assert data[1]["a"] == 35

    def test_withColumns_and_rename(self, spark):
        """Test withColumns with withColumnsRenamed."""
        df = spark.createDataFrame([{"x": 5}])
        # Test withColumnsRenamed alone
        result = df.withColumnsRenamed({"x": "original"})
        assert "original" in result.columns
        assert "x" not in result.columns
        row = result.collect()[0]
        assert row["original"] == 5


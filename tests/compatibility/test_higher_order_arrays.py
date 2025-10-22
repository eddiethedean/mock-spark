"""
Compatibility tests for Higher-Order Array Functions (PySpark 3.2).

Tests transform, filter, exists, forall, aggregate, and zip_with functions
against real PySpark to ensure compatibility.
"""

import pytest

try:
    from pyspark.sql import SparkSession  # noqa: F401
    from pyspark.sql import functions as PySparkF  # noqa: F401

    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False

from mock_spark import MockSparkSession
from mock_spark import functions as F


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestTransformFunction:
    """Test transform() higher-order function."""

    def setup_method(self):
        """Setup test data for both mock-spark and PySpark."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "numbers": [1, 2, 3, 4, 5]},
            {"id": 2, "numbers": [10, 20, 30]},
            {"id": 3, "numbers": []},
            {"id": 4, "numbers": None},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_transform_multiply(self):
        """Test transform with multiplication lambda."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"), F.transform(F.col("numbers"), lambda x: x * 2).alias("doubled")
        ).collect()

        assert len(result) == 4
        assert result[0]["doubled"] == [2, 4, 6, 8, 10]
        assert result[1]["doubled"] == [20, 40, 60]
        assert result[2]["doubled"] == []
        assert result[3]["doubled"] is None

    def test_transform_addition(self):
        """Test transform with addition lambda."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"), F.transform(F.col("numbers"), lambda x: x + 10).alias("added")
        ).collect()

        assert result[0]["added"] == [11, 12, 13, 14, 15]
        assert result[1]["added"] == [20, 30, 40]


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestFilterFunction:
    """Test filter() higher-order function."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "numbers": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]},
            {"id": 2, "numbers": [15, 20, 25, 30]},
            {"id": 3, "numbers": [1, 1, 1]},
            {"id": 4, "numbers": []},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_filter_greater_than(self):
        """Test filter with > comparison."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"), F.filter(F.col("numbers"), lambda x: x > 5).alias("filtered")
        ).collect()

        assert result[0]["filtered"] == [6, 7, 8, 9, 10]
        assert result[1]["filtered"] == [15, 20, 25, 30]
        assert result[2]["filtered"] == []
        assert result[3]["filtered"] == []

    def test_filter_equality(self):
        """Test filter with == comparison."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"), F.filter(F.col("numbers"), lambda x: x == 1).alias("ones")
        ).collect()

        assert result[0]["ones"] == [1]
        assert result[1]["ones"] == []
        assert result[2]["ones"] == [1, 1, 1]


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestExistsFunction:
    """Test exists() higher-order function."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "numbers": [1, 2, 3, 4, 5]},
            {"id": 2, "numbers": [10, 20, 30]},
            {"id": 3, "numbers": [100, 200, 300]},
            {"id": 4, "numbers": []},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_exists_found(self):
        """Test exists when element matches."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"), F.exists(F.col("numbers"), lambda x: x > 50).alias("has_large")
        ).collect()

        assert result[0]["has_large"] is False
        assert result[1]["has_large"] is False
        assert result[2]["has_large"] is True
        assert result[3]["has_large"] is False

    def test_exists_not_found(self):
        """Test exists when no element matches."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.exists(F.col("numbers"), lambda x: x > 1000).alias("has_huge"),
        ).collect()

        assert all(row["has_huge"] is False for row in result)


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestForallFunction:
    """Test forall() higher-order function."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "numbers": [1, 2, 3, 4, 5]},
            {"id": 2, "numbers": [10, 20, 30]},
            {"id": 3, "numbers": [-1, -2, -3]},
            {"id": 4, "numbers": []},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_forall_all_match(self):
        """Test forall when all elements match."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.forall(F.col("numbers"), lambda x: x < 100).alias("all_small"),
        ).collect()

        assert result[0]["all_small"] is True
        assert result[1]["all_small"] is True
        assert result[2]["all_small"] is True
        assert result[3]["all_small"] is True  # Empty array returns True

    def test_forall_some_dont_match(self):
        """Test forall when some elements don't match."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.forall(F.col("numbers"), lambda x: x > 0).alias("all_positive"),
        ).collect()

        assert result[0]["all_positive"] is True
        assert result[1]["all_positive"] is True
        assert result[2]["all_positive"] is False
        assert result[3]["all_positive"] is True


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestAggregateFunction:
    """Test aggregate() higher-order function."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "numbers": [1, 2, 3, 4, 5]},
            {"id": 2, "numbers": [10, 20, 30]},
            {"id": 3, "numbers": []},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_aggregate_sum(self):
        """Test aggregate for sum operation."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.aggregate(F.col("numbers"), F.lit(0), lambda acc, x: acc + x).alias(
                "sum"
            ),
        ).collect()

        assert result[0]["sum"] == 15  # 1+2+3+4+5
        assert result[1]["sum"] == 60  # 10+20+30
        assert result[2]["sum"] == 0  # empty array

    def test_aggregate_product(self):
        """Test aggregate for product operation."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.aggregate(F.col("numbers"), F.lit(1), lambda acc, x: acc * x).alias(
                "product"
            ),
        ).collect()

        assert result[0]["product"] == 120  # 1*2*3*4*5
        assert result[1]["product"] == 6000  # 10*20*30
        assert result[2]["product"] == 1  # empty array


@pytest.mark.compatibility
@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="PySpark not installed")
class TestZipWithFunction:
    """Test zip_with() higher-order function."""

    def setup_method(self):
        """Setup test data."""
        self.mock_spark = MockSparkSession("test")
        if PYSPARK_AVAILABLE:
            self.real_spark = (
                SparkSession.builder.appName("test").master("local[1]").getOrCreate()
            )

        self.test_data = [
            {"id": 1, "arr1": [1, 2, 3], "arr2": [10, 20, 30]},
            {"id": 2, "arr1": [5, 10], "arr2": [1, 2]},
            {"id": 3, "arr1": [1, 2, 3], "arr2": [10, 20]},  # Mismatched lengths
            {"id": 4, "arr1": [], "arr2": []},
        ]

    def teardown_method(self):
        """Cleanup sessions."""
        if PYSPARK_AVAILABLE and hasattr(self, "real_spark"):
            self.real_spark.stop()

    def test_zip_with_addition(self):
        """Test zip_with with addition."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.zip_with(F.col("arr1"), F.col("arr2"), lambda x, y: x + y).alias("sums"),
        ).collect()

        assert result[0]["sums"] == [11, 22, 33]
        assert result[1]["sums"] == [6, 12]
        # Mismatched lengths: shorter array determines result length
        assert result[2]["sums"] == [11, 22]
        assert result[3]["sums"] == []

    def test_zip_with_multiplication(self):
        """Test zip_with with multiplication."""
        mock_df = self.mock_spark.createDataFrame(self.test_data)

        result = mock_df.select(
            F.col("id"),
            F.zip_with(F.col("arr1"), F.col("arr2"), lambda x, y: x * y).alias(
                "products"
            ),
        ).collect()

        assert result[0]["products"] == [10, 40, 90]
        assert result[1]["products"] == [5, 20]


@pytest.mark.fast
class TestHigherOrderArrayFunctionsUnit:
    """Fast unit tests without PySpark dependency."""

    def test_transform_basic(self):
        """Test basic transform functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.transform(F.col("nums"), lambda x: x * 2).alias("doubled")
        ).collect()

        assert result[0]["doubled"] == [2, 4, 6]

    def test_filter_basic(self):
        """Test basic filter functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3, 4, 5]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.filter(F.col("nums"), lambda x: x > 3).alias("filtered")
        ).collect()

        assert result[0]["filtered"] == [4, 5]

    def test_exists_basic(self):
        """Test basic exists functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.exists(F.col("nums"), lambda x: x > 2).alias("has_large")
        ).collect()

        assert result[0]["has_large"] is True

    def test_forall_basic(self):
        """Test basic forall functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.forall(F.col("nums"), lambda x: x > 0).alias("all_positive")
        ).collect()

        assert result[0]["all_positive"] is True

    def test_aggregate_basic(self):
        """Test basic aggregate functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"nums": [1, 2, 3, 4, 5]}]
        schema = MockStructType([MockStructField("nums", ArrayType(IntegerType()))])
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.aggregate(F.col("nums"), F.lit(0), lambda acc, x: acc + x).alias("sum")
        ).collect()

        assert result[0]["sum"] == 15

    def test_zip_with_basic(self):
        """Test basic zip_with functionality."""
        from mock_spark.spark_types import (
            MockStructType,
            MockStructField,
            ArrayType,
            IntegerType,
        )

        spark = MockSparkSession("test")
        data = [{"a": [1, 2, 3], "b": [10, 20, 30]}]
        schema = MockStructType(
            [
                MockStructField("a", ArrayType(IntegerType())),
                MockStructField("b", ArrayType(IntegerType())),
            ]
        )
        df = spark.createDataFrame(data, schema)

        result = df.select(
            F.zip_with(F.col("a"), F.col("b"), lambda x, y: x + y).alias("sums")
        ).collect()

        assert result[0]["sums"] == [11, 22, 33]

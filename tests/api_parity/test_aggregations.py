"""
API parity tests for aggregation operations.

Tests groupBy, agg operations between MockSpark and PySpark
to ensure identical behavior and results.
"""

from tests.api_parity.conftest import ParityTestBase, compare_aggregations


class TestAggregations(ParityTestBase):
    """Test aggregation operations for API parity."""

    def test_group_by_count(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with count aggregation."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").count()

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").count()

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_group_by_avg(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with avg aggregation."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").avg("salary")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").avg("salary")

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_group_by_sum(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with sum aggregation."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").sum("salary")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").sum("salary")

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_group_by_max(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with max aggregation."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").max("salary")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").max("salary")

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_group_by_min(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with min aggregation."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").min("salary")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").min("salary")

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_agg_multiple_functions(self, mock_spark, pyspark_spark, sample_data):
        """Test agg with multiple aggregation functions."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            mock_df.salary.count().alias("count"),
            mock_df.salary.avg().alias("avg_salary"),
            mock_df.salary.sum().alias("total_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            pyspark_df.salary.count().alias("count"),
            pyspark_df.salary.avg().alias("avg_salary"),
            pyspark_df.salary.sum().alias("total_salary"),
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_agg_with_functions_module(self, mock_spark, pyspark_spark, sample_data):
        """Test agg using functions module."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.count("salary").alias("count"),
            MockF.avg("salary").alias("avg_salary"),
            MockF.sum("salary").alias("total_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.count("salary").alias("count"),
            PySparkF.avg("salary").alias("avg_salary"),
            PySparkF.sum("salary").alias("total_salary"),
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_count_distinct(self, mock_spark, pyspark_spark, sample_data):
        """Test countDistinct aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.countDistinct("name").alias("unique_names")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.countDistinct("name").alias("unique_names")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_stddev_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test stddev aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.stddev("salary").alias("salary_stddev")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.stddev("salary").alias("salary_stddev")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_variance_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test variance aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.variance("salary").alias("salary_variance")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.variance("salary").alias("salary_variance")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_collect_list_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test collect_list aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.collect_list("name").alias("names")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.collect_list("name").alias("names")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_collect_set_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test collect_set aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.collect_set("name").alias("unique_names")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.collect_set("name").alias("unique_names")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_first_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test first aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.first("name").alias("first_name")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.first("name").alias("first_name")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_last_aggregation(self, mock_spark, pyspark_spark, sample_data):
        """Test last aggregation."""
        from mock_spark import F as MockF
        from pyspark.sql import functions as PySparkF

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.groupBy("department").agg(
            MockF.last("name").alias("last_name")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.groupBy("department").agg(
            PySparkF.last("name").alias("last_name")
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_multiple_group_by_columns(self, mock_spark, pyspark_spark, sample_data):
        """Test groupBy with multiple columns."""
        # Add more data with different combinations
        extended_data = sample_data + [
            {
                "id": 6,
                "name": "Frank",
                "age": 25,
                "salary": 55000.0,
                "department": "IT",
            },
            {
                "id": 7,
                "name": "Grace",
                "age": 30,
                "salary": 65000.0,
                "department": "HR",
            },
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(extended_data)
        mock_result = mock_df.groupBy("department", "age").count()

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(extended_data)
        pyspark_result = pyspark_df.groupBy("department", "age").count()

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_global_aggregations(self, mock_spark, pyspark_spark, sample_data):
        """Test global aggregations (no groupBy)."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.agg(
            mock_df.salary.count().alias("total_count"),
            mock_df.salary.avg().alias("avg_salary"),
            mock_df.salary.sum().alias("total_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.agg(
            pyspark_df.salary.count().alias("total_count"),
            pyspark_df.salary.avg().alias("avg_salary"),
            pyspark_df.salary.sum().alias("total_salary"),
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

    def test_aggregation_with_nulls(self, mock_spark, pyspark_spark):
        """Test aggregations with null values."""
        data_with_nulls = [
            {"id": 1, "name": "Alice", "salary": 50000.0},
            {"id": 2, "name": "Bob", "salary": None},
            {"id": 3, "name": "Charlie", "salary": 70000.0},
            {"id": 4, "name": "David", "salary": None},
        ]

        # MockSpark
        mock_df = mock_spark.createDataFrame(data_with_nulls)
        mock_result = mock_df.agg(
            mock_df.salary.count().alias("count"),
            mock_df.salary.avg().alias("avg_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(data_with_nulls)
        pyspark_result = pyspark_df.agg(
            pyspark_df.salary.count().alias("count"),
            pyspark_df.salary.avg().alias("avg_salary"),
        )

        # Compare
        compare_aggregations(mock_result, pyspark_result)

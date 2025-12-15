"""
API parity tests for window operations.

Tests window functions between MockSpark and PySpark
to ensure identical behavior and results.
"""

from tests.api_parity.conftest import ParityTestBase, compare_dataframes


class TestWindowOperations(ParityTestBase):
    """Test window operations for API parity."""

    def test_row_number_window(self, mock_spark, pyspark_spark, sample_data):
        """Test row_number window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.row_number().over(mock_window).alias("row_num"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.row_number().over(pyspark_window).alias("row_num"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_rank_window(self, mock_spark, pyspark_spark, sample_data):
        """Test rank window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name", "department", "salary", MockF.rank().over(mock_window).alias("rank")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.rank().over(pyspark_window).alias("rank"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_dense_rank_window(self, mock_spark, pyspark_spark, sample_data):
        """Test dense_rank window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.dense_rank().over(mock_window).alias("dense_rank"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.dense_rank().over(pyspark_window).alias("dense_rank"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_lag_window(self, mock_spark, pyspark_spark, sample_data):
        """Test lag window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.lag("salary", 1).over(mock_window).alias("prev_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.lag("salary", 1).over(pyspark_window).alias("prev_salary"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_lead_window(self, mock_spark, pyspark_spark, sample_data):
        """Test lead window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.lead("salary", 1).over(mock_window).alias("next_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.lead("salary", 1).over(pyspark_window).alias("next_salary"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_first_window(self, mock_spark, pyspark_spark, sample_data):
        """Test first window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.first("salary").over(mock_window).alias("first_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.first("salary").over(pyspark_window).alias("first_salary"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_last_window(self, mock_spark, pyspark_spark, sample_data):
        """Test last window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.last("salary").over(mock_window).alias("last_salary"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.last("salary").over(pyspark_window).alias("last_salary"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_sum_window(self, mock_spark, pyspark_spark, sample_data):
        """Test sum window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.sum("salary").over(mock_window).alias("running_sum"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.sum("salary").over(pyspark_window).alias("running_sum"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_avg_window(self, mock_spark, pyspark_spark, sample_data):
        """Test avg window function."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.avg("salary").over(mock_window).alias("running_avg"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.avg("salary").over(pyspark_window).alias("running_avg"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_window_with_rows_between(self, mock_spark, pyspark_spark, sample_data):
        """Test window with ROWS BETWEEN frame."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = (
            MockWindow.partitionBy("department")
            .orderBy("salary")
            .rowsBetween(MockWindow.currentRow, MockWindow.unboundedFollowing)
        )
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.sum("salary").over(mock_window).alias("sum_following"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = (
            PySparkWindow.partitionBy("department")
            .orderBy("salary")
            .rowsBetween(PySparkWindow.currentRow, PySparkWindow.unboundedFollowing)
        )
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.sum("salary").over(pyspark_window).alias("sum_following"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_window_with_range_between(self, mock_spark, pyspark_spark, sample_data):
        """Test window with RANGE BETWEEN frame."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = (
            MockWindow.partitionBy("department")
            .orderBy("salary")
            .rangeBetween(MockWindow.unboundedPreceding, MockWindow.currentRow)
        )
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.avg("salary").over(mock_window).alias("avg_preceding"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = (
            PySparkWindow.partitionBy("department")
            .orderBy("salary")
            .rangeBetween(PySparkWindow.unboundedPreceding, PySparkWindow.currentRow)
        )
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.avg("salary").over(pyspark_window).alias("avg_preceding"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_multiple_window_functions(self, mock_spark, pyspark_spark, sample_data):
        """Test multiple window functions in one query."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.row_number().over(mock_window).alias("row_num"),
            MockF.rank().over(mock_window).alias("rank"),
            MockF.sum("salary").over(mock_window).alias("running_sum"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.row_number().over(pyspark_window).alias("row_num"),
            PySparkF.rank().over(pyspark_window).alias("rank"),
            PySparkF.sum("salary").over(pyspark_window).alias("running_sum"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_window_with_complex_ordering(self, mock_spark, pyspark_spark, sample_data):
        """Test window with complex ordering expressions."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.partitionBy("department").orderBy(
            mock_df.salary.desc(), mock_df.name.asc()
        )
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.row_number().over(mock_window).alias("row_num"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.partitionBy("department").orderBy(
            pyspark_df.salary.desc(), pyspark_df.name.asc()
        )
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.row_number().over(pyspark_window).alias("row_num"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_window_without_partition(self, mock_spark, pyspark_spark, sample_data):
        """Test window function without partitioning."""
        from sparkless import F as MockF
        from sparkless.window import Window as MockWindow
        from pyspark.sql import functions as PySparkF
        from pyspark.sql.window import Window as PySparkWindow

        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_window = MockWindow.orderBy("salary")
        mock_result = mock_df.select(
            "name",
            "department",
            "salary",
            MockF.row_number().over(mock_window).alias("row_num"),
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_window = PySparkWindow.orderBy("salary")
        pyspark_result = pyspark_df.select(
            "name",
            "department",
            "salary",
            PySparkF.row_number().over(pyspark_window).alias("row_num"),
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

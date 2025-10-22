"""
Advanced Window Functions Compatibility Tests

Tests for comprehensive window function support including rank, dense_rank, lag, lead, etc.
"""

from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestAdvancedWindowFunctions:
    """Test advanced window functions compatibility."""

    def test_rank_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test rank() function with ties."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.rank().over(mock_window).alias("rank_num"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.rank().over(pyspark_window).alias("rank_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_dense_rank_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test dense_rank() function with ties."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.dense_rank().over(mock_window).alias("dense_rank_num"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.dense_rank()
                .over(pyspark_window)
                .alias("dense_rank_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_lag_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lag() function."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.lag(mock_functions.col("salary"), 1)
                .over(mock_window)
                .alias("prev_salary"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.lag(pyspark_functions.col("salary"), 1)
                .over(pyspark_window)
                .alias("prev_salary"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_lead_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test lead() function."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.lead(mock_functions.col("salary"), 1)
                .over(mock_window)
                .alias("next_salary"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.lead(pyspark_functions.col("salary"), 1)
                .over(pyspark_window)
                .alias("next_salary"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_window_partition_by(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test window functions with partitionBy."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification with partition
            mock_window = MockWindow.partitionBy(
                mock_functions.col("department")
            ).orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.partitionBy(
                pyspark_functions.col("department")
            ).orderBy(pyspark_functions.col("age"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.row_number().over(mock_window).alias("dept_row_num"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.row_number()
                .over(pyspark_window)
                .alias("dept_row_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_window_rows_between(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test window functions with rowsBetween."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification with rows between
            mock_window = MockWindow.orderBy(mock_functions.col("age")).rowsBetween(
                -1, 1
            )
            pyspark_window = PySparkWindow.orderBy(
                pyspark_functions.col("age")
            ).rowsBetween(-1, 1)

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.avg(mock_functions.col("salary"))
                .over(mock_window)
                .alias("avg_salary"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.avg(pyspark_functions.col("salary"))
                .over(pyspark_window)
                .alias("avg_salary"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )


class TestWindowFunctionEdgeCases:
    """Test edge cases for window functions."""

    def test_window_empty_dataframe(
        self,
        mock_empty_dataframe,
        pyspark_empty_dataframe,
        mock_functions,
        pyspark_functions,
    ):
        """Test window functions on empty DataFrame."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("id"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("id"))

            mock_result = mock_empty_dataframe.select(
                mock_functions.col("*"),
                mock_functions.row_number().over(mock_window).alias("row_num"),
            )
            pyspark_result = pyspark_empty_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.row_number().over(pyspark_window).alias("row_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_window_single_row(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test window functions on single row."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Filter to single row
            mock_single = mock_dataframe.filter(mock_functions.col("id") == 1)
            pyspark_single = pyspark_dataframe.filter(pyspark_functions.col("id") == 1)

            # Create window specification
            mock_window = MockWindow.orderBy(mock_functions.col("age"))
            pyspark_window = PySparkWindow.orderBy(pyspark_functions.col("age"))

            mock_result = mock_single.select(
                mock_functions.col("*"),
                mock_functions.row_number().over(mock_window).alias("row_num"),
            )
            pyspark_result = pyspark_single.select(
                pyspark_functions.col("*"),
                pyspark_functions.row_number().over(pyspark_window).alias("row_num"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

    def test_window_multiple_columns(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test window functions with multiple window specifications."""
        try:
            from pyspark.sql.window import Window as PySparkWindow
            from mock_spark.window import MockWindow

            # Create multiple window specifications
            mock_window1 = MockWindow.orderBy(mock_functions.col("age"))
            mock_window2 = MockWindow.partitionBy(
                mock_functions.col("department")
            ).orderBy(mock_functions.col("salary"))
            pyspark_window1 = PySparkWindow.orderBy(pyspark_functions.col("age"))
            pyspark_window2 = PySparkWindow.partitionBy(
                pyspark_functions.col("department")
            ).orderBy(pyspark_functions.col("salary"))

            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.row_number().over(mock_window1).alias("global_row"),
                mock_functions.row_number().over(mock_window2).alias("dept_row"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.row_number()
                .over(pyspark_window1)
                .alias("global_row"),
                pyspark_functions.row_number().over(pyspark_window2).alias("dept_row"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (ImportError, AttributeError) as e:
            # Advanced window functions should now be implemented
            raise AssertionError(
                f"Advanced window functions should be implemented: {e}"
            )

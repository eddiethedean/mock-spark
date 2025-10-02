"""
Compatibility tests for DataFrame operations between mock_spark and PySpark.
"""

import pytest
from tests.compatibility.utils.comparison import (
    assert_dataframes_equal, 
    assert_schemas_equal,
    compare_dataframes,
    compare_schemas
)


class TestDataFrameCreation:
    """Test DataFrame creation compatibility."""
    
    def test_create_dataframe_simple(self, mock_dataframe, pyspark_dataframe):
        """Test simple DataFrame creation."""
        assert_dataframes_equal(mock_dataframe, pyspark_dataframe)
    
    def test_create_dataframe_complex(self, mock_complex_dataframe, pyspark_complex_dataframe):
        """Test complex DataFrame creation."""
        # For complex data, we might need to adjust comparison based on what PySpark actually supports
        try:
            assert_dataframes_equal(mock_complex_dataframe, pyspark_complex_dataframe)
        except AssertionError as e:
            # If complex data fails, we need to fix the mock to match PySpark behavior
            pytest.fail(f"Complex DataFrame creation mismatch: {e}")
    
    def test_create_dataframe_empty(self, mock_environment, pyspark_environment, empty_data):
        """Test empty DataFrame creation."""
        mock_df = mock_environment["session"].createDataFrame(empty_data)
        pyspark_df = pyspark_environment["session"].createDataFrame(empty_data)
        assert_dataframes_equal(mock_df, pyspark_df)


class TestDataFrameBasicOperations:
    """Test basic DataFrame operations compatibility."""
    
    def test_select_columns(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test column selection."""
        mock_result = mock_dataframe.select(mock_functions.col("name"), mock_functions.col("age"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.col("name"), pyspark_functions.col("age"))
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_filter_equality(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test filter with equality condition."""
        mock_result = mock_dataframe.filter(mock_functions.col("age") == 25)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") == 25)
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_filter_greater_than(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test filter with greater than condition."""
        mock_result = mock_dataframe.filter(mock_functions.col("age") > 30)
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("age") > 30)
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_filter_is_null(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test filter with null check."""
        mock_result = mock_dataframe.filter(mock_functions.col("name").isNull())
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("name").isNull())
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_filter_is_not_null(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test filter with not null check."""
        mock_result = mock_dataframe.filter(mock_functions.col("name").isNotNull())
        pyspark_result = pyspark_dataframe.filter(pyspark_functions.col("name").isNotNull())
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_with_column(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test adding new column."""
        mock_result = mock_dataframe.withColumn("age_plus_one", mock_functions.col("age") + 1)
        pyspark_result = pyspark_dataframe.withColumn("age_plus_one", pyspark_functions.col("age") + 1)
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_with_column_literal(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test adding column with literal value."""
        mock_result = mock_dataframe.withColumn("constant", mock_functions.lit("test"))
        pyspark_result = pyspark_dataframe.withColumn("constant", pyspark_functions.lit("test"))
        assert_dataframes_equal(mock_result, pyspark_result)


class TestDataFrameAggregations:
    """Test DataFrame aggregation operations compatibility."""
    
    def test_group_by_count(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test groupBy with count."""
        mock_result = mock_dataframe.groupBy("department").agg(mock_functions.count())
        pyspark_result = pyspark_dataframe.groupBy("department").agg(pyspark_functions.count())
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_group_by_sum(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test groupBy with sum."""
        mock_result = mock_dataframe.groupBy("department").agg(mock_functions.sum("salary"))
        pyspark_result = pyspark_dataframe.groupBy("department").agg(pyspark_functions.sum("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_group_by_avg(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test groupBy with average."""
        mock_result = mock_dataframe.groupBy("department").agg(mock_functions.avg("salary"))
        pyspark_result = pyspark_dataframe.groupBy("department").agg(pyspark_functions.avg("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_group_by_max(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test groupBy with max."""
        mock_result = mock_dataframe.groupBy("department").agg(mock_functions.max("salary"))
        pyspark_result = pyspark_dataframe.groupBy("department").agg(pyspark_functions.max("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_group_by_min(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test groupBy with min."""
        mock_result = mock_dataframe.groupBy("department").agg(mock_functions.min("salary"))
        pyspark_result = pyspark_dataframe.groupBy("department").agg(pyspark_functions.min("salary"))
        assert_dataframes_equal(mock_result, pyspark_result)


class TestDataFrameOrdering:
    """Test DataFrame ordering operations compatibility."""
    
    def test_order_by_ascending(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test orderBy ascending."""
        mock_result = mock_dataframe.orderBy(mock_functions.col("age"))
        pyspark_result = pyspark_dataframe.orderBy(pyspark_functions.col("age"))
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_order_by_descending(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test orderBy descending."""
        mock_result = mock_dataframe.orderBy(mock_functions.col("age").desc())
        pyspark_result = pyspark_dataframe.orderBy(pyspark_functions.col("age").desc())
        assert_dataframes_equal(mock_result, pyspark_result)


class TestDataFrameLimiting:
    """Test DataFrame limiting operations compatibility."""
    
    def test_limit(self, mock_dataframe, pyspark_dataframe):
        """Test limit operation."""
        mock_result = mock_dataframe.limit(2)
        pyspark_result = pyspark_dataframe.limit(2)
        assert_dataframes_equal(mock_result, pyspark_result)
    
    def test_take(self, mock_dataframe, pyspark_dataframe):
        """Test take operation."""
        mock_result = mock_dataframe.take(2)
        pyspark_result = pyspark_dataframe.take(2)
        
        # Compare the results directly since take returns lists
        assert len(mock_result) == len(pyspark_result)
        for mock_row, pyspark_row in zip(mock_result, pyspark_result):
            assert mock_row == pyspark_row


class TestDataFrameSchema:
    """Test DataFrame schema operations compatibility."""
    
    def test_schema_comparison(self, mock_dataframe, pyspark_dataframe):
        """Test schema comparison."""
        assert_schemas_equal(mock_dataframe, pyspark_dataframe)
    
    def test_columns_property(self, mock_dataframe, pyspark_dataframe):
        """Test columns property."""
        mock_columns = mock_dataframe.columns
        pyspark_columns = pyspark_dataframe.columns
        
        assert mock_columns == pyspark_columns
    
    def test_dtypes_property(self, mock_dataframe, pyspark_dataframe):
        """Test dtypes property."""
        mock_dtypes = mock_dataframe.dtypes
        pyspark_dtypes = pyspark_dataframe.dtypes
        
        assert mock_dtypes == pyspark_dtypes


class TestDataFrameCollect:
    """Test DataFrame collect operations compatibility."""
    
    def test_collect(self, mock_dataframe, pyspark_dataframe):
        """Test collect operation."""
        mock_result = mock_dataframe.collect()
        pyspark_result = pyspark_dataframe.collect()
        
        # Compare collected data
        assert len(mock_result) == len(pyspark_result)
        for mock_row, pyspark_row in zip(mock_result, pyspark_result):
            assert mock_row == pyspark_row
    
    def test_to_pandas(self, mock_dataframe, pyspark_dataframe):
        """Test toPandas operation."""
        mock_pandas = mock_dataframe.toPandas()
        pyspark_pandas = pyspark_dataframe.toPandas()
        
        # Compare pandas DataFrames
        assert mock_pandas.equals(pyspark_pandas)
    
    def test_to_pandas_schema_compatibility(self, mock_dataframe, pyspark_dataframe):
        """Test that toPandas preserves schema information."""
        mock_pandas = mock_dataframe.toPandas()
        pyspark_pandas = pyspark_dataframe.toPandas()
        
        # Check column names match
        assert list(mock_pandas.columns) == list(pyspark_pandas.columns)
        
        # Check data types are compatible
        assert len(mock_pandas) == len(pyspark_pandas)
        
        # Check that data values match
        for col in mock_pandas.columns:
            mock_values = mock_pandas[col].tolist()
            pyspark_values = pyspark_pandas[col].tolist()
            assert mock_values == pyspark_values
    
    def test_to_pandas_with_literals(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test toPandas with literal columns."""
        # Create DataFrames with literal columns
        mock_result = mock_dataframe.select(
            mock_functions.col("name"), 
            mock_functions.lit("constant_value")
        )
        pyspark_result = pyspark_dataframe.select(
            pyspark_functions.col("name"), 
            pyspark_functions.lit("constant_value")
        )
        
        # Convert to pandas
        mock_pandas = mock_result.toPandas()
        pyspark_pandas = pyspark_result.toPandas()
        
        # Compare pandas DataFrames
        assert list(mock_pandas.columns) == list(pyspark_pandas.columns)
        assert len(mock_pandas) == len(pyspark_pandas)
        
        # Check literal column values
        assert all(val == "constant_value" for val in mock_pandas["constant_value"])
        assert all(val == "constant_value" for val in pyspark_pandas["constant_value"])


class TestDataFrameCount:
    """Test DataFrame count operations compatibility."""
    
    def test_count(self, mock_dataframe, pyspark_dataframe):
        """Test count operation."""
        mock_count = mock_dataframe.count()
        pyspark_count = pyspark_dataframe.count()
        
        assert mock_count == pyspark_count
    
    def test_count_distinct(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Test countDistinct operation."""
        mock_result = mock_dataframe.select(mock_functions.countDistinct("department"))
        pyspark_result = pyspark_dataframe.select(pyspark_functions.countDistinct("department"))
        assert_dataframes_equal(mock_result, pyspark_result)


# Test that will help identify discrepancies and guide fixes
class TestDiscrepancyDetection:
    """Tests specifically designed to detect discrepancies between mock and PySpark."""
    
    def test_comprehensive_comparison(self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions):
        """Comprehensive test to identify any discrepancies."""
        # Test multiple operations in sequence
        operations = [
            ("select", lambda df, f: df.select(f.col("name"), f.col("age"))),
            ("filter", lambda df, f: df.filter(f.col("age") > 25)),
            ("withColumn", lambda df, f: df.withColumn("age_double", f.col("age") * 2)),
            ("groupBy", lambda df, f: df.groupBy("department").count()),
            ("orderBy", lambda df, f: df.orderBy(f.col("age"))),
            ("limit", lambda df, f: df.limit(2)),
        ]
        
        mock_df = mock_dataframe
        pyspark_df = pyspark_dataframe
        
        for op_name, operation in operations:
            try:
                mock_result = operation(mock_df, mock_functions)
                pyspark_result = operation(pyspark_df, pyspark_functions)
                
                comparison = compare_dataframes(mock_result, pyspark_result)
                if not comparison["equivalent"]:
                    pytest.fail(f"Discrepancy found in {op_name} operation: {comparison['errors']}")
                
                # Update DataFrames for next operation
                mock_df = mock_result
                pyspark_df = pyspark_result
                
            except Exception as e:
                pytest.fail(f"Error in {op_name} operation: {str(e)}")
    
    def test_edge_cases(self, mock_environment, pyspark_environment):
        """Test edge cases to identify discrepancies."""
        # Test with empty DataFrame
        empty_data = []
        mock_empty = mock_environment["session"].createDataFrame(empty_data)
        pyspark_empty = pyspark_environment["session"].createDataFrame(empty_data)
        
        assert_dataframes_equal(mock_empty, pyspark_empty)
        
        # Test with single row
        single_data = [{"id": 1, "name": "test", "value": 42}]
        mock_single = mock_environment["session"].createDataFrame(single_data)
        pyspark_single = pyspark_environment["session"].createDataFrame(single_data)
        
        assert_dataframes_equal(mock_single, pyspark_single)

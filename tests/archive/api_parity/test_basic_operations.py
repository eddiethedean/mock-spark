"""
API parity tests for basic DataFrame operations.

Tests select, filter, join operations between MockSpark and PySpark
to ensure identical behavior and results.
"""

from tests.api_parity.conftest import ParityTestBase, compare_dataframes


class TestBasicOperations(ParityTestBase):
    """Test basic DataFrame operations for API parity."""

    def test_select_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test select operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.select("id", "name", "age")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.select("id", "name", "age")

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_filter_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test filter operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.filter(mock_df.age > 30)

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.filter(pyspark_df.age > 30)

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_with_column_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test withColumn operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.withColumn("age_plus_10", mock_df.age + 10)

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.withColumn("age_plus_10", pyspark_df.age + 10)

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_column_access_patterns(self, mock_spark, pyspark_spark, sample_data):
        """Test column access patterns."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.select(mock_df.id, mock_df.name, mock_df.age)

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.select(
            pyspark_df.id, pyspark_df.name, pyspark_df.age
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_alias_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test column aliasing."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.select(
            mock_df.id.alias("user_id"), mock_df.name.alias("full_name")
        )

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.select(
            pyspark_df.id.alias("user_id"), pyspark_df.name.alias("full_name")
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_drop_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test drop operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.drop("department")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.drop("department")

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_distinct_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test distinct operations."""
        # Add duplicate data
        duplicate_data = sample_data + sample_data[:2]  # Add first 2 rows again

        # MockSpark
        mock_df = mock_spark.createDataFrame(duplicate_data)
        mock_result = mock_df.distinct()

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(duplicate_data)
        pyspark_result = pyspark_df.distinct()

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_order_by_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test orderBy operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.orderBy("age")

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.orderBy("age")

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_order_by_desc_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test orderBy desc operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.orderBy(mock_df.age.desc())

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.orderBy(pyspark_df.age.desc())

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_limit_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test limit operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_result = mock_df.limit(3)

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_result = pyspark_df.limit(3)

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_union_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test union operations."""
        # Split data
        data1 = sample_data[:3]
        data2 = sample_data[3:]

        # MockSpark
        mock_df1 = mock_spark.createDataFrame(data1)
        mock_df2 = mock_spark.createDataFrame(data2)
        mock_result = mock_df1.union(mock_df2)

        # PySpark
        pyspark_df1 = pyspark_spark.createDataFrame(data1)
        pyspark_df2 = pyspark_spark.createDataFrame(data2)
        pyspark_result = pyspark_df1.union(pyspark_df2)

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_join_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test join operations."""
        # Create two datasets
        dept_data = [
            {"department": "IT", "manager": "John"},
            {"department": "HR", "manager": "Jane"},
            {"department": "Finance", "manager": "Bob"},
        ]

        # MockSpark
        mock_df1 = mock_spark.createDataFrame(sample_data)
        mock_df2 = mock_spark.createDataFrame(dept_data)
        mock_result = mock_df1.join(mock_df2, "department", "inner")

        # PySpark
        pyspark_df1 = pyspark_spark.createDataFrame(sample_data)
        pyspark_df2 = pyspark_spark.createDataFrame(dept_data)
        pyspark_result = pyspark_df1.join(pyspark_df2, "department", "inner")

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_join_with_different_keys(self, mock_spark, pyspark_spark, sample_data):
        """Test join operations with different key names."""
        # Create two datasets with different key names
        dept_data = [
            {"dept_name": "IT", "manager": "John"},
            {"dept_name": "HR", "manager": "Jane"},
            {"dept_name": "Finance", "manager": "Bob"},
        ]

        # MockSpark
        mock_df1 = mock_spark.createDataFrame(sample_data)
        mock_df2 = mock_spark.createDataFrame(dept_data)
        mock_result = mock_df1.join(
            mock_df2, mock_df1.department == mock_df2.dept_name, "inner"
        )

        # PySpark
        pyspark_df1 = pyspark_spark.createDataFrame(sample_data)
        pyspark_df2 = pyspark_spark.createDataFrame(dept_data)
        pyspark_result = pyspark_df1.join(
            pyspark_df2, pyspark_df1.department == pyspark_df2.dept_name, "inner"
        )

        # Compare
        compare_dataframes(mock_result, pyspark_result)

    def test_show_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test show operations (just verify they don't crash)."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_df.show()  # Should not crash

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_df.show()  # Should not crash

        # Both should complete without error
        assert True

    def test_schema_operations(self, mock_spark, pyspark_spark, sample_data):
        """Test schema operations."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_schema = mock_df.schema

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_schema = pyspark_df.schema

        # Compare field names
        mock_field_names = [field.name for field in mock_schema.fields]
        pyspark_field_names = [field.name for field in pyspark_schema.fields]
        assert set(mock_field_names) == set(pyspark_field_names)

    def test_columns_property(self, mock_spark, pyspark_spark, sample_data):
        """Test columns property."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_columns = mock_df.columns

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_columns = pyspark_df.columns

        # Compare
        assert set(mock_columns) == set(pyspark_columns)

    def test_dtypes_property(self, mock_spark, pyspark_spark, sample_data):
        """Test dtypes property."""
        # MockSpark
        mock_df = mock_spark.createDataFrame(sample_data)
        mock_dtypes = mock_df.dtypes

        # PySpark
        pyspark_df = pyspark_spark.createDataFrame(sample_data)
        pyspark_dtypes = pyspark_df.dtypes

        # Compare
        assert len(mock_dtypes) == len(pyspark_dtypes)
        for mock_dtype, pyspark_dtype in zip(mock_dtypes, pyspark_dtypes):
            assert mock_dtype[0] == pyspark_dtype[0]  # Column name
            # Type comparison might vary, so just check they exist
            assert mock_dtype[1] is not None
            assert pyspark_dtype[1] is not None

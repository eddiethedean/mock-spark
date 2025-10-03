"""
Advanced Data Types Compatibility Tests

Tests for complex data types like ArrayType, MapType, StructType, and advanced SQL functions.
"""

import pytest
from tests.compatibility.utils.comparison import assert_dataframes_equal


class TestComplexDataTypes:
    """Test complex data type support."""

    def test_array_type_creation(
        self, mock_complex_dataframe, pyspark_complex_dataframe
    ):
        """Test ArrayType data creation and access."""
        try:
            # Test that complex data types are properly created
            mock_schema = mock_complex_dataframe.schema
            pyspark_schema = pyspark_complex_dataframe.schema

            # Check that array fields exist in both schemas
            mock_array_fields = [
                f for f in mock_schema.fields if "array" in str(f.dataType).lower()
            ]
            pyspark_array_fields = [
                f for f in pyspark_schema.fields if "array" in str(f.dataType).lower()
            ]

            assert len(mock_array_fields) == len(
                pyspark_array_fields
            ), "Array type field count mismatch"

            # Test data access
            mock_data = mock_complex_dataframe.collect()
            pyspark_data = pyspark_complex_dataframe.collect()

            assert len(mock_data) == len(
                pyspark_data
            ), "Complex data row count mismatch"

        except Exception as e:
            # Complex data types should now be implemented
            raise AssertionError(f"Complex data types should be implemented: {e}")

    def test_map_type_creation(self, mock_complex_dataframe, pyspark_complex_dataframe):
        """Test MapType data creation and access."""
        try:
            # Test that map fields exist
            mock_schema = mock_complex_dataframe.schema
            pyspark_schema = pyspark_complex_dataframe.schema

            # Check that map fields exist in both schemas
            mock_map_fields = [
                f for f in mock_schema.fields if "map" in str(f.dataType).lower()
            ]
            pyspark_map_fields = [
                f for f in pyspark_schema.fields if "map" in str(f.dataType).lower()
            ]

            assert len(mock_map_fields) == len(
                pyspark_map_fields
            ), "Map type field count mismatch"

        except Exception as e:
            # Complex data types should now be implemented
            raise AssertionError(f"Complex data types should be implemented: {e}")

    def test_struct_type_creation(
        self, mock_complex_dataframe, pyspark_complex_dataframe
    ):
        """Test StructType nested data creation and access."""
        try:
            # Test that struct fields exist
            mock_schema = mock_complex_dataframe.schema
            pyspark_schema = pyspark_complex_dataframe.schema

            # Check that struct fields exist in both schemas
            mock_struct_fields = [
                f for f in mock_schema.fields if "struct" in str(f.dataType).lower()
            ]
            pyspark_struct_fields = [
                f for f in pyspark_schema.fields if "struct" in str(f.dataType).lower()
            ]

            assert len(mock_struct_fields) == len(
                pyspark_struct_fields
            ), "Struct type field count mismatch"

        except Exception as e:
            # Complex data types should now be implemented
            raise AssertionError(f"Complex data types should be implemented: {e}")


class TestAdvancedSQLFunctions:
    """Test advanced SQL function support."""

    def test_case_when_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test CASE WHEN statement."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.when(mock_functions.col("age") > 30, "Senior")
                .when(mock_functions.col("age") > 25, "Mid")
                .otherwise("Junior")
                .alias("level"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.when(pyspark_functions.col("age") > 30, "Senior")
                .when(pyspark_functions.col("age") > 25, "Mid")
                .otherwise("Junior")
                .alias("level"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # CASE WHEN function should now be implemented
            raise AssertionError("CASE WHEN function should be implemented")

    def test_coalesce_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test coalesce() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.coalesce(
                    mock_functions.col("name"), mock_functions.lit("Unknown")
                ).alias("coalesced_name")
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.coalesce(
                    pyspark_functions.col("name"), pyspark_functions.lit("Unknown")
                ).alias("coalesced_name")
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # coalesce() function should now be implemented
            raise AssertionError("coalesce() function should be implemented")

    def test_isnull_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test isnull() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.isnull(mock_functions.col("name")).alias("is_null_name"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.isnull(pyspark_functions.col("name")).alias(
                    "is_null_name"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # isnull() function should now be implemented
            raise AssertionError("isnull() function should be implemented")

    def test_isnan_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test isnan() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.isnan(mock_functions.col("salary")).alias(
                    "is_nan_salary"
                ),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.isnan(pyspark_functions.col("salary")).alias(
                    "is_nan_salary"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # isnan() function should now be implemented
            raise AssertionError("isnan() function should be implemented")


class TestDateAndTimeFunctions:
    """Test date and time function support."""

    def test_current_timestamp_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test current_timestamp() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.current_timestamp().alias("current_ts"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.current_timestamp().alias("current_ts"),
            )
            # Note: We can't compare exact timestamp values, so we just check the structure
            assert len(mock_result.collect()) == len(pyspark_result.collect())
            assert "current_ts" in [f.name for f in mock_result.schema.fields]
        except (AttributeError, NotImplementedError):
            # current_timestamp() function should now be implemented
            raise AssertionError("current_timestamp() function should be implemented")

    def test_current_date_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test current_date() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.current_date().alias("current_dt"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.current_date().alias("current_dt"),
            )
            # Note: We can't compare exact date values, so we just check the structure
            assert len(mock_result.collect()) == len(pyspark_result.collect())
            assert "current_dt" in [f.name for f in mock_result.schema.fields]
        except (AttributeError, NotImplementedError):
            # current_date() function should now be implemented
            raise AssertionError("current_date() function should be implemented")


class TestStringAdvancedFunctions:
    """Test advanced string function support."""

    def test_trim_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test trim() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.trim(mock_functions.col("name")).alias("trimmed_name"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.trim(pyspark_functions.col("name")).alias(
                    "trimmed_name"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # trim() function should now be implemented
            raise AssertionError("trim() function should be implemented")

    def test_regexp_replace_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test regexp_replace() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.regexp_replace(
                    mock_functions.col("name"), "e", "X"
                ).alias("replaced_name"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.regexp_replace(
                    pyspark_functions.col("name"), "e", "X"
                ).alias("replaced_name"),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # regexp_replace() function should now be implemented
            raise AssertionError("regexp_replace() function should be implemented")

    def test_split_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test split() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.split(mock_functions.col("department"), " ").alias(
                    "split_dept"
                ),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.split(pyspark_functions.col("department"), " ").alias(
                    "split_dept"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # split() function should now be implemented
            raise AssertionError("split() function should be implemented")


class TestMathematicalAdvancedFunctions:
    """Test advanced mathematical function support."""

    def test_ceil_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test ceil() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.ceil(mock_functions.col("salary") / 1000).alias(
                    "salary_k"
                ),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.ceil(pyspark_functions.col("salary") / 1000).alias(
                    "salary_k"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # ceil() function should now be implemented
            raise AssertionError("ceil() function should be implemented")

    def test_floor_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test floor() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.floor(mock_functions.col("salary") / 1000).alias(
                    "salary_k"
                ),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.floor(pyspark_functions.col("salary") / 1000).alias(
                    "salary_k"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # floor() function should now be implemented
            raise AssertionError("floor() function should be implemented")

    def test_sqrt_function(
        self, mock_dataframe, pyspark_dataframe, mock_functions, pyspark_functions
    ):
        """Test sqrt() function."""
        try:
            mock_result = mock_dataframe.select(
                mock_functions.col("*"),
                mock_functions.sqrt(mock_functions.col("salary")).alias("sqrt_salary"),
            )
            pyspark_result = pyspark_dataframe.select(
                pyspark_functions.col("*"),
                pyspark_functions.sqrt(pyspark_functions.col("salary")).alias(
                    "sqrt_salary"
                ),
            )
            assert_dataframes_equal(mock_result, pyspark_result)
        except (AttributeError, NotImplementedError):
            # sqrt() function should now be implemented
            raise AssertionError("sqrt() function should be implemented")

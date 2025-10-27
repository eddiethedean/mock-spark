"""
Compatibility tests for DataFrame methods.

Tests DataFrame operations against expected outputs generated from PySpark.
"""

import pytest
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal
from mock_spark import F


class TestDataFrameMethodsExtendedCompatibility:
    """Test DataFrame methods compatibility with PySpark."""

    def test_basic_select(self, spark):
        """Test basic DataFrame select operation."""
        expected = load_expected_output("dataframe_operations", "basic_select")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select("id", "name", "age")
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="select_with_alias not yet implemented correctly")
    def test_select_with_alias(self, spark):
        """Test DataFrame select with column aliases."""
        expected = load_expected_output("dataframe_operations", "select_with_alias")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(
            df.id.alias("employee_id"),
            df.name.alias("employee_name"),
            df.age.alias("employee_age")
        )
        
        assert_dataframes_equal(result, expected)

    def test_filter_operations(self, spark):
        """Test DataFrame filter operations."""
        expected = load_expected_output("dataframe_operations", "filter_operations")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.filter(df.age >= 35)
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="filter_with_boolean not yet implemented correctly")
    def test_filter_with_boolean(self, spark):
        """Test DataFrame filter with boolean conditions."""
        expected = load_expected_output("dataframe_operations", "filter_with_boolean")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.filter((df.age > 25) & (df.department == "IT"))
        
        assert_dataframes_equal(result, expected)

    def test_with_column(self, spark):
        """Test DataFrame withColumn operation."""
        expected = load_expected_output("dataframe_operations", "with_column")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.withColumn("bonus", df.salary * 0.1)
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="drop_column not yet implemented correctly")
    def test_drop_column(self, spark):
        """Test DataFrame drop operation."""
        expected = load_expected_output("dataframe_operations", "drop_column")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.drop("salary")
        
        assert_dataframes_equal(result, expected)

    def test_distinct(self, spark):
        """Test DataFrame distinct operation."""
        expected = load_expected_output("dataframe_operations", "distinct")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select("department").distinct()
        
        assert_dataframes_equal(result, expected)

    def test_limit(self, spark):
        """Test DataFrame limit operation."""
        expected = load_expected_output("dataframe_operations", "limit")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.limit(2)
        
        assert_dataframes_equal(result, expected)

    def test_order_by(self, spark):
        """Test DataFrame orderBy operation."""
        expected = load_expected_output("dataframe_operations", "order_by")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy("age")
        
        assert_dataframes_equal(result, expected)

    def test_order_by_desc(self, spark):
        """Test DataFrame orderBy descending operation."""
        expected = load_expected_output("dataframe_operations", "order_by_desc")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.orderBy(df.age.desc())
        
        assert_dataframes_equal(result, expected)

    def test_group_by(self, spark):
        """Test DataFrame groupBy operation."""
        expected = load_expected_output("dataframe_operations", "group_by")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").count()
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="aggregation with dict not yet implemented correctly")
    def test_aggregation(self, spark):
        """Test DataFrame aggregation operations."""
        expected = load_expected_output("dataframe_operations", "aggregation")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.groupBy("department").agg(
            {"salary": "avg", "id": "count"}
        ).withColumnRenamed("avg(salary)", "avg_salary")
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="column_access not yet implemented correctly")
    def test_column_access(self, spark):
        """Test DataFrame column access operations."""
        expected = load_expected_output("dataframe_operations", "column_access")
        
        df = spark.createDataFrame(expected["input_data"])
        result = df.select(F.col("name"), df.age, F.col("salary"))
        
        assert_dataframes_equal(result, expected)

    def test_inner_join(self, spark):
        """Test DataFrame inner join operation."""
        expected = load_expected_output("joins", "inner_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "inner")
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="left_join not yet implemented correctly")
    def test_left_join(self, spark):
        """Test DataFrame left join operation."""
        expected = load_expected_output("joins", "left_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "left")
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="right_join not yet implemented correctly")
    def test_right_join(self, spark):
        """Test DataFrame right join operation."""
        expected = load_expected_output("joins", "right_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "right")
        
        assert_dataframes_equal(result, expected)

    @pytest.mark.skip(reason="outer_join not yet implemented correctly")
    def test_outer_join(self, spark):
        """Test DataFrame outer join operation."""
        expected = load_expected_output("joins", "outer_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "outer")
        
        assert_dataframes_equal(result, expected)

    def test_cross_join(self, spark):
        """Test DataFrame cross join operation."""
        expected = load_expected_output("joins", "cross_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.crossJoin(departments_df)
        
        assert_dataframes_equal(result, expected)

    def test_semi_join(self, spark):
        """Test DataFrame semi join operation."""
        expected = load_expected_output("joins", "semi_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "left_semi")
        
        assert_dataframes_equal(result, expected)

    def test_anti_join(self, spark):
        """Test DataFrame anti join operation."""
        expected = load_expected_output("joins", "anti_join")
        
        # Split input data into two DataFrames
        employees_data = [row for row in expected["input_data"] if "salary" in row]
        departments_data = [row for row in expected["input_data"] if "location" in row]
        
        employees_df = spark.createDataFrame(employees_data)
        departments_df = spark.createDataFrame(departments_data)
        
        result = employees_df.join(departments_df, "dept_id", "left_anti")
        
        assert_dataframes_equal(result, expected)

    def test_union(self, spark):
        """Test DataFrame union operation."""
        expected = load_expected_output("set_operations", "union")
        
        # Split input data into two DataFrames
        df1_data = expected["input_data"][:3]  # First 3 rows
        df2_data = expected["input_data"][3:]  # Last 3 rows
        
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        
        result = df1.union(df2)
        
        assert_dataframes_equal(result, expected)

    def test_union_all(self, spark):
        """Test DataFrame unionAll operation."""
        expected = load_expected_output("set_operations", "union_all")
        
        # Split input data into two DataFrames
        df1_data = expected["input_data"][:3]  # First 3 rows
        df2_data = expected["input_data"][3:]  # Last 3 rows
        
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        
        result = df1.unionAll(df2)
        
        assert_dataframes_equal(result, expected)

    def test_intersect(self, spark):
        """Test DataFrame intersect operation."""
        expected = load_expected_output("set_operations", "intersect")
        
        # Split input data into two DataFrames
        df1_data = expected["input_data"][:3]  # First 3 rows
        df2_data = expected["input_data"][3:]  # Last 3 rows
        
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        
        result = df1.intersect(df2)
        
        assert_dataframes_equal(result, expected)

    def test_subtract(self, spark):
        """Test DataFrame subtract operation."""
        expected = load_expected_output("set_operations", "subtract")
        
        # Split input data into two DataFrames
        df1_data = expected["input_data"][:3]  # First 3 rows
        df2_data = expected["input_data"][3:]  # Last 3 rows
        
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        
        result = df1.subtract(df2)
        
        assert_dataframes_equal(result, expected)

    def test_except(self, spark):
        """Test DataFrame except operation."""
        expected = load_expected_output("set_operations", "except")
        
        # Split input data into two DataFrames
        df1_data = expected["input_data"][:3]  # First 3 rows
        df2_data = expected["input_data"][3:]  # Last 3 rows
        
        df1 = spark.createDataFrame(df1_data)
        df2 = spark.createDataFrame(df2_data)
        
        result = df1.exceptAll(df2)
        
        assert_dataframes_equal(result, expected)

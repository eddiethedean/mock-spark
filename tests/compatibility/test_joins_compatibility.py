"""
Compatibility tests for join operations using expected outputs.

This module tests MockSpark's join operations against PySpark-generated expected outputs
to ensure compatibility across different join types and conditions.
"""

import pytest
from mock_spark import MockSparkSession, F
from tests.tools.output_loader import load_expected_output
from tests.tools.comparison_utils import assert_dataframes_equal


@pytest.mark.compatibility
class TestJoinsCompatibility:
    """Tests for join operations compatibility using expected outputs."""

    @pytest.fixture(scope="class")
    def expected_outputs(self):
        """Load expected outputs for join operations."""
        return load_expected_output("joins", "inner_join", "3.2")

    def test_inner_join(self, mock_spark_session, expected_outputs):
        """Test inner join against expected output."""
        # Create test data
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        # Perform inner join
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "inner")
        
        # Load expected output
        expected = load_expected_output("joins", "inner_join")
        assert_dataframes_equal(result, expected)

    def test_left_join(self, mock_spark_session):
        """Test left join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left")
        
        expected = load_expected_output("joins", "left_join")
        assert_dataframes_equal(result, expected)

    def test_right_join(self, mock_spark_session):
        """Test right join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "right")
        
        expected = load_expected_output("joins", "right_join")
        assert_dataframes_equal(result, expected)

    def test_outer_join(self, mock_spark_session):
        """Test outer join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "outer")
        
        expected = load_expected_output("joins", "outer_join")
        assert_dataframes_equal(result, expected)

    def test_cross_join(self, mock_spark_session):
        """Test cross join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.crossJoin(dept_df)
        
        expected = load_expected_output("joins", "cross_join")
        assert_dataframes_equal(result, expected)

    def test_semi_join(self, mock_spark_session):
        """Test semi join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_semi")
        
        expected = load_expected_output("joins", "semi_join")
        assert_dataframes_equal(result, expected)

    def test_anti_join(self, mock_spark_session):
        """Test anti join against expected output."""
        employees_data = [
            {"id": 1, "name": "Alice", "dept_id": 10, "salary": 50000},
            {"id": 2, "name": "Bob", "dept_id": 20, "salary": 60000},
            {"id": 3, "name": "Charlie", "dept_id": 10, "salary": 70000},
            {"id": 4, "name": "David", "dept_id": 30, "salary": 55000},
        ]
        
        departments_data = [
            {"dept_id": 10, "name": "IT", "location": "NYC"},
            {"dept_id": 20, "name": "HR", "location": "LA"},
            {"dept_id": 40, "name": "Finance", "location": "Chicago"},
        ]
        
        emp_df = mock_spark_session.createDataFrame(employees_data)
        dept_df = mock_spark_session.createDataFrame(departments_data)
        
        result = emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left_anti")
        
        expected = load_expected_output("joins", "anti_join")
        assert_dataframes_equal(result, expected)

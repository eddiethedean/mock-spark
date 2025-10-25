"""
Unit tests for the ExpressionEvaluator class.
"""

import pytest
import datetime as dt_module
from mock_spark.dataframe.evaluation.expression_evaluator import ExpressionEvaluator
from mock_spark.functions import MockColumn, MockColumnOperation, MockLiteral
from mock_spark.functions.conditional import MockCaseWhen
from mock_spark.spark_types import StringType, IntegerType


class TestExpressionEvaluator:
    """Test cases for ExpressionEvaluator."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.evaluator = ExpressionEvaluator()
        self.row = {
            "name": "Alice",
            "age": 25,
            "salary": 50000.0,
            "timestamp": "2023-01-01T10:30:00",
            "null_col": None,
        }

    def test_evaluate_expression_mock_column(self):
        """Test evaluation of MockColumn expressions."""
        col = MockColumn("name")
        result = self.evaluator.evaluate_expression(self.row, col)
        assert result == "Alice"

    def test_evaluate_expression_mock_literal(self):
        """Test evaluation of MockLiteral expressions."""
        literal = MockLiteral("test", StringType())
        result = self.evaluator.evaluate_expression(self.row, literal)
        assert result == "test"

    def test_evaluate_expression_arithmetic_operation(self):
        """Test evaluation of arithmetic operations."""
        # Test addition
        op = MockColumnOperation(MockColumn("age"), "+", MockLiteral(5, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 30

        # Test multiplication
        op = MockColumnOperation(MockColumn("age"), "*", MockLiteral(2, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50

        # Test division
        op = MockColumnOperation(
            MockColumn("salary"), "/", MockLiteral(1000, IntegerType())
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50.0

    def test_evaluate_expression_comparison_operation(self):
        """Test evaluation of comparison operations."""
        # Test equality
        op = MockColumnOperation(
            MockColumn("age"), "==", MockLiteral(25, IntegerType())
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is True

        # Test greater than
        op = MockColumnOperation(MockColumn("age"), ">", MockLiteral(20, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is True

        # Test less than
        op = MockColumnOperation(MockColumn("age"), "<", MockLiteral(30, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is True

    def test_evaluate_expression_string_functions(self):
        """Test evaluation of string functions."""
        # Test upper function
        op = MockColumnOperation(MockColumn("name"), "upper", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "ALICE"

        # Test lower function
        op = MockColumnOperation(MockColumn("name"), "lower", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "alice"

        # Test length function
        op = MockColumnOperation(MockColumn("name"), "length", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 5

        # Test trim function
        op = MockColumnOperation(MockColumn("name"), "trim", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "Alice"

    def test_evaluate_expression_math_functions(self):
        """Test evaluation of math functions."""
        # Test abs function
        op = MockColumnOperation(MockColumn("age"), "abs", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 25

        # Test round function
        op = MockColumnOperation(MockColumn("salary"), "round", None)
        op.precision = 0
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50000

        # Test ceil function
        op = MockColumnOperation(MockColumn("salary"), "ceil", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50000

        # Test floor function
        op = MockColumnOperation(MockColumn("salary"), "floor", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50000

        # Test sqrt function
        op = MockColumnOperation(MockColumn("age"), "sqrt", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 5.0

    def test_evaluate_expression_datetime_functions(self):
        """Test evaluation of datetime functions."""
        # Test hour function
        op = MockColumnOperation(MockColumn("timestamp"), "hour", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 10

        # Test day function
        op = MockColumnOperation(MockColumn("timestamp"), "day", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 1

        # Test month function
        op = MockColumnOperation(MockColumn("timestamp"), "month", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 1

        # Test year function
        op = MockColumnOperation(MockColumn("timestamp"), "year", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 2023

    def test_evaluate_expression_cast_function(self):
        """Test evaluation of cast function."""
        # Test cast to string
        op = MockColumnOperation(MockColumn("age"), "cast", "string")
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "25"

        # Test cast to double
        op = MockColumnOperation(MockColumn("age"), "cast", "double")
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 25.0

        # Test cast to int
        op = MockColumnOperation(MockColumn("salary"), "cast", "int")
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 50000

    def test_evaluate_expression_coalesce_function(self):
        """Test evaluation of coalesce function."""
        # Test coalesce with non-null value
        op = MockColumnOperation(
            MockColumn("name"), "coalesce", [MockLiteral("default", StringType())]
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "Alice"

        # Test coalesce with null value
        op = MockColumnOperation(
            MockColumn("null_col"), "coalesce", [MockLiteral("default", StringType())]
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "default"

    def test_evaluate_expression_isnull_function(self):
        """Test evaluation of isnull function."""
        # Test isnull with non-null value
        op = MockColumnOperation(MockColumn("name"), "isnull", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is False

        # Test isnull with null value
        op = MockColumnOperation(MockColumn("null_col"), "isnull", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is True

    def test_evaluate_expression_isnan_function(self):
        """Test evaluation of isnan function."""
        # Test isnan with normal float
        op = MockColumnOperation(MockColumn("salary"), "isnan", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is False

        # Test isnan with NaN
        row_with_nan = self.row.copy()
        row_with_nan["salary"] = float("nan")
        result = self.evaluator.evaluate_expression(row_with_nan, op)
        assert result is True

    def test_evaluate_expression_current_timestamp_function(self):
        """Test evaluation of current_timestamp function."""
        op = MockColumnOperation(None, "current_timestamp", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert isinstance(result, dt_module.datetime)

    def test_evaluate_expression_current_date_function(self):
        """Test evaluation of current_date function."""
        op = MockColumnOperation(None, "current_date", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert isinstance(result, dt_module.date)

    def test_evaluate_expression_case_when(self):
        """Test evaluation of case when expressions."""
        # Create a MockCaseWhen object using method chaining
        case_when = (
            MockCaseWhen()
            .when(
                MockColumnOperation(
                    MockColumn("age"), ">", MockLiteral(20, IntegerType())
                ),
                MockLiteral("adult", StringType()),
            )
            .when(
                MockColumnOperation(
                    MockColumn("age"), ">", MockLiteral(10, IntegerType())
                ),
                MockLiteral("teen", StringType()),
            )
            .otherwise(MockLiteral("child", StringType()))
        )

        result = self.evaluator.evaluate_expression(self.row, case_when)
        assert result == "adult"

    def test_evaluate_expression_nested_operations(self):
        """Test evaluation of nested operations."""
        # Test nested arithmetic: (age + 5) * 2
        inner_op = MockColumnOperation(
            MockColumn("age"), "+", MockLiteral(5, IntegerType())
        )
        outer_op = MockColumnOperation(inner_op, "*", MockLiteral(2, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, outer_op)
        assert result == 60

    def test_evaluate_expression_null_handling(self):
        """Test evaluation with null values."""
        # Test arithmetic with null
        op = MockColumnOperation(
            MockColumn("null_col"), "+", MockLiteral(5, IntegerType())
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is None

        # Test comparison with null
        op = MockColumnOperation(
            MockColumn("null_col"), "==", MockLiteral(5, IntegerType())
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is None

    def test_evaluate_expression_division_by_zero(self):
        """Test evaluation of division by zero."""
        op = MockColumnOperation(MockColumn("age"), "/", MockLiteral(0, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is None

    def test_evaluate_expression_modulo_by_zero(self):
        """Test evaluation of modulo by zero."""
        op = MockColumnOperation(MockColumn("age"), "%", MockLiteral(0, IntegerType()))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result is None

    def test_evaluate_expression_unary_minus(self):
        """Test evaluation of unary minus operation."""
        op = MockColumnOperation(MockColumn("age"), "-", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == -25

    def test_evaluate_expression_direct_value(self):
        """Test evaluation of direct values."""
        result = self.evaluator.evaluate_expression(self.row, "direct_string")
        assert result == "direct_string"

        result = self.evaluator.evaluate_expression(self.row, 42)
        assert result == 42

    def test_evaluate_expression_aliased_function_call(self):
        """Test evaluation of aliased function calls."""
        # Create an aliased column
        original_col = MockColumn("upper(name)")
        aliased_col = MockColumn("upper_name")
        aliased_col._original_column = original_col

        result = self.evaluator.evaluate_expression(self.row, aliased_col)
        assert result == "ALICE"

    def test_evaluate_expression_function_call_by_name(self):
        """Test evaluation of function calls by name."""
        # Test upper function by name
        result = self.evaluator._evaluate_function_call_by_name(self.row, "upper(name)")
        assert result == "ALICE"

        # Test lower function by name
        result = self.evaluator._evaluate_function_call_by_name(self.row, "lower(name)")
        assert result == "alice"

    def test_evaluate_expression_format_string_function(self):
        """Test evaluation of format_string function."""
        op = MockColumnOperation(
            MockColumn("name"), "format_string", ("Hello %s!", [MockColumn("name")])
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "Hello Alice!"

    def test_evaluate_expression_expr_function(self):
        """Test evaluation of expr function."""
        op = MockColumnOperation(None, "expr", "upper(name)")
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "ALICE"

    def test_evaluate_expression_regexp_replace_function(self):
        """Test evaluation of regexp_replace function."""
        op = MockColumnOperation(MockColumn("name"), "regexp_replace", ("A", "X"))
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == "Xlice"

    def test_evaluate_expression_split_function(self):
        """Test evaluation of split function."""
        op = MockColumnOperation(MockColumn("name"), "split", "l")
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == ["A", "ice"]

    def test_evaluate_expression_base64_functions(self):
        """Test evaluation of base64 functions."""
        # Test base64 encode
        op = MockColumnOperation(MockColumn("name"), "base64", None)
        encoded_result = self.evaluator.evaluate_expression(self.row, op)
        assert isinstance(encoded_result, str)

        # Test base64 decode with the encoded result
        # We need to create a new row with the encoded value
        encoded_row = {"name": encoded_result}
        op = MockColumnOperation(MockColumn("name"), "unbase64", None)
        result = self.evaluator.evaluate_expression(encoded_row, op)
        assert isinstance(result, bytes)

    def test_evaluate_expression_ascii_function(self):
        """Test evaluation of ascii function."""
        op = MockColumnOperation(MockColumn("name"), "ascii", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 65  # ASCII value of 'A'

    def test_evaluate_expression_quarter_function(self):
        """Test evaluation of quarter function."""
        op = MockColumnOperation(MockColumn("timestamp"), "quarter", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 1  # January is Q1

    def test_evaluate_expression_dayofweek_function(self):
        """Test evaluation of dayofweek function."""
        op = MockColumnOperation(MockColumn("timestamp"), "dayofweek", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result in range(1, 8)  # Sunday=1, Monday=2, etc.

    def test_evaluate_expression_dayofyear_function(self):
        """Test evaluation of dayofyear function."""
        op = MockColumnOperation(MockColumn("timestamp"), "dayofyear", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 1  # January 1st is day 1

    def test_evaluate_expression_weekofyear_function(self):
        """Test evaluation of weekofyear function."""
        op = MockColumnOperation(MockColumn("timestamp"), "weekofyear", None)
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result in range(1, 54)  # Week numbers 1-53

    def test_evaluate_expression_datediff_function(self):
        """Test evaluation of datediff function."""
        op = MockColumnOperation(
            MockColumn("timestamp"), "datediff", MockColumn("timestamp")
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 0  # Same date should have 0 difference

    def test_evaluate_expression_months_between_function(self):
        """Test evaluation of months_between function."""
        op = MockColumnOperation(
            MockColumn("timestamp"), "months_between", MockColumn("timestamp")
        )
        result = self.evaluator.evaluate_expression(self.row, op)
        assert result == 0.0  # Same date should have 0 months difference

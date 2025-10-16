"""
Lambda Expression Parser for Mock Spark.

This module provides AST parsing and translation of Python lambda expressions
to DuckDB-compatible lambda syntax for use in higher-order functions.

Key Features:
    - Parse Python lambda expressions using AST module
    - Translate to DuckDB lambda syntax: `x -> expression`
    - Support 1-arg and 2-arg lambdas
    - Handle arithmetic, comparison, and logical operators
    - Type-safe with comprehensive error handling

Example:
    >>> from mock_spark.functions.core.lambda_parser import LambdaParser
    >>> parser = LambdaParser(lambda x: x * 2)
    >>> parser.to_duckdb_lambda()
    'x -> (x * 2)'
"""

import ast
import inspect
from typing import Any, Callable, List


class LambdaTranslationError(Exception):
    """Raised when a lambda expression cannot be translated to DuckDB syntax."""
    pass


class LambdaParser:
    """Parser for Python lambda expressions to DuckDB lambda syntax.
    
    Parses Python lambda functions using the AST module and translates them
    to DuckDB-compatible lambda syntax for use in LIST_TRANSFORM, LIST_FILTER,
    and other higher-order array/map functions.
    
    Attributes:
        lambda_func: The Python lambda function to parse.
        ast_node: The parsed AST Lambda node.
        param_names: List of parameter names from the lambda.
    """

    def __init__(self, lambda_func: Callable[..., Any]):
        """Initialize LambdaParser.
        
        Args:
            lambda_func: A Python lambda function to parse.
            
        Raises:
            LambdaTranslationError: If the function is not a lambda or cannot be parsed.
        """
        self.lambda_func = lambda_func
        
        # Get the source code of the lambda
        try:
            source = inspect.getsource(lambda_func)
        except (OSError, TypeError) as e:
            raise LambdaTranslationError(f"Cannot get source for lambda: {e}")
        
        # Parse the lambda expression
        try:
            # Clean up the source - extract just the lambda part
            # Look for "lambda " (with space) or "lambda:" to find the actual lambda keyword
            lambda_start = -1
            for pattern in ["lambda ", "lambda:"]:
                idx = source.find(pattern)
                if idx != -1:
                    lambda_start = idx
                    break
            
            if lambda_start == -1:
                raise LambdaTranslationError("Not a lambda function")
            
            # Extract from 'lambda' onward
            lambda_expr = source[lambda_start:]
            
            # Remove trailing characters that might cause issues
            # We need to find where the lambda expression ends
            # This is tricky because lambda can be nested in function calls
            
            # Try to find the end by matching parentheses
            # Key insight: only after the ':' (colon) in lambda should we look for
            # terminating commas. Before the colon, commas are parameter separators.
            paren_depth = 0
            end_idx = len(lambda_expr)
            seen_colon = False
            
            for i, char in enumerate(lambda_expr):
                if char == ':':
                    seen_colon = True
                elif char == '(':
                    paren_depth += 1
                elif char == ')':
                    if paren_depth == 0:
                        # This closing paren is not part of the lambda
                        end_idx = i
                        break
                    paren_depth -= 1
                elif char in [',', '\n'] and paren_depth == 0 and seen_colon:
                    # End of lambda expression (only after we've seen the colon)
                    end_idx = i
                    break
            
            lambda_expr = lambda_expr[:end_idx].strip()
            
            # Try to parse as an expression
            tree = ast.parse(lambda_expr, mode='eval')
            
            if not isinstance(tree.body, ast.Lambda):
                raise LambdaTranslationError("Parsed expression is not a lambda")
                
            self.ast_node = tree.body
            
        except SyntaxError as e:
            raise LambdaTranslationError(f"Cannot parse lambda: {e}")
        
        # Extract parameter names
        self.param_names = self.get_param_names()

    def get_param_names(self) -> List[str]:
        """Extract parameter names from the lambda.
        
        Returns:
            List of parameter names.
        """
        args = self.ast_node.args
        param_names = []
        
        for arg in args.args:
            param_names.append(arg.arg)
            
        return param_names

    def to_duckdb_lambda(self) -> str:
        """Translate the Python lambda to DuckDB lambda syntax.
        
        Returns:
            DuckDB lambda expression as a string.
            
        Example:
            Python: lambda x: x * 2
            DuckDB: x -> (x * 2)
            
            Python: lambda x, y: x + y
            DuckDB: (x, y) -> (x + y)
        """
        # Format parameters
        if len(self.param_names) == 1:
            params = self.param_names[0]
        else:
            params = f"({', '.join(self.param_names)})"
        
        # Translate body
        body_expr = self._translate_expression(self.ast_node.body)
        
        return f"{params} -> {body_expr}"

    def _translate_expression(self, node: ast.expr) -> str:
        """Recursively translate an AST expression node to DuckDB SQL.
        
        Args:
            node: AST expression node.
            
        Returns:
            DuckDB SQL expression string.
            
        Raises:
            LambdaTranslationError: If the expression type is not supported.
        """
        if isinstance(node, ast.Name):
            # Variable reference (parameter name)
            return node.id
        
        elif isinstance(node, ast.Constant):
            # Literal value (numbers, strings, etc.)
            if isinstance(node.value, str):
                return f"'{node.value}'"
            return str(node.value)
        
        elif isinstance(node, ast.Num):  # Python 3.7 compatibility
            return str(node.n)
        
        elif isinstance(node, ast.BinOp):
            # Binary operation (x + y, x * y, etc.)
            left = self._translate_expression(node.left)
            right = self._translate_expression(node.right)
            op = self._translate_operator(node.op)
            return f"({left} {op} {right})"
        
        elif isinstance(node, ast.Compare):
            # Comparison (x > 10, x == 5, etc.)
            left = self._translate_expression(node.left)
            
            if len(node.ops) == 1 and len(node.comparators) == 1:
                op = self._translate_comparison(node.ops[0])
                right = self._translate_expression(node.comparators[0])
                return f"({left} {op} {right})"
            else:
                # Multiple comparisons (x > 0 and x < 100)
                parts = [left]
                for op, comp in zip(node.ops, node.comparators):
                    op_str = self._translate_comparison(op)
                    right = self._translate_expression(comp)
                    parts.append(f"{op_str} {right}")
                return f"({' '.join(parts)})"
        
        elif isinstance(node, ast.BoolOp):
            # Boolean operation (and, or)
            op = self._translate_bool_op(node.op)
            values = [self._translate_expression(v) for v in node.values]
            return f"({f' {op} '.join(values)})"
        
        elif isinstance(node, ast.UnaryOp):
            # Unary operation (-x, not x)
            operand = self._translate_expression(node.operand)
            op = self._translate_unary_op(node.op)
            return f"({op}{operand})"
        
        elif isinstance(node, ast.Call):
            # Function call - may be a Spark function
            # For now, raise an error - we'll add support later
            raise LambdaTranslationError(
                "Function calls in lambdas not yet supported. "
                "Lambda body must be a simple expression."
            )
        
        else:
            raise LambdaTranslationError(
                f"Unsupported expression type: {type(node).__name__}"
            )

    def _translate_operator(self, op: ast.operator) -> str:
        """Translate Python operator to DuckDB operator.
        
        Args:
            op: AST operator node.
            
        Returns:
            DuckDB operator string.
        """
        operator_map = {
            ast.Add: '+',
            ast.Sub: '-',
            ast.Mult: '*',
            ast.Div: '/',
            ast.FloorDiv: '//',
            ast.Mod: '%',
            ast.Pow: '**',
        }
        
        op_type = type(op)
        if op_type in operator_map:
            return operator_map[op_type]
        
        raise LambdaTranslationError(f"Unsupported operator: {op_type.__name__}")

    def _translate_comparison(self, op: ast.cmpop) -> str:
        """Translate Python comparison operator to DuckDB.
        
        Args:
            op: AST comparison operator node.
            
        Returns:
            DuckDB comparison operator string.
        """
        comparison_map = {
            ast.Eq: '=',  # DuckDB uses = for equality
            ast.NotEq: '!=',
            ast.Lt: '<',
            ast.LtE: '<=',
            ast.Gt: '>',
            ast.GtE: '>=',
        }
        
        op_type = type(op)
        if op_type in comparison_map:
            return comparison_map[op_type]
        
        raise LambdaTranslationError(f"Unsupported comparison: {op_type.__name__}")

    def _translate_bool_op(self, op: ast.boolop) -> str:
        """Translate Python boolean operator to DuckDB.
        
        Args:
            op: AST boolean operator node.
            
        Returns:
            DuckDB boolean operator string.
        """
        if isinstance(op, ast.And):
            return "AND"
        elif isinstance(op, ast.Or):
            return "OR"
        
        raise LambdaTranslationError(f"Unsupported boolean operator: {type(op).__name__}")

    def _translate_unary_op(self, op: ast.unaryop) -> str:
        """Translate Python unary operator to DuckDB.
        
        Args:
            op: AST unary operator node.
            
        Returns:
            DuckDB unary operator string.
        """
        if isinstance(op, ast.USub):
            return "-"
        elif isinstance(op, ast.Not):
            return "NOT "
        
        raise LambdaTranslationError(f"Unsupported unary operator: {type(op).__name__}")


class MockLambdaExpression:
    """Wrapper for Python lambda expressions used in Spark functions.
    
    This class wraps a Python lambda function and provides methods to
    translate it to DuckDB lambda syntax for use in higher-order functions
    like transform, filter, exists, etc.
    
    Attributes:
        lambda_func: The Python lambda function.
        parser: LambdaParser instance for this lambda.
        param_count: Number of parameters in the lambda.
    
    Example:
        >>> expr = MockLambdaExpression(lambda x: x * 2)
        >>> expr.to_duckdb_lambda()
        'x -> (x * 2)'
    """

    def __init__(self, lambda_func: Callable[..., Any]):
        """Initialize MockLambdaExpression.
        
        Args:
            lambda_func: A Python lambda function.
        """
        self.lambda_func = lambda_func
        self.parser = LambdaParser(lambda_func)
        self.param_count = len(self.parser.param_names)

    def to_duckdb_lambda(self) -> str:
        """Convert to DuckDB lambda syntax.
        
        Returns:
            DuckDB lambda expression string.
        """
        return self.parser.to_duckdb_lambda()

    def get_param_names(self) -> List[str]:
        """Get parameter names.
        
        Returns:
            List of parameter names.
        """
        return self.parser.param_names

    def __repr__(self) -> str:
        """String representation."""
        try:
            lambda_str = self.to_duckdb_lambda()
            return f"MockLambdaExpression({lambda_str})"
        except Exception:
            return f"MockLambdaExpression(params={self.param_count})"


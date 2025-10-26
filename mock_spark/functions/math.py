"""
Mathematical functions for Mock Spark.

This module provides comprehensive mathematical functions that match PySpark's
math function API. Includes arithmetic operations, rounding functions, trigonometric
functions, and mathematical transformations for numerical processing in DataFrames.

Key Features:
    - Complete PySpark math function API compatibility
    - Arithmetic operations (abs, round, ceil, floor)
    - Advanced math functions (sqrt, exp, log, pow)
    - Trigonometric functions (sin, cos, tan)
    - Type-safe operations with proper return types
    - Support for both column references and numeric literals
    - Proper handling of edge cases and null values

Example:
    >>> from mock_spark import MockSparkSession, F
    >>> spark = MockSparkSession("test")
    >>> data = [{"value": 3.7, "angle": 1.57}]
    >>> df = spark.createDataFrame(data)
    >>> df.select(
    ...     F.round(F.col("value"), 1),
    ...     F.ceil(F.col("value")),
    ...     F.sin(F.col("angle"))
    ... ).show()
    +--- MockDataFrame: 1 rows ---+
    round(value, 1) |  ceil(value) |   sin(angle)
    ---------------------------------------------
             4.0 |            4 |         1.57
"""

from typing import Union, Optional
from mock_spark.functions.base import MockColumn, MockColumnOperation


class MathFunctions:
    """Collection of mathematical functions."""

    @staticmethod
    def abs(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get absolute value.

        Args:
            column: The column to get absolute value of.

        Returns:
            MockColumnOperation representing the abs function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "abs", name=f"abs({column.name})")
        return operation

    @staticmethod
    def round(column: Union[MockColumn, str], scale: int = 0) -> MockColumnOperation:
        """Round to specified number of decimal places.

        Args:
            column: The column to round.
            scale: Number of decimal places (default: 0).

        Returns:
            MockColumnOperation representing the round function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(
            column, "round", scale, name=f"round({column.name}, {scale})"
        )
        return operation

    @staticmethod
    def ceil(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round up to nearest integer.

        Args:
            column: The column to round up.

        Returns:
            MockColumnOperation representing the ceil function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "ceil", name=f"CEIL({column.name})")
        return operation

    @staticmethod
    def floor(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Round down to nearest integer.

        Args:
            column: The column to round down.

        Returns:
            MockColumnOperation representing the floor function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "floor", name=f"FLOOR({column.name})")
        return operation

    @staticmethod
    def sqrt(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get square root.

        Args:
            column: The column to get square root of.

        Returns:
            MockColumnOperation representing the sqrt function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "sqrt", name=f"SQRT({column.name})")
        return operation

    @staticmethod
    def exp(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get exponential (e^x).

        Args:
            column: The column to get exponential of.

        Returns:
            MockColumnOperation representing the exp function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # PySpark uses uppercase EXP in column names
        operation = MockColumnOperation(column, "exp", name=f"EXP({column.name})")
        return operation

    @staticmethod
    def log(
        column: Union[MockColumn, str], base: Optional[float] = None
    ) -> MockColumnOperation:
        """Get logarithm.

        Args:
            column: The column to get logarithm of.
            base: Optional base for logarithm (default: natural log).

        Returns:
            MockColumnOperation representing the log function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # PySpark's log() uses natural logarithm and names the column with 'ln'
        name = (
            f"log({base}, {column.name})" if base is not None else f"ln({column.name})"
        )
        operation = MockColumnOperation(column, "log", base, name=name)
        return operation

    @staticmethod
    def log10(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get base-10 logarithm (PySpark 3.0+).

        Args:
            column: The column to get log10 of.

        Returns:
            MockColumnOperation representing the log10 function.

        Example:
            >>> df.select(F.log10(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "log10", name=f"log10({column.name})")

    @staticmethod
    def log2(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get base-2 logarithm (PySpark 3.0+).

        Args:
            column: The column to get log2 of.

        Returns:
            MockColumnOperation representing the log2 function.

        Example:
            >>> df.select(F.log2(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "log2", name=f"log2({column.name})")

    @staticmethod
    def log1p(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get natural logarithm of (1 + x) (PySpark 3.0+).

        Computes ln(1 + x) accurately for small values of x.

        Args:
            column: The column to compute log1p of.

        Returns:
            MockColumnOperation representing the log1p function.

        Example:
            >>> df.select(F.log1p(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "log1p", name=f"log1p({column.name})")

    @staticmethod
    def expm1(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get exp(x) - 1 (PySpark 3.0+).

        Computes e^x - 1 accurately for small values of x.

        Args:
            column: The column to compute expm1 of.

        Returns:
            MockColumnOperation representing the expm1 function.

        Example:
            >>> df.select(F.expm1(F.col("value")))
        """
        if isinstance(column, str):
            column = MockColumn(column)

        return MockColumnOperation(column, "expm1", name=f"expm1({column.name})")

    @staticmethod
    def pow(
        column: Union[MockColumn, str], exponent: Union[MockColumn, float, int]
    ) -> MockColumnOperation:
        """Raise to power.

        Args:
            column: The column to raise to power.
            exponent: The exponent.

        Returns:
            MockColumnOperation representing the pow function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # PySpark uses uppercase POWER in column names with decimal exponent
        exponent_str = f"{float(exponent)}" if isinstance(exponent, (int, float)) else str(exponent)
        operation = MockColumnOperation(
            column, "pow", exponent, name=f"POWER({column.name}, {exponent_str})"
        )
        return operation

    @staticmethod
    def sin(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get sine.

        Args:
            column: The column to get sine of.

        Returns:
            MockColumnOperation representing the sin function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "sin", name=f"SIN({column.name})")
        return operation

    @staticmethod
    def cos(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get cosine.

        Args:
            column: The column to get cosine of.

        Returns:
            MockColumnOperation representing the cos function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "cos", name=f"COS({column.name})")
        return operation

    @staticmethod
    def tan(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get tangent.

        Args:
            column: The column to get tangent of.

        Returns:
            MockColumnOperation representing the tan function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        operation = MockColumnOperation(column, "tan", name=f"TAN({column.name})")
        return operation

    @staticmethod
    def sign(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Get sign of number (-1, 0, or 1).

        Args:
            column: The column to get sign of.

        Returns:
            MockColumnOperation representing the sign function.
        """
        if isinstance(column, str):
            column = MockColumn(column)

        # PySpark 3.2 uses signum, not sign, as the function name
        operation = MockColumnOperation(column, "signum", name=f"signum({column.name})")
        return operation

    @staticmethod
    def greatest(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Get the greatest value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            MockColumnOperation representing the greatest function.
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = (
            MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        )
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = MockColumnOperation(
            base_column,
            "greatest",
            columns[1:],
            name=f"greatest({', '.join(column_names)})",
        )
        return operation

    @staticmethod
    def least(*columns: Union[MockColumn, str]) -> MockColumnOperation:
        """Get the least value among columns.

        Args:
            *columns: Columns to compare.

        Returns:
            MockColumnOperation representing the least function.
        """
        if not columns:
            raise ValueError("At least one column must be provided")

        base_column = (
            MockColumn(columns[0]) if isinstance(columns[0], str) else columns[0]
        )
        column_names = [
            col.name if hasattr(col, "name") else str(col) for col in columns
        ]
        operation = MockColumnOperation(
            base_column, "least", columns[1:], name=f"least({', '.join(column_names)})"
        )
        return operation

    @staticmethod
    def acosh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse hyperbolic cosine (arc hyperbolic cosine).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the acosh function.

        Note:
            Input must be >= 1. Returns NaN for invalid inputs.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "acosh", name=f"acosh({column.name})")

    @staticmethod
    def asinh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse hyperbolic sine (arc hyperbolic sine).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the asinh function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "asinh", name=f"asinh({column.name})")

    @staticmethod
    def atanh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse hyperbolic tangent (arc hyperbolic tangent).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the atanh function.

        Note:
            Input must be in range (-1, 1). Returns NaN for invalid inputs.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "atanh", name=f"atanh({column.name})")

    @staticmethod
    def acos(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse cosine (arc cosine).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the acos function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "acos", name=f"acos({column.name})")

    @staticmethod
    def asin(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse sine (arc sine).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the asin function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "asin", name=f"asin({column.name})")

    @staticmethod
    def atan(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute inverse tangent (arc tangent).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the atan function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "atan", name=f"atan({column.name})")

    @staticmethod
    def atan2(
        y: Union[MockColumn, str, float, int], x: Union[MockColumn, str, float, int]
    ) -> MockColumnOperation:
        """Compute 2-argument arctangent (PySpark 3.0+).

        Returns the angle theta from the conversion of rectangular coordinates (x, y)
        to polar coordinates (r, theta).

        Args:
            y: Y coordinate (column or numeric value).
            x: X coordinate (column or numeric value).

        Returns:
            MockColumnOperation representing the atan2 function.

        Example:
            >>> df.select(F.atan2(F.col("y"), F.col("x")))
            >>> df.select(F.atan2(F.lit(1.0), F.lit(1.0)))  # Returns π/4
        """
        if isinstance(y, str):
            y = MockColumn(y)
        elif isinstance(y, (int, float)):
            from mock_spark.functions.core.literals import MockLiteral

            y = MockLiteral(y)  # type: ignore[assignment]

        return MockColumnOperation(y, "atan2", x, name=f"atan2({y}, {x})")

    @staticmethod
    def cosh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute hyperbolic cosine.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the cosh function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "cosh", name=f"cosh({column.name})")

    @staticmethod
    def sinh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute hyperbolic sine.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the sinh function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "sinh", name=f"sinh({column.name})")

    @staticmethod
    def tanh(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute hyperbolic tangent.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the tanh function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "tanh", name=f"tanh({column.name})")

    @staticmethod
    def degrees(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert radians to degrees.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the degrees function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "degrees", name=f"degrees({column.name})")

    @staticmethod
    def radians(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Convert degrees to radians.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the radians function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "radians", name=f"radians({column.name})")

    @staticmethod
    def cbrt(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute cube root.

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the cbrt function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "cbrt", name=f"cbrt({column.name})")

    @staticmethod
    def factorial(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute factorial.

        Args:
            col: Column or column name (non-negative integers).

        Returns:
            MockColumnOperation representing the factorial function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(
            column, "factorial", name=f"factorial({column.name})"
        )

    @staticmethod
    def rand(seed: Optional[int] = None) -> MockColumnOperation:
        """Generate a random column with i.i.d. samples from U[0.0, 1.0].

        Args:
            seed: Random seed (optional).

        Returns:
            MockColumnOperation representing the rand function.
        """
        from mock_spark.functions.core.literals import MockLiteral

        return MockColumnOperation(
            MockLiteral(0),
            "rand",
            value=seed,
            name=f"rand({seed})" if seed is not None else "rand()",
        )

    @staticmethod
    def randn(seed: Optional[int] = None) -> MockColumnOperation:
        """Generate a random column with i.i.d. samples from standard normal distribution.

        Args:
            seed: Random seed (optional).

        Returns:
            MockColumnOperation representing the randn function.
        """
        from mock_spark.functions.core.literals import MockLiteral

        return MockColumnOperation(
            MockLiteral(0),
            "randn",
            value=seed,
            name=f"randn({seed})" if seed is not None else "randn()",
        )

    @staticmethod
    def rint(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Round to nearest integer using banker's rounding (half to even).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the rint function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "rint", name=f"rint({column.name})")

    @staticmethod
    def bround(col: Union[MockColumn, str], scale: int = 0) -> MockColumnOperation:
        """Round using HALF_EVEN rounding mode (banker's rounding).

        Args:
            col: Column or column name.
            scale: Number of decimal places (default 0).

        Returns:
            MockColumnOperation representing the bround function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(
            column, "bround", value=scale, name=f"bround({column.name}, {scale})"
        )

    @staticmethod
    def hypot(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Compute sqrt(col1^2 + col2^2) (hypotenuse).

        Args:
            col1: First column
            col2: Second column

        Returns:
            MockColumnOperation representing the hypot function.
        """
        column1 = MockColumn(col1) if isinstance(col1, str) else col1
        column2 = MockColumn(col2) if isinstance(col2, str) else col2

        return MockColumnOperation(
            column1,
            "hypot",
            value=column2,
            name=f"hypot({column1.name}, {column2.name})",
        )

    @staticmethod
    def nanvl(
        col1: Union[MockColumn, str], col2: Union[MockColumn, str]
    ) -> MockColumnOperation:
        """Returns col1 if not NaN, or col2 if col1 is NaN.

        Args:
            col1: First column
            col2: Second column (replacement for NaN)

        Returns:
            MockColumnOperation representing the nanvl function.
        """
        column1 = MockColumn(col1) if isinstance(col1, str) else col1
        column2 = MockColumn(col2) if isinstance(col2, str) else col2

        return MockColumnOperation(
            column1,
            "nanvl",
            value=column2,
            name=f"nanvl({column1.name}, {column2.name})",
        )

    @staticmethod
    def signum(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute the signum function (sign: -1, 0, or 1).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the signum function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "signum", name=f"signum({column.name})")

    # Priority 2: New Math Functions (PySpark 3.3+/3.5+)
    @staticmethod
    def cot(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute cotangent (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the cot function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "cot", name=f"cot({column.name})")

    @staticmethod
    def csc(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute cosecant (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the csc function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "csc", name=f"csc({column.name})")

    @staticmethod
    def sec(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute secant (PySpark 3.3+).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the sec function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "sec", name=f"sec({column.name})")

    @staticmethod
    def e() -> MockColumnOperation:
        """Return Euler's number e (PySpark 3.5+).

        Returns:
            MockColumnOperation representing Euler's number constant.
        """
        from mock_spark.functions.core.literals import MockLiteral
        import math

        return MockColumnOperation(MockLiteral(math.e), "lit", name="E()")

    @staticmethod
    def pi() -> MockColumnOperation:
        """Return the value of pi (PySpark 3.5+).

        Returns:
            MockColumnOperation representing pi constant.
        """
        from mock_spark.functions.core.literals import MockLiteral
        import math

        return MockColumnOperation(MockLiteral(math.pi), "lit", name="PI()")

    @staticmethod
    def ln(col: Union[MockColumn, str]) -> MockColumnOperation:
        """Compute natural logarithm (alias for log) (PySpark 3.5+).

        Args:
            col: Column or column name.

        Returns:
            MockColumnOperation representing the ln function.
        """
        column = MockColumn(col) if isinstance(col, str) else col
        return MockColumnOperation(column, "log", name=f"ln({column.name})")

    # Deprecated Aliases
    @staticmethod
    def toDegrees(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for degrees (all PySpark versions).

        Use degrees instead.

        Args:
            column: Angle in radians.

        Returns:
            MockColumnOperation representing the degrees conversion.
        """
        import warnings

        warnings.warn(
            "toDegrees is deprecated. Use degrees instead.", FutureWarning, stacklevel=2
        )
        return MathFunctions.degrees(column)

    @staticmethod
    def toRadians(column: Union[MockColumn, str]) -> MockColumnOperation:
        """Deprecated alias for radians (all PySpark versions).

        Use radians instead.

        Args:
            column: Angle in degrees.

        Returns:
            MockColumnOperation representing the radians conversion.
        """
        import warnings

        warnings.warn(
            "toRadians is deprecated. Use radians instead.", FutureWarning, stacklevel=2
        )
        return MathFunctions.radians(column)

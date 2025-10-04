"""
SQL Executor for Mock Spark.

This module provides SQL execution functionality for Mock Spark,
executing parsed SQL queries and returning appropriate results.
It handles different types of SQL operations and integrates with
the storage and DataFrame systems.

Key Features:
    - SQL query execution and result generation
    - Integration with DataFrame operations
    - Support for DDL and DML operations
    - Error handling and validation
    - Result set formatting

Example:
    >>> from mock_spark.session.sql import MockSQLExecutor
    >>> executor = MockSQLExecutor(session)
    >>> result = executor.execute("SELECT * FROM users WHERE age > 18")
    >>> result.show()
"""

from typing import Any, Dict, List
from ...core.interfaces.session import ISession
from ...core.interfaces.dataframe import IDataFrame
from ...core.exceptions.execution import QueryExecutionException
from ...spark_types import MockStructType
from .parser import MockSQLAST


class MockSQLExecutor:
    """SQL Executor for Mock Spark.

    Provides SQL execution functionality that processes parsed SQL queries
    and returns appropriate results. Handles different types of SQL operations
    including SELECT, INSERT, CREATE, DROP, and other DDL/DML operations.

    Attributes:
        session: Mock Spark session instance.
        parser: SQL parser instance.

    Example:
        >>> executor = MockSQLExecutor(session)
        >>> result = executor.execute("SELECT name, age FROM users")
        >>> result.show()
    """

    def __init__(self, session: ISession):
        """Initialize MockSQLExecutor.

        Args:
            session: Mock Spark session instance.
        """
        self.session = session
        from .parser import MockSQLParser

        self.parser = MockSQLParser()

    def execute(self, query: str) -> IDataFrame:
        """Execute SQL query.

        Args:
            query: SQL query string.

        Returns:
            DataFrame with query results.

        Raises:
            QueryExecutionException: If query execution fails.
        """
        try:
            # Parse the query
            ast = self.parser.parse(query)

            # Execute based on query type
            if ast.query_type == "SELECT":
                return self._execute_select(ast)
            elif ast.query_type == "CREATE":
                return self._execute_create(ast)
            elif ast.query_type == "DROP":
                return self._execute_drop(ast)
            elif ast.query_type == "INSERT":
                return self._execute_insert(ast)
            elif ast.query_type == "UPDATE":
                return self._execute_update(ast)
            elif ast.query_type == "DELETE":
                return self._execute_delete(ast)
            elif ast.query_type == "SHOW":
                return self._execute_show(ast)
            elif ast.query_type == "DESCRIBE":
                return self._execute_describe(ast)
            else:
                raise QueryExecutionException(f"Unsupported query type: {ast.query_type}")

        except Exception as e:
            if isinstance(e, QueryExecutionException):
                raise
            raise QueryExecutionException(f"Failed to execute query: {str(e)}")

    def _execute_select(self, ast: MockSQLAST) -> IDataFrame:
        """Execute SELECT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with SELECT results.
        """
        components = ast.components

        # Get table name - handle queries without FROM clause
        from_tables = components.get("from_tables", [])
        if not from_tables:
            # Query without FROM clause (e.g., SELECT 1 as test_col)
            # Create a single row DataFrame with the literal values
            from ...dataframe import MockDataFrame
            from ...spark_types import (
                MockStructType,
                MockStructField,
                StringType,
                LongType,
                DoubleType,
                BooleanType,
            )

            # For now, create a simple DataFrame with one row
            # This is a basic implementation for literal SELECT queries
            data: List[Dict[str, Any]] = [{}]  # Empty row, we'll populate based on SELECT columns
            schema = MockStructType([])
            df = MockDataFrame(data, schema)
        else:
            table_name = from_tables[0]
            # Try to get table as DataFrame
            try:
                df = self.session.table(table_name)
                # Convert IDataFrame to MockDataFrame if needed
                if not isinstance(df, MockDataFrame):  # type: ignore[unreachable]
                    from ...dataframe import MockDataFrame

                    df = MockDataFrame(df.collect(), df.schema)
            except Exception:
                # If table doesn't exist, return empty DataFrame
                from ...dataframe import MockDataFrame
                from ...spark_types import MockStructType

                return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

        # Apply WHERE conditions
        where_conditions = components.get("where_conditions", [])
        if where_conditions:
            # Simple WHERE condition handling
            condition = where_conditions[0]
            # This is a mock implementation - real implementation would parse conditions
            pass

        # Apply column selection
        select_columns = components.get("select_columns", ["*"])
        if select_columns != ["*"]:
            df = df.select(*select_columns)

        # Apply GROUP BY
        group_by_columns = components.get("group_by_columns", [])
        if group_by_columns:
            grouped_df = df.groupBy(*group_by_columns)
            # For now, convert grouped data back to DataFrame
            # In a real implementation, this would depend on the aggregation functions
            df = MockDataFrame([], MockStructType([]))

        # Apply ORDER BY
        order_by_columns = components.get("order_by_columns", [])
        if order_by_columns:
            df = df.orderBy(*order_by_columns)

        # Apply LIMIT
        limit_value = components.get("limit_value")
        if limit_value:
            df = df.limit(limit_value)

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, df)

    def _execute_create(self, ast: MockSQLAST) -> IDataFrame:
        """Execute CREATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        components = ast.components
        object_type = components.get("object_type", "TABLE")
        object_name = components.get("object_name", "unknown")

        if object_type.upper() == "DATABASE":
            self.session.catalog.createDatabase(object_name)
        elif object_type.upper() == "TABLE":
            # Mock table creation
            pass

        # Return empty DataFrame to indicate success
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_drop(self, ast: MockSQLAST) -> IDataFrame:
        """Execute DROP query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        components = ast.components
        object_type = components.get("object_type", "TABLE")
        object_name = components.get("object_name", "unknown")

        if object_type.upper() == "DATABASE":
            # Mock database drop
            pass
        elif object_type.upper() == "TABLE":
            # Mock table drop
            pass

        # Return empty DataFrame to indicate success
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_insert(self, ast: MockSQLAST) -> IDataFrame:
        """Execute INSERT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_update(self, ast: MockSQLAST) -> IDataFrame:
        """Execute UPDATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_delete(self, ast: MockSQLAST) -> IDataFrame:
        """Execute DELETE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_show(self, ast: MockSQLAST) -> IDataFrame:
        """Execute SHOW query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with SHOW results.
        """
        # Mock implementation - show databases or tables
        from ...dataframe import MockDataFrame

        # Simple mock data for SHOW commands
        if "databases" in ast.components.get("original_query", "").lower():
            data = [{"databaseName": "default"}, {"databaseName": "test"}]
            from ...spark_types import MockStructType, MockStructField, StringType

            schema = MockStructType([MockStructField("databaseName", StringType())])
            return MockDataFrame(data, schema)  # type: ignore[return-value]
        elif "tables" in ast.components.get("original_query", "").lower():
            data = [{"tableName": "users"}, {"tableName": "orders"}]
            from ...spark_types import MockStructType, MockStructField, StringType

            schema = MockStructType([MockStructField("tableName", StringType())])
            return MockDataFrame(data, schema)  # type: ignore[return-value]
        else:
            return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

    def _execute_describe(self, ast: MockSQLAST) -> IDataFrame:
        """Execute DESCRIBE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with DESCRIBE results.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame

        return MockDataFrame([], MockStructType([]))  # type: ignore[return-value]

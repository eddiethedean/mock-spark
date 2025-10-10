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
            elif ast.query_type == "MERGE":
                return self._execute_merge(ast)
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
                df_any = self.session.table(table_name)
                # Convert IDataFrame to MockDataFrame if needed
                from ...dataframe import MockDataFrame

                if isinstance(df_any, MockDataFrame):  # type: ignore[unreachable]
                    df = df_any  # type: ignore[unreachable]
                else:
                    # df_any may be an IDataFrame; construct MockDataFrame from its public API
                    from ...spark_types import MockStructType

                    # Convert ISchema to MockStructType if needed
                    if hasattr(df_any.schema, "fields"):
                        schema = MockStructType(df_any.schema.fields)  # type: ignore[arg-type]
                    else:
                        schema = MockStructType([])
                    df = MockDataFrame(df_any.collect(), schema)
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
        object_type = components.get("object_type", "TABLE").upper()
        object_name = components.get("object_name", "unknown")
        # Default to True for backward compatibility and safer behavior
        ignore_if_exists = components.get("ignore_if_exists", True)

        # Handle both DATABASE and SCHEMA keywords (they're synonymous in Spark)
        if object_type in ("DATABASE", "SCHEMA"):
            self.session.catalog.createDatabase(object_name, ignoreIfExists=ignore_if_exists)
        elif object_type == "TABLE":
            # Mock table creation
            pass

        # Return empty DataFrame to indicate success
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def _execute_drop(self, ast: MockSQLAST) -> IDataFrame:
        """Execute DROP query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        components = ast.components
        object_type = components.get("object_type", "TABLE").upper()
        object_name = components.get("object_name", "unknown")
        # Default to True for backward compatibility and safer behavior
        ignore_if_not_exists = components.get("ignore_if_not_exists", True)

        # Handle both DATABASE and SCHEMA keywords (they're synonymous in Spark)
        if object_type in ("DATABASE", "SCHEMA"):
            self.session.catalog.dropDatabase(object_name, ignoreIfNotExists=ignore_if_not_exists)
        elif object_type == "TABLE":
            # Mock table drop
            pass

        # Return empty DataFrame to indicate success
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def _execute_insert(self, ast: MockSQLAST) -> IDataFrame:
        """Execute INSERT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def _execute_update(self, ast: MockSQLAST) -> IDataFrame:
        """Execute UPDATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

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
            from typing import cast
            from ...core.interfaces.dataframe import IDataFrame

            return cast(IDataFrame, MockDataFrame(data, schema))
        elif "tables" in ast.components.get("original_query", "").lower():
            data = [{"tableName": "users"}, {"tableName": "orders"}]
            from ...spark_types import MockStructType, MockStructField, StringType

            schema = MockStructType([MockStructField("tableName", StringType())])
            from typing import cast
            from ...core.interfaces.dataframe import IDataFrame

            return cast(IDataFrame, MockDataFrame(data, schema))
        else:
            from ...spark_types import MockStructType
            from typing import cast
            from ...core.interfaces.dataframe import IDataFrame

            return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def _execute_describe(self, ast: MockSQLAST) -> IDataFrame:
        """Execute DESCRIBE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with DESCRIBE results.
        """
        # Check for DESCRIBE HISTORY
        query = ast.query if hasattr(ast, 'query') else ""
        
        if "HISTORY" in query.upper():
            # DESCRIBE HISTORY table_name
            import re
            match = re.search(r"DESCRIBE\s+HISTORY\s+(\w+(?:\.\w+)?)", query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                
                # Parse schema and table
                if "." in table_name:
                    schema_name, table_only = table_name.split(".", 1)
                else:
                    schema_name, table_only = "default", table_name
                
                # Get table metadata
                meta = self.session.storage.get_table_metadata(schema_name, table_only)  # type: ignore[attr-defined]
                
                if not meta or meta.get("format") != "delta":
                    from ...errors import AnalysisException
                    raise AnalysisException(
                        f"Table {table_name} is not a Delta table. "
                        "DESCRIBE HISTORY can only be used with Delta format tables."
                    )
                
                version_history = meta.get("version_history", [])
                
                # Create DataFrame with history
                from ...dataframe import MockDataFrame
                from ...spark_types import MockStructType, MockStructField, IntegerType, StringType, TimestampType
                from typing import cast
                from ...core.interfaces.dataframe import IDataFrame
                
                # Build history rows
                history_data = []
                for v in version_history:
                    # Handle both MockDeltaVersion objects and dicts
                    if hasattr(v, 'version'):
                        row = {
                            "version": v.version,
                            "timestamp": v.timestamp,
                            "operation": v.operation
                        }
                    else:
                        row = {
                            "version": v.get("version"),
                            "timestamp": v.get("timestamp"),
                            "operation": v.get("operation")
                        }
                    history_data.append(row)
                
                # Return DataFrame using session's createDataFrame
                return cast(IDataFrame, self.session.createDataFrame(history_data))
        
        # Default DESCRIBE implementation
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType

        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame

        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

    def _execute_merge(self, ast: MockSQLAST) -> IDataFrame:
        """Execute MERGE INTO query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame (MERGE returns no results).
        """
        import re
        from ...dataframe import MockDataFrame
        from ...spark_types import MockStructType
        from typing import cast
        from ...core.interfaces.dataframe import IDataFrame
        
        # Extract components
        target_table = ast.components.get("target_table", "")
        source_table = ast.components.get("source_table", "")
        on_condition = ast.components.get("on_condition", "")
        target_alias = ast.components.get("target_alias")
        source_alias = ast.components.get("source_alias")
        when_matched = ast.components.get("when_matched", [])
        when_not_matched = ast.components.get("when_not_matched", [])
        
        # Parse table names (schema.table)
        if "." in target_table:
            target_schema, target_name = target_table.split(".", 1)
        else:
            target_schema, target_name = "default", target_table
        
        # Get target and source data
        target_df = self.session.table(target_table)
        target_data = target_df.collect()
        target_data_dict = {id(row): row.asDict() for row in target_data}
        
        source_df = self.session.table(source_table)
        source_data = source_df.collect()
        source_data_list = [row.asDict() for row in source_data]
        
        # Parse ON condition - simple equality for now
        # Example: "t.id = s.id" or "t.id = s.id AND t.category = s.category"
        condition_parts = []
        for part in on_condition.split(" AND "):
            part = part.strip()
            match = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", part)
            if match:
                condition_parts.append({
                    "left_alias": match.group(1),
                    "left_col": match.group(2),
                    "right_alias": match.group(3),
                    "right_col": match.group(4)
                })
        
        # Track which target rows were matched
        matched_target_ids = set()
        updated_rows = []
        
        # Process WHEN MATCHED clauses
        if when_matched:
            for target_row in target_data:
                target_dict = target_row.asDict()
                
                # Check if this target row matches any source row
                for source_dict in source_data_list:
                    matches = all(
                        target_dict.get(cond["left_col"]) == source_dict.get(cond["right_col"])
                        for cond in condition_parts
                    )
                    
                    if matches:
                        matched_target_ids.add(id(target_row))
                        
                        # Execute WHEN MATCHED action
                        for clause in when_matched:
                            if clause["action"] == "UPDATE":
                                # Parse SET clause: "t.name = s.name, t.score = s.score"
                                set_clause = clause["set_clause"]
                                updated_row = target_dict.copy()
                                
                                for assignment in set_clause.split(","):
                                    assignment = assignment.strip()
                                    # Match: t.column = s.column or t.column = value
                                    match = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", assignment)
                                    if match:
                                        target_col = match.group(2)
                                        source_col = match.group(4)
                                        updated_row[target_col] = source_dict.get(source_col)
                                
                                updated_rows.append(updated_row)
                            elif clause["action"] == "DELETE":
                                # Don't add to updated_rows (effectively deletes)
                                pass
                        break  # Only match first source row
        
        # Add unmatched target rows (unchanged)
        for target_row in target_data:
            if id(target_row) not in matched_target_ids:
                updated_rows.append(target_row.asDict())
        
        # Process WHEN NOT MATCHED clauses (inserts)
        if when_not_matched:
            for source_dict in source_data_list:
                # Check if this source row matches any target row
                matched = False
                for target_row in target_data:
                    target_dict = target_row.asDict()
                    matches = all(
                        target_dict.get(cond["left_col"]) == source_dict.get(cond["right_col"])
                        for cond in condition_parts
                    )
                    if matches:
                        matched = True
                        break
                
                if not matched:
                    # Execute WHEN NOT MATCHED action
                    for clause in when_not_matched:
                        if clause["action"] == "INSERT":
                            # Parse: (id, name, score) VALUES (s.id, s.name, s.score)
                            insert_clause = clause["insert_clause"]
                            
                            # Simple parsing: just insert all source columns
                            # In production, would parse the column list and values
                            updated_rows.append(source_dict.copy())
        
        # Write merged data back to target table
        from ...spark_types import MockRow
        
        self.session.storage.drop_table(target_schema, target_name)  # type: ignore[attr-defined]
        self.session.storage.create_table(target_schema, target_name, target_df.schema.fields)  # type: ignore[attr-defined]
        if updated_rows:
            self.session.storage.insert_data(target_schema, target_name, updated_rows, mode="append")  # type: ignore[attr-defined]
        
        # MERGE returns empty DataFrame
        return cast(IDataFrame, MockDataFrame([], MockStructType([])))

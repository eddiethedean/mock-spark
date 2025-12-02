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
    >>> from mock_spark.session.sql import SQLExecutor
    >>> executor = SQLExecutor(session)
    >>> result = executor.execute("SELECT * FROM users WHERE age > 18")
    >>> result.show()
"""

from typing import TYPE_CHECKING, Any, cast
import re
from ...core.exceptions.execution import QueryExecutionException
from ...core.interfaces.dataframe import IDataFrame
from ...core.interfaces.session import ISession
from ...dataframe import DataFrame
from ...spark_types import StructType
from .parser import SQLAST

if TYPE_CHECKING:
    from ...dataframe.protocols import SupportsDataFrameOps


class SQLExecutor:
    """SQL Executor for Mock Spark.

    Provides SQL execution functionality that processes parsed SQL queries
    and returns appropriate results. Handles different types of SQL operations
    including SELECT, INSERT, CREATE, DROP, and other DDL/DML operations.

    Attributes:
        session: Mock Spark session instance.
        parser: SQL parser instance.

    Example:
        >>> executor = SQLExecutor(session)
        >>> result = executor.execute("SELECT name, age FROM users")
        >>> result.show()
    """

    def __init__(self, session: ISession):
        """Initialize SQLExecutor.

        Args:
            session: Mock Spark session instance.
        """
        self.session = session
        from .parser import SQLParser

        self.parser = SQLParser()

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
                raise QueryExecutionException(
                    f"Unsupported query type: {ast.query_type}"
                )

        except Exception as e:
            if isinstance(e, QueryExecutionException):
                raise
            raise QueryExecutionException(f"Failed to execute query: {str(e)}")

    def _execute_select(self, ast: SQLAST) -> IDataFrame:
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
            from ...dataframe import DataFrame
            from ...spark_types import (
                StructType,
            )

            # For now, create a simple DataFrame with one row
            # This is a basic implementation for literal SELECT queries
            data: list[dict[str, Any]] = [
                {}
            ]  # Empty row, we'll populate based on SELECT columns
            schema = StructType([])
            df_literal = DataFrame(data, schema)
            df = df_literal
        else:
            # Check for JOINs
            joins = components.get("joins", [])
            table_aliases = components.get("table_aliases", {})

            if joins:
                # Handle JOIN operation
                table_name = from_tables[0]
                try:
                    df1_any = self.session.table(table_name)
                    df1: DataFrame
                    if not isinstance(df1_any, DataFrame):
                        from ...spark_types import StructType

                        schema = (
                            StructType(df1_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df1_any.schema, "fields")
                            else StructType([])
                        )
                        df1 = DataFrame(df1_any.collect(), schema)
                    else:
                        df1 = df1_any

                    # Get second table
                    join_info = joins[0]
                    table2_name = join_info["table"]
                    df2_any = self.session.table(table2_name)
                    df2: DataFrame
                    if not isinstance(df2_any, DataFrame):
                        from ...spark_types import StructType

                        schema = (
                            StructType(df2_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df2_any.schema, "fields")
                            else StructType([])
                        )
                        df2 = DataFrame(df2_any.collect(), schema)
                    else:
                        df2 = df2_any

                    # Parse join condition (e.g., "u.id = d.id")
                    join_condition = join_info.get("condition", "")
                    if join_condition:
                        # Extract column names from join condition
                        # Pattern: alias1.col1 = alias2.col2
                        match = re.search(
                            r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", join_condition
                        )
                        if match:
                            alias1, col1, alias2, col2 = match.groups()
                            # Find actual table names from aliases
                            table1_col = None
                            table2_col = None
                            for table, alias in table_aliases.items():
                                if alias == alias1:
                                    table1_col = col1
                                if alias == alias2:
                                    table2_col = col2

                            if table1_col and table2_col:
                                # Perform join
                                join_col = df1[table1_col] == df2[table2_col]
                                df = df1.join(df2, join_col, "inner")  # type: ignore[arg-type]
                            else:
                                # Fallback: try direct column names
                                join_col = df1[col1] == df2[col2]
                                df = df1.join(df2, join_col, "inner")  # type: ignore[arg-type]
                        else:
                            # Fallback: try to join on common column names
                            common_cols = set(df1.columns) & set(df2.columns)
                            if common_cols:
                                join_col_name = list(common_cols)[0]
                                join_condition = (
                                    df1[join_col_name] == df2[join_col_name]
                                )
                                df = df1.join(df2, join_condition, "inner")  # type: ignore[arg-type]
                            else:
                                df = df1.crossJoin(df2)
                    else:
                        # No condition - cross join
                        df = df1.crossJoin(df2)
                except Exception:
                    from ...dataframe import DataFrame
                    from ...spark_types import StructType

                    return DataFrame([], StructType([]))  # type: ignore[return-value]
            else:
                # Single table (no JOIN)
                table_name = from_tables[0]
                # Try to get table as DataFrame
                try:
                    df_any = self.session.table(table_name)
                    # Convert IDataFrame to DataFrame if needed
                    from ...dataframe import DataFrame

                    if isinstance(df_any, DataFrame):
                        df = df_any
                    else:
                        # df_any may be an IDataFrame; construct DataFrame from its public API
                        from ...spark_types import StructType

                        # Convert ISchema to StructType if needed
                        if hasattr(df_any.schema, "fields"):
                            schema = StructType(df_any.schema.fields)  # type: ignore[arg-type]
                        else:
                            schema = StructType([])
                        df = DataFrame(df_any.collect(), schema)
                except Exception:
                    # If table doesn't exist, return empty DataFrame
                    from ...dataframe import DataFrame
                    from ...spark_types import StructType

                    # Log the error for debugging (can be removed in production)
                    # This helps identify why table lookup fails
                    return DataFrame([], StructType([]))  # type: ignore[return-value]

        df_ops = cast("SupportsDataFrameOps", df)

        # Import F for WHERE clause filtering
        from ...functions import F

        # Apply WHERE conditions (before GROUP BY)
        where_conditions = components.get("where_conditions", [])
        if where_conditions:
            # Parse simple WHERE conditions like "column > value", "column < value", etc.
            where_condition = where_conditions[0]
            # Try to parse simple conditions
            match = re.search(r"(\w+)\s*([><=]+)\s*(\d+)", where_condition)
            if match:
                col_name = match.group(1)
                operator = match.group(2)
                value = int(match.group(3))

                # Check if column exists in DataFrame
                if col_name in df.columns:
                    if operator == ">":
                        df = cast("DataFrame", df_ops.filter(F.col(col_name) > value))
                    elif operator == "<":
                        df = cast("DataFrame", df_ops.filter(F.col(col_name) < value))
                    elif operator in ("=", "=="):
                        df = cast("DataFrame", df_ops.filter(F.col(col_name) == value))
                    elif operator == ">=":
                        df = cast("DataFrame", df_ops.filter(F.col(col_name) >= value))
                    elif operator == "<=":
                        df = cast("DataFrame", df_ops.filter(F.col(col_name) <= value))
                    df_ops = cast("SupportsDataFrameOps", df)

        # Check if we have GROUP BY
        group_by_columns = components.get("group_by_columns", [])
        select_columns = components.get("select_columns", ["*"])

        if group_by_columns:
            # Parse aggregate functions from SELECT columns
            from ...functions import F

            agg_exprs = []
            select_exprs = []

            for col_expr in select_columns:
                col_expr = col_expr.strip()

                # Extract alias if present (handle both " AS " and " as ")
                alias = None
                alias_match = re.search(r"\s+[Aa][Ss]\s+(\w+)$", col_expr)
                if alias_match:
                    alias = alias_match.group(1)
                    # Remove alias from col_expr
                    col_expr = re.sub(r"\s+[Aa][Ss]\s+\w+$", "", col_expr).strip()

                col_upper = col_expr.upper()

                # Check for aggregate functions
                if col_upper.startswith("COUNT("):
                    # Extract column name from COUNT(*column_name) or COUNT(*)
                    if "*" in col_expr:
                        expr = F.count("*")
                    else:
                        # Extract content between parentheses
                        inner = col_expr[
                            col_expr.index("(") + 1 : col_expr.rindex(")")
                        ].strip()
                        expr = F.count(inner) if inner != "*" else F.count("*")
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("SUM("):
                    # Extract content between parentheses
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    # Handle expressions like SUM(quantity * price)
                    if "*" in inner:
                        parts = [p.strip() for p in inner.split("*")]
                        if len(parts) == 2:
                            col_expr = F.col(parts[0]) * F.col(parts[1])
                            expr = F.sum(col_expr.name)  # type: ignore[arg-type]
                        else:
                            # More complex expression - try to parse
                            expr = F.sum(inner)  # Fallback
                    else:
                        expr = F.sum(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("AVG("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    if "*" in inner:
                        parts = [p.strip() for p in inner.split("*")]
                        if len(parts) == 2:
                            col_expr = F.col(parts[0]) * F.col(parts[1])
                            expr = F.avg(col_expr.name)  # type: ignore[arg-type]
                        else:
                            expr = F.avg(inner)
                    else:
                        expr = F.avg(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("MAX("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    expr = F.max(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                elif col_upper.startswith("MIN("):
                    inner = col_expr[
                        col_expr.index("(") + 1 : col_expr.rindex(")")
                    ].strip()
                    expr = F.min(inner)
                    agg_exprs.append(expr.alias(alias) if alias else expr)
                else:
                    # Non-aggregate column (should be in GROUP BY)
                    col_name = (
                        col_expr.split(" AS ")[0].strip()
                        if " AS " in col_upper
                        else col_expr
                    )
                    # Don't add group-by columns to select_exprs - they're automatically included
                    if col_name not in group_by_columns:
                        select_exprs.append(
                            F.col(col_name).alias(alias) if alias else F.col(col_name)
                        )

            # Perform GROUP BY with aggregations
            grouped = df_ops.groupBy(*group_by_columns)
            if agg_exprs:
                # Only add aggregate expressions - group by columns are automatically included
                df = cast("DataFrame", grouped.agg(*agg_exprs))
            else:
                # No aggregations, just group by
                if select_exprs:
                    df = cast("DataFrame", grouped.agg(*select_exprs))
                else:
                    df = cast("DataFrame", grouped.agg())

            df_ops = cast("SupportsDataFrameOps", df)

            # Apply HAVING conditions (after GROUP BY)
            having_conditions = components.get("having_conditions", [])
            if having_conditions:
                # Parse HAVING condition - simple implementation
                # Example: "SUM(quantity * price) > 100"
                having_condition = having_conditions[0]
                # Try to parse simple conditions like "column > value" or "column < value"
                # Match patterns like "SUM(...) > 100" or "total_spent > 100"
                match = re.search(r"(\w+)\s*([><=]+)\s*(\d+)", having_condition)
                if match:
                    col_name = match.group(1)
                    operator = match.group(2)
                    value = int(match.group(3))

                    # Check if column exists in result
                    if col_name in df.columns:
                        if operator == ">":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) > value)
                            )
                        elif operator == "<":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) < value)
                            )
                        elif operator in ("=", "=="):
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) == value)
                            )
                        elif operator == ">=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) >= value)
                            )
                        elif operator == "<=":
                            df = cast(
                                "DataFrame", df_ops.filter(F.col(col_name) <= value)
                            )
                        df_ops = cast("SupportsDataFrameOps", df)
        else:
            # No GROUP BY - just apply column selection
            if select_columns != ["*"]:
                # Handle table aliases (e.g., "u.name" -> "name")
                cleaned_columns = []
                for col in select_columns:
                    # Remove table alias prefix if present (e.g., "u.name" -> "name")
                    if (
                        "." in col
                        and not col.startswith("'")
                        and not col.startswith('"')
                    ):
                        parts = col.split(".", 1)
                        if len(parts) == 2:
                            cleaned_columns.append(
                                parts[1]
                            )  # Use column name without alias
                        else:
                            cleaned_columns.append(col)
                    else:
                        cleaned_columns.append(col)
                df = cast("DataFrame", df_ops.select(*cleaned_columns))
            df_ops = cast("SupportsDataFrameOps", df)

        # Apply ORDER BY
        order_by_columns = components.get("order_by_columns", [])
        if order_by_columns:
            # Parse ORDER BY columns - handle DESC/ASC, preserve original case
            # Note: Parser already strips ASC, so we only need to handle DESC
            order_exprs = []
            for col_expr in order_by_columns:
                col_expr_upper = col_expr.upper()
                if " DESC" in col_expr_upper or col_expr_upper.endswith(" DESC"):
                    # Extract column name preserving original case
                    col_name = re.sub(
                        r"\s+DESC\s*$", "", col_expr, flags=re.IGNORECASE
                    ).strip()
                    order_exprs.append(F.col(col_name).desc())
                else:
                    # No DESC specified, default to ascending (ASC is already stripped by parser)
                    order_exprs.append(F.col(col_expr).asc())
            if order_exprs:
                df = cast("DataFrame", df_ops.orderBy(*order_exprs))
                df_ops = cast("SupportsDataFrameOps", df)

        # Apply LIMIT
        limit_value = components.get("limit_value")
        if limit_value:
            df = cast("DataFrame", df_ops.limit(limit_value))
            df_ops = cast("SupportsDataFrameOps", df)

        return cast("IDataFrame", df)

    def _execute_create(self, ast: SQLAST) -> IDataFrame:
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
            self.session.catalog.createDatabase(
                object_name, ignoreIfExists=ignore_if_exists
            )
        elif object_type == "TABLE":
            # Mock table creation
            pass

        # Return empty DataFrame to indicate success
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_drop(self, ast: SQLAST) -> IDataFrame:
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
            self.session.catalog.dropDatabase(
                object_name, ignoreIfNotExists=ignore_if_not_exists
            )
        elif object_type == "TABLE":
            # Mock table drop
            pass

        # Return empty DataFrame to indicate success
        from ...dataframe import DataFrame
        from ...spark_types import StructType

        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_insert(self, ast: SQLAST) -> IDataFrame:
        """Execute INSERT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import DataFrame
        from ...spark_types import StructType

        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_update(self, ast: SQLAST) -> IDataFrame:
        """Execute UPDATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import DataFrame
        from ...spark_types import StructType

        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_delete(self, ast: SQLAST) -> IDataFrame:
        """Execute DELETE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.
        """
        # Mock implementation
        from ...dataframe import DataFrame

        return DataFrame([], StructType([]))  # type: ignore[return-value]

    def _execute_show(self, ast: SQLAST) -> IDataFrame:
        """Execute SHOW query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with SHOW results.
        """
        # Mock implementation - show databases or tables
        from ...dataframe import DataFrame

        # Simple mock data for SHOW commands
        if "databases" in ast.components.get("original_query", "").lower():
            data = [{"databaseName": "default"}, {"databaseName": "test"}]
            from ...spark_types import StructType, StructField, StringType

            schema = StructType([StructField("databaseName", StringType())])
            from typing import cast

            return cast("IDataFrame", DataFrame(data, schema))
        elif "tables" in ast.components.get("original_query", "").lower():
            data = [{"tableName": "users"}, {"tableName": "orders"}]
            from ...spark_types import StructType, StructField, StringType

            schema = StructType([StructField("tableName", StringType())])
            from typing import cast

            return cast("IDataFrame", DataFrame(data, schema))
        else:
            from ...spark_types import StructType
            from typing import cast

            return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_describe(self, ast: SQLAST) -> IDataFrame:
        """Execute DESCRIBE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            DataFrame with DESCRIBE results.
        """
        # Check for DESCRIBE HISTORY
        query = ast.query if hasattr(ast, "query") else ""

        if "HISTORY" in query.upper():
            # DESCRIBE HISTORY table_name
            match = re.search(
                r"DESCRIBE\s+HISTORY\s+(\w+(?:\.\w+)?)", query, re.IGNORECASE
            )
            if match:
                table_name = match.group(1)

                # Parse schema and table
                if "." in table_name:
                    schema_name, table_only = table_name.split(".", 1)
                else:
                    schema_name, table_only = "default", table_name

                # Get table metadata
                meta = self.session.storage.get_table_metadata(schema_name, table_only)

                if not meta or meta.get("format") != "delta":
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {table_name} is not a Delta table. "
                        "DESCRIBE HISTORY can only be used with Delta format tables."
                    )

                version_history = meta.get("version_history", [])

                # Create DataFrame with history
                from ...dataframe import DataFrame
                from ...spark_types import (
                    StructType,
                )
                from typing import cast

                # Build history rows
                history_data = []
                for v in version_history:
                    # Handle both MockDeltaVersion objects and dicts
                    if hasattr(v, "version"):
                        row = {
                            "version": v.version,
                            "timestamp": v.timestamp,
                            "operation": v.operation,
                        }
                    else:
                        row = {
                            "version": v.get("version"),
                            "timestamp": v.get("timestamp"),
                            "operation": v.get("operation"),
                        }
                    history_data.append(row)

                # Return DataFrame using session's createDataFrame
                return self.session.createDataFrame(history_data)

        # Default DESCRIBE implementation
        from ...dataframe import DataFrame
        from ...spark_types import StructType

        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_merge(self, ast: SQLAST) -> IDataFrame:
        """Execute MERGE INTO query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame (MERGE returns no results).
        """
        from ...dataframe import DataFrame
        from ...spark_types import StructType
        from typing import cast

        # Extract components
        target_table = ast.components.get("target_table", "")
        source_table = ast.components.get("source_table", "")
        on_condition = ast.components.get("on_condition", "")
        ast.components.get("target_alias")
        ast.components.get("source_alias")
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
        {id(row): row.asDict() for row in target_data}

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
                condition_parts.append(
                    {
                        "left_alias": match.group(1),
                        "left_col": match.group(2),
                        "right_alias": match.group(3),
                        "right_col": match.group(4),
                    }
                )

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
                        target_dict.get(cond["left_col"])
                        == source_dict.get(cond["right_col"])
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
                                    match = re.match(
                                        r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", assignment
                                    )
                                    if match:
                                        target_col = match.group(2)
                                        source_col = match.group(4)
                                        updated_row[target_col] = source_dict.get(
                                            source_col
                                        )

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
                        target_dict.get(cond["left_col"])
                        == source_dict.get(cond["right_col"])
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
                            clause["insert_clause"]

                            # Simple parsing: just insert all source columns
                            # In production, would parse the column list and values
                            updated_rows.append(source_dict.copy())

        # Write merged data back to target table

        self.session.storage.drop_table(target_schema, target_name)
        self.session.storage.create_table(
            target_schema, target_name, target_df.schema.fields
        )
        if updated_rows:
            self.session.storage.insert_data(target_schema, target_name, updated_rows)

        # MERGE returns empty DataFrame
        return cast("IDataFrame", DataFrame([], StructType([])))

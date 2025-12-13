"""
SQL Executor for Sparkless.

This module provides SQL execution functionality for Sparkless,
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
    >>> from sparkless.session.sql import SQLExecutor
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
    """SQL Executor for Sparkless.

    Provides SQL execution functionality that processes parsed SQL queries
    and returns appropriate results. Handles different types of SQL operations
    including SELECT, INSERT, CREATE, DROP, and other DDL/DML operations.

    Attributes:
        session: Sparkless session instance.
        parser: SQL parser instance.

    Example:
        >>> executor = SQLExecutor(session)
        >>> result = executor.execute("SELECT name, age FROM users")
        >>> result.show()
    """

    def __init__(self, session: ISession):
        """Initialize SQLExecutor.

        Args:
            session: Sparkless session instance.
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
            elif ast.query_type == "REFRESH":
                return self._execute_refresh(ast)
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
                    if not isinstance(df1_any, DataFrame):  # type: ignore[unreachable]
                        from ...spark_types import StructType

                        schema = (
                            StructType(df1_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df1_any.schema, "fields")
                            else StructType([])
                        )
                        df1 = DataFrame(df1_any.collect(), schema)
                    else:
                        df1 = df1_any  # type: ignore[unreachable]

                    # Get second table
                    join_info = joins[0]
                    table2_name = join_info["table"]
                    df2_any = self.session.table(table2_name)
                    df2: DataFrame
                    if not isinstance(df2_any, DataFrame):  # type: ignore[unreachable]
                        from ...spark_types import StructType

                        schema = (
                            StructType(df2_any.schema.fields)  # type: ignore[arg-type]
                            if hasattr(df2_any.schema, "fields")
                            else StructType([])
                        )
                        df2 = DataFrame(df2_any.collect(), schema)
                    else:
                        df2 = df2_any  # type: ignore[unreachable]

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
                                df = cast("DataFrame", df1.join(df2, join_col, "inner"))  # type: ignore[arg-type]
                            else:
                                # Fallback: try direct column names
                                join_col = df1[col1] == df2[col2]
                                df = cast("DataFrame", df1.join(df2, join_col, "inner"))  # type: ignore[arg-type]
                        else:
                            # Fallback: try to join on common column names
                            common_cols = set(df1.columns) & set(df2.columns)
                            if common_cols:
                                join_col_name = list(common_cols)[0]
                                join_condition = (
                                    df1[join_col_name] == df2[join_col_name]
                                )
                                df = cast(
                                    "DataFrame",
                                    df1.join(
                                        cast("SupportsDataFrameOps", df2),
                                        join_condition,  # type: ignore[arg-type]
                                        "inner",
                                    ),
                                )
                            else:
                                # Cast df2 to SupportsDataFrameOps to satisfy type checker
                                # DataFrame implements the protocol at runtime
                                df = cast(
                                    "DataFrame",
                                    df1.crossJoin(cast("SupportsDataFrameOps", df2)),
                                )
                    else:
                        # No condition - cross join
                        # Cast df2 to SupportsDataFrameOps to satisfy type checker
                        df = cast(
                            "DataFrame",
                            df1.crossJoin(cast("SupportsDataFrameOps", df2)),
                        )
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

                    if isinstance(df_any, DataFrame):  # type: ignore[unreachable]
                        df = df_any  # type: ignore[unreachable]
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
                            expr = F.sum(col_expr.name)
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
                            expr = F.avg(col_expr.name)
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

        Raises:
            AnalysisException: If table already exists and IF NOT EXISTS is not specified.
            QueryExecutionException: If schema parsing fails.
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
            # Parse schema and table name
            schema_name = components.get("schema_name")
            table_name = object_name
            # Access storage through catalog (ISession protocol doesn't expose _storage)
            storage = getattr(self.session, "_storage", None)
            if storage is None:
                storage = self.session.catalog._storage  # type: ignore[attr-defined]
            if schema_name is None:
                schema_name = storage.get_current_schema()

            # Check if table exists
            if storage.table_exists(schema_name, table_name):
                if not ignore_if_exists:
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {schema_name}.{table_name} already exists"
                    )
                # Table exists and IF NOT EXISTS is specified, skip creation
                return cast("IDataFrame", DataFrame([], StructType([])))

            # Parse column definitions
            column_definitions = components.get("column_definitions", "")
            if not column_definitions:
                from ...errors import QueryExecutionException

                raise QueryExecutionException(
                    "CREATE TABLE requires column definitions"
                )

            # Parse DDL schema using DDL adapter
            from ...core.ddl_adapter import parse_ddl_schema

            try:
                schema = parse_ddl_schema(column_definitions)
            except Exception as e:
                from ...errors import QueryExecutionException

                raise QueryExecutionException(
                    f"Failed to parse table schema: {str(e)}"
                ) from e

            # Create table in storage backend
            storage.create_table(schema_name, table_name, schema.fields)

        # Return empty DataFrame to indicate success
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_drop(self, ast: SQLAST) -> IDataFrame:
        """Execute DROP query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist and IF EXISTS is not specified.
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
            # Parse schema and table name
            schema_name = components.get("schema_name")
            table_name = object_name
            # Access storage through catalog (ISession protocol doesn't expose _storage)
            storage = getattr(self.session, "_storage", None)
            if storage is None:
                storage = self.session.catalog._storage  # type: ignore[attr-defined]
            if schema_name is None:
                schema_name = storage.get_current_schema()

            # Check if table exists
            if not storage.table_exists(schema_name, table_name):
                if not ignore_if_not_exists:
                    from ...errors import AnalysisException

                    raise AnalysisException(
                        f"Table {schema_name}.{table_name} does not exist"
                    )
                # Table doesn't exist and IF EXISTS is specified, skip drop
                return cast("IDataFrame", DataFrame([], StructType([])))

            # Drop table from storage backend
            storage.drop_table(schema_name, table_name)

        # Return empty DataFrame to indicate success
        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_insert(self, ast: SQLAST) -> IDataFrame:
        """Execute INSERT query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
            QueryExecutionException: If INSERT parsing or execution fails.
        """
        from ...errors import AnalysisException, QueryExecutionException

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        insert_type = components.get("type", "unknown")

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {schema_name}.{table_name} does not exist")

        # Get table schema
        table_schema = storage.get_table_schema(schema_name, table_name)
        if not isinstance(table_schema, StructType):
            raise QueryExecutionException(
                f"Failed to get schema for table {schema_name}.{table_name}"
            )

        data: list[dict[str, Any]] = []

        if insert_type == "VALUES":
            # Parse VALUES-based INSERT
            values = components.get("values", [])
            columns = components.get("columns", [])

            # If columns specified, use them; otherwise use all table columns in order
            target_columns = columns or [field.name for field in table_schema.fields]

            # Convert string values to Python types
            for row_values in values:
                row_dict: dict[str, Any] = {}
                for i, value_str in enumerate(row_values):
                    if i >= len(target_columns):
                        break  # Skip extra values
                    col_name = target_columns[i]
                    # Parse value (handle strings, numbers, null, booleans)
                    parsed_value = self._parse_sql_value(value_str.strip())
                    row_dict[col_name] = parsed_value

                # Fill missing columns with None
                for field in table_schema.fields:
                    if field.name not in row_dict:
                        row_dict[field.name] = None

                data.append(row_dict)

        elif insert_type == "SELECT":
            # Execute SELECT query and get DataFrame
            select_query = components.get("select_query", "")
            if not select_query:
                raise QueryExecutionException(
                    "SELECT query is missing in INSERT ... SELECT"
                )

            # Execute SELECT query
            select_df = self.session.sql(f"SELECT {select_query}")
            # Convert DataFrame to list of dictionaries
            data = [
                dict(row) if hasattr(row, "__dict__") else row
                for row in select_df.collect()
            ]

        else:
            raise QueryExecutionException(f"Unsupported INSERT type: {insert_type}")

        # Validate and coerce data
        if data:
            from ...core.data_validation import DataValidator

            validator = DataValidator(
                table_schema,
                validation_mode="relaxed",
                enable_coercion=True,
            )
            data = validator.coerce(data)

        # Insert data into storage backend
        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if data:
            storage.insert_data(schema_name, table_name, data)

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _parse_sql_value(self, value_str: str) -> Any:
        """Parse a SQL value string into Python type.

        Args:
            value_str: SQL value string (e.g., "123", "'text'", "NULL", "true")

        Returns:
            Parsed Python value
        """
        value_str = value_str.strip()

        # Handle NULL
        if value_str.upper() == "NULL" or value_str == "":
            return None

        # Handle quoted strings
        if (value_str.startswith("'") and value_str.endswith("'")) or (
            value_str.startswith('"') and value_str.endswith('"')
        ):
            return value_str[1:-1]  # Remove quotes

        # Handle booleans
        if value_str.upper() == "TRUE":
            return True
        if value_str.upper() == "FALSE":
            return False

        # Handle numbers
        try:
            # Try integer first
            if "." not in value_str and "e" not in value_str.lower():
                return int(value_str)
            # Try float
            return float(value_str)
        except ValueError:
            pass

        # Default: return as string
        return value_str

    def _execute_update(self, ast: SQLAST) -> IDataFrame:
        """Execute UPDATE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
            QueryExecutionException: If UPDATE execution fails.
        """
        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        set_clauses = components.get("set_clauses", [])
        where_conditions = components.get("where_conditions", [])

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            from ...errors import AnalysisException

            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Get table data and schema directly (avoid DataFrame operations that use lazy evaluation)
        rows = storage.get_data(schema_name, table_name)
        table_schema = storage.get_table_schema(schema_name, table_name)

        # Import required modules
        import re
        from types import SimpleNamespace

        # Helper function to evaluate condition for a row
        def evaluate_condition(row: dict[str, Any], condition: str) -> bool:
            """Evaluate WHERE condition for a single row."""
            context = dict(row)
            row_ns = SimpleNamespace(**row)
            context["target"] = row_ns
            try:
                return bool(eval(condition, {"__builtins__": {}}, context))
            except Exception:
                return False

        # Normalize WHERE condition if present
        normalized_condition = None
        if where_conditions:
            where_expr = where_conditions[0]
            # Normalize SQL expression to Python-compatible syntax
            normalized_condition = re.sub(
                r"\bAND\b", "and", where_expr, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bOR\b", "or", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bNOT\b", "not", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"(?<![<>!=])=(?!=)", "==", normalized_condition
            )

        # Helper function to parse and evaluate SET value
        def evaluate_set_value(value_expr: Any, row: dict[str, Any]) -> Any:
            """Parse and evaluate SET clause value."""
            if isinstance(value_expr, str):
                expr = value_expr.strip()
                # Handle string literals
                if (expr.startswith("'") and expr.endswith("'")) or (
                    expr.startswith('"') and expr.endswith('"')
                ):
                    return expr[1:-1]
                # Handle NULL
                elif expr.upper() == "NULL":
                    return None
                # Handle booleans
                elif expr.upper() == "TRUE":
                    return True
                elif expr.upper() == "FALSE":
                    return False
                # Handle numbers
                elif expr.replace(".", "", 1).replace("-", "", 1).isdigit():
                    if "." in expr:
                        return float(expr)
                    else:
                        return int(expr)
                # Try to evaluate as expression (column reference or simple expression)
                else:
                    context = dict(row)
                    row_ns = SimpleNamespace(**row)
                    context["target"] = row_ns
                    # Normalize expression
                    normalized = re.sub(r"(?<![<>!=])=(?!=)", "==", expr)
                    try:
                        return eval(normalized, {"__builtins__": {}}, context)
                    except Exception:
                        # If evaluation fails, return as string
                        return expr
            return value_expr

        # Apply UPDATE to rows
        updated_rows: list[dict[str, Any]] = []
        for row in rows:
            # Check if row matches WHERE condition
            if normalized_condition:
                should_update = evaluate_condition(row, normalized_condition)
            else:
                # No WHERE clause - update all rows
                should_update = True

            if not should_update:
                # Keep row unchanged
                updated_rows.append(row)
                continue

            # Update row with SET clauses
            new_row = dict(row)
            for set_clause in set_clauses:
                column = set_clause.get("column")
                value_expr = set_clause.get("value")

                if not column:
                    continue

                # Evaluate and set new value
                new_value = evaluate_set_value(value_expr, row)
                new_row[column] = new_value

            updated_rows.append(new_row)

        # Overwrite table with updated data
        if updated_rows:
            # Create DataFrame with updated data
            updated_dataframe = self.session.createDataFrame(updated_rows, table_schema)
            # Overwrite table
            updated_dataframe.write.format("delta").mode("overwrite").saveAsTable(
                qualified_name
            )
        else:
            # Empty result - clear table
            empty_df = self.session.createDataFrame([], table_schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(qualified_name)

        # Return empty DataFrame to indicate success
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

        Raises:
            AnalysisException: If table does not exist.
        """
        from ...errors import AnalysisException
        import re
        from types import SimpleNamespace

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")
        where_conditions = components.get("where_conditions", [])

        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Get table data and schema
        rows = storage.get_data(schema_name, table_name)
        table_schema = storage.get_table_schema(schema_name, table_name)

        # Normalize WHERE condition if present
        normalized_condition = None
        if where_conditions:
            where_expr = where_conditions[0]
            # Normalize SQL expression to Python-compatible syntax
            normalized_condition = re.sub(
                r"\bAND\b", "and", where_expr, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bOR\b", "or", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"\bNOT\b", "not", normalized_condition, flags=re.IGNORECASE
            )
            normalized_condition = re.sub(
                r"(?<![<>!=])=(?!=)", "==", normalized_condition
            )

        # Helper function to evaluate condition for a row
        def evaluate_condition(row: dict[str, Any], condition: str) -> bool:
            """Evaluate WHERE condition for a single row."""
            context = dict(row)
            row_ns = SimpleNamespace(**row)
            context["target"] = row_ns
            try:
                return bool(eval(condition, {"__builtins__": {}}, context))
            except Exception:
                return False

        # Filter rows - keep rows that DON'T match WHERE condition
        if normalized_condition:
            remaining_rows = [
                row for row in rows if not evaluate_condition(row, normalized_condition)
            ]
        else:
            # No WHERE clause - delete all rows (truncate table)
            remaining_rows = []

        # Overwrite table with remaining data, preserving schema
        # Get table format from metadata (default to parquet if not specified)
        table_metadata = storage.get_table_metadata(schema_name, table_name)
        table_format = "parquet"  # Default format
        if isinstance(table_metadata, dict) and "format" in table_metadata:
            table_format = table_metadata["format"]

        if remaining_rows:
            # Create DataFrame with remaining data
            remaining_dataframe = self.session.createDataFrame(
                remaining_rows, table_schema
            )
            # Overwrite table with same format, preserving schema
            remaining_dataframe.write.format(table_format).mode(
                "overwrite"
            ).saveAsTable(qualified_name)
        else:
            # Empty result - clear table but preserve schema
            empty_df = self.session.createDataFrame([], table_schema)
            empty_df.write.format(table_format).mode("overwrite").saveAsTable(
                qualified_name
            )

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

    def _execute_refresh(self, ast: SQLAST) -> IDataFrame:
        """Execute REFRESH TABLE query.

        Args:
            ast: Parsed SQL AST.

        Returns:
            Empty DataFrame indicating success.

        Raises:
            AnalysisException: If table does not exist.
        """
        from ...errors import AnalysisException

        components = ast.components
        table_name = components.get("table_name", "unknown")
        schema_name = components.get("schema_name")

        # Access storage through catalog
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        if schema_name is None:
            schema_name = storage.get_current_schema()

        # Build qualified table name
        qualified_name = f"{schema_name}.{table_name}" if schema_name else table_name

        # Check if table exists
        if not storage.table_exists(schema_name, table_name):
            raise AnalysisException(f"Table {qualified_name} does not exist")

        # Refresh table metadata - for sparkless, this is a no-op
        # but we verify the table exists and is accessible
        # In a real implementation, this would reload metadata from storage
        # For sparkless, tables are already immediately accessible after writes

        # Return empty DataFrame to indicate success
        from typing import cast

        return cast("IDataFrame", DataFrame([], StructType([])))

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
                # Access storage through catalog (ISession protocol doesn't expose _storage)
                storage = getattr(self.session, "_storage", None)
                if storage is None:
                    storage = self.session.catalog._storage  # type: ignore[attr-defined]
                meta = storage.get_table_metadata(schema_name, table_only)

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
        # Access storage through catalog (ISession protocol doesn't expose _storage)
        storage = getattr(self.session, "_storage", None)
        if storage is None:
            storage = self.session.catalog._storage  # type: ignore[attr-defined]
        storage.drop_table(target_schema, target_name)
        storage.create_table(target_schema, target_name, target_df.schema.fields)
        if updated_rows:
            storage.insert_data(target_schema, target_name, updated_rows)

        # MERGE returns empty DataFrame
        return cast("IDataFrame", DataFrame([], StructType([])))

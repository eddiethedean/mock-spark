"""
SQLAlchemy-based materializer for Mock Spark lazy evaluation.

This module uses SQLAlchemy with DuckDB to provide SQL generation
and execution capabilities for complex DataFrame operations.
"""

# mypy: disable-error-code="arg-type"

from typing import Any, Dict, List, Tuple
from sqlalchemy import (
    create_engine,
    select,
    func,
    desc,
    asc,
    and_,
    or_,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Float,
    Double,
    Boolean,
    DateTime,
    literal,
    insert,
    text,
)
from sqlalchemy.orm import Session
from mock_spark.spark_types import (
    MockStructType,
    MockRow,
)
from mock_spark.functions import MockColumn, MockColumnOperation, MockLiteral


class SQLAlchemyMaterializer:
    """Materializes lazy DataFrames using SQLAlchemy with DuckDB."""

    def __init__(self, engine_url: str = "duckdb:///:memory:"):
        # Create DuckDB engine with SQLAlchemy
        self.engine = create_engine(engine_url, echo=False)
        self._temp_table_counter = 0
        self._created_tables: Dict[str, Any] = {}  # Track created tables
        self.metadata = MetaData()

    def materialize(
        self, data: List[Dict[str, Any]], schema: MockStructType, operations: List[Tuple[str, Any]]
    ) -> List[MockRow]:
        """
        Materializes the DataFrame by building and executing operations using SQLAlchemy.
        """
        if not operations:
            # No operations to apply, return original data as rows
            return [MockRow(row) for row in data]

        # Create initial table with data
        current_table_name = f"temp_table_{self._temp_table_counter}"
        self._temp_table_counter += 1

        # Create table and insert data
        self._create_table_with_data(current_table_name, data)

        # Apply operations step by step
        temp_counter = 1
        for op_name, op_val in operations:
            next_table_name = f"temp_table_{self._temp_table_counter}_{temp_counter}"
            temp_counter += 1

            if op_name == "filter":
                self._apply_filter(current_table_name, next_table_name, op_val)
            elif op_name == "select":
                self._apply_select(current_table_name, next_table_name, op_val)
            elif op_name == "withColumn":
                col_name, col = op_val
                self._apply_with_column(current_table_name, next_table_name, col_name, col)
            elif op_name == "orderBy":
                self._apply_order_by(current_table_name, next_table_name, op_val)
            elif op_name == "limit":
                self._apply_limit(current_table_name, next_table_name, op_val)
            elif op_name == "join":
                other_df, on, how = op_val
                self._apply_join(current_table_name, next_table_name, op_val)
            elif op_name == "union":
                other_df = op_val
                self._apply_union(current_table_name, next_table_name, other_df)

            current_table_name = next_table_name

        # Get final results
        return self._get_table_results(current_table_name)

    def _create_table_with_data(self, table_name: str, data: List[Dict[str, Any]]) -> None:
        """Create a table and insert data using SQLAlchemy Table."""
        if not data:
            # Create a minimal table with at least one column to avoid "Table must have at least one column!" error
            columns = [Column("id", Integer)]
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine, checkfirst=True)
            self._created_tables[table_name] = table
            return

        # Create table using SQLAlchemy Table approach
        columns = []
        has_map_columns = False
        map_column_names = []
        
        if data:
            for key, value in data[0].items():
                # Debug type detection
                # print(f"DEBUG: Column {key}, value type: {type(value)}, value: {value}")
                
                if isinstance(value, int):
                    columns.append(Column(key, Integer))
                elif isinstance(value, float):
                    columns.append(Column(key, Float))
                elif isinstance(value, bool):
                    columns.append(Column(key, Boolean))
                elif isinstance(value, list):
                    # For arrays, infer element type from first element
                    from sqlalchemy import ARRAY, VARCHAR
                    if value and len(value) > 0:
                        # Infer element type from first non-None element
                        first_elem = next((elem for elem in value if elem is not None), None)
                        if isinstance(first_elem, int):
                            columns.append(Column(key, ARRAY(Integer)))
                        elif isinstance(first_elem, float):
                            columns.append(Column(key, ARRAY(Float)))
                        elif isinstance(first_elem, bool):
                            columns.append(Column(key, ARRAY(Boolean)))
                        else:
                            columns.append(Column(key, ARRAY(VARCHAR)))
                    else:
                        # Empty array - default to VARCHAR
                        columns.append(Column(key, ARRAY(VARCHAR)))
                elif isinstance(value, dict):
                    # For maps, mark for raw SQL handling
                    has_map_columns = True
                    map_column_names.append(key)
                    # print(f"DEBUG: Found MAP column: {key}")
                    columns.append(Column(key, String))  # Placeholder
                else:
                    columns.append(Column(key, String))

        # Create table - use raw SQL for MAP columns
        if has_map_columns:
            # Build CREATE TABLE with proper MAP types using raw SQL
            from sqlalchemy import ARRAY
            # print(f"DEBUG: Creating table {table_name} with MAP columns: {map_column_names}")
            
            col_defs = []
            for col in columns:
                if col.name in map_column_names:
                    col_defs.append(f'"{col.name}" MAP(VARCHAR, VARCHAR)')
                elif type(col.type).__name__ == 'ARRAY':
                    # Determine array element type
                    from sqlalchemy import ARRAY
                    if isinstance(col.type, ARRAY):
                        elem_type = col.type.item_type
                        if isinstance(elem_type, Integer):
                            col_defs.append(f'"{col.name}" INTEGER[]')
                        elif isinstance(elem_type, Float) or isinstance(elem_type, Double):
                            col_defs.append(f'"{col.name}" DOUBLE[]')
                        elif isinstance(elem_type, Boolean):
                            col_defs.append(f'"{col.name}" BOOLEAN[]')
                        else:
                            col_defs.append(f'"{col.name}" VARCHAR[]')
                    else:
                        col_defs.append(f'"{col.name}" VARCHAR[]')
                elif isinstance(col.type, Integer):
                    col_defs.append(f'"{col.name}" INTEGER')
                elif isinstance(col.type, Float) or isinstance(col.type, Double):
                    col_defs.append(f'"{col.name}" DOUBLE')
                elif isinstance(col.type, Boolean):
                    col_defs.append(f'"{col.name}" BOOLEAN')
                else:
                    col_defs.append(f'"{col.name}" VARCHAR')
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            with Session(self.engine) as session:
                session.execute(text(create_sql))
                session.commit()
            
            # Register table in metadata manually
            table = Table(table_name, self.metadata, *columns, extend_existing=True)
            self._created_tables[table_name] = table
        else:
            # Normal table creation
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine, checkfirst=True)
            self._created_tables[table_name] = table

        # Insert data using raw SQL - handle dict/list conversion for DuckDB
        with Session(self.engine) as session:
            for row_data in data:
                # Convert row data to values for insert, handling special types
                insert_values: Dict[str, Any] = {}
                for col in columns:
                    value = row_data[col.name]
                    
                    # Convert Python dict to DuckDB MAP
                    if isinstance(value, dict):
                        # Convert dict to MAP syntax: MAP(['keys'], ['values'])
                        if value:
                            keys = list(value.keys())
                            vals = list(value.values())
                            # Create MAP using raw SQL
                            map_sql = f"MAP({keys!r}, {vals!r})"
                            insert_values[col.name] = text(map_sql)
                        else:
                            insert_values[col.name] = None
                    else:
                        insert_values[col.name] = value

                # Insert using parameterized values for non-MAP columns
                # and raw SQL for MAP columns
                if any(isinstance(v, type(text(""))) for v in insert_values.values()):
                    # Has MAP columns - use raw SQL
                    col_names = []
                    col_values = []
                    for col_name, col_value in insert_values.items():
                        col_names.append(f'"{col_name}"')
                        if hasattr(col_value, 'text'):  # TextClause
                            col_values.append(col_value.text)
                        elif isinstance(col_value, str):
                            col_values.append(f"'{col_value}'")
                        elif col_value is None:
                            col_values.append("NULL")
                        else:
                            col_values.append(str(col_value))
                    
                    raw_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({', '.join(col_values)})"
                    session.execute(text(raw_sql))
                else:
                    # Normal insert
                    insert_stmt = table.insert().values(insert_values)
                    session.execute(insert_stmt)
            session.commit()

    def _apply_filter(self, source_table: str, target_table: str, condition: Any) -> None:
        """Apply a filter operation using SQLAlchemy expressions."""
        source_table_obj = self._created_tables[source_table]

        # Check if source table has any rows
        with Session(self.engine) as session:
            row_count = session.execute(select(func.count()).select_from(source_table_obj)).scalar()

        # Set flag to enable strict column validation for filters
        # Only validate if table has rows (errors should only occur when processing actual data)
        self._strict_column_validation = bool(row_count and row_count > 0)

        # Convert condition to SQLAlchemy expression
        try:
            filter_expr = self._condition_to_sqlalchemy(source_table_obj, condition)
        finally:
            self._strict_column_validation = False

        # Create target table with same structure
        self._copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

        # Execute filter and insert results
        with Session(self.engine) as session:
            # Build raw SQL query
            column_names = [col.name for col in source_table_obj.columns]
            sql = f"SELECT {', '.join(column_names)} FROM {source_table}"

            if filter_expr is not None:
                # Convert SQLAlchemy expression to SQL string
                filter_sql = str(filter_expr.compile(compile_kwargs={"literal_binds": True}))
                sql += f" WHERE {filter_sql}"

            results = session.execute(text(sql)).all()

            # Insert into target table
            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def _apply_select(self, source_table: str, target_table: str, columns: Tuple[Any, ...]) -> None:
        """Apply a select operation."""
        source_table_obj = self._created_tables[source_table]

        # print(f"DEBUG: _apply_select called with columns: {[str(col) for col in columns]}")

        # Check if we have window functions or aggregate functions - if so, use raw SQL
        has_window_functions = any(
            (hasattr(col, "function_name") and hasattr(col, "window_spec"))  # MockWindowFunction
            or (
                hasattr(col, "function_name")
                and hasattr(col, "column")
                and col.__class__.__name__ == "MockAggregateFunction"
            )
            for col in columns
        )

        # print(f"DEBUG: has_window_functions: {has_window_functions}")

        if has_window_functions:
            # Use raw SQL for window functions
            # print("DEBUG: Using window functions path")
            self._apply_select_with_window_functions(source_table, target_table, columns)
            return

        # Build select columns and new table structure
        select_columns = []
        new_columns: List[Any] = []

        for col in columns:
            # print(f"DEBUG _apply_select: Processing {type(col).__name__}, name={getattr(col, 'name', 'N/A')}, has_operation={hasattr(col, 'operation')}")
            if isinstance(col, str):
                if col == "*":
                    # Select all columns
                    for column in source_table_obj.columns:
                        select_columns.append(column)
                        new_columns.append(Column(column.name, column.type, primary_key=False))
                else:
                    # Select specific column
                    try:
                        source_column = source_table_obj.c[col]
                        select_columns.append(source_column)
                        new_columns.append(Column(col, source_column.type, primary_key=False))
                    except KeyError:
                        # Column not found - raise AnalysisException
                        from mock_spark.core.exceptions import AnalysisException

                        raise AnalysisException(
                            f"Column '{col}' not found. Available columns: {list(source_table_obj.c.keys())}"
                        )
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # Handle MockLiteral objects (literal values) - check this before MockColumn
                if isinstance(col.value, str):
                    select_columns.append(text(f"'{col.value}'"))
                else:
                    select_columns.append(text(str(col.value)))
                # Use appropriate column type based on the literal value
                if isinstance(col.value, int):
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif isinstance(col.value, float):
                    new_columns.append(Column(col.name, Double, primary_key=False))
                elif isinstance(col.value, str):
                    new_columns.append(Column(col.name, String, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            elif hasattr(col, "name") and hasattr(col, "column_type"):
                # Handle MockColumn objects
                col_name = col.name
                # print(f"DEBUG: Handling MockColumn: {col_name}")

                # Check if this is an aliased column (check both _original_column and original_column)
                original_col = getattr(col, "_original_column", None) or getattr(
                    col, "original_column", None
                )
                if original_col is not None:
                    # Use original column name for lookup, alias name for output
                    original_name = original_col.name
                    alias_name = col.name
                    try:
                        source_column = source_table_obj.c[original_name]
                        select_columns.append(source_column.label(alias_name))
                        new_columns.append(
                            Column(alias_name, source_column.type, primary_key=False)
                        )
                    except KeyError:
                        print(
                            f"Warning: Column '{original_name}' not found in table {source_table}"
                        )
                        continue
                # Check if this is a wildcard selector
                elif col_name == "*":
                    # Select all columns
                    for column in source_table_obj.columns:
                        select_columns.append(column)
                        new_columns.append(Column(column.name, column.type, primary_key=False))
                else:
                    # Check if column exists in source table (might come from join)
                    if col_name in source_table_obj.c:
                        source_column = source_table_obj.c[col_name]
                        select_columns.append(source_column)
                        new_columns.append(Column(col_name, source_column.type, primary_key=False))
                    else:
                        # Column doesn't exist in source, might come from join
                        # Add as text column reference with default String type
                        select_columns.append(text(f'"{col_name}"'))
                        new_columns.append(Column(col_name, String, primary_key=False))
            elif hasattr(col, "conditions") and hasattr(col, "default_value"):
                # Handle MockCaseWhen objects
                # print(f"DEBUG: Handling MockCaseWhen: {col.name}")
                try:
                    # Build CASE WHEN SQL expression
                    case_expr = self._build_case_when_sql(col, source_table_obj)
                    select_columns.append(text(case_expr))
                    new_columns.append(Column(col.name, String, primary_key=False))
                except Exception as e:
                    print(f"Warning: Error handling MockCaseWhen: {e}")
                    continue
            elif (
                hasattr(col, "operation")
                and hasattr(col, "column")
                and hasattr(col, "function_name")
            ):
                # Check if this is an arithmetic operation
                if col.function_name in ["+", "-", "*", "/", "%"]:
                    # Handle arithmetic operations
                    # print(f"DEBUG: Handling arithmetic operation: {col.function_name}")
                    try:
                        left_col = source_table_obj.c[col.column.name]
                        # Extract value from MockLiteral or MockColumn
                        if hasattr(col.value, "value") and hasattr(col.value, "data_type"):
                            # This is a MockLiteral
                            right_val = col.value.value
                        elif hasattr(col.value, "name"):
                            # This is a MockColumn - convert to SQL column reference
                            right_val = source_table_obj.c[col.value.name]
                        else:
                            right_val = col.value

                        # Apply arithmetic operation
                        if col.function_name == "+":
                            expr = left_col + right_val
                        elif col.function_name == "-":
                            expr = left_col - right_val
                        elif col.function_name == "*":
                            expr = left_col * right_val
                        elif col.function_name == "/":
                            expr = left_col / right_val
                        elif col.function_name == "%":
                            expr = left_col % right_val

                        select_columns.append(expr.label(col.name))
                        # For arithmetic operations, determine result type based on operand types
                        if col.function_name == "/":
                            # Division always returns float
                            new_columns.append(Column(col.name, Float, primary_key=False))
                        else:
                            # For other operations, if either operand is float, result is float
                            left_is_float = (
                                "FLOAT" in str(left_col.type).upper()
                                or "DOUBLE" in str(left_col.type).upper()
                                or "REAL" in str(left_col.type).upper()
                            )
                            right_is_float = False
                            if hasattr(right_val, "type"):
                                right_is_float = (
                                    "FLOAT" in str(right_val.type).upper()
                                    or "DOUBLE" in str(right_val.type).upper()
                                    or "REAL" in str(right_val.type).upper()
                                )

                            if left_is_float or right_is_float:
                                new_columns.append(Column(col.name, Float, primary_key=False))
                            else:
                                new_columns.append(
                                    Column(col.name, left_col.type, primary_key=False)
                                )
                        # print(f"DEBUG: Successfully handled arithmetic operation: {col.function_name}")
                    except KeyError:
                        print(
                            f"Warning: Column '{col.column.name}' not found in table {source_table}"
                        )
                        continue
                elif col.function_name in ["==", "!=", ">", "<", ">=", "<="]:
                    # Handle comparison operations
                    try:
                        left_col = source_table_obj.c[col.column.name]
                        # Extract value from MockLiteral if needed
                        if hasattr(col.value, "value") and hasattr(col.value, "data_type"):
                            # This is a MockLiteral
                            right_val = col.value.value
                        else:
                            right_val = col.value

                        # Apply comparison operation
                        if col.function_name == "==":
                            if right_val is None:
                                expr = left_col.is_(None)
                            else:
                                expr = left_col == right_val
                        elif col.function_name == "!=":
                            if right_val is None:
                                expr = left_col.isnot(None)
                            else:
                                expr = left_col != right_val
                        elif col.function_name == ">":
                            expr = left_col > right_val
                        elif col.function_name == "<":
                            expr = left_col < right_val
                        elif col.function_name == ">=":
                            expr = left_col >= right_val
                        elif col.function_name == "<=":
                            expr = left_col <= right_val

                        select_columns.append(expr.label(col.name))
                        new_columns.append(Column(col.name, Boolean, primary_key=False))
                    except KeyError:
                        print(
                            f"Warning: Column '{col.column.name}' not found in table {source_table}"
                        )
                        continue
                else:
                    # Handle function operations like F.upper(F.col("name"))
                    # print(f"DEBUG: Handling function operation: {col.function_name} on column {col.column.name}")

                    # Special handling for expr() which doesn't reference a real column
                    if (
                        col.function_name == "expr"
                        and hasattr(col, "value")
                        and col.value is not None
                    ):
                        # Use the SQL expression directly
                        func_expr: Any = text(col.value)  # Can be TextClause or Function[Any]
                        # Handle labeling
                        try:
                            select_columns.append(func_expr.label(col.name))
                        except (NotImplementedError, AttributeError):
                            select_columns.append(text(f"({col.value}) AS {col.name}"))
                        # Infer column type as String for now
                        new_columns.append(Column(col.name, String, primary_key=False))
                        continue

                    # Special handling for cast() which needs CAST(column AS type) syntax
                    if (
                        col.function_name == "cast"
                        and hasattr(col, "value")
                        and col.value is not None
                    ):
                        # Map Mock Spark data types to DuckDB types
                        type_mapping = {
                            "StringType": "VARCHAR",
                            "IntegerType": "INTEGER",
                            "LongType": "BIGINT",
                            "DoubleType": "DOUBLE",
                            "FloatType": "DOUBLE",
                            "BooleanType": "BOOLEAN",
                            "DateType": "DATE",
                            "TimestampType": "TIMESTAMP",
                            # Also support lowercase string versions
                            "string": "VARCHAR",
                            "int": "INTEGER",
                            "integer": "INTEGER",
                            "long": "BIGINT",
                            "bigint": "BIGINT",
                            "double": "DOUBLE",
                            "float": "DOUBLE",
                            "boolean": "BOOLEAN",
                            "date": "DATE",
                            "timestamp": "TIMESTAMP",
                        }
                        # Get the data type name
                        # Check if col.value is a string (e.g., "double", "integer")
                        if isinstance(col.value, str):
                            type_name = col.value
                        elif hasattr(col.value, "__class__"):
                            type_name = col.value.__class__.__name__
                        else:
                            type_name = str(col.value)

                        sql_type = type_mapping.get(type_name, "VARCHAR")

                        # Build CAST expression
                        try:
                            source_column = source_table_obj.c[col.column.name]
                            # Use raw SQL with AS clause since text expressions don't support .label()
                            cast_sql = f"CAST({col.column.name} AS {sql_type}) AS {col.name}"
                            select_columns.append(text(cast_sql))
                            # Infer column type based on cast target
                            if sql_type in ["INTEGER", "BIGINT"]:
                                new_columns.append(Column(col.name, Integer, primary_key=False))
                            elif sql_type == "DOUBLE":
                                new_columns.append(Column(col.name, Double, primary_key=False))
                            elif sql_type == "BOOLEAN":
                                new_columns.append(Column(col.name, Boolean, primary_key=False))
                            else:
                                new_columns.append(Column(col.name, String, primary_key=False))
                            continue
                        except KeyError:
                            print(
                                f"Warning: Column '{col.column.name}' not found in table {source_table}"
                            )
                            continue

                    try:
                        # Check if the column is a complex expression (e.g., arithmetic operation)
                        if hasattr(col.column, "function_name") and col.column.function_name in [
                            "+",
                            "-",
                            "*",
                            "/",
                            "%",
                        ]:
                            # Build the arithmetic expression first
                            left_col = source_table_obj.c[col.column.column.name]
                            if isinstance(col.column.value, (int, float)):
                                right_val = col.column.value
                            else:
                                right_val = (
                                    source_table_obj.c[col.column.value.name]
                                    if hasattr(col.column.value, "name")
                                    else col.column.value
                                )

                            if col.column.function_name == "+":
                                base_expr = left_col + right_val
                            elif col.column.function_name == "-":
                                base_expr = left_col - right_val
                            elif col.column.function_name == "*":
                                base_expr = left_col * right_val
                            elif col.column.function_name == "/":
                                base_expr = left_col / right_val
                            elif col.column.function_name == "%":
                                base_expr = left_col % right_val
                            else:
                                base_expr = left_col
                        else:
                            # Simple column reference
                            base_expr = source_table_obj.c[col.column.name]

                        source_column = base_expr
                        # Apply the function using SQLAlchemy
                        if col.function_name == "upper":
                            func_expr = func.upper(source_column)
                        elif col.function_name == "lower":
                            func_expr = func.lower(source_column)
                        elif col.function_name == "length":
                            func_expr = func.length(source_column)
                        elif col.function_name == "abs":
                            func_expr = func.abs(source_column)
                        elif col.function_name == "round":
                            # For round function, check if there's a precision parameter
                            if hasattr(col, "value") and col.value is not None:
                                func_expr = func.round(source_column, col.value)
                            else:
                                func_expr = func.round(source_column)
                        elif col.function_name == "ceil":
                            func_expr = func.ceil(source_column)
                        elif col.function_name == "floor":
                            func_expr = func.floor(source_column)
                        elif col.function_name == "sqrt":
                            func_expr = func.sqrt(source_column)
                        else:
                            # Map PySpark function names to DuckDB function names
                            function_mapping = {
                                "signum": "sign",
                                "greatest": "greatest",
                                "least": "least",
                                "format_string": "format",
                                "translate": "translate",
                                "base64": "base64",
                                "ascii": "ascii",
                                "months_between": "months_between",
                                "minute": "minute",
                                "second": "second",
                                "add_months": "date_add",
                                "date_add": "date_add",
                                "date_sub": "date_sub",
                                "coalesce": "coalesce",
                                "isnull": "isnull",  # Handled by special_functions with IS NULL syntax
                                "isnan": "isnan",
                                # Array functions - DuckDB uses list_ prefix
                                "array_distinct": "list_distinct",
                                "array_intersect": "list_intersect",
                                "array_union": "list_concat",  # DuckDB uses concat for union
                                "array_except": "list_except",
                                "array_position": "list_position",
                                "array_remove": "array_remove",  # Will need custom handling
                                # Add more mappings as needed
                            }

                            # Functions that require type casting
                            type_casting_functions = {
                                "base64": "CAST({} AS BLOB)",  # DuckDB base64 expects BLOB
                                "ascii": "CAST({} AS VARCHAR)",  # Ensure VARCHAR for ascii
                                "minute": "CAST({} AS TIMESTAMP)",  # DuckDB minute expects TIMESTAMP
                                "second": "CAST({} AS TIMESTAMP)",  # DuckDB second expects TIMESTAMP
                                "add_months": "CAST({} AS DATE)",  # DuckDB add_months expects DATE
                                "date_add": "CAST({} AS DATE)",  # DuckDB date_add expects DATE
                                "date_sub": "CAST({} AS DATE)",  # DuckDB date_sub expects DATE
                            }

                            # Special handling for functions that need custom SQL generation
                            special_functions = {
                                "add_months": "({} + INTERVAL {} MONTH)",
                                "months_between": "DATEDIFF('MONTH', {}, {})",
                                "date_add": "({} + INTERVAL {} DAY)",
                                "date_sub": "({} - INTERVAL {} DAY)",
                                "timestampadd": "timestampadd",  # Special handling below
                                "timestampdiff": "timestampdiff",  # Special handling below
                                "initcap": "initcap",  # Special handling below
                                "soundex": "soundex",  # Special handling below
                                "array_join": "array_join",  # Special handling below
                                "regexp_extract_all": "regexp_extract_all",  # Special handling below
                                "repeat": "repeat",  # Needs parameter handling
                                "array_distinct": "array_distinct",  # Special handling below
                                "array_intersect": "array_intersect",  # Special handling below
                                "array_union": "array_union",  # Special handling below
                                "array_except": "array_except",  # Special handling below
                                "array_position": "array_position",  # Special handling below
                                "array_remove": "array_remove",  # Special handling below
                                "transform": "transform",  # Higher-order function - special handling below
                                "filter": "filter",  # Higher-order function - special handling below
                                "exists": "exists",  # Higher-order function - special handling below
                                "forall": "forall",  # Higher-order function - special handling below
                                "aggregate": "aggregate",  # Higher-order function - special handling below
                                "zip_with": "zip_with",  # Higher-order function - special handling below
                                "array_compact": "array_compact",  # Special handling below
                                "slice": "slice",  # Special handling below
                                "element_at": "element_at",  # Special handling below
                                "array_append": "array_append",  # Special handling below
                                "array_prepend": "array_prepend",  # Special handling below
                                "array_insert": "array_insert",  # Special handling below
                                "array_size": "array_size",  # Special handling below
                                "array_sort": "array_sort",  # Special handling below
                                "arrays_overlap": "arrays_overlap",  # Special handling below
                                "create_map": "create_map",  # Special handling below
                                "map_contains_key": "map_contains_key",  # Special handling below
                                "map_from_entries": "map_from_entries",  # Special handling below
                                "map_filter": "map_filter",  # Higher-order function - special handling below
                                "transform_keys": "transform_keys",  # Higher-order function - special handling below
                                "transform_values": "transform_values",  # Higher-order function - special handling below
                                "struct": "struct",  # Special handling below
                                "named_struct": "named_struct",  # Special handling below
                                "bit_count": "bit_count",  # Special handling below
                                "bit_get": "bit_get",  # Special handling below
                                "bitwise_not": "bitwise_not",  # Special handling below
                                "convert_timezone": "convert_timezone",  # Special handling below
                                "current_timezone": "current_timezone",  # Special handling below
                                "from_utc_timestamp": "from_utc_timestamp",  # Special handling below
                                "to_utc_timestamp": "to_utc_timestamp",  # Special handling below
                                "parse_url": "parse_url",  # Special handling below
                                "url_encode": "url_encode",  # Special handling below
                                "url_decode": "url_decode",  # Special handling below
                                "date_part": "date_part",  # Special handling below
                                "dayname": "dayname",  # Special handling below
                                "assert_true": "assert_true",  # Special handling below
                                "from_xml": "from_xml",  # Special handling below
                                "to_xml": "to_xml",  # Special handling below
                                "schema_of_xml": "schema_of_xml",  # Special handling below
                                "xpath": "xpath",  # Special handling below
                                "xpath_boolean": "xpath_boolean",  # Special handling below
                                "xpath_double": "xpath_double",  # Special handling below
                                "xpath_float": "xpath_float",  # Special handling below
                                "xpath_int": "xpath_int",  # Special handling below
                                "xpath_long": "xpath_long",  # Special handling below
                                "xpath_short": "xpath_short",  # Special handling below
                                "xpath_string": "xpath_string",  # Special handling below
                                "isnull": "({} IS NULL)",
                                "expr": "{}",  # expr() function directly uses the SQL expression
                                "coalesce": "coalesce",  # Mark for special handling
                            }
                            duckdb_function_name = function_mapping.get(
                                col.function_name, col.function_name
                            )

                            # Check if this function needs type casting
                            column_expr = col.column.name
                            if col.function_name in type_casting_functions:
                                column_expr = type_casting_functions[col.function_name].format(
                                    col.column.name
                                )

                            # Handle special functions that need custom SQL regardless of parameters
                            if col.function_name == "initcap":
                                # Custom initcap implementation for DuckDB
                                special_sql = f"UPPER(SUBSTRING({column_expr}, 1, 1)) || LOWER(SUBSTRING({column_expr}, 2))"
                                func_expr = text(special_sql)
                            elif col.function_name == "soundex":
                                # DuckDB doesn't have soundex, just return original
                                func_expr = source_column
                            elif col.function_name == "array_distinct" and (not hasattr(col, "value") or col.value is None):
                                # array_distinct without parameters - cast to array if needed
                                special_sql = f"LIST_DISTINCT(CAST({column_expr} AS VARCHAR[]))"
                                func_expr = text(special_sql)
                            elif col.function_name == "map_keys" and (not hasattr(col, "value") or col.value is None):
                                # map_keys(map) - DuckDB: MAP_KEYS(map)
                                # Convert dict to map if needed
                                special_sql = f"MAP_KEYS({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "map_values" and (not hasattr(col, "value") or col.value is None):
                                # map_values(map) - DuckDB: MAP_VALUES(map)
                                special_sql = f"MAP_VALUES({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "array_compact" and (not hasattr(col, "value") or col.value is None):
                                # array_compact(array) -> LIST_FILTER(array, x -> x IS NOT NULL)
                                special_sql = f"LIST_FILTER({column_expr}, x -> x IS NOT NULL)"
                                func_expr = text(special_sql)
                            elif col.function_name == "array_size" and (not hasattr(col, "value") or col.value is None):
                                # array_size(array) -> LEN(array)
                                special_sql = f"LEN({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "array_sort" and (not hasattr(col, "value") or col.value is None):
                                # array_sort(array) -> LIST_SORT(array)
                                special_sql = f"LIST_SORT({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "map_from_entries" and (not hasattr(col, "value") or col.value is None):
                                # map_from_entries(array) -> MAP_FROM_ENTRIES(array)
                                special_sql = f"MAP_FROM_ENTRIES({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "bit_count" and (not hasattr(col, "value") or col.value is None):
                                # bit_count(col) -> BIT_COUNT(col)
                                special_sql = f"BIT_COUNT({column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "bitwise_not" and (not hasattr(col, "value") or col.value is None):
                                # bitwise_not(col) -> ~col
                                special_sql = f"(~{column_expr})"
                                func_expr = text(special_sql)
                            elif col.function_name == "url_encode" and (not hasattr(col, "value") or col.value is None):
                                # URL encode - use REPLACE for basic encoding
                                special_sql = f"REPLACE(REPLACE({column_expr}, ' ', '%20'), '#', '%23')"
                                func_expr = text(special_sql)
                            elif col.function_name == "url_decode" and (not hasattr(col, "value") or col.value is None):
                                # URL decode
                                special_sql = f"REPLACE({column_expr}, '%20', ' ')"
                                func_expr = text(special_sql)
                            elif col.function_name == "dayname" and (not hasattr(col, "value") or col.value is None):
                                # dayname(date) -> DAYNAME(date::DATE)
                                special_sql = f"DAYNAME({column_expr}::DATE)"
                                func_expr = text(special_sql)
                            elif col.function_name == "schema_of_xml" and (not hasattr(col, "value") or col.value is None):
                                # schema_of_xml(xml) - simplified: return fixed schema string
                                special_sql = "'STRUCT<>'"
                                func_expr = text(special_sql)
                            # Handle functions with parameters
                            elif hasattr(col, "value") and col.value is not None:
                                # Check if this is a special function that needs custom SQL generation
                                if col.function_name in special_functions:
                                    # Handle special functions like add_months, months_between, date_add, date_sub, expr
                                    if col.function_name == "expr":
                                        # For expr(), use the SQL expression directly
                                        func_expr = text(col.value)
                                    elif col.function_name == "months_between":
                                        # For months_between, we need both column names with DATE casting
                                        column1_name = f"CAST({col.column.name} AS DATE)"
                                        column2_name = (
                                            f"CAST({col.value.name} AS DATE)"
                                            if hasattr(col.value, "name")
                                            else f"CAST({col.value} AS DATE)"
                                        )
                                        special_sql = special_functions[col.function_name].format(
                                            column1_name, column2_name
                                        )
                                        func_expr = text(special_sql)
                                    elif col.function_name in ["date_add", "date_sub"]:
                                        # For date_add and date_sub, we need the column and the number of days
                                        if isinstance(col.value, str):
                                            param_value = f"'{col.value}'"
                                        elif hasattr(col.value, "value") and hasattr(
                                            col.value, "data_type"
                                        ):
                                            # Handle MockLiteral objects
                                            if isinstance(col.value.value, str):
                                                param_value = f"'{col.value.value}'"
                                            else:
                                                param_value = str(col.value.value)
                                        else:
                                            param_value = str(col.value)

                                        special_sql = special_functions[col.function_name].format(
                                            column_expr, param_value
                                        )
                                        func_expr = text(special_sql)
                                    elif col.function_name == "isnull":
                                        # For isnull, we only need the column name (no parameters)
                                        special_sql = special_functions[col.function_name].format(
                                            column_expr
                                        )
                                        func_expr = text(special_sql)
                                    elif col.function_name == "coalesce":
                                        # For coalesce, cast all arguments to VARCHAR to ensure type compatibility
                                        params = []
                                        params.append(f"CAST({column_expr} AS VARCHAR)")

                                        # Handle the additional parameters
                                        if isinstance(col.value, (tuple, list)):
                                            for param in col.value:
                                                if hasattr(param, "value") and hasattr(
                                                    param, "data_type"
                                                ):
                                                    # MockLiteral - check if it's a string literal
                                                    if isinstance(param.value, str):
                                                        params.append(
                                                            f"CAST('{param.value}' AS VARCHAR)"
                                                        )
                                                    else:
                                                        params.append(
                                                            f"CAST({param.value} AS VARCHAR)"
                                                        )
                                                elif hasattr(param, "name"):
                                                    # MockColumn
                                                    params.append(
                                                        f'CAST("{param.name}" AS VARCHAR)'
                                                    )
                                                else:
                                                    params.append(f"CAST({param} AS VARCHAR)")
                                        else:
                                            # Single parameter
                                            if hasattr(col.value, "value") and hasattr(
                                                col.value, "data_type"
                                            ):
                                                # MockLiteral - check if it's a string literal
                                                if isinstance(col.value.value, str):
                                                    params.append(
                                                        f"CAST('{col.value.value}' AS VARCHAR)"
                                                    )
                                                else:
                                                    params.append(
                                                        f"CAST({col.value.value} AS VARCHAR)"
                                                    )
                                            elif hasattr(col.value, "name"):
                                                params.append(
                                                    f'CAST("{col.value.name}" AS VARCHAR)'
                                                )
                                            else:
                                                params.append(f"CAST({col.value} AS VARCHAR)")

                                        coalesce_sql = f"coalesce({', '.join(params)})"
                                        func_expr = text(coalesce_sql)
                                    elif col.function_name == "timestampadd":
                                        # timestampadd(unit, quantity, timestamp)
                                        # DuckDB uses interval arithmetic: timestamp + INTERVAL quantity unit
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            unit = col.value[0].upper()
                                            quantity = col.value[1]
                                            # Format quantity
                                            if isinstance(quantity, (int, float)):
                                                qty_str = str(quantity)
                                            elif hasattr(quantity, "name"):
                                                qty_str = f'"{quantity.name}"'
                                            else:
                                                qty_str = str(quantity)
                                            # Map units to DuckDB interval types
                                            unit_map = {
                                                "YEAR": "YEAR", "QUARTER": "QUARTER", "MONTH": "MONTH",
                                                "WEEK": "WEEK", "DAY": "DAY", "HOUR": "HOUR",
                                                "MINUTE": "MINUTE", "SECOND": "SECOND"
                                            }
                                            interval_unit = unit_map.get(unit, unit)
                                            # Cast to timestamp for interval arithmetic
                                            special_sql = f"(CAST({column_expr} AS TIMESTAMP) + INTERVAL ({qty_str}) {interval_unit})"
                                            func_expr = text(special_sql)
                                        else:
                                            func_expr = source_column
                                    elif col.function_name == "timestampdiff":
                                        # timestampdiff(unit, start, end)
                                        # DuckDB: DATE_DIFF(unit, start, end) 
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            unit = col.value[0].lower()  # DuckDB uses lowercase
                                            end = col.value[1]
                                            # Format end timestamp
                                            if hasattr(end, "name"):
                                                end_str = f'CAST("{end.name}" AS TIMESTAMP)'
                                            else:
                                                end_str = f"CAST('{end}' AS TIMESTAMP)"
                                            # Cast start column to timestamp too
                                            start_str = f"CAST({column_expr} AS TIMESTAMP)"
                                            special_sql = f"DATE_DIFF('{unit}', {start_str}, {end_str})"
                                            func_expr = text(special_sql)
                                        else:
                                            func_expr = source_column
                                    elif col.function_name == "array_join":
                                        # array_join(array, delimiter, null_replacement)
                                        # DuckDB: ARRAY_TO_STRING or LIST_AGGREGATE
                                        if isinstance(col.value, tuple):
                                            delimiter = col.value[0]
                                            null_replacement = col.value[1] if len(col.value) > 1 else None
                                            if null_replacement and null_replacement != "None":
                                                special_sql = f"ARRAY_TO_STRING({column_expr}, '{delimiter}', '{null_replacement}')"
                                            else:
                                                special_sql = f"ARRAY_TO_STRING({column_expr}, '{delimiter}')"
                                            func_expr = text(special_sql)
                                        else:
                                            func_expr = source_column
                                    elif col.function_name == "regexp_extract_all":
                                        # regexp_extract_all(column, pattern, idx)
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            pattern = col.value[0]
                                            # idx parameter not used in DuckDB implementation
                                            special_sql = f"REGEXP_EXTRACT_ALL({column_expr}, '{pattern}')"
                                            func_expr = text(special_sql)
                                        else:
                                            func_expr = source_column
                                    elif col.function_name == "repeat":
                                        # repeat(column, n)
                                        # DuckDB: REPEAT(string, n)
                                        n = col.value if not isinstance(col.value, tuple) else col.value[0]
                                        special_sql = f"REPEAT({column_expr}, {n})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_distinct":
                                        # array_distinct(array) -> list_distinct(array)
                                        special_sql = f"LIST_DISTINCT({column_expr})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_intersect":
                                        # array_intersect(array1, array2) -> list_intersect(array1, array2)
                                        if isinstance(col.value, MockColumn):
                                            array2 = f'CAST("{col.value.name}" AS VARCHAR[])'
                                        else:
                                            array2 = str(col.value)
                                        special_sql = f"LIST_INTERSECT(CAST({column_expr} AS VARCHAR[]), {array2})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_union":
                                        # array_union(array1, array2) -> list_concat + list_distinct
                                        if isinstance(col.value, MockColumn):
                                            array2 = f'CAST("{col.value.name}" AS VARCHAR[])'
                                        else:
                                            array2 = str(col.value)
                                        special_sql = f"LIST_DISTINCT(LIST_CONCAT(CAST({column_expr} AS VARCHAR[]), {array2}))"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_except":
                                        # array_except(array1, array2) - DuckDB doesn't have list_except
                                        # Use LIST_FILTER: filter out elements that are in array2
                                        if isinstance(col.value, MockColumn):
                                            array2 = f'CAST("{col.value.name}" AS VARCHAR[])'
                                        else:
                                            array2 = str(col.value)
                                        special_sql = f"LIST_FILTER(CAST({column_expr} AS VARCHAR[]), x -> NOT LIST_CONTAINS({array2}, x))"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_position":
                                        # array_position(array, element) -> list_position(array, element)
                                        if isinstance(col.value, str):
                                            element = f"'{col.value}'"
                                        else:
                                            element = str(col.value)
                                        special_sql = f"LIST_POSITION(CAST({column_expr} AS VARCHAR[]), {element})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_remove":
                                        # array_remove(array, element) - DuckDB doesn't have direct remove
                                        # Use LIST_FILTER: list_filter(array, x -> x != element)
                                        if isinstance(col.value, str):
                                            element = f"'{col.value}'"
                                        else:
                                            element = str(col.value)
                                        special_sql = f"LIST_FILTER(CAST({column_expr} AS VARCHAR[]), x -> x != {element})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "transform":
                                        # transform(array, lambda) -> LIST_TRANSFORM(array, lambda)
                                        from mock_spark.functions.base import MockLambdaExpression
                                        if isinstance(col.value, MockLambdaExpression):
                                            lambda_sql = col.value.to_duckdb_lambda()
                                            # Don't cast - let DuckDB infer array type
                                            special_sql = f"LIST_TRANSFORM({column_expr}, {lambda_sql})"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("transform requires a lambda function")
                                    elif col.function_name == "filter":
                                        # filter(array, lambda) -> LIST_FILTER(array, lambda)
                                        from mock_spark.functions.base import MockLambdaExpression
                                        if isinstance(col.value, MockLambdaExpression):
                                            lambda_sql = col.value.to_duckdb_lambda()
                                            special_sql = f"LIST_FILTER({column_expr}, {lambda_sql})"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("filter requires a lambda function")
                                    elif col.function_name == "exists":
                                        # exists(array, lambda) -> LIST_ANY(LIST_FILTER(array, lambda))
                                        # DuckDB doesn't have LIST_ANY directly, workaround: check if filtered list has length > 0
                                        from mock_spark.functions.base import MockLambdaExpression
                                        if isinstance(col.value, MockLambdaExpression):
                                            lambda_sql = col.value.to_duckdb_lambda()
                                            special_sql = f"LEN(LIST_FILTER({column_expr}, {lambda_sql})) > 0"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("exists requires a lambda function")
                                    elif col.function_name == "forall":
                                        # forall(array, lambda) -> check if all elements satisfy condition
                                        # Workaround: LEN(LIST_FILTER(array, NOT lambda)) == 0
                                        # Or: LEN(LIST_FILTER(array, lambda)) == LEN(array)
                                        from mock_spark.functions.base import MockLambdaExpression
                                        if isinstance(col.value, MockLambdaExpression):
                                            lambda_sql = col.value.to_duckdb_lambda()
                                            # Use approach: LEN(filtered) == LEN(original)
                                            special_sql = f"(LEN(LIST_FILTER({column_expr}, {lambda_sql})) = LEN({column_expr}))"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("forall requires a lambda function")
                                    elif col.function_name == "aggregate":
                                        # aggregate(array, init, merge, finish) -> LIST_REDUCE or custom
                                        # DuckDB has LIST_REDUCE but with different signature
                                        from mock_spark.functions.base import MockLambdaExpression, MockLiteral
                                        # col.value is initial_value, col.value2 is lambda_data dict
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            initial_value = col.value[0]
                                            lambda_data = col.value[1]
                                            
                                            # Get initial value
                                            if isinstance(initial_value, MockLiteral):
                                                init_val = str(initial_value.value)
                                            else:
                                                init_val = str(initial_value)
                                            
                                            # Get merge lambda
                                            if isinstance(lambda_data, dict) and "merge" in lambda_data:
                                                merge_expr = lambda_data["merge"]
                                                if isinstance(merge_expr, MockLambdaExpression):
                                                    lambda_sql = merge_expr.to_duckdb_lambda()
                                                    # DuckDB LIST_REDUCE: list_reduce(list, lambda, initial_value)
                                                    special_sql = f"LIST_REDUCE({column_expr}, {lambda_sql}, {init_val})"
                                                    func_expr = text(special_sql)
                                                else:
                                                    raise ValueError("aggregate merge must be a lambda function")
                                            else:
                                                raise ValueError("aggregate requires merge lambda")
                                        else:
                                            raise ValueError("aggregate requires initial value and merge function")
                                    elif col.function_name == "zip_with":
                                        # zip_with(array1, array2, lambda) -> Custom SQL with LIST_ZIP
                                        from mock_spark.functions.base import MockLambdaExpression, MockColumn
                                        # col.value is tuple of (array2, lambda)
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            array2 = col.value[0]
                                            lambda_expr = col.value[1]
                                            
                                            # Get second array expression
                                            if isinstance(array2, MockColumn):
                                                array2_expr = f'"{array2.name}"'
                                            else:
                                                array2_expr = str(array2)
                                            
                                            # Get lambda
                                            if isinstance(lambda_expr, MockLambdaExpression):
                                                # DuckDB LIST_ZIP creates STRUCT(elem1, elem2)
                                                # We need to transform the 2-arg lambda to access struct fields
                                                # Original lambda: (x, y) -> (x + y)
                                                # DuckDB needs: s -> (s.field1 + s.field2) OR s -> (s[1] + s[2])
                                                param_names = lambda_expr.get_param_names()
                                                if len(param_names) == 2:
                                                    # Get the body of the lambda by translating with modified params
                                                    # We'll use a wrapper lambda that unpacks the struct
                                                    lambda_sql = lambda_expr.to_duckdb_lambda()
                                                    # Replace (x, y) -> expr with s -> expr where x becomes s[1] and y becomes s[2]
                                                    x_name = param_names[0]
                                                    y_name = param_names[1]
                                                    # Get the body part after the ->
                                                    body_part = lambda_sql.split(' -> ', 1)[1]
                                                    # Replace x and y with struct accessors
                                                    # Use word boundaries to avoid partial replacements
                                                    import re
                                                    body_part = re.sub(rf'\b{x_name}\b', 's[1]', body_part)
                                                    body_part = re.sub(rf'\b{y_name}\b', 's[2]', body_part)
                                                    modified_lambda = f"s -> {body_part}"
                                                    special_sql = f"LIST_TRANSFORM(LIST_ZIP({column_expr}, {array2_expr}), {modified_lambda})"
                                                else:
                                                    # Single param lambda - just use it as is
                                                    lambda_sql = lambda_expr.to_duckdb_lambda()
                                                    special_sql = f"LIST_TRANSFORM(LIST_ZIP({column_expr}, {array2_expr}), {lambda_sql})"
                                                func_expr = text(special_sql)
                                            else:
                                                raise ValueError("zip_with requires a lambda function")
                                        else:
                                            raise ValueError("zip_with requires array2 and lambda function")
                                    elif col.function_name == "current_timezone":
                                        # current_timezone() -> current_setting('TIMEZONE')
                                        # This function doesn't use the column, ignore column_expr
                                        special_sql = "current_setting('TIMEZONE')"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_compact":
                                        # array_compact(array) -> LIST_FILTER(array, x -> x IS NOT NULL)
                                        special_sql = f"LIST_FILTER({column_expr}, x -> x IS NOT NULL)"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "slice":
                                        # slice(array, start, length) -> LIST_SLICE(array, start, start+length-1)
                                        # PySpark uses 1-based indexing
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            start_pos = col.value[0]
                                            length = col.value[1]
                                            # DuckDB LIST_SLICE is 1-based like Spark
                                            end_pos = start_pos + length - 1
                                            special_sql = f"LIST_SLICE({column_expr}, {start_pos}, {end_pos})"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("slice requires start and length")
                                    elif col.function_name == "element_at":
                                        # element_at(array, index) -> array[index]
                                        # PySpark uses 1-based indexing, DuckDB uses 1-based too
                                        # Negative indices count from end
                                        index = col.value
                                        special_sql = f"LIST_EXTRACT({column_expr}, {index})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_append":
                                        # array_append(array, element) -> LIST_CONCAT(array, [element])
                                        # DuckDB doesn't have LIST_APPEND
                                        element = col.value
                                        if isinstance(element, str):
                                            element_sql = f"'{element}'"
                                        else:
                                            element_sql = str(element)
                                        special_sql = f"LIST_CONCAT({column_expr}, [{element_sql}])"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_prepend":
                                        # array_prepend(array, element) -> LIST_CONCAT([element], array)
                                        # DuckDB doesn't have LIST_PREPEND
                                        element = col.value
                                        if isinstance(element, str):
                                            element_sql = f"'{element}'"
                                        else:
                                            element_sql = str(element)
                                        special_sql = f"LIST_CONCAT([{element_sql}], {column_expr})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_insert":
                                        # array_insert(array, pos, value) -> custom SQL with slicing
                                        if isinstance(col.value, tuple) and len(col.value) >= 2:
                                            pos = col.value[0]
                                            value = col.value[1]
                                            # DuckDB: slice before + [value] + slice after
                                            if isinstance(value, str):
                                                value_sql = f"'{value}'"
                                            else:
                                                value_sql = str(value)
                                            # LIST_CONCAT(LIST_SLICE(arr, 1, pos-1), [value], LIST_SLICE(arr, pos, len))
                                            special_sql = f"LIST_CONCAT(LIST_SLICE({column_expr}, 1, {pos}-1), [{value_sql}], LIST_SLICE({column_expr}, {pos}, LEN({column_expr})))"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("array_insert requires pos and value")
                                    elif col.function_name == "array_size":
                                        # array_size(array) -> LEN(array) or LIST_LENGTH(array)
                                        special_sql = f"LEN({column_expr})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "array_sort":
                                        # array_sort(array) -> LIST_SORT(array)
                                        special_sql = f"LIST_SORT({column_expr})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "arrays_overlap":
                                        # arrays_overlap(arr1, arr2) -> LEN(LIST_INTERSECT(arr1, arr2)) > 0
                                        from mock_spark.functions.base import MockColumn
                                        if isinstance(col.value, MockColumn):
                                            array2_expr = f'"{col.value.name}"'
                                        else:
                                            array2_expr = str(col.value)
                                        special_sql = f"LEN(LIST_INTERSECT({column_expr}, {array2_expr})) > 0"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "create_map":
                                        # create_map(k1, v1, k2, v2, ...) -> MAP([k1, k2, ...], [v1, v2, ...])
                                        # col.value contains alternating key-value columns
                                        from mock_spark.functions.base import MockColumn, MockLiteral
                                        if isinstance(col.value, (tuple, list)):
                                            # Build keys and values arrays
                                            keys = [column_expr]
                                            values = []
                                            for i, arg in enumerate(col.value):
                                                if i % 2 == 0:
                                                    # Even index = value for previous key
                                                    if isinstance(arg, MockColumn):
                                                        values.append(f'"{arg.name}"')
                                                    elif isinstance(arg, MockLiteral):
                                                        if isinstance(arg.value, str):
                                                            values.append(f"'{arg.value}'")
                                                        else:
                                                            values.append(str(arg.value))
                                                    elif isinstance(arg, str):
                                                        values.append(f"'{arg}'")
                                                    else:
                                                        values.append(str(arg))
                                                else:
                                                    # Odd index = key
                                                    if isinstance(arg, MockColumn):
                                                        keys.append(f'"{arg.name}"')
                                                    elif isinstance(arg, MockLiteral):
                                                        if isinstance(arg.value, str):
                                                            keys.append(f"'{arg.value}'")
                                                        else:
                                                            keys.append(str(arg.value))
                                                    elif isinstance(arg, str):
                                                        keys.append(f"'{arg}'")
                                                    else:
                                                        keys.append(str(arg))
                                            keys_array = f"[{', '.join(keys)}]"
                                            values_array = f"[{', '.join(values)}]"
                                            special_sql = f"MAP({keys_array}, {values_array})"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("create_map requires key-value pairs")
                                    elif col.function_name == "map_contains_key":
                                        # map_contains_key(map, key) -> MAP_EXTRACT(map, key) IS NOT NULL
                                        key = col.value
                                        if isinstance(key, str):
                                            key_sql = f"'{key}'"
                                        else:
                                            key_sql = str(key)
                                        special_sql = f"(MAP_EXTRACT({column_expr}, {key_sql}) IS NOT NULL)"
                                        func_expr = text(special_sql)
                                    elif col.function_name in ["map_filter", "transform_keys", "transform_values"]:
                                        # Higher-order map functions with lambdas
                                        # These are complex - DuckDB doesn't have native map lambda functions
                                        # We'll need to convert map to entries, transform, convert back
                                        from mock_spark.functions.base import MockLambdaExpression
                                        if isinstance(col.value, MockLambdaExpression):
                                            lambda_sql = col.value.to_duckdb_lambda()
                                            
                                            if col.function_name == "map_filter":
                                                # map_filter: Convert to entries, filter, convert back
                                                # MAP_FROM_ENTRIES(LIST_FILTER(MAP_ENTRIES(map), e -> lambda(e.key, e.value)))
                                                param_names = col.value.get_param_names()
                                                if len(param_names) == 2:
                                                    k_name = param_names[0]
                                                    v_name = param_names[1]
                                                    body_part = lambda_sql.split(' -> ', 1)[1]
                                                    import re
                                                    body_part = re.sub(rf'\b{k_name}\b', 'e.key', body_part)
                                                    body_part = re.sub(rf'\b{v_name}\b', 'e.value', body_part)
                                                    modified_lambda = f"e -> {body_part}"
                                                    special_sql = f"MAP_FROM_ENTRIES(LIST_FILTER(MAP_ENTRIES({column_expr}), {modified_lambda}))"
                                                else:
                                                    raise ValueError("map_filter requires 2-parameter lambda")
                                            elif col.function_name == "transform_keys":
                                                # transform_keys: Convert entries, transform keys, convert back
                                                # MAP_FROM_ENTRIES(LIST_TRANSFORM(MAP_ENTRIES(map), e -> {key: lambda(e.key, e.value), value: e.value}))
                                                param_names = col.value.get_param_names()
                                                if len(param_names) == 2:
                                                    k_name = param_names[0]
                                                    v_name = param_names[1]
                                                    body_part = lambda_sql.split(' -> ', 1)[1]
                                                    import re
                                                    body_part = re.sub(rf'\b{k_name}\b', 'e.key', body_part)
                                                    body_part = re.sub(rf'\b{v_name}\b', 'e.value', body_part)
                                                    modified_lambda = f"e -> {{key: {body_part}, value: e.value}}"
                                                    special_sql = f"MAP_FROM_ENTRIES(LIST_TRANSFORM(MAP_ENTRIES({column_expr}), {modified_lambda}))"
                                                else:
                                                    raise ValueError("transform_keys requires 2-parameter lambda")
                                            elif col.function_name == "transform_values":
                                                # transform_values: Convert entries, transform values, convert back
                                                param_names = col.value.get_param_names()
                                                if len(param_names) == 2:
                                                    k_name = param_names[0]
                                                    v_name = param_names[1]
                                                    body_part = lambda_sql.split(' -> ', 1)[1]
                                                    import re
                                                    body_part = re.sub(rf'\b{k_name}\b', 'e.key', body_part)
                                                    body_part = re.sub(rf'\b{v_name}\b', 'e.value', body_part)
                                                    modified_lambda = f"e -> {{key: e.key, value: {body_part}}}"
                                                    special_sql = f"MAP_FROM_ENTRIES(LIST_TRANSFORM(MAP_ENTRIES({column_expr}), {modified_lambda}))"
                                                else:
                                                    raise ValueError("transform_values requires 2-parameter lambda")
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError(f"{col.function_name} requires a lambda function")
                                    elif col.function_name == "struct":
                                        # struct(col1, col2, ...) -> {col1, col2, ...} or STRUCT_PACK
                                        from mock_spark.functions.base import MockColumn
                                        cols = [column_expr]
                                        if isinstance(col.value, (tuple, list)):
                                            for c in col.value:
                                                if isinstance(c, MockColumn):
                                                    cols.append(f'"{c.name}"')
                                                elif hasattr(c, 'name'):
                                                    cols.append(f'"{c.name}"')
                                                else:
                                                    cols.append(str(c))
                                        special_sql = f"STRUCT_PACK({', '.join(cols)})"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "named_struct":
                                        # named_struct(name1, col1, name2, col2, ...) -> {name1: col1, name2: col2}
                                        from mock_spark.functions.base import MockColumn
                                        if isinstance(col.value, (tuple, list)):
                                            pairs = []
                                            for i in range(0, len(col.value), 2):
                                                if i+1 < len(col.value):
                                                    field_name = col.value[i]
                                                    field_val = col.value[i+1]
                                                    if isinstance(field_val, MockColumn):
                                                        val_sql = f'"{field_val.name}"'
                                                    elif hasattr(field_val, 'name'):
                                                        val_sql = f'"{field_val.name}"'
                                                    else:
                                                        val_sql = str(field_val)
                                                    pairs.append(f"{field_name}: {val_sql}")
                                            special_sql = f"{{{', '.join(pairs)}}}"
                                            func_expr = text(special_sql)
                                        else:
                                            raise ValueError("named_struct requires field name-value pairs")
                                    elif col.function_name == "bit_get":
                                        # bit_get(col, pos) -> (col >> pos) & 1
                                        # DuckDB doesn't have BIT_GET, use bit shifting
                                        pos = col.value
                                        special_sql = f"(({column_expr} >> {pos}) & 1)"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "convert_timezone":
                                        # convert_timezone(sourceTz, targetTz, sourceTs)
                                        # value is tuple (sourceTz, targetTz, sourceTs)
                                        source_tz, target_tz, source_ts = col.value
                                        if hasattr(source_ts, 'name'):
                                            ts_expr = source_ts.name
                                        else:
                                            ts_expr = f"'{source_ts}'"
                                        # Cast to TIMESTAMP explicitly for DuckDB
                                        special_sql = f"timezone('{target_tz}', timezone('{source_tz}', {ts_expr}::TIMESTAMP))"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "from_utc_timestamp":
                                        # from_utc_timestamp(ts, tz) -> timezone(tz, ts::TIMESTAMP)
                                        tz = col.value
                                        special_sql = f"timezone('{tz}', {column_expr}::TIMESTAMP)"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "to_utc_timestamp":
                                        # to_utc_timestamp(ts, tz) -> timezone('UTC', timezone(tz, ts::TIMESTAMP))
                                        tz = col.value
                                        special_sql = f"timezone('UTC', timezone('{tz}', {column_expr}::TIMESTAMP))"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "parse_url":
                                        # parse_url(url, part) - extract URL component
                                        part = col.value.upper()
                                        if part == "HOST":
                                            special_sql = f"regexp_extract({column_expr}, '://([^/]+)', 1)"
                                        elif part == "PROTOCOL":
                                            special_sql = f"regexp_extract({column_expr}, '([^:]+)://', 1)"
                                        elif part == "PATH":
                                            special_sql = f"regexp_extract({column_expr}, '://[^/]+(/[^?#]*)', 1)"
                                        elif part == "QUERY":
                                            special_sql = f"regexp_extract({column_expr}, '\\?([^#]*)', 1)"
                                        else:
                                            # Default: return the URL as-is
                                            special_sql = column_expr
                                        func_expr = text(special_sql)
                                    elif col.function_name == "date_part":
                                        # date_part(field, source) -> DATE_PART(field, source::DATE)
                                        field = col.value
                                        special_sql = f"DATE_PART('{field}', {column_expr}::DATE)"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "assert_true":
                                        # assert_true(condition) - if condition is false, raise error
                                        # For mock implementation, just return the condition
                                        # In real Spark, this would throw an exception
                                        if hasattr(col.value, 'name'):
                                            condition_sql = col.value.name
                                        else:
                                            # col.value is a MockColumnOperation, need to extract SQL
                                            condition_sql = column_expr
                                        special_sql = f"CASE WHEN {condition_sql} THEN TRUE ELSE NULL END"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "to_xml":
                                        # to_xml(struct) - simplified: return XML-like string
                                        # In real Spark, would convert struct to proper XML
                                        if hasattr(col.value, 'name'):
                                            struct_sql = col.value.name
                                        else:
                                            struct_sql = column_expr
                                        special_sql = f"'<row>' || CAST({struct_sql} AS VARCHAR) || '</row>'"
                                        func_expr = text(special_sql)
                                    elif col.function_name in ["xpath_string", "xpath_int", "xpath_long", 
                                                               "xpath_short", "xpath_float", "xpath_double"]:
                                        # XPath extraction - simplified: return NULL
                                        # Real implementation would parse XML and extract values
                                        # xpath = col.value  # Currently unused in simplified implementation
                                        special_sql = "NULL"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "xpath_boolean":
                                        # XPath boolean - simplified: return FALSE
                                        # xpath = col.value  # Currently unused in simplified implementation
                                        special_sql = "FALSE"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "xpath":
                                        # XPath array - simplified: return empty array
                                        # xpath = col.value  # Currently unused in simplified implementation
                                        special_sql = "[]"
                                        func_expr = text(special_sql)
                                    elif col.function_name == "from_xml":
                                        # from_xml(xml, schema) - simplified: return NULL
                                        # schema = col.value  # Currently unused in simplified implementation
                                        special_sql = "NULL"
                                        func_expr = text(special_sql)
                                    else:
                                        # Handle other special functions like add_months
                                        if isinstance(col.value, str):
                                            param_value = f"'{col.value}'"
                                        elif hasattr(col.value, "value") and hasattr(
                                            col.value, "data_type"
                                        ):
                                            # Handle MockLiteral objects
                                            if isinstance(col.value.value, str):
                                                param_value = f"'{col.value.value}'"
                                            else:
                                                param_value = str(col.value.value)
                                        else:
                                            param_value = str(col.value)

                                        special_sql = special_functions[col.function_name].format(
                                            column_expr, param_value
                                        )
                                        func_expr = text(special_sql)
                                elif isinstance(col.value, (tuple, list)):
                                    # Flatten nested tuples/lists and process parameters
                                    flattened_params: List[Any] = []
                                    for param in col.value:
                                        if isinstance(param, (tuple, list)):
                                            # Handle nested tuples/lists (like format_string)
                                            flattened_params.extend(param)
                                        else:
                                            flattened_params.append(param)

                                    # Filter out empty tuples/lists and None values
                                    filtered_params = [
                                        p
                                        for p in flattened_params
                                        if p is not None and p != () and p != []
                                    ]

                                    if filtered_params:
                                        # Join parameters with commas, handling MockLiteral and MockColumn objects
                                        formatted_params = []
                                        for param in filtered_params:
                                            if isinstance(param, str):
                                                formatted_params.append(f"'{param}'")
                                            elif hasattr(param, "value") and hasattr(
                                                param, "data_type"
                                            ):
                                                # Handle MockLiteral objects
                                                if isinstance(param.value, str):
                                                    formatted_params.append(f"'{param.value}'")
                                                else:
                                                    formatted_params.append(str(param.value))
                                            elif hasattr(param, "name"):
                                                # Handle MockColumn objects
                                                formatted_params.append(f'"{param.name}"')
                                            else:
                                                formatted_params.append(str(param))
                                        param_str = ", ".join(formatted_params)
                                        func_expr = text(
                                            f"{duckdb_function_name}({column_expr}, {param_str})"
                                        )
                                    else:
                                        # No valid parameters - single argument function
                                        func_expr = text(f"{duckdb_function_name}({column_expr})")
                                else:
                                    # Single parameter
                                    if isinstance(col.value, str):
                                        func_expr = text(
                                            f"{duckdb_function_name}({column_expr}, '{col.value}')"
                                        )
                                    elif hasattr(col.value, "value") and hasattr(
                                        col.value, "data_type"
                                    ):
                                        # Handle MockLiteral objects
                                        if isinstance(col.value.value, str):
                                            func_expr = text(
                                                f"{duckdb_function_name}({column_expr}, '{col.value.value}')"
                                            )
                                        else:
                                            func_expr = text(
                                                f"{duckdb_function_name}({column_expr}, {col.value.value})"
                                            )
                                    else:
                                        # Handle MockColumn objects in parameters
                                        if hasattr(col.value, "name"):
                                            # This is a MockColumn object, extract its name
                                            param_name = col.value.name
                                            func_expr = text(
                                                f"{duckdb_function_name}({column_expr}, {param_name})"
                                            )
                                        else:
                                            func_expr = text(
                                                f"{duckdb_function_name}({column_expr}, {col.value})"
                                            )
                            else:
                                # No parameters - single argument function
                                # Check if this is a special function that doesn't use standard function syntax
                                if col.function_name in special_functions:
                                    special_sql = special_functions[col.function_name].format(
                                        column_expr
                                    )
                                    func_expr = text(special_sql)
                                else:
                                    func_expr = text(f"{duckdb_function_name}({column_expr})")

                        # Handle labeling more carefully to avoid NotImplementedError
                        # Sanitize column name for SQL alias (remove problematic characters)
                        safe_alias = col.name.replace("(", "_").replace(")", "_").replace(" ", "_").replace(",", "_")
                        
                        try:
                            select_columns.append(func_expr.label(safe_alias))
                        except (NotImplementedError, AttributeError):
                            # For expressions that don't support .label() or .alias(),
                            # create a raw SQL expression with AS clause
                            if hasattr(func_expr, "text"):
                                # For TextClause objects, create a new text expression with alias
                                select_columns.append(text(f"({func_expr.text}) AS {safe_alias}"))
                            else:
                                # Fallback: try to convert to string and wrap in parentheses
                                select_columns.append(text(f"({str(func_expr)}) AS {safe_alias}"))
                        # Infer column type based on function
                        if col.function_name in ["length", "abs", "ceil", "floor"]:
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name in ["round", "sqrt"]:
                            new_columns.append(Column(col.name, Float, primary_key=False))
                        elif col.function_name in ["isnull", "isnan", "isnotnull"]:
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name in ["exists", "forall"]:
                            # exists and forall return boolean
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name in ["transform", "filter", "array_distinct", "array_intersect", "array_union", "array_except", "array_remove", "array_compact", "slice", "array_append", "array_prepend", "array_insert", "array_sort"]:
                            # Array functions return arrays - try to infer element type from source
                            from sqlalchemy import ARRAY
                            source_col = source_table_obj.columns.get(col.column.name)
                            if source_col is not None and isinstance(source_col.type, ARRAY):
                                # Preserve array element type from source
                                new_columns.append(Column(col.name, source_col.type, primary_key=False))
                            else:
                                # Default to VARCHAR array
                                new_columns.append(Column(col.name, ARRAY(String), primary_key=False))
                        elif col.function_name == "element_at":
                            # element_at returns scalar element - infer from array element type
                            from sqlalchemy import ARRAY
                            source_col = source_table_obj.columns.get(col.column.name)
                            if source_col is not None and isinstance(source_col.type, ARRAY):
                                # Return element type from array
                                new_columns.append(Column(col.name, source_col.type.item_type, primary_key=False))
                            else:
                                # Default to String
                                new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name in ["array_size", "bit_count", "bit_get", "bitwise_not"]:
                            # These functions return integer
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name in ["convert_timezone", "from_utc_timestamp", "to_utc_timestamp"]:
                            # Timezone functions return timestamps
                            new_columns.append(Column(col.name, DateTime, primary_key=False))
                        elif col.function_name == "current_timezone":
                            # current_timezone returns string
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name in ["parse_url", "url_encode", "url_decode", "dayname"]:
                            # String functions return strings
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name == "date_part":
                            # date_part returns integer
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name == "assert_true":
                            # assert_true returns boolean
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name in ["to_xml", "schema_of_xml", "xpath_string"]:
                            # XML string functions return strings
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name in ["xpath_int", "xpath_short"]:
                            # XPath integer functions return integers
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name in ["xpath_long"]:
                            # XPath long returns integer (DuckDB uses BIGINT)
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name in ["xpath_float"]:
                            # XPath float returns float
                            new_columns.append(Column(col.name, Float, primary_key=False))
                        elif col.function_name in ["xpath_double"]:
                            # XPath double returns double
                            new_columns.append(Column(col.name, Double, primary_key=False))
                        elif col.function_name == "xpath_boolean":
                            # XPath boolean returns boolean
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name == "xpath":
                            # XPath returns array
                            from sqlalchemy import ARRAY
                            new_columns.append(Column(col.name, ARRAY(String), primary_key=False))
                        elif col.function_name == "from_xml":
                            # from_xml returns struct (simplified as String)
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name == "arrays_overlap":
                            # arrays_overlap returns boolean
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name == "map_contains_key":
                            # map_contains_key returns boolean
                            new_columns.append(Column(col.name, Boolean, primary_key=False))
                        elif col.function_name in ["create_map", "map_filter", "transform_keys", "transform_values"]:
                            # Map functions return maps - default to MAP(VARCHAR, VARCHAR)
                            # DuckDB doesn't have a direct SQLAlchemy type for MAP, use String
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name == "map_from_entries":
                            # map_from_entries returns map
                            new_columns.append(Column(col.name, String, primary_key=False))
                        elif col.function_name == "aggregate":
                            # aggregate returns scalar - infer from initial value or default to Integer
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        elif col.function_name == "zip_with":
                            # zip_with returns array - try to infer element type
                            from sqlalchemy import ARRAY
                            source_col = source_table_obj.columns.get(col.column.name)
                            if source_col is not None and isinstance(source_col.type, ARRAY):
                                new_columns.append(Column(col.name, source_col.type, primary_key=False))
                            else:
                                new_columns.append(Column(col.name, ARRAY(Integer), primary_key=False))
                        else:
                            new_columns.append(Column(col.name, String, primary_key=False))
                        # print(f"DEBUG: Successfully handled function operation: {col.function_name}")
                    except KeyError:
                        print(
                            f"Warning: Column '{col.column.name}' not found in table {source_table}"
                        )
                        continue
            elif hasattr(col, "name"):
                # Handle F.col("column_name") case
                # Check for wildcard first
                if col.name == "*":
                    print("DEBUG: Handling wildcard!")
                    # Select all columns from source table
                    for column in source_table_obj.columns:
                        select_columns.append(column)
                        new_columns.append(Column(column.name, column.type, primary_key=False))
                    continue

                # Check if this is an aliased column (check both _original_column and original_column)
                original_col = getattr(col, "_original_column", None) or getattr(
                    col, "original_column", None
                )
                if original_col is not None:
                    # Use original column name for lookup, alias name for output
                    original_name = original_col.name
                    alias_name = col.name
                    try:
                        source_column = source_table_obj.c[original_name]
                        select_columns.append(source_column.label(alias_name))
                        new_columns.append(
                            Column(alias_name, source_column.type, primary_key=False)
                        )
                    except KeyError:
                        print(
                            f"Warning: Column '{original_name}' not found in table {source_table}"
                        )
                        continue
                else:
                    # Regular column (no alias)
                    try:
                        source_column = source_table_obj.c[col.name]
                        select_columns.append(source_column)
                        new_columns.append(Column(col.name, source_column.type, primary_key=False))
                    except KeyError:
                        # Column not found - raise AnalysisException
                        from mock_spark.core.exceptions import AnalysisException

                        raise AnalysisException(
                            f"Column '{col.name}' not found. Available columns: {list(source_table_obj.c.keys())}"
                        )

        # Ensure we have at least one column
        if not new_columns:
            # Add a placeholder column to avoid "Table must have at least one column!" error
            new_columns = [Column("placeholder", String, primary_key=False)]
            select_columns = [text("'placeholder' as placeholder")]

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Execute select and insert results
        with Session(self.engine) as session:
            # If we have literals, we need to select from the source table to replicate them
            if any(
                hasattr(col, "text") or str(type(col)).find("TextClause") != -1
                for col in select_columns
            ):
                # Use raw SQL to ensure literals are replicated for each row
                source_table_obj = self._created_tables[source_table]
                select_clause = ", ".join(
                    [
                        (
                            str(col)
                            if hasattr(col, "text")
                            or str(type(col)).find("TextClause") != -1
                            or str(type(col)).find("Label") != -1
                            else f'"{col.name}"'
                        )
                        for col in select_columns
                    ]
                )
                sql = f"SELECT {select_clause} FROM {source_table}"
                results = session.execute(text(sql)).all()
            else:
                query = select(*select_columns)
                results = session.execute(query).all()

            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(new_columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def _apply_select_with_window_functions(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply select operation with window functions using raw SQL."""
        source_table_obj = self._created_tables[source_table]

        # Build the SELECT clause
        select_parts = []
        new_columns: List[Any] = []

        for col in columns:
            # print(f"DEBUG _apply_select_with_window_functions: Processing {type(col).__name__}, name={getattr(col, 'name', 'N/A')}, has_operation={hasattr(col, 'operation')}, has_function_name={hasattr(col, 'function_name')}")
            if isinstance(col, str):
                if col == "*":
                    # Select all columns
                    for column in source_table_obj.columns:
                        select_parts.append(f'"{column.name}"')
                        new_columns.append(Column(column.name, column.type, primary_key=False))
                else:
                    select_parts.append(f'"{col}"')
                    source_column = source_table_obj.c[col]
                    new_columns.append(Column(col, source_column.type, primary_key=False))
            elif (
                hasattr(col, "function_name")
                and hasattr(col, "column")
                and col.__class__.__name__ == "MockAggregateFunction"
            ):
                # Handle MockAggregateFunction objects like F.count(), F.sum(), etc.
                if col.function_name == "count":
                    if col.column is None or col.column == "*":
                        select_parts.append("COUNT(*)")
                    else:
                        column_name = col.column.name if hasattr(col.column, "name") else col.column
                        select_parts.append(f"COUNT({column_name})")
                elif col.function_name == "countDistinct":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"COUNT(DISTINCT {column_name})")
                elif col.function_name == "percentile_approx":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    # DuckDB doesn't have percentile functions, use AVG as approximation
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "corr":
                    # CORR function requires two columns, but we only have one
                    # This is a limitation - we'll use AVG as fallback
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "covar_samp":
                    # COVAR_SAMP function requires two columns, but we only have one
                    # This is a limitation - we'll use AVG as fallback
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "sum":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"SUM({column_name})")
                elif col.function_name == "avg":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "max":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"MAX({column_name})")
                elif col.function_name == "min":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"MIN({column_name})")
                else:
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"{col.function_name.upper()}({column_name})")

                # Add appropriate column type
                if col.function_name in ["count", "countDistinct"]:
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif col.function_name == "sum":
                    # Preserve source column type for SUM
                    if col.column and hasattr(col.column, "name"):
                        column_name = col.column.name
                        source_column = source_table_obj.c[column_name]
                        source_type = str(source_column.type).upper()
                        # Use Integer for integer types, Float for floating types
                        if any(
                            int_type in source_type
                            for int_type in ["INTEGER", "BIGINT", "SMALLINT", "INT"]
                        ):
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        else:
                            new_columns.append(Column(col.name, Float, primary_key=False))
                    elif isinstance(col.column, str):
                        column_name = col.column
                        if column_name in source_table_obj.c:
                            source_column = source_table_obj.c[column_name]
                            source_type = str(source_column.type).upper()
                            if any(
                                int_type in source_type
                                for int_type in ["INTEGER", "BIGINT", "SMALLINT", "INT"]
                            ):
                                new_columns.append(Column(col.name, Integer, primary_key=False))
                            else:
                                new_columns.append(Column(col.name, Float, primary_key=False))
                        else:
                            new_columns.append(Column(col.name, Float, primary_key=False))
                    else:
                        new_columns.append(Column(col.name, Float, primary_key=False))
                elif col.function_name in ["avg", "percentile_approx", "corr", "covar_samp"]:
                    new_columns.append(Column(col.name, Float, primary_key=False))
                elif col.function_name in ["max", "min"]:
                    # For max/min, use the same type as the source column
                    if col.column and hasattr(col.column, "name"):
                        source_column = source_table_obj.c[col.column.name]
                        new_columns.append(Column(col.name, source_column.type, primary_key=False))
                    else:
                        new_columns.append(Column(col.name, Integer, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            elif hasattr(col, "name") and col.name == "*":
                # Handle F.col("*") case - only add columns from source table
                # Check which columns are already in new_columns to avoid duplicates
                existing_col_names = {c.name for c in new_columns}
                for column in source_table_obj.columns:
                    if column.name not in existing_col_names:
                        select_parts.append(f'"{column.name}"')
                        new_columns.append(Column(column.name, column.type, primary_key=False))
            elif hasattr(col, "name") and not hasattr(col, "alias"):
                # Handle F.col("column_name") case (but not window functions)
                select_parts.append(f'"{col.name}"')
                source_column = source_table_obj.c[col.name]
                new_columns.append(Column(col.name, source_column.type, primary_key=False))
            elif hasattr(col, "operation") and hasattr(col, "column") and hasattr(col, "value"):
                # Handle MockColumnOperation objects (arithmetic and string operations)
                # Check if this is an arithmetic operation (not a function)
                if hasattr(col, "function_name") and col.function_name in ["+", "-", "*", "/", "%"]:
                    # This is an arithmetic operation, not a function
                    col_expr = self._expression_to_sql(col)
                    select_parts.append(f"({col_expr})")
                    # For arithmetic operations, handle division specially
                    if col.function_name == "/":
                        # Division always returns floating-point type
                        new_columns.append(Column(col.name, Float, primary_key=False))
                    elif hasattr(col, "column") and hasattr(col.column, "name"):
                        source_column = source_table_obj.c[col.column.name]
                        new_columns.append(Column(col.name, source_column.type, primary_key=False))
                    else:
                        new_columns.append(Column(col.name, Float, primary_key=False))
                elif hasattr(col, "function_name") and col.function_name in [
                    "upper",
                    "lower",
                    "trim",
                ]:
                    # This is a string function operation
                    col_expr = self._expression_to_sql(col)
                    select_parts.append(f"({col_expr})")
                    new_columns.append(Column(col.name, String, primary_key=False))
                elif hasattr(col, "function_name") and col.function_name in [
                    "length",
                    "abs",
                    "round",
                ]:
                    # This is a math function operation that returns numeric types
                    col_expr = self._expression_to_sql(col)
                    select_parts.append(f"({col_expr})")
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif hasattr(col, "function_name") and col.function_name in [
                    "==",
                    "!=",
                    ">",
                    "<",
                    ">=",
                    "<=",
                ]:
                    # This is a comparison operation (returns boolean)
                    col_expr = self._expression_to_sql(col)
                    select_parts.append(f"{col_expr}")
                    new_columns.append(Column(col.name, Boolean, primary_key=False))
                elif not hasattr(col, "function_name"):
                    # This is an operation without function_name (must be arithmetic)
                    col_expr = self._expression_to_sql(col)
                    select_parts.append(f"({col_expr})")
                    # Arithmetic operation (returns numeric)
                    new_columns.append(Column(col.name, Float, primary_key=False))
            elif hasattr(col, "function_name") and hasattr(col, "window_spec"):
                # Handle MockWindowFunction objects like F.row_number().over(...).alias("rank")
                if col.function_name == "row_number":
                    # Build the window specification from the window_spec
                    window_sql = self._window_spec_to_sql(col.window_spec, source_table_obj)
                    select_parts.append(f"ROW_NUMBER() OVER ({window_sql})")
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif col.function_name == "rank":
                    window_sql = self._window_spec_to_sql(col.window_spec, source_table_obj)
                    select_parts.append(f"RANK() OVER ({window_sql})")
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif col.function_name == "dense_rank":
                    window_sql = self._window_spec_to_sql(col.window_spec, source_table_obj)
                    select_parts.append(f"DENSE_RANK() OVER ({window_sql})")
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                else:
                    # Generic window function - handle parameters
                    window_sql = self._window_spec_to_sql(col.window_spec, source_table_obj)

                    # Build function call with parameters
                    # Get parameters from the original function stored in the MockWindowFunction
                    original_function = getattr(col, "function", None)
                    if (
                        original_function
                        and hasattr(original_function, "value")
                        and original_function.value is not None
                    ):
                        # Handle parameters for functions like NTH_VALUE, LAG, LEAD, etc.
                        if isinstance(original_function.value, tuple):
                            # Special handling for LAG and LEAD which need column name + tuple params
                            if col.function_name in ["lag", "lead"]:
                                # Get the column name from the original function
                                column_name = getattr(
                                    getattr(original_function, "column", None), "name", "unknown"
                                )
                                # Extract offset and default_value from tuple
                                params = [f'"{column_name}"']
                                for param in original_function.value:
                                    if param is not None:
                                        if isinstance(param, str):
                                            params.append(f"'{param}'")
                                        else:
                                            params.append(str(param))
                                param_str = ", ".join(params)
                                select_parts.append(
                                    f"{col.function_name.upper()}({param_str}) OVER ({window_sql})"
                                )
                            else:
                                # Extract parameters from tuple for other functions
                                params = []
                                for param in original_function.value:
                                    if param is not None:
                                        if isinstance(param, str):
                                            params.append(f"'{param}'")
                                        else:
                                            params.append(str(param))
                                param_str = ", ".join(params)
                                select_parts.append(
                                    f"{col.function_name.upper()}({param_str}) OVER ({window_sql})"
                                )
                        else:
                            # For functions like NTH_VALUE, we need both the column and the value
                            if col.function_name in ["nth_value"]:
                                # Extract column name from the original function's column attribute
                                column_name = getattr(
                                    getattr(original_function, "column", None), "name", "unknown"
                                )
                                param_str = f'"{column_name}", {original_function.value}'
                                select_parts.append(
                                    f"{col.function_name.upper()}({param_str}) OVER ({window_sql})"
                                )
                            else:
                                # Single parameter for other functions
                                if isinstance(original_function.value, str):
                                    select_parts.append(
                                        f"{col.function_name.upper()}('{original_function.value}') OVER ({window_sql})"
                                    )
                                else:
                                    select_parts.append(
                                        f"{col.function_name.upper()}({original_function.value}) OVER ({window_sql})"
                                    )
                    else:
                        # No parameters in value, but check if there's a column (for aggregate functions)
                        # Some functions like CUME_DIST, PERCENT_RANK, RANK, DENSE_RANK don't take parameters
                        if col.function_name in ["cume_dist", "percent_rank", "rank", "dense_rank"]:
                            # These functions don't take parameters
                            select_parts.append(
                                f"{col.function_name.upper()}() OVER ({window_sql})"
                            )
                        elif (
                            original_function
                            and hasattr(original_function, "column")
                            and original_function.column
                        ):
                            column_name = getattr(original_function.column, "name", "unknown")
                            # Check if column exists in table before adding to SQL
                            if column_name != "unknown" and column_name in source_table_obj.c:
                                select_parts.append(
                                    f'{col.function_name.upper()}("{column_name}") OVER ({window_sql})'
                                )
                            else:
                                # Column doesn't exist, skip this window function or use placeholder
                                # Add NULL as placeholder to maintain column position
                                select_parts.append(f"NULL AS {col.name}")
                                new_columns.append(Column(col.name, String, primary_key=False))
                                continue
                        else:
                            # Truly no parameters
                            select_parts.append(
                                f"{col.function_name.upper()}() OVER ({window_sql})"
                            )

                    new_columns.append(Column(col.name, Integer, primary_key=False))
            elif (
                hasattr(col, "operation")
                and hasattr(col, "column")
                and hasattr(col, "function_name")
            ):
                # Handle MockColumnOperation objects with function operations like F.upper()
                # Note: These need AS alias clause when col.name differs from column.name
                if col.function_name == "upper":
                    select_parts.append(f"UPPER({col.column.name}) AS {col.name}")
                elif col.function_name == "lower":
                    select_parts.append(f"LOWER({col.column.name}) AS {col.name}")
                elif col.function_name == "length":
                    select_parts.append(f"LENGTH({col.column.name}) AS {col.name}")
                elif col.function_name == "abs":
                    select_parts.append(f"ABS({col.column.name}) AS {col.name}")
                elif col.function_name == "round":
                    # For round function, check if there's a precision parameter
                    if hasattr(col, "value") and col.value is not None:
                        select_parts.append(f"ROUND({col.column.name}, {col.value}) AS {col.name}")
                    else:
                        select_parts.append(f"ROUND({col.column.name}) AS {col.name}")
                elif col.function_name == "ceil":
                    select_parts.append(f"CEIL({col.column.name}) AS {col.name}")
                elif col.function_name == "floor":
                    select_parts.append(f"FLOOR({col.column.name}) AS {col.name}")
                elif col.function_name == "sqrt":
                    select_parts.append(f"SQRT({col.column.name}) AS {col.name}")
                elif col.function_name == "months_between":
                    # For months_between, we need both column names
                    column1_name = col.column.name if hasattr(col.column, "name") else col.column
                    column2_name = col.value.name if hasattr(col.value, "name") else col.value
                    select_parts.append(
                        f"MONTHS_BETWEEN({column1_name}, {column2_name}) AS {col.name}"
                    )
                elif col.function_name == "split":
                    # For split, use DuckDB's string_split or str_split function
                    delimiter = col.value if isinstance(col.value, str) else str(col.value)
                    select_parts.append(
                        f"STRING_SPLIT({col.column.name}, '{delimiter}') AS {col.name}"
                    )
                else:
                    # Fallback to raw SQL for unknown functions
                    select_parts.append(f"{col.function_name}({col.column.name}) AS {col.name}")

                # Infer column type based on function
                if col.function_name in ["length", "abs", "ceil", "floor"]:
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif col.function_name in ["round", "sqrt", "months_between"]:
                    new_columns.append(Column(col.name, Float, primary_key=False))
                elif col.function_name == "split":
                    # split returns an array type - use String with JSON for now (DuckDB arrays)
                    # We'll mark it but DuckDB will handle the array natively
                    from sqlalchemy import ARRAY

                    try:
                        new_columns.append(Column(col.name, ARRAY(String), primary_key=False))
                    except:  # noqa: E722
                        # Fallback to String if ARRAY not supported
                        new_columns.append(Column(col.name, String, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            elif hasattr(col, "value") and hasattr(col, "data_type"):
                # Handle MockLiteral objects (literal values)
                if isinstance(col.value, str):
                    select_parts.append(f"'{col.value}'")
                else:
                    select_parts.append(str(col.value))
                # Use appropriate column type based on the literal value
                if isinstance(col.value, int):
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif isinstance(col.value, float):
                    new_columns.append(Column(col.name, Float, primary_key=False))
                elif isinstance(col.value, str):
                    new_columns.append(Column(col.name, String, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            elif (
                hasattr(col, "function_name")
                and hasattr(col, "column")
                and col.__class__.__name__ == "MockAggregateFunction"
            ):
                # Handle MockAggregateFunction objects like F.count(), F.sum(), etc.
                if col.function_name == "count":
                    if col.column is None or col.column == "*":
                        select_parts.append("COUNT(*)")
                    else:
                        column_name = col.column.name if hasattr(col.column, "name") else col.column
                        select_parts.append(f"COUNT({column_name})")
                elif col.function_name == "countDistinct":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"COUNT(DISTINCT {column_name})")
                elif col.function_name == "percentile_approx":
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    # DuckDB doesn't have percentile functions, use AVG as approximation
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "corr":
                    # CORR function requires two columns, but we only have one
                    # This is a limitation - we'll use AVG as fallback
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "covar_samp":
                    # COVAR_SAMP function requires two columns, but we only have one
                    # This is a limitation - we'll use AVG as fallback
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    select_parts.append(f"AVG({column_name})")
                elif col.function_name == "sum":
                    select_parts.append(f"SUM({col.column.name})")
                elif col.function_name == "avg":
                    select_parts.append(f"AVG({col.column.name})")
                elif col.function_name == "max":
                    select_parts.append(f"MAX({col.column.name})")
                elif col.function_name == "min":
                    select_parts.append(f"MIN({col.column.name})")
                else:
                    # Fallback for unknown aggregate functions
                    select_parts.append(
                        f"{col.function_name.upper()}({col.column.name if col.column else '*'})"
                    )

                # Add column with appropriate type
                if col.function_name == "count":
                    new_columns.append(Column(col.name, Integer, primary_key=False))
                elif col.function_name == "sum":
                    # Preserve source column type for SUM
                    column_name = col.column.name if hasattr(col.column, "name") else col.column
                    if column_name in source_table_obj.c:
                        source_type = str(source_table_obj.c[column_name].type).upper()
                        # Use Integer for integer types, Float for floating types
                        if any(
                            int_type in source_type
                            for int_type in ["INTEGER", "BIGINT", "SMALLINT", "INT"]
                        ):
                            new_columns.append(Column(col.name, Integer, primary_key=False))
                        else:
                            new_columns.append(Column(col.name, Float, primary_key=False))
                    else:
                        new_columns.append(Column(col.name, Float, primary_key=False))
                elif col.function_name == "avg":
                    new_columns.append(Column(col.name, Float, primary_key=False))
                else:
                    new_columns.append(Column(col.name, String, primary_key=False))
            else:
                pass

        # Ensure we have at least one column
        if not new_columns:
            # Add a placeholder column to avoid "Table must have at least one column!" error
            new_columns = [Column("placeholder", String, primary_key=False)]
            select_parts = ["'placeholder' as placeholder"]

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Build and execute raw SQL
        select_clause = ", ".join(select_parts)
        sql = f"""
        INSERT INTO {target_table}
        SELECT {select_clause}
        FROM {source_table}
        """

        with Session(self.engine) as session:
            session.execute(text(sql))
            session.commit()

    def _apply_with_column(
        self, source_table: str, target_table: str, col_name: str, col: Any
    ) -> None:
        """Apply a withColumn operation."""
        source_table_obj = self._created_tables[source_table]

        # Copy existing columns and add new column
        new_columns: List[Any] = []

        # Copy all existing columns
        for column in source_table_obj.columns:
            new_columns.append(Column(column.name, column.type, primary_key=False))

        # Add new computed column - determine type based on operation
        if hasattr(col, "function_name") and hasattr(col, "window_spec"):
            new_columns.append(Column(col_name, Integer, primary_key=False))
        elif hasattr(col, "operation") and hasattr(col, "column") and hasattr(col, "value"):
            # Handle arithmetic operations - preserve source column type
            if hasattr(col, "function_name") and col.function_name in ["+", "-", "*", "/", "%"]:
                if col.function_name == "/":
                    # Division always returns floating-point type
                    new_columns.append(Column(col_name, Float, primary_key=False))
                elif hasattr(col.column, "name") and (
                    not hasattr(col.column, "operation") or col.column.operation is None
                ):
                    # Simple column reference - preserve its type
                    source_column = source_table_obj.c[col.column.name]
                    new_columns.append(Column(col_name, source_column.type, primary_key=False))
                else:
                    # Complex expression or nested operation - use Float for safety
                    new_columns.append(Column(col_name, Float, primary_key=False))
            else:
                new_columns.append(Column(col_name, String, primary_key=False))
        else:
            new_columns.append(Column(col_name, String, primary_key=False))

        # Handle window functions
        if hasattr(col, "function_name") and hasattr(col, "window_spec"):
            # For window functions, we need to use raw SQL
            self._apply_window_function(source_table, target_table, col_name, col, new_columns)
            return

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # For now, use raw SQL for complex expressions
        self._apply_with_column_sql(source_table, target_table, col_name, col)

    def _apply_window_function(
        self,
        source_table: str,
        target_table: str,
        col_name: str,
        window_func: Any,
        new_columns: List[Column],
    ) -> None:
        """Apply a window function using raw SQL."""
        source_table_obj = self._created_tables[source_table]

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Build window function SQL
        window_sql = self._window_spec_to_sql(window_func.window_spec, source_table_obj)
        func_name = window_func.function_name.upper()

        # Generate function call based on type
        if func_name in ["ROW_NUMBER", "RANK", "DENSE_RANK", "CUME_DIST", "PERCENT_RANK"]:
            # These functions don't take parameters
            func_call = f"{func_name}() OVER ({window_sql})"
        else:
            # Get column from the original function if it exists
            original_function = getattr(window_func, "function", None)
            if (
                original_function
                and hasattr(original_function, "column")
                and original_function.column
            ):
                column_name = getattr(original_function.column, "name", "unknown")
                func_call = f'{func_name}("{column_name}") OVER ({window_sql})'
            else:
                func_call = f"{func_name}() OVER ({window_sql})"

        # Select all existing columns plus the window function result
        existing_cols = ", ".join([f'"{c.name}"' for c in source_table_obj.columns])

        sql = f"""
        INSERT INTO {target_table}
        SELECT {existing_cols}, {func_call} as {col_name}
        FROM {source_table}
        """

        # Execute SQL
        with Session(self.engine) as session:
            session.execute(text(sql))
            session.commit()

    def _apply_with_column_sql(
        self, source_table: str, target_table: str, col_name: str, col: Any
    ) -> None:
        """Apply withColumn using SQLAlchemy expressions for arithmetic operations."""
        # Get all existing columns from source
        source_table_obj = self._created_tables[source_table]
        existing_columns = [col.name for col in source_table_obj.columns]

        # Build the select statement using SQLAlchemy expressions
        select_columns = []
        for col_name_existing in existing_columns:
            select_columns.append(source_table_obj.c[col_name_existing])

        # Handle the new column expression using SQLAlchemy
        if hasattr(col, "operation") and hasattr(col, "column") and hasattr(col, "value"):
            # Handle arithmetic operations like MockColumnOperation
            # Check if left operand is a simple column or nested expression
            if hasattr(col.column, "name") and (
                not hasattr(col.column, "operation") or col.column.operation is None
            ):
                # Simple column
                left_col = source_table_obj.c[col.column.name]
            elif hasattr(col.column, "operation"):
                # Nested operation - convert to SQL first, then wrap in literal_column
                from sqlalchemy import literal_column

                nested_sql = self._expression_to_sql(col.column)
                left_col = literal_column(nested_sql)
            else:
                # Fallback
                left_col = source_table_obj.c[str(col.column)]

            # Convert right operand - handle MockColumn, MockLiteral, and raw values
            if hasattr(col.value, "name") and (
                not hasattr(col.value, "operation") or col.value.operation is None
            ):
                # MockColumn - convert to SQLAlchemy column reference
                right_val = source_table_obj.c[col.value.name]
            elif hasattr(col.value, "value"):
                # MockLiteral - extract the value
                right_val = col.value.value
            else:
                # Raw value (int, float, str)
                right_val = col.value

            if col.operation == "*":
                new_expr = left_col * right_val
            elif col.operation == "+":
                new_expr = left_col + right_val
            elif col.operation == "-":
                new_expr = left_col - right_val
            elif col.operation == "/":
                new_expr = left_col / right_val
            elif col.operation == "&":
                # Logical AND - recursively process both sides
                left_expr = self._expression_to_sqlalchemy(col.column, source_table_obj)
                right_expr = self._expression_to_sqlalchemy(col.value, source_table_obj)
                new_expr = and_(left_expr, right_expr)
            elif col.operation == "|":
                # Logical OR - recursively process both sides
                left_expr = self._expression_to_sqlalchemy(col.column, source_table_obj)
                right_expr = self._expression_to_sqlalchemy(col.value, source_table_obj)
                new_expr = or_(left_expr, right_expr)
            # Handle datetime functions (unary - value is None)
            elif col.value is None and col.operation in [
                "to_date",
                "to_timestamp",
                "hour",
                "minute",
                "second",
                "year",
                "month",
                "day",
                "dayofmonth",
                "dayofweek",
                "dayofyear",
                "weekofyear",
                "quarter",
            ]:
                datetime_sql = self._expression_to_sql(col)
                from sqlalchemy import literal_column

                new_expr = literal_column(datetime_sql)
            else:
                # Fallback to raw SQL for other operations
                new_expr = text(f"({left_col.name} {col.operation} {right_val})")

            # Safe label application - use .label() if available, otherwise use literal_column
            try:
                select_columns.append(new_expr.label(col_name))
            except (NotImplementedError, AttributeError):
                # For expressions that don't support .label(), use literal_column
                from sqlalchemy import literal_column

                select_columns.append(literal_column(str(new_expr)).label(col_name))
        else:
            # Fallback to raw SQL for other expressions
            from sqlalchemy import literal_column

            new_col_sql = self._expression_to_sql(col)
            # For literals, use literal() instead of text()
            if hasattr(col, "value") and isinstance(col.value, str):
                select_columns.append(literal(col.value).label(col_name))
            else:
                select_columns.append(literal_column(new_col_sql).label(col_name))

        # Create the select statement
        select_stmt = select(*select_columns).select_from(source_table_obj)

        # Create the target table with the new column
        new_columns: List[Any] = []
        for col_name_existing in existing_columns:
            col_type = source_table_obj.c[col_name_existing].type
            new_columns.append(Column(col_name_existing, col_type, primary_key=False))

        # Add the new column with appropriate type
        if hasattr(col, "operation") and hasattr(col, "column"):
            # Determine type based on operation
            if col.operation in ["to_date"]:
                from sqlalchemy import Date

                new_columns.append(Column(col_name, Date, primary_key=False))
            elif col.operation in ["to_timestamp", "current_timestamp"]:
                from sqlalchemy import DateTime

                new_columns.append(Column(col_name, DateTime, primary_key=False))
            elif col.operation in [
                "hour",
                "minute",
                "second",
                "year",
                "month",
                "day",
                "dayofmonth",
                "dayofweek",
                "dayofyear",
                "weekofyear",
                "quarter",
            ]:
                # All datetime component extractions return integers
                new_columns.append(Column(col_name, Integer, primary_key=False))
            elif hasattr(col, "value") and col.value is not None:
                # For arithmetic operations with a value, use Float type
                new_columns.append(Column(col_name, Float, primary_key=False))
            else:
                # Default to String for unknown operations
                new_columns.append(Column(col_name, String, primary_key=False))
        else:
            new_columns.append(Column(col_name, String, primary_key=False))

        target_table_obj = Table(target_table, self.metadata, *new_columns, extend_existing=True)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Execute the insert
        with self.engine.connect() as conn:
            conn.execute(
                insert(target_table_obj).from_select([c.name for c in new_columns], select_stmt)
            )
            conn.commit()

    def _apply_order_by(
        self, source_table: str, target_table: str, columns: Tuple[Any, ...]
    ) -> None:
        """Apply an orderBy operation using SQLAlchemy expressions."""
        source_table_obj = self._created_tables[source_table]

        # Copy table structure
        self._copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

        # Build SQLAlchemy order by expressions
        order_expressions = []
        [c.name for c in source_table_obj.columns]
        # print(f"DEBUG: Available columns in {source_table}: {available_columns}")
        # print(f"DEBUG: Order by columns: {[col.name if hasattr(col, 'name') else str(col) for col in columns]}")

        for col in columns:
            if isinstance(col, str):
                order_expressions.append(source_table_obj.c[col])
            elif hasattr(col, "operation") and col.operation == "desc":
                order_expressions.append(desc(source_table_obj.c[col.column.name]))
            elif hasattr(col, "operation") and col.operation == "asc":
                order_expressions.append(asc(source_table_obj.c[col.column.name]))
            elif hasattr(col, "name"):
                # Handle MockColumn objects - check if they have a desc or asc operation
                if hasattr(col, "operation") and col.operation == "desc":
                    order_expressions.append(desc(source_table_obj.c[col.name]))
                elif hasattr(col, "operation") and col.operation == "asc":
                    order_expressions.append(asc(source_table_obj.c[col.name]))
                else:
                    # Default to ascending order
                    order_expressions.append(asc(source_table_obj.c[col.name]))
            else:
                # Fallback: try to convert to string
                order_expressions.append(source_table_obj.c[str(col)])

        # Execute with ORDER BY using SQLAlchemy
        with Session(self.engine) as session:
            query = select(*source_table_obj.columns).order_by(*order_expressions)
            results: List[Any] = list(session.execute(query).all())

            # Insert into target table
            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def _apply_limit(self, source_table: str, target_table: str, limit_count: int) -> None:
        """Apply a limit operation using SQLAlchemy expressions."""
        source_table_obj = self._created_tables[source_table]

        # Copy table structure
        self._copy_table_structure(source_table, target_table)
        target_table_obj = self._created_tables[target_table]

        # Execute with LIMIT using SQLAlchemy
        with Session(self.engine) as session:
            query = select(*source_table_obj.columns).limit(limit_count)
            results: List[Any] = list(session.execute(query).all())

            # Insert into target table
            for result in results:
                # Convert result to dict using column names
                result_dict = {}
                for i, column in enumerate(source_table_obj.columns):
                    result_dict[column.name] = result[i]
                insert_stmt = target_table_obj.insert().values(result_dict)
                session.execute(insert_stmt)
            session.commit()

    def _build_case_when_sql(self, case_when_obj: Any, source_table_obj: Any) -> str:
        """Build SQL CASE WHEN expression from MockCaseWhen object."""
        sql_parts = ["CASE"]

        # Add WHEN conditions
        for condition, value in case_when_obj.conditions:
            # Convert condition to SQL
            condition_sql = self._condition_to_sql(condition, source_table_obj)
            # Convert value to SQL
            value_sql = self._value_to_sql(value)
            sql_parts.append(f"WHEN {condition_sql} THEN {value_sql}")

        # Add ELSE clause if default_value is set
        if case_when_obj.default_value is not None:
            else_sql = self._value_to_sql(case_when_obj.default_value)
            sql_parts.append(f"ELSE {else_sql}")

        sql_parts.append("END")
        return " ".join(sql_parts)

    def _condition_to_sql(self, condition: Any, source_table_obj: Any) -> str:
        """Convert a condition to SQL."""
        if hasattr(condition, "column") and hasattr(condition, "function_name"):
            # Handle column operations like F.col("age") > 30
            column_name = condition.column.name
            value = condition.value

            # Convert value to SQL
            value_sql = self._value_to_sql(value)

            if condition.function_name == ">" or condition.function_name == "gt":
                return f'"{column_name}" > {value_sql}'
            elif condition.function_name == "<" or condition.function_name == "lt":
                return f'"{column_name}" < {value_sql}'
            elif condition.function_name == "==" or condition.function_name == "eq":
                return f'"{column_name}" = {value_sql}'
            elif condition.function_name == "!=" or condition.function_name == "ne":
                return f'"{column_name}" != {value_sql}'
            elif condition.function_name == ">=" or condition.function_name == "ge":
                return f'"{column_name}" >= {value_sql}'
            elif condition.function_name == "<=" or condition.function_name == "le":
                return f'"{column_name}" <= {value_sql}'
        return str(condition)

    def _value_to_sql(self, value: Any) -> str:
        """Convert a value to SQL."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, (int, float)):
            return str(value)
        elif hasattr(value, "operation") and hasattr(value, "column") and hasattr(value, "value"):
            # Handle arithmetic/comparison operations (MockColumnOperation)
            return self._expression_to_sql(value)
        elif hasattr(value, "name") and not hasattr(value, "operation"):
            # Handle MockColumn - convert to SQL column reference
            return self._column_to_sql(value)
        elif hasattr(value, "value") and hasattr(value, "data_type"):
            # Handle MockLiteral
            if value.value is None:
                return "NULL"
            elif isinstance(value.value, str):
                return f"'{value.value}'"
            else:
                return str(value.value)
        elif hasattr(value, "name"):
            # Handle column references (MockColumn)
            return f'"{value.name}"'
        else:
            return str(value)

    def _copy_table_structure(self, source_table: str, target_table: str) -> None:
        """Copy table structure from source to target."""
        source_table_obj = self._created_tables[source_table]

        # Copy all columns from source table
        new_columns: List[Any] = []
        for column in source_table_obj.columns:
            new_columns.append(Column(column.name, column.type, primary_key=False))

        # print(f"DEBUG: Copying table structure from {source_table} to {target_table}")
        # print(f"DEBUG: Source columns: {[c.name for c in source_table_obj.columns]}")
        # print(f"DEBUG: Target columns: {[c.name for c in new_columns]}")

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

    def _get_table_results(self, table_name: str) -> List[MockRow]:
        """Get all results from a table as MockRow objects."""
        table_obj = self._created_tables[table_name]

        with Session(self.engine) as session:
            # Build raw SQL query
            # Escape double quotes in column names by doubling them
            column_names = [
                f'"{col.name.replace(chr(34), chr(34)+chr(34))}"' for col in table_obj.columns
            ]
            sql = f"SELECT {', '.join(column_names)} FROM {table_name}"
            results = session.execute(text(sql)).all()

            mock_rows = []
            for result in results:
                # Convert result to dict using column names with type conversion
                result_dict: Dict[str, Any] = {}
                for i, column in enumerate(table_obj.columns):
                    value = result[i]
                    # Convert value to appropriate type based on column type
                    from sqlalchemy import ARRAY
                    # Check for ARRAY type - need to check type name too since ARRAY is complex
                    is_array_column = (isinstance(column.type, ARRAY) or 
                                      type(column.type).__name__ == 'ARRAY')
                    # DEBUG
                    # print(f"DEBUG _get_table_results: column={column.name}, type={type(column.type).__name__}, is_array={is_array_column}, value_type={type(value)}, value={value}")
                    if is_array_column and value is not None:
                        # Array columns might be returned as lists or strings
                        if isinstance(value, list):
                            result_dict[column.name] = value
                        elif isinstance(value, str):
                            # Parse string representation back to list
                            # DuckDB sometimes returns arrays as strings like "[1, 2, 3]"
                            try:
                                import ast
                                result_dict[column.name] = ast.literal_eval(value)
                            except (ValueError, SyntaxError):
                                result_dict[column.name] = value
                        else:
                            result_dict[column.name] = value
                    elif isinstance(column.type, String) and isinstance(value, str) and value.startswith('{') and value.endswith('}'):
                        # Map columns returned as strings like "{a=1, b=2}" - parse to dict
                        try:
                            import ast
                            # Try to parse as dict literal
                            result_dict[column.name] = ast.literal_eval(value)
                        except (ValueError, SyntaxError):
                            # If that fails, leave as string
                            result_dict[column.name] = value
                    elif isinstance(value, dict):
                        # Already a dict (map)
                        result_dict[column.name] = value
                    elif isinstance(column.type, Integer) and value is not None:
                        try:
                            result_dict[column.name] = int(value)
                        except (ValueError, TypeError):
                            result_dict[column.name] = value
                    elif isinstance(column.type, Float) and value is not None:
                        try:
                            result_dict[column.name] = float(value)
                        except (ValueError, TypeError):
                            result_dict[column.name] = value
                    elif isinstance(column.type, Boolean) and value is not None:
                        if isinstance(value, str):
                            result_dict[column.name] = value.lower() in ("true", "1", "yes", "on")
                        else:
                            result_dict[column.name] = bool(value)
                    else:
                        result_dict[column.name] = value
                mock_rows.append(MockRow(result_dict))

            return mock_rows

    def _condition_to_sqlalchemy(self, table_obj: Any, condition: Any) -> Any:
        """Convert a condition to SQLAlchemy expression."""
        if isinstance(condition, MockColumnOperation):
            if hasattr(condition, "operation") and hasattr(condition, "column"):
                left = self._column_to_sqlalchemy(table_obj, condition.column)
                right = self._value_to_sqlalchemy(condition.value)

                if condition.operation == "==":
                    return left == right
                elif condition.operation == "!=":
                    return left != right
                elif condition.operation == ">":
                    return left > right
                elif condition.operation == "<":
                    return left < right
                elif condition.operation == ">=":
                    return left >= right
                elif condition.operation == "<=":
                    return left <= right
                elif condition.operation == "&":
                    # Logical AND operation
                    left_expr = self._condition_to_sqlalchemy(table_obj, condition.column)
                    right_expr = self._condition_to_sqlalchemy(table_obj, condition.value)
                    return and_(left_expr, right_expr)
                elif condition.operation == "|":
                    # Logical OR operation
                    left_expr = self._condition_to_sqlalchemy(table_obj, condition.column)
                    right_expr = self._condition_to_sqlalchemy(table_obj, condition.value)
                    return or_(left_expr, right_expr)
                elif condition.operation == "!":
                    # Logical NOT operation
                    expr = self._condition_to_sqlalchemy(table_obj, condition.column)
                    return ~expr
                elif condition.operation == "isnull":
                    # IS NULL operation
                    left = self._column_to_sqlalchemy(table_obj, condition.column)
                    return left.is_(None)
                elif condition.operation == "isnotnull":
                    # IS NOT NULL operation
                    left = self._column_to_sqlalchemy(table_obj, condition.column)
                    return left.isnot(None)
        elif isinstance(condition, MockColumn):
            return table_obj.c[condition.name]

        return None  # Fallback

    def _column_to_sqlalchemy(self, table_obj: Any, column: Any) -> Any:
        """Convert a MockColumn to SQLAlchemy expression."""
        if isinstance(column, MockColumn):
            column_name = column.name
        elif isinstance(column, str):
            column_name = column
        else:
            return column

        # Validate column exists
        if column_name not in table_obj.c:
            # Only raise errors if we're in strict validation mode (e.g., filters)
            # Window functions and other operations handle missing columns differently
            if getattr(self, "_strict_column_validation", False):
                from mock_spark.core.exceptions import AnalysisException

                available_columns = list(table_obj.c.keys())
                raise AnalysisException(
                    f"Column '{column_name}' not found. Available columns: {available_columns}"
                )
            else:
                # For window functions and other contexts, return literal False
                return literal(False)

        return table_obj.c[column_name]

    def _expression_to_sqlalchemy(self, expr: Any, table_obj: Any) -> Any:
        """Convert a complex expression (including AND/OR) to SQLAlchemy."""
        if isinstance(expr, MockColumnOperation):
            # Recursively process left and right sides
            if hasattr(expr, "column"):
                left = self._expression_to_sqlalchemy(expr.column, table_obj)
            else:
                left = None

            if hasattr(expr, "value") and expr.value is not None:
                if isinstance(expr.value, (MockColumn, MockColumnOperation)):
                    right = self._expression_to_sqlalchemy(expr.value, table_obj)
                elif isinstance(expr.value, MockLiteral):
                    right = expr.value.value
                else:
                    right = expr.value
            else:
                right = None

            # Apply operation
            if expr.operation == ">":
                return left > right
            elif expr.operation == "<":
                return left < right
            elif expr.operation == ">=":
                return left >= right
            elif expr.operation == "<=":
                return left <= right
            elif expr.operation == "==":
                return left == right
            elif expr.operation == "!=":
                return left != right
            elif expr.operation == "&":
                return and_(left, right)
            elif expr.operation == "|":
                return or_(left, right)
            elif expr.operation == "!":
                return ~left
            else:
                # Fallback
                return table_obj.c[str(expr)]
        elif isinstance(expr, MockColumn):
            return table_obj.c[expr.name]
        elif isinstance(expr, MockLiteral):
            return expr.value
        else:
            # Literal value
            return expr

    def _value_to_sqlalchemy(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy expression."""
        if isinstance(value, MockLiteral):
            return value.value
        elif isinstance(value, MockColumn):
            # This would need the table context, but for now return the name
            return value.name
        return value

    def _column_to_orm(self, table_class: Any, column: Any) -> Any:
        """Convert a MockColumn to SQLAlchemy ORM expression."""
        if isinstance(column, MockColumn):
            return getattr(table_class, column.name)
        elif isinstance(column, str):
            return getattr(table_class, column)
        return column

    def _value_to_orm(self, value: Any) -> Any:
        """Convert a value to SQLAlchemy ORM expression."""
        if isinstance(value, MockLiteral):
            return value.value
        elif isinstance(value, MockColumn):
            # This would need the table class context, but for now return the name
            return value.name
        return value

    def _window_function_to_orm(self, table_class: Any, window_func: Any) -> Any:
        """Convert a window function to SQLAlchemy ORM expression."""
        function_name = getattr(window_func, "function_name", "window_function")

        # Get window specification
        window_spec = window_func.window_spec

        # Build partition_by and order_by
        partition_by = []
        order_by = []

        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            for col in window_spec._partition_by:
                if isinstance(col, str):
                    partition_by.append(getattr(table_class, col))
                elif hasattr(col, "name"):
                    partition_by.append(getattr(table_class, col.name))

        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            for col in window_spec._order_by:
                if isinstance(col, str):
                    order_by.append(getattr(table_class, col))
                elif hasattr(col, "operation") and col.operation == "desc":
                    order_by.append(desc(getattr(table_class, col.column.name)))
                elif hasattr(col, "name"):
                    order_by.append(getattr(table_class, col.name))

        # Build window expression
        if function_name == "rank":
            return func.rank().over(partition_by=partition_by, order_by=order_by)
        elif function_name == "row_number":
            return func.row_number().over(partition_by=partition_by, order_by=order_by)
        elif function_name == "dense_rank":
            return func.dense_rank().over(partition_by=partition_by, order_by=order_by)
        else:
            # Generic window function
            return getattr(func, function_name)().over(partition_by=partition_by, order_by=order_by)

    def _window_spec_to_sql(self, window_spec: Any, table_obj: Any = None) -> str:
        """Convert window specification to SQL."""
        parts = []

        # Get available columns if table_obj provided
        available_columns = set(table_obj.c.keys()) if table_obj is not None else None

        # Handle PARTITION BY
        if hasattr(window_spec, "_partition_by") and window_spec._partition_by:
            partition_cols = []
            for col in window_spec._partition_by:
                col_name = None
                if isinstance(col, str):
                    col_name = col
                elif hasattr(col, "name"):
                    col_name = col.name

                # Validate column exists if available_columns is set
                if available_columns is not None and col_name and col_name not in available_columns:
                    continue  # Skip non-existent columns

                if col_name:
                    partition_cols.append(f'"{col_name}"')

            if partition_cols:
                parts.append(f"PARTITION BY {', '.join(partition_cols)}")

        # Handle ORDER BY
        if hasattr(window_spec, "_order_by") and window_spec._order_by:
            order_cols = []
            for col in window_spec._order_by:
                col_name = None
                is_desc = False

                if isinstance(col, str):
                    col_name = col
                elif isinstance(col, MockColumnOperation):
                    if hasattr(col, "operation") and col.operation == "desc":
                        col_name = col.column.name
                        is_desc = True
                    else:
                        col_name = col.column.name
                elif hasattr(col, "name"):
                    col_name = col.name

                # Validate column exists if available_columns is set
                if available_columns is not None and col_name and col_name not in available_columns:
                    continue  # Skip non-existent columns

                if col_name:
                    if is_desc:
                        order_cols.append(f'"{col_name}" DESC')
                    else:
                        order_cols.append(f'"{col_name}"')

            if order_cols:
                parts.append(f"ORDER BY {', '.join(order_cols)}")

        # Handle ROWS BETWEEN
        if hasattr(window_spec, "_rows_between") and window_spec._rows_between:
            start, end = window_spec._rows_between
            # Convert to SQL ROWS BETWEEN syntax
            # Negative values are PRECEDING, positive are FOLLOWING
            if start == 0:
                start_clause = "CURRENT ROW"
            elif start < 0:
                start_clause = f"{abs(start)} PRECEDING"
            else:
                start_clause = f"{start} FOLLOWING"

            if end == 0:
                end_clause = "CURRENT ROW"
            elif end < 0:
                end_clause = f"{abs(end)} PRECEDING"
            else:
                end_clause = f"{end} FOLLOWING"

            parts.append(f"ROWS BETWEEN {start_clause} AND {end_clause}")

        # Handle RANGE BETWEEN
        if hasattr(window_spec, "_range_between") and window_spec._range_between:
            start, end = window_spec._range_between
            # Convert to SQL RANGE BETWEEN syntax
            if start == 0:
                start_clause = "CURRENT ROW"
            elif start < 0:
                start_clause = f"{abs(start)} PRECEDING"
            else:
                start_clause = f"{start} FOLLOWING"

            if end == 0:
                end_clause = "CURRENT ROW"
            elif end < 0:
                end_clause = f"{abs(end)} PRECEDING"
            else:
                end_clause = f"{end} FOLLOWING"

            parts.append(f"RANGE BETWEEN {start_clause} AND {end_clause}")

        return " ".join(parts)

    def _apply_join(
        self, source_table: str, target_table: str, join_params: Tuple[Any, ...]
    ) -> None:
        """Apply a join operation."""
        other_df, on, how = join_params

        source_table_obj = self._created_tables[source_table]

        # Materialize the other DataFrame to get its data
        other_materialized = other_df._materialize_if_lazy() if other_df.is_lazy else other_df
        other_data = other_materialized.data
        other_schema = other_materialized.schema

        # Normalize 'on' parameter to list
        if isinstance(on, str):
            on_columns = [on]
        elif isinstance(on, list):
            on_columns = on
        else:
            on_columns = [on]

        # Create target table with combined schema
        new_columns: List[Any] = []

        # Add all columns from source table
        for column in source_table_obj.columns:
            new_columns.append(Column(column.name, column.type, primary_key=False))

        # Add columns from other DataFrame (except join keys already in source)
        for field in other_schema.fields:
            if field.name not in on_columns and field.name not in [
                c.name for c in source_table_obj.columns
            ]:
                # Convert MockSpark types to SQLAlchemy types
                sql_type: Any = String  # Default, can be Integer, Float, or other types
                field_type_name = type(field.dataType).__name__
                if field_type_name in ["LongType", "IntegerType"]:
                    sql_type = Integer
                elif field_type_name in ["DoubleType", "FloatType"]:
                    sql_type = Float
                new_columns.append(Column(field.name, sql_type, primary_key=False))

        # Create target table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Perform the actual join operation
        with Session(self.engine) as session:
            # Get source data
            source_data = session.execute(select(*source_table_obj.columns)).all()

            # Create a lookup dictionary from other_data (key -> list of matching rows)
            other_lookup: Dict[Any, Any] = {}
            for other_row in other_data:
                # Create join key from on_columns
                join_key = tuple(other_row.get(col) for col in on_columns)
                if join_key not in other_lookup:
                    other_lookup[join_key] = []
                other_lookup[join_key].append(other_row)

            # Perform join - create one output row for each matching pair
            for row in source_data:
                row_dict = dict(row._mapping)

                # Create join key from source row
                source_join_key = tuple(row_dict.get(col) for col in on_columns)

                # Look up all matching rows from other DataFrame
                if source_join_key in other_lookup:
                    for other_row in other_lookup[source_join_key]:
                        # Create a new combined row for each match
                        combined_row = row_dict.copy()

                        # Add columns from other DataFrame
                        for field in other_schema.fields:
                            if field.name not in on_columns and field.name not in combined_row:
                                combined_row[field.name] = other_row.get(field.name)

                        # Ensure all target columns have values
                        target_column_names = [col.name for col in target_table_obj.columns]
                        complete_row = {}
                        for col_name in target_column_names:
                            complete_row[col_name] = combined_row.get(col_name, None)

                        # Insert into target table
                        insert_stmt = target_table_obj.insert().values(complete_row)
                        session.execute(insert_stmt)

            session.commit()

    def _apply_union(self, source_table: str, target_table: str, other_df: Any) -> None:
        """Apply a union operation."""
        # Get source table structure
        source_table_obj = self._created_tables[source_table]
        new_columns: List[Any] = []
        for column in source_table_obj.columns:
            new_columns.append(Column(column.name, column.type, primary_key=False))

        # Create target table with same structure
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

        # Combine data from both dataframes
        with Session(self.engine) as session:
            # Get source data
            source_data = session.execute(select(*source_table_obj.columns)).all()
            for row in source_data:
                row_dict = dict(row._mapping)
                insert_stmt = target_table_obj.insert().values(row_dict)
                session.execute(insert_stmt)

            # Get other dataframe data by materializing it
            other_data = other_df.collect()
            for row in other_data:
                row_dict = dict(row.asDict())
                insert_stmt = target_table_obj.insert().values(row_dict)
                session.execute(insert_stmt)

            session.commit()

    def _expression_to_sql(self, expr: Any) -> str:
        """Convert an expression to SQL."""
        if isinstance(expr, str):
            return f'"{expr}"'
        elif hasattr(expr, "conditions") and hasattr(expr, "default_value"):
            # Handle MockCaseWhen objects
            return self._build_case_when_sql(expr, None)
        elif hasattr(expr, "operation") and hasattr(expr, "column") and hasattr(expr, "value"):
            # Handle string/math functions like upper, lower, abs, etc.
            if expr.operation in ["upper", "lower", "length", "trim", "abs", "round"]:
                column_name = self._column_to_sql(expr.column)
                return f"{expr.operation.upper()}({column_name})"

            # Handle unary operations (value is None)
            if expr.value is None:
                left = self._column_to_sql(expr.column)
                if expr.operation == "-":
                    return f"(-{left})"
                elif expr.operation == "+":
                    return f"(+{left})"
                # Handle datetime functions
                elif expr.operation in ["to_date", "to_timestamp"]:
                    # DuckDB: CAST(column AS DATE/TIMESTAMP)
                    target_type = "DATE" if expr.operation == "to_date" else "TIMESTAMP"
                    return f"CAST({left} AS {target_type})"
                elif expr.operation in ["hour", "minute", "second"]:
                    # DuckDB: extract(part from timestamp)
                    return f"extract({expr.operation} from CAST({left} AS TIMESTAMP))"
                elif expr.operation in ["year", "month", "day", "dayofmonth"]:
                    # DuckDB: extract(part from date)
                    part = "day" if expr.operation == "dayofmonth" else expr.operation
                    return f"extract({part} from CAST({left} AS DATE))"
                elif expr.operation in ["dayofweek", "dayofyear", "weekofyear", "quarter"]:
                    # DuckDB date part extraction
                    part_map = {
                        "dayofweek": "dow",
                        "dayofyear": "doy",
                        "weekofyear": "week",
                        "quarter": "quarter",
                    }
                    part = part_map.get(expr.operation, expr.operation)
                    return f"extract({part} from CAST({left} AS DATE))"
                else:
                    # For other unary operations, treat as function
                    return f"{expr.operation.upper()}({left})"

            # Handle arithmetic operations like MockColumnOperation
            # For column references in expressions, don't quote them
            left = self._column_to_sql(expr.column)
            right = self._value_to_sql(expr.value)

            # Handle comparison operations
            if expr.operation == "==":
                # Handle NULL comparisons specially
                if right == "NULL":
                    return f"({left} IS NULL)"
                return f"({left} = {right})"
            elif expr.operation == "!=":
                # Handle NULL comparisons specially
                if right == "NULL":
                    return f"({left} IS NOT NULL)"
                return f"({left} <> {right})"
            elif expr.operation == ">":
                return f"({left} > {right})"
            elif expr.operation == "<":
                return f"({left} < {right})"
            elif expr.operation == ">=":
                return f"({left} >= {right})"
            elif expr.operation == "<=":
                return f"({left} <= {right})"
            # Handle arithmetic operations
            elif expr.operation == "*":
                return f"({left} * {right})"
            elif expr.operation == "+":
                return f"({left} + {right})"
            elif expr.operation == "-":
                return f"({left} - {right})"
            elif expr.operation == "/":
                return f"({left} / {right})"
            else:
                return f"({left} {expr.operation} {right})"
        elif hasattr(expr, "name"):
            return f'"{expr.name}"'
        elif hasattr(expr, "value"):
            # Handle literals
            if isinstance(expr.value, str):
                return f"'{expr.value}'"
            else:
                return str(expr.value)
        else:
            return str(expr)

    def _column_to_sql(self, expr: Any) -> str:
        """Convert a column reference to SQL without quotes for expressions."""
        if isinstance(expr, str):
            return expr
        elif hasattr(expr, "name"):
            return expr.name
        else:
            return str(expr)

    def close(self) -> None:
        """Close the SQLAlchemy engine."""
        try:
            if hasattr(self, "engine") and self.engine:
                self.engine.dispose()
                self.engine = None  # type: ignore[assignment]
        except Exception:
            pass  # Ignore errors during cleanup

    def __del__(self) -> None:
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:  # noqa: E722
            pass

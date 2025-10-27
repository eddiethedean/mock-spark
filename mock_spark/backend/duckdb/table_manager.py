"""
Table management utilities for Mock Spark.

This module provides table lifecycle management for DuckDB operations.
"""

from typing import Any, Dict, List, Optional
from sqlalchemy import (
    Column,
    Integer,
    Float,
    Double,
    Boolean,
    String,
    DateTime,
    ARRAY,
    VARCHAR,
    Table,
    text,
)
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.sql.schema import MetaData as MetaDataType

from ...spark_types import MockRow


class DuckDBTableManager:
    """Manages table lifecycle for DuckDB operations."""

    def __init__(self, engine: Engine, metadata: MetaDataType):
        """Initialize table manager.

        Args:
            engine: SQLAlchemy engine instance
            metadata: SQLAlchemy metadata instance
        """
        self.engine = engine
        self.metadata = metadata
        self._created_tables: Dict[str, Any] = {}

    def create_table_with_data(
        self, table_name: str, data: List[Dict[str, Any]]
    ) -> Table:
        """Create a table and insert data using SQLAlchemy Table.

        Args:
            table_name: Name of the table to create
            data: Data to insert into the table

        Returns:
            Created SQLAlchemy Table object
        """
        if not data:
            # Create a minimal table with at least one column to avoid "Table must have at least one column!" error
            columns = [Column("id", Integer)]
            table = Table(table_name, self.metadata, *columns)
            table.create(self.engine, checkfirst=True)
            self._created_tables[table_name] = table
            return table

        # Create table using SQLAlchemy Table approach
        columns = []
        has_map_columns = False
        map_column_names = []

        if data:
            for key, value in data[0].items():
                if isinstance(value, int):
                    columns.append(Column(key, Integer))
                elif isinstance(value, float):
                    columns.append(Column(key, Float))  # type: ignore[arg-type]
                elif isinstance(value, bool):
                    columns.append(Column(key, Boolean))  # type: ignore[arg-type]
                elif isinstance(value, list):
                    # For arrays, infer element type from first element
                    if value and len(value) > 0:
                        # Infer element type from first non-None element
                        first_elem = next(
                            (elem for elem in value if elem is not None), None
                        )
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
                    columns.append(Column(key, String))  # type: ignore[arg-type] # Placeholder
                else:
                    columns.append(Column(key, String))  # type: ignore[arg-type]

        # Create table - use raw SQL for MAP/ARRAY columns
        has_array_columns = any(
            type(col.type).__name__ == "ArrayType" for col in columns
        )
        if has_map_columns or has_array_columns:
            # Build CREATE TABLE with proper MAP/ARRAY types using raw SQL
            col_defs = []
            for col in columns:
                if col.name in map_column_names:
                    col_defs.append(f'"{col.name}" MAP(VARCHAR, VARCHAR)')
                elif type(col.type).__name__ == "ArrayType":
                    # Determine array element type
                    if hasattr(col.type, "element_type"):
                        elem_type = col.type.element_type
                    else:
                        # Fallback for non-array types
                        col_defs.append(f'"{col.name}" VARCHAR[]')
                        continue
                    if elem_type.__class__.__name__ == "LongType":
                        col_defs.append(f'"{col.name}" INTEGER[]')
                    elif elem_type.__class__.__name__ in ["FloatType", "DoubleType"]:
                        col_defs.append(f'"{col.name}" DOUBLE[]')
                    elif elem_type.__class__.__name__ == "BooleanType":
                        col_defs.append(f'"{col.name}" BOOLEAN[]')
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
                    # Convert Python list to DuckDB ARRAY
                    elif isinstance(value, list):
                        # Convert list to ARRAY syntax: [element1, element2, ...]
                        if value:
                            # Escape string values properly
                            array_elements = []
                            for elem in value:
                                if isinstance(elem, str):
                                    array_elements.append(f"'{elem}'")
                                else:
                                    array_elements.append(str(elem))
                            array_sql = f"[{', '.join(array_elements)}]"
                            insert_values[col.name] = text(array_sql)
                        else:
                            insert_values[col.name] = None
                    else:
                        insert_values[col.name] = value

                # Insert using parameterized values for non-MAP/ARRAY columns
                # and raw SQL for MAP/ARRAY columns
                if any(isinstance(v, type(text(""))) for v in insert_values.values()):
                    # Has MAP columns - use raw SQL
                    col_names = []
                    col_values = []
                    for col_name, col_value in insert_values.items():
                        col_names.append(f'"{col_name}"')
                        if hasattr(col_value, "text"):  # TextClause
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
                    # Normal insert - handle overflow values gracefully
                    try:
                        insert_stmt = table.insert().values(insert_values)
                        session.execute(insert_stmt)
                    except Exception as e:
                        if "out of range" in str(e) or "Conversion Error" in str(e):
                            # Rollback the failed transaction
                            session.rollback()

                            # Handle overflow by inserting NULL for problematic values
                            safe_values: Dict[str, Any] = {}
                            for key, value in insert_values.items():
                                col_type = table.c[key].type
                                # Check if this is an Integer column and value is too large
                                if isinstance(col_type, Integer):
                                    if isinstance(value, (int, float)):
                                        # Check if value exceeds INT32 range
                                        if value > 2147483647 or value < -2147483648:
                                            safe_values[key] = None
                                        else:
                                            safe_values[key] = value
                                    else:
                                        safe_values[key] = value
                                else:
                                    safe_values[key] = value

                            if safe_values:
                                safe_insert_stmt = table.insert().values(safe_values)
                                session.execute(safe_insert_stmt)
                        else:
                            raise e
            session.commit()

        return table

    def copy_table_structure(self, source_table: str, target_table: str) -> None:
        """Copy table structure from source to target.

        Args:
            source_table: Name of source table
            target_table: Name of target table
        """
        source_table_obj = self._created_tables[source_table]

        # Copy all columns from source table
        new_columns: List[Any] = []
        for column in source_table_obj.columns:
            new_columns.append(Column(column.name, column.type, primary_key=False))

        # Create target table using SQLAlchemy Table
        target_table_obj = Table(target_table, self.metadata, *new_columns)
        target_table_obj.create(self.engine, checkfirst=True)
        self._created_tables[target_table] = target_table_obj

    def get_table_results(self, table_name: str) -> List[MockRow]:
        """Get all results from a table as MockRow objects.

        Args:
            table_name: Name of the table to query

        Returns:
            List of MockRow objects
        """
        table_obj = self._created_tables[table_name]

        with Session(self.engine) as session:
            # Build raw SQL query
            # Escape double quotes in column names by doubling them
            column_names = [
                f'"{col.name.replace(chr(34), chr(34) + chr(34))}"'
                for col in table_obj.columns
            ]
            sql = f"SELECT {', '.join(column_names)} FROM {table_name}"
            results = session.execute(text(sql)).all()

            mock_rows = []
            for result in results:
                # Convert result to list of values in schema order (preserves duplicates)
                # Don't use dict to avoid losing duplicate column names
                row_values: List[Any] = []
                field_names = []

                for i, column in enumerate(table_obj.columns):
                    value = result[i]

                    # Convert value to appropriate type based on column type
                    is_array_column = (
                        isinstance(column.type, ARRAY)
                        or type(column.type).__name__ == "ARRAY"
                    )
                    if is_array_column and value is not None:
                        # Array columns might be returned as lists or strings
                        if isinstance(value, list):
                            row_values.append(value)
                        elif isinstance(value, str):
                            # Parse string representation back to list
                            # DuckDB sometimes returns arrays as strings like "[1, 2, 3]"
                            try:
                                import ast

                                row_values.append(ast.literal_eval(value))
                            except (ValueError, SyntaxError):
                                row_values.append(value)
                        else:
                            row_values.append(value)
                    elif (
                        isinstance(column.type, String)
                        and isinstance(value, str)
                        and value.startswith("{")
                        and value.endswith("}")
                    ):
                        # Map columns returned as strings like "{a=1, b=2}" - parse to dict
                        try:
                            import ast

                            # Try to parse as dict literal
                            row_values.append(ast.literal_eval(value))
                        except (ValueError, SyntaxError):
                            # If that fails, leave as string
                            row_values.append(value)
                    elif isinstance(value, dict):
                        # Already a dict (map)
                        row_values.append(value)
                    elif isinstance(column.type, Integer) and value is not None:
                        try:
                            row_values.append(int(value))
                        except (ValueError, TypeError):
                            row_values.append(value)
                    elif isinstance(column.type, Float) and value is not None:
                        try:
                            row_values.append(float(value))
                        except (ValueError, TypeError):
                            row_values.append(value)
                    elif isinstance(column.type, Boolean) and value is not None:
                        if isinstance(value, str):
                            row_values.append(
                                value.lower()
                                in (
                                    "true",
                                    "1",
                                    "yes",
                                    "on",
                                )
                            )
                        else:
                            row_values.append(bool(value))
                    elif isinstance(column.type, DateTime) and value is not None:
                        # Preserve datetime/timestamp types - don't convert to string
                        row_values.append(value)
                    else:
                        row_values.append(value)

                    field_names.append(column.name)

                # Create MockRow with values in order
                # Convert to list of tuples to preserve duplicate column names
                row_data = list(zip(field_names, row_values))
                mock_rows.append(MockRow(row_data))

            return mock_rows

    def get_table(self, table_name: str) -> Optional[Table]:
        """Get a table object by name.

        Args:
            table_name: Name of the table

        Returns:
            SQLAlchemy Table object or None if not found
        """
        return self._created_tables.get(table_name)

    def cleanup_temp_tables(self) -> None:
        """Clean up all temporary tables."""
        with Session(self.engine) as session:
            for table_name in list(self._created_tables.keys()):
                try:
                    session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                    session.commit()
                except Exception:
                    # Ignore errors during cleanup
                    pass
        self._created_tables.clear()

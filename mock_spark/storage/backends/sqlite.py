"""
SQLite storage backend.

This module provides a SQLite-based storage implementation.
"""

import sqlite3
import os
from typing import List, Dict, Any, Optional, Union
from ..interfaces import IStorageManager, ITable, ISchema
from mock_spark.spark_types import MockStructType, MockStructField


class SQLiteTable(ITable):
    """SQLite table implementation."""
    
    def __init__(self, name: str, schema: MockStructType, connection: sqlite3.Connection):
        """Initialize SQLite table.
        
        Args:
            name: Table name.
            schema: Table schema.
            connection: SQLite database connection.
        """
        self.name = name
        self.schema = schema
        self.connection = connection
        self.metadata = {
            "created_at": "2024-01-01T00:00:00Z",
            "row_count": 0,
            "schema_version": "1.0"
        }
    
    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table.
        
        Args:
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if not data:
            return
        
        # Get column names from schema
        columns = [field.name for field in self.schema.fields]
        
        if mode == "overwrite":
            # Clear existing data
            cursor = self.connection.cursor()
            cursor.execute(f"DELETE FROM {self.name}")
            self.connection.commit()
        
        # Insert data
        cursor = self.connection.cursor()
        placeholders = ", ".join(["?" for _ in columns])
        
        if mode == "ignore":
            # Use INSERT OR IGNORE to skip duplicates
            query = f"INSERT OR IGNORE INTO {self.name} ({', '.join(columns)}) VALUES ({placeholders})"
        else:
            # Use regular INSERT for append and overwrite modes
            query = f"INSERT INTO {self.name} ({', '.join(columns)}) VALUES ({placeholders})"
        
        for row in data:
            values = [row.get(col) for col in columns]
            cursor.execute(query, values)
        
        self.connection.commit()
        self.metadata["row_count"] = len(data)
    
    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table.
        
        Args:
            filter_expr: Optional filter expression.
        
        Returns:
            List of data rows.
        """
        cursor = self.connection.cursor()
        
        if filter_expr:
            query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
        else:
            query = f"SELECT * FROM {self.name}"
        
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]
    
    def get_schema(self) -> MockStructType:
        """Get table schema.
        
        Returns:
            Table schema.
        """
        return self.schema
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata.
        
        Returns:
            Table metadata.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.name}")
        row_count = cursor.fetchone()[0]
        
        metadata = self.metadata.copy()
        metadata["row_count"] = row_count
        return metadata


class SQLiteSchema(ISchema):
    """SQLite schema implementation."""
    
    def __init__(self, name: str, connection: sqlite3.Connection):
        """Initialize SQLite schema.
        
        Args:
            name: Schema name.
            connection: SQLite database connection.
        """
        self.name = name
        self.connection = connection
        self.tables: Dict[str, SQLiteTable] = {}
    
    def create_table(
        self, 
        table: str, 
        columns: Union[List[MockStructField], MockStructType]
    ) -> "SQLiteTable":
        """Create a new table in this schema.
        
        Args:
            table: Name of the table.
            columns: Table columns definition.
            
        Returns:
            Created SQLiteTable instance.
        """
        if isinstance(columns, list):
            schema = MockStructType(columns)
        else:
            schema = columns
        
        # Create table in SQLite
        cursor = self.connection.cursor()
        column_defs = []
        for field in schema.fields:
            sqlite_type = self._get_sqlite_type(field.dataType)
            column_defs.append(f"{field.name} {sqlite_type}")
        
        create_query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(column_defs)})"
        cursor.execute(create_query)
        self.connection.commit()
        
        sqlite_table = SQLiteTable(table, schema, self.connection)
        self.tables[table] = sqlite_table
        return sqlite_table
    
    def _get_sqlite_type(self, data_type) -> str:
        """Convert MockSpark data type to SQLite type.
        
        Args:
            data_type: MockSpark data type.
        
        Returns:
            SQLite type string.
        """
        type_name = type(data_type).__name__
        if "String" in type_name:
            return "TEXT"
        elif "Integer" in type_name or "Long" in type_name:
            return "INTEGER"
        elif "Double" in type_name or "Float" in type_name:
            return "REAL"
        elif "Boolean" in type_name:
            return "INTEGER"
        else:
            return "TEXT"
    
    def table_exists(self, table: str) -> bool:
        """Check if table exists in this schema.
        
        Args:
            table: Name of the table.
        
        Returns:
            True if table exists, False otherwise.
        """
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
            (table,)
        )
        return cursor.fetchone() is not None
    
    def drop_table(self, table: str) -> None:
        """Drop a table from this schema.
        
        Args:
            table: Name of the table.
        """
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.connection.commit()
        
        if table in self.tables:
            del self.tables[table]
    
    def list_tables(self) -> List[str]:
        """List all tables in this schema.
        
        Returns:
            List of table names.
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]


class SQLiteStorageManager(IStorageManager):
    """SQLite storage manager implementation."""
    
    def __init__(self, db_path: str = "mock_spark.db"):
        """Initialize SQLite storage manager.
        
        Args:
            db_path: Path to SQLite database file.
        """
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.schemas: Dict[str, SQLiteSchema] = {}
        # Create default schema
        self.schemas["default"] = SQLiteSchema("default", self.connection)
    
    def create_schema(self, schema: str) -> None:
        """Create a new schema.
        
        Args:
            schema: Name of the schema to create.
        """
        if schema not in self.schemas:
            self.schemas[schema] = SQLiteSchema(schema, self.connection)
    
    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists.
        
        Args:
            schema: Name of the schema to check.
        
        Returns:
            True if schema exists, False otherwise.
        """
        return schema in self.schemas
    
    def drop_schema(self, schema: str) -> None:
        """Drop a schema.
        
        Args:
            schema: Name of the schema to drop.
        """
        if schema in self.schemas and schema != "default":
            del self.schemas[schema]
    
    def list_schemas(self) -> List[str]:
        """List all schemas.
        
        Returns:
            List of schema names.
        """
        return list(self.schemas.keys())
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists.
        
        Args:
            table: Name of the table.
            schema: Name of the schema (default: "default").
        
        Returns:
            True if table exists, False otherwise.
        """
        if schema not in self.schemas:
            return False
        return self.schemas[schema].table_exists(table)
    
    def create_table(
        self, 
        schema: str, 
        table: str, 
        fields: Union[List[MockStructField], MockStructType]
    ) -> "SQLiteTable":
        """Create a new table.
        
        Args:
            table: Name of the table.
            columns: Table columns definition.
            schema: Name of the schema (default: "default").
        
        Returns:
            Created SQLiteTable instance.
        """
        if schema not in self.schemas:
            self.create_schema(schema)
        
        return self.schemas[schema].create_table(table, fields)
    
    def drop_table(self, schema: str, table: str) -> None:
        """Drop a table.
        
        Args:
            table: Name of the table.
            schema: Name of the schema (default: "default").
        """
        if schema in self.schemas:
            self.schemas[schema].drop_table(table)
    
    def insert_data(
        self, 
        schema: str, 
        table: str, 
        data: List[Dict[str, Any]], 
        mode: str = "append"
    ) -> None:
        """Insert data into table.
        
        Args:
            schema: Name of the schema.
            table: Name of the table.
            data: Data to insert.
            mode: Insert mode ("append", "overwrite", "ignore").
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            self.schemas[schema].tables[table].insert_data(data, mode)
    
    def query_table(
        self, 
        schema: str, 
        table: str, 
        filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data from table.
        
        Args:
            schema: Name of the schema.
            table: Name of the table.
            filter_expr: Optional filter expression.
        
        Returns:
            List of data rows.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].query_data(filter_expr)
        return []
    
    def get_table_schema(self, schema: str, table: str) -> Optional[MockStructType]:
        """Get table schema.
        
        Args:
            schema: Name of the schema.
            table: Name of the table.
        
        Returns:
            Table schema or None if table doesn't exist.
        """
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].get_schema()
        return None
    
    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table.
        
        Args:
            schema: Name of the schema.
            table: Name of the table.
        
        Returns:
            List of data rows.
        """
        return self.query_table(schema, table)
    
    def create_temp_view(self, name: str, dataframe) -> None:
        """Create a temporary view from a DataFrame.
        
        Args:
            name: Name of the temporary view.
            dataframe: DataFrame to create view from.
        """
        # Create a schema and table for the temporary view
        schema = "default"
        self.create_schema(schema)
        
        # Convert DataFrame data to table format
        data = dataframe.data
        schema_obj = dataframe.schema
        
        # Create the table
        self.create_table(schema, name, schema_obj)
        
        # Insert the data
        self.insert_data(schema, name, data, mode="overwrite")
    
    def get_table(self, schema: str, table: str) -> Optional[SQLiteTable]:
        """Get an existing table.
        
        Args:
            schema: Name of the schema.
            table: Name of the table.
        
        Returns:
            SQLiteTable instance or None if table doesn't exist.
        """
        if schema not in self.schemas:
            return None
        return self.schemas[schema].tables.get(table)
    
    def list_tables(self, schema: str = "default") -> List[str]:
        """List tables in schema.
        
        Args:
            schema: Name of the schema (default: "default").
        
        Returns:
            List of table names.
        """
        if schema not in self.schemas:
            return []
        return self.schemas[schema].list_tables()
    
    def get_database_info(self) -> Dict[str, Any]:
        """Get database information.
        
        Returns:
            Dictionary containing database metadata.
        """
        tables = {}
        total_tables = 0
        
        for schema_name, schema in self.schemas.items():
            schema_tables = schema.list_tables()
            tables.update({f"{schema_name}.{table}": table for table in schema_tables})
            total_tables += len(schema_tables)
        
        return {
            "tables": tables,
            "total_tables": total_tables,
            "schemas": list(self.schemas.keys())
        }
    
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

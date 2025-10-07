"""
SQLModel models for type-safe DuckDB storage operations.

This module provides Pydantic-based models for Mock Spark's storage layer,
ensuring type safety and runtime validation for all database operations.
"""

from sqlmodel import SQLModel, Field, create_engine, Session
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


class StorageMode(str, Enum):
    """Storage operation modes with type safety."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"


class MockTableMetadata(SQLModel, table=True):
    """Type-safe table metadata model for DuckDB storage."""

    id: Optional[int] = Field(default=None, primary_key=True)
    table_name: str = Field(index=True, max_length=255)
    schema_name: str = Field(default="default", index=True, max_length=255)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default=None)
    row_count: int = Field(default=0, ge=0)
    schema_version: str = Field(default="1.0", max_length=50)
    storage_format: str = Field(default="columnar", max_length=50)
    is_temporary: bool = Field(default=False)

    class Config:
        """Pydantic configuration."""

        validate_assignment = True
        use_enum_values = True


class MockColumnDefinition(SQLModel, table=True):
    """Type-safe column definition model for DuckDB tables."""

    id: Optional[int] = Field(default=None, primary_key=True)
    table_id: int = Field(foreign_key="mocktablemetadata.id", index=True)
    column_name: str = Field(max_length=255)
    column_type: str = Field(max_length=100)
    is_nullable: bool = Field(default=True)
    is_primary_key: bool = Field(default=False)
    default_value: Optional[str] = Field(default=None, max_length=1000)
    column_order: int = Field(default=0, ge=0)

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class DuckDBTableModel(SQLModel):
    """Base model for DuckDB table operations with type safety."""

    table_name: str = Field(max_length=255)
    schema_name: str = Field(default="default", max_length=255)

    def get_full_name(self) -> str:
        """Get fully qualified table name."""
        return (
            f"{self.schema_name}.{self.table_name}"
            if self.schema_name != "default"
            else self.table_name
        )

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class DuckDBConnectionConfig(SQLModel):
    """Type-safe configuration for DuckDB connections."""

    database_path: str = Field(default="mock_spark.duckdb")
    read_only: bool = Field(default=False)
    memory_limit: Optional[str] = Field(default=None, max_length=50)
    thread_count: Optional[int] = Field(default=None, ge=1, le=64)
    enable_extensions: bool = Field(default=True)

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class StorageOperationResult(SQLModel):
    """Type-safe result model for storage operations."""

    success: bool
    rows_affected: int = Field(ge=0)
    operation_type: str
    table_name: str
    error_message: Optional[str] = Field(default=None)
    execution_time_ms: Optional[float] = Field(default=None, ge=0)

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


class QueryResult(SQLModel):
    """Type-safe model for query results."""

    data: List[Dict[str, Any]]
    row_count: int = Field(ge=0)
    column_count: int = Field(ge=0)
    execution_time_ms: Optional[float] = Field(default=None, ge=0)
    query: str

    class Config:
        """Pydantic configuration."""

        validate_assignment = True


def create_duckdb_engine(database_path: str = "mock_spark.duckdb") -> Any:
    """Create a DuckDB engine with SQLModel integration."""
    from sqlalchemy import create_engine as sa_create_engine

    # DuckDB connection string
    connection_string = f"duckdb:///{database_path}"

    # Create SQLAlchemy engine
    engine = sa_create_engine(
        connection_string,
        echo=False,  # Set to True for SQL debugging
        pool_pre_ping=True,
        pool_recycle=3600,
    )

    return engine


def create_session(engine: Any) -> Session:
    """Create a SQLModel session for database operations."""
    return Session(engine)


def initialize_metadata_tables(engine: Any) -> None:
    """Initialize metadata tables for Mock Spark storage."""
    # Create all tables
    SQLModel.metadata.create_all(engine)

    # Create indexes for better performance
    with Session(engine) as session:
        # Add any additional setup here
        session.commit()

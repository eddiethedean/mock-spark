"""
DuckDB storage backend with SQLAlchemy integration.

This module provides a type-safe, high-performance storage backend using DuckDB
with SQLAlchemy for enhanced type safety and maintainability.
"""

import duckdb
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional, Union, Callable
from datetime import datetime, timezone
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from sqlalchemy import create_engine, MetaData, Table, insert, inspect
from sqlalchemy.engine import Engine

from mock_spark.core.interfaces.storage import IStorageManager, ITable
from mock_spark.storage.models import (
    StorageMode,
    StorageOperationResult,
    QueryResult,
)
from mock_spark.spark_types import MockStructType, MockStructField
from mock_spark.storage.sqlalchemy_helpers import (
    create_table_from_mock_schema,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TimeoutError(Exception):
    """Custom timeout error for database operations."""

    pass


def with_timeout(func: Callable, timeout_seconds: float = 5) -> Any:
    """Execute function with timeout using ThreadPoolExecutor.

    This works cross-platform (unlike signal.SIGALRM which only works on Unix).

    Args:
        func: Function to execute
        timeout_seconds: Timeout in seconds

    Returns:
        Result of function execution

    Raises:
        TimeoutError: If function doesn't complete within timeout
    """
    print(
        f"[TIMEOUT] Executing function with {timeout_seconds}s timeout...", flush=True
    )
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            result = future.result(timeout=timeout_seconds)
            duration = time.time() - start_time
            print(f"[TIMEOUT] Function completed in {duration:.3f}s", flush=True)
            if duration > 1.0:
                print(
                    f"[TIMEOUT] WARNING: Function took {duration:.3f}s (>1s)",
                    flush=True,
                )
            return result
        except FutureTimeoutError:
            print(f"[TIMEOUT] Function TIMED OUT after {timeout_seconds}s", flush=True)
            raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")
        except Exception as e:
            print(f"[TIMEOUT] Function raised exception: {e}", flush=True)
            raise


# Thread-local schema cache for tracking schemas created in each thread
_thread_schema_cache = threading.local()
_thread_connection_cache = threading.local()

# Lock for protecting DuckDB connection access (connections are not thread-safe)
_connection_lock = threading.Lock()


def _get_thread_schema_cache():
    """Get thread-local set of schemas created in this thread."""
    if not hasattr(_thread_schema_cache, "schemas"):
        _thread_schema_cache.schemas = set()
    return _thread_schema_cache.schemas


# Removed _get_thread_connection - using main connection for simplicity
# Thread-local connections were causing hangs, so we use the shared connection


class DuckDBTable(ITable):
    """Type-safe DuckDB table implementation with simplified metadata."""

    def __init__(
        self,
        name: str,
        schema: MockStructType,
        connection: duckdb.DuckDBPyConnection,
        sqlalchemy_session: Optional[Session],
        engine: Optional[Engine] = None,
        schema_name: Optional[str] = None,
    ):
        """Initialize DuckDB table with type safety."""
        self._name = name
        self._schema = schema
        self._schema_name = schema_name or "default"
        self.connection = connection
        self.sqlalchemy_session = sqlalchemy_session

        # Create or use provided SQLAlchemy engine for type-safe operations
        if engine is None:
            self.engine = create_engine("duckdb:///:memory:")
        else:
            self.engine = engine

        self.table_metadata = MetaData()
        self.sqlalchemy_table: Optional[Table] = None

        # Create simplified metadata using dataclasses
        self._metadata = {
            "table_name": name,
            "schema_name": self._schema_name,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": None,
            "row_count": 0,
            "schema_version": "1.0",
            "storage_format": "columnar",
            "is_temporary": False,
        }

        # Create table using SQLAlchemy
        self._create_table_from_schema()

    def _create_table_from_schema(self) -> None:
        """Create table from MockSpark schema using SQLAlchemy."""
        # Check if table already exists in metadata
        if self._name in self.table_metadata.tables:
            self.sqlalchemy_table = self.table_metadata.tables[self._name]
        else:
            # Create SQLAlchemy table from MockSpark schema with schema name
            self.sqlalchemy_table = create_table_from_mock_schema(
                self._name, self._schema, self.table_metadata, schema=self._schema_name
            )

        # Create the table in the database
        # Schemas should already exist before table creation (handled by storage manager)
        # Use checkfirst=True to avoid errors if table already exists
        # For multi-threaded scenarios, schemas are ensured to exist before this is called
        self.sqlalchemy_table.create(self.engine, checkfirst=True)

    def _get_duckdb_type(self, data_type: Any) -> str:
        """Convert MockSpark data type to DuckDB type."""
        type_name = type(data_type).__name__
        if "String" in type_name:
            return "VARCHAR"
        elif "Integer" in type_name or "Long" in type_name:
            return "INTEGER"
        elif "Double" in type_name or "Float" in type_name:
            return "DOUBLE"
        elif "Boolean" in type_name:
            return "BOOLEAN"
        elif "Date" in type_name:
            return "DATE"
        elif "Timestamp" in type_name:
            return "TIMESTAMP"
        elif "ArrayType" in type_name or "Array" in type_name:
            # For arrays, use VARCHAR[] for simplicity
            return "VARCHAR[]"
        elif "MapType" in type_name or "Map" in type_name:
            # For maps, use MAP(VARCHAR, VARCHAR)
            return "MAP(VARCHAR, VARCHAR)"
        else:
            return "VARCHAR"

    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Type-safe data insertion with validation using SQLAlchemy."""
        if not data:
            return

        start_time = time.time()

        try:
            # Validate data against schema
            validated_data = self._validate_data(data)

            # Handle mode-specific operations
            if mode == StorageMode.OVERWRITE:
                # Drop and recreate table using SQLAlchemy
                if self.sqlalchemy_table is not None:
                    self.sqlalchemy_table.drop(self.engine, checkfirst=True)
                self._create_table_from_schema()
            elif mode == StorageMode.IGNORE:
                # Use INSERT OR IGNORE for DuckDB
                pass

            # Type-safe insertion using SQLAlchemy bulk insert
            if self.sqlalchemy_table is not None:
                # Check if we have MAP columns that need special handling
                has_map_columns = any(
                    "MapType" in type(field.dataType).__name__
                    for field in self.schema.fields
                )

                if has_map_columns:
                    # Use raw SQL for MAP insertion
                    from sqlalchemy import text

                    for row in validated_data:
                        col_names = []
                        col_values = []

                        for field in self.schema.fields:
                            col_names.append(f'"{field.name}"')
                            value = row.get(field.name)

                            # Convert dict to DuckDB MAP format
                            if isinstance(value, dict) and value:
                                keys = list(value.keys())
                                vals = list(value.values())
                                # Use DuckDB MAP syntax
                                col_values.append(f"MAP({keys!r}, {vals!r})")
                            elif value is None:
                                col_values.append("NULL")
                            elif isinstance(value, str):
                                col_values.append(f"'{value}'")
                            elif isinstance(value, list):
                                col_values.append(f"{value!r}")
                            else:
                                col_values.append(str(value))

                        insert_sql = f"INSERT INTO {self.name} ({', '.join(col_names)}) VALUES ({', '.join(col_values)})"
                        with self.engine.begin() as conn:
                            conn.execute(text(insert_sql))
                else:
                    # Use SQLAlchemy bulk insert for better performance
                    with self.engine.begin() as conn:
                        conn.execute(insert(self.sqlalchemy_table), validated_data)

            # Update metadata with type safety
            self._update_row_count(len(validated_data))

            execution_time = (time.time() - start_time) * 1000

            # Log operation result
            StorageOperationResult(
                success=True,
                rows_affected=len(validated_data),
                operation_type=f"insert_{mode}",
                table_name=self.name,
                execution_time_ms=execution_time,
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            StorageOperationResult(
                success=False,
                rows_affected=0,
                operation_type=f"insert_{mode}",
                table_name=self.name,
                error_message=str(e),
                execution_time_ms=execution_time,
            )
            raise ValueError(f"Failed to insert data: {e}") from e

    def _validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate data against schema using type checking."""
        validated = []
        for row in data:
            # Check required fields exist
            for field in self.schema.fields:
                if field.name not in row:
                    raise ValueError(f"Missing required field: {field.name}")

            validated.append(row)
        return validated

    def _update_row_count(self, new_rows: int) -> None:
        """Update row count with type safety."""
        current_count = self._metadata.get("row_count", 0)
        self._metadata["row_count"] = (
            int(current_count) + new_rows if current_count else new_rows
        )
        self._metadata["updated_at"] = datetime.now(timezone.utc).isoformat()

    def _query_data_internal(
        self, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Optimized querying using SQLAlchemy select()."""
        start_time = time.time()

        try:
            if self.sqlalchemy_table is None:
                return []

            # Build SQLAlchemy select statement
            from sqlalchemy import select as sa_select

            stmt = sa_select(self.sqlalchemy_table)

            # Add filter if provided (Note: filter_expr is a string, will need translation)
            if filter_expr:
                # For now, we'll need to use the SQL translator for complex filters
                # This is a limitation we'll address in Phase 3
                # As a temporary measure, use DuckDB directly for filtered queries
                query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
                duckdb_result = self.connection.execute(query).fetchall()
                description = self.connection.description
                columns = [desc[0] for desc in description] if description else []
                data = [dict(zip(columns, row)) for row in duckdb_result]
            else:
                # Use SQLAlchemy for unfiltered queries
                with self.engine.connect() as conn:
                    sqlalchemy_result: Any = conn.execute(
                        stmt
                    ).fetchall()  # Sequence[Row]
                    columns = [col.name for col in self.sqlalchemy_table.columns]
                    data = [dict(zip(columns, row)) for row in sqlalchemy_result]

            execution_time = (time.time() - start_time) * 1000

            # Create query result
            QueryResult(
                data=data,
                row_count=len(data),
                column_count=len(columns),
                execution_time_ms=execution_time,
                query=(
                    str(stmt)
                    if not filter_expr
                    else f"SELECT * FROM {self.name} WHERE {filter_expr}"
                ),
            )

            return data

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            raise ValueError(f"Query failed: {e}") from e

    def get_schema(self) -> MockStructType:
        """Get table schema."""
        return self._schema

    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata with type safety."""
        return self._metadata.copy()

    # ITable interface implementation
    @property
    def name(self) -> str:
        """Get table name."""
        return self._name

    @property
    def schema(self) -> MockStructType:
        """Get table schema."""
        return self._schema

    @property
    def metadata(self) -> Dict[str, Any]:
        """Get table metadata."""
        return self._metadata

    def insert(self, data: List[Dict[str, Any]]) -> None:
        """Insert data into table."""
        self.insert_data(data)

    def query(self, **filters: Any) -> List[Dict[str, Any]]:
        """Query data from table."""
        return self._query_data_internal()

    def count(self) -> int:
        """Count rows in table."""
        count = self._metadata.get("row_count", 0)
        return int(count) if count is not None else 0

    def truncate(self) -> None:
        """Truncate table."""
        # Clear all data but keep table structure
        if self.sqlalchemy_table is not None:
            with self.engine.connect() as conn:
                conn.execute(self.sqlalchemy_table.delete())
                conn.commit()
        self._metadata["row_count"] = 0
        self._metadata["updated_at"] = datetime.now(timezone.utc).isoformat()

    def drop(self) -> None:
        """Drop table."""
        if self.sqlalchemy_table is not None:
            with self.engine.connect() as conn:
                self.sqlalchemy_table.drop(conn)
                conn.commit()

    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table (interface compliance)."""
        return self._query_data_internal(filter_expr)


class DuckDBSchema:
    """DuckDB database schema (namespace) implementation with type safety."""

    def __init__(
        self,
        name: str,
        connection: duckdb.DuckDBPyConnection,
        sqlalchemy_session: Optional[Session],
        engine: Optional[Engine] = None,
    ):
        """Initialize DuckDB schema."""
        self.name = name
        self.connection = connection
        self.sqlalchemy_session = sqlalchemy_session
        self.tables: Dict[str, DuckDBTable] = {}
        # Lock for protecting tables dictionary access in multi-threaded scenarios
        self._tables_lock = threading.Lock()

        # Create or use provided SQLAlchemy engine
        if engine is None:
            self.engine = create_engine("duckdb:///:memory:")
        else:
            self.engine = engine

    def create_table(
        self, table: str, columns: Union[List[MockStructField], MockStructType]
    ) -> Optional[DuckDBTable]:
        """Create a new table with type safety."""
        if isinstance(columns, list):
            # Type narrowing: when columns is a list, it's List[MockStructField]
            schema = MockStructType(columns)  # type: ignore[arg-type]
        else:
            schema = columns

        # Create table using SQLAlchemy with schema name
        # Schema existence is ensured in DuckDBTable._create_table_from_schema()
        duckdb_table = DuckDBTable(
            table,
            schema,
            self.connection,
            self.sqlalchemy_session,
            self.engine,
            schema_name=self.name,
        )

        # Atomically add table to registry with lock to ensure thread-safety
        # This ensures the table is immediately visible to other threads
        with self._tables_lock:
            self.tables[table] = duckdb_table

        return duckdb_table

    def table_exists(self, table: str) -> bool:
        """Check if table exists using table registry."""
        # Always check registry first - this is the source of truth for tables
        # created through our API, especially in multi-threaded scenarios.
        # Dictionary reads are thread-safe in Python (GIL ensures atomicity),
        # so we don't need a lock here. The lock is only needed for writes.
        if table in self.tables:
            return True

        # For in-memory databases, skip database checks to avoid hangs and
        # connection isolation issues. For persistent databases, check the database.
        engine_url = str(self.engine.url)
        is_in_memory = ":memory:" in engine_url or engine_url.endswith(
            "duckdb:///:memory:"
        )

        if not is_in_memory:
            # For persistent databases, check the database
            try:
                inspector = inspect(self.engine)
                if inspector.has_table(table, schema=self.name):
                    return True
            except Exception:
                pass

        return False

    def drop_table(self, table: str) -> None:
        """Drop a table using SQLAlchemy."""
        # Use lock to protect tables dictionary access
        with self._tables_lock:
            # Drop using SQLAlchemy if we have the table object
            if table in self.tables and self.tables[table].sqlalchemy_table is not None:
                table_obj = self.tables[table].sqlalchemy_table
                assert table_obj is not None  # Type narrowing for mypy
                table_obj.drop(self.engine, checkfirst=True)
            else:
                # Try to reflect and drop
                try:
                    metadata = MetaData()
                    table_obj = Table(table, metadata, autoload_with=self.engine)
                    table_obj.drop(self.engine, checkfirst=True)
                except:  # noqa: E722
                    pass  # Table doesn't exist

            # Remove from metadata
            if table in self.tables:
                del self.tables[table]

    def list_tables(self) -> List[str]:
        """List all tables in schema using SQLAlchemy Inspector."""
        inspector = inspect(self.engine)
        return inspector.get_table_names()


class DuckDBStorageManager(IStorageManager):
    """Type-safe DuckDB storage manager with in-memory storage by default.

    Thread Safety:
        DuckDB connections are thread-local, meaning each thread gets its own
        connection from the SQLAlchemy connection pool. Schemas created in one
        thread's connection are not visible to other threads (especially for
        in-memory databases). This class implements thread-safe schema creation
        by:
        1. Tracking schemas per thread using thread-local storage
        2. Ensuring schemas are created in each thread's connection
        3. Using retry logic to handle race conditions

        When using this storage manager in parallel execution contexts (e.g.,
        ThreadPoolExecutor or pytest-xdist), schemas are automatically created
        in each thread's connection as needed.
    """

    def __init__(
        self,
        db_path: Optional[str] = None,
        max_memory: str = "1GB",
        allow_disk_spillover: bool = False,
    ):
        """Initialize DuckDB storage manager with in-memory storage by default.

        Args:
            db_path: Optional path to database file. If None, uses in-memory storage.
            max_memory: Maximum memory for DuckDB to use (e.g., '1GB', '4GB', '8GB').
                       Default is '1GB' for test isolation.
            allow_disk_spillover: If True, allows DuckDB to spill to disk when memory is full.
                                 If False (default), disables spillover for test isolation.
                                 When enabled, uses a unique temp directory per connection.
        """
        self.db_path = db_path
        self._temp_dir = None

        # Store configuration for reuse
        self.max_memory = max_memory
        self.allow_disk_spillover = allow_disk_spillover

        # Create SQLAlchemy engine FIRST with connection factory that applies configuration
        # This ensures all connections use the same configuration from the start
        db_url = f"duckdb:///{db_path}" if db_path else "duckdb:///:memory:"

        # Prepare temp directory if needed
        if allow_disk_spillover:
            import tempfile
            import uuid

            self._temp_dir = tempfile.mkdtemp(
                prefix=f"duckdb_test_{uuid.uuid4().hex[:8]}_"
            )
        else:
            self._temp_dir = None

        def _configure_connection(conn):
            """Configure a DuckDB connection with the same settings."""
            try:
                conn.execute(f"SET max_memory='{max_memory}'")
                if allow_disk_spillover and self._temp_dir:
                    conn.execute(f"SET temp_directory='{self._temp_dir}'")
                else:
                    conn.execute("SET temp_directory=''")
            except:  # noqa: E722
                pass  # Ignore if settings not supported

        # Create engine with connection factory that configures connections immediately
        # Use event listener to configure connections when they're created (before DuckDB checks)
        from sqlalchemy import event

        # Create engine first
        self.engine = create_engine(
            db_url,
            pool_pre_ping=True,  # Verify connections before using
        )

        # Apply configuration to all connections in the pool when they're created
        @event.listens_for(self.engine, "connect")
        def set_duckdb_config(dbapi_conn, connection_record):
            """Configure DuckDB connection immediately when it's created."""
            _configure_connection(dbapi_conn)

        # Now get a connection from the engine for direct use (will be configured via event)
        # This ensures we use the same connection pool and configuration
        self.connection = self.engine.raw_connection()
        self.is_in_memory = db_path is None

        # The connection is already configured via the event listener, but apply again to be sure
        _configure_connection(self.connection)

        self.schemas: Dict[str, DuckDBSchema] = {}

        # For persistent databases, discover existing schemas and tables
        if not self.is_in_memory and db_path:
            try:
                # Query DuckDB for existing schemas
                schema_result = self.connection.execute(
                    "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_temp')"
                ).fetchall()

                # Create DuckDBSchema objects for each existing schema
                for (schema_name,) in schema_result:
                    if schema_name not in self.schemas:
                        schema_obj = DuckDBSchema(
                            schema_name, self.connection, None, self.engine
                        )
                        self.schemas[schema_name] = schema_obj

                        # Tables will be discovered on-demand via SQLAlchemy inspector
                        # No need to pre-populate self.tables - DuckDBSchema.table_exists()
                        # will use SQLAlchemy inspector as fallback
                        pass
            except Exception:
                # If schema discovery fails, just create default
                pass

        # Always ensure default schema exists
        if "default" not in self.schemas:
            self.schemas["default"] = DuckDBSchema(
                "default", self.connection, None, self.engine
            )

        # Track current schema
        self._current_schema = "default"

        # Enable extensions using DuckDB Python API (zero raw SQL)
        try:
            self.connection.install_extension("sqlite")
            self.connection.load_extension("sqlite")
        except:  # noqa: E722
            pass  # Extensions might not be available

    def _ensure_schema_in_connection(
        self, schema: str, connection: duckdb.DuckDBPyConnection
    ) -> bool:
        """Ensure schema exists in a specific DuckDB connection with retry logic.

        Args:
            schema: Schema name to ensure exists
            connection: DuckDB connection to check/create schema in

        Returns:
            True if schema exists or was successfully created, False otherwise
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        logger.debug(
            f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') START"
        )
        print(
            f"[{thread_id}] _ensure_schema_in_connection('{schema}') START", flush=True
        )

        try:
            # Default schema always exists
            if schema == "default":
                logger.debug(
                    f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') END (default schema, {time.time() - start_time:.3f}s)"
                )
                print(
                    f"[{thread_id}] _ensure_schema_in_connection('{schema}') END (default, {time.time() - start_time:.3f}s)",
                    flush=True,
                )
                return True

            # Check if schema already exists in this connection
            # Use lock to protect DuckDB connection access (not thread-safe)
            print(f"[{thread_id}] Checking if schema '{schema}' exists...", flush=True)
            try:
                check_start = time.time()
                with _connection_lock:
                    print(
                        f"[{thread_id}] Acquired connection lock for schema check",
                        flush=True,
                    )
                    result = connection.execute(
                        f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'"
                    ).fetchone()
                    print(
                        f"[{thread_id}] Released connection lock after schema check",
                        flush=True,
                    )
                check_duration = time.time() - check_start
                print(
                    f"[{thread_id}] Schema check completed in {check_duration:.3f}s",
                    flush=True,
                )
                if check_duration > 1.0:
                    print(
                        f"[{thread_id}] WARNING: Schema check took {check_duration:.3f}s (>1s)",
                        flush=True,
                    )
                if check_duration > 5.0:
                    print(
                        f"[{thread_id}] ERROR: Schema check took {check_duration:.3f}s (>5s) - possible hang!",
                        flush=True,
                    )
                if result:
                    logger.debug(
                        f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') END (exists, {time.time() - start_time:.3f}s)"
                    )
                    print(
                        f"[{thread_id}] _ensure_schema_in_connection('{schema}') END (exists, {time.time() - start_time:.3f}s)",
                        flush=True,
                    )
                    return True
            except Exception as e:
                print(f"[{thread_id}] Schema check exception: {e}", flush=True)
                logger.debug(f"[Thread {thread_id}] Schema check exception: {e}")

            # Try to create schema with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    print(
                        f"[{thread_id}] Attempt {attempt + 1}/{max_retries}: Creating schema '{schema}'...",
                        flush=True,
                    )
                    create_start = time.time()
                    # Use lock to protect DuckDB connection access (not thread-safe)
                    with _connection_lock:
                        print(
                            f"[{thread_id}] Acquired connection lock for CREATE SCHEMA",
                            flush=True,
                        )
                        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                        print(
                            f"[{thread_id}] Released connection lock after CREATE SCHEMA",
                            flush=True,
                        )
                    create_duration = time.time() - create_start
                    print(
                        f"[{thread_id}] CREATE SCHEMA completed in {create_duration:.3f}s",
                        flush=True,
                    )
                    if create_duration > 1.0:
                        print(
                            f"[{thread_id}] WARNING: CREATE SCHEMA took {create_duration:.3f}s (>1s)",
                            flush=True,
                        )
                    if create_duration > 5.0:
                        print(
                            f"[{thread_id}] ERROR: CREATE SCHEMA took {create_duration:.3f}s (>5s) - possible hang!",
                            flush=True,
                        )

                    # Verify creation
                    print(f"[{thread_id}] Verifying schema creation...", flush=True)
                    verify_start = time.time()
                    with _connection_lock:
                        print(
                            f"[{thread_id}] Acquired connection lock for verification",
                            flush=True,
                        )
                        result = connection.execute(
                            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}'"
                        ).fetchone()
                        print(
                            f"[{thread_id}] Released connection lock after verification",
                            flush=True,
                        )
                    verify_duration = time.time() - verify_start
                    print(
                        f"[{thread_id}] Verification completed in {verify_duration:.3f}s",
                        flush=True,
                    )
                    if verify_duration > 1.0:
                        print(
                            f"[{thread_id}] WARNING: Verification took {verify_duration:.3f}s (>1s)",
                            flush=True,
                        )
                    if verify_duration > 5.0:
                        print(
                            f"[{thread_id}] ERROR: Verification took {verify_duration:.3f}s (>5s) - possible hang!",
                            flush=True,
                        )

                    if result:
                        logger.debug(
                            f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') END (created, {time.time() - start_time:.3f}s)"
                        )
                        print(
                            f"[{thread_id}] _ensure_schema_in_connection('{schema}') END (created, {time.time() - start_time:.3f}s)",
                            flush=True,
                        )
                        return True
                except Exception as e:
                    print(
                        f"[{thread_id}] Attempt {attempt + 1} exception: {e}",
                        flush=True,
                    )
                    logger.debug(
                        f"[Thread {thread_id}] Attempt {attempt + 1} exception: {e}"
                    )
                    if attempt == max_retries - 1:
                        # Last attempt failed, return False
                        logger.debug(
                            f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') END (failed, {time.time() - start_time:.3f}s)"
                        )
                        print(
                            f"[{thread_id}] _ensure_schema_in_connection('{schema}') END (failed, {time.time() - start_time:.3f}s)",
                            flush=True,
                        )
                        return False
                    # Exponential backoff
                    sleep_time = 0.01 * (attempt + 1)
                    print(
                        f"[{thread_id}] Sleeping {sleep_time}s before retry...",
                        flush=True,
                    )
                    time.sleep(sleep_time)

            logger.debug(
                f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') END (failed all retries, {time.time() - start_time:.3f}s)"
            )
            print(
                f"[{thread_id}] _ensure_schema_in_connection('{schema}') END (failed all retries, {time.time() - start_time:.3f}s)",
                flush=True,
            )
            return False
        except Exception as e:
            logger.error(
                f"[Thread {thread_id}] _ensure_schema_in_connection('{schema}') ERROR: {e}"
            )
            print(
                f"[{thread_id}] _ensure_schema_in_connection('{schema}') ERROR: {e}",
                flush=True,
            )
            raise

    def create_schema(self, schema: str) -> None:
        """Create a new schema with thread-safe retry logic.

        This method ensures that schemas are created in the current thread's
        DuckDB connection, which is necessary because DuckDB connections are
        thread-local and schemas created in one connection are not visible to
        other connections (especially for in-memory databases).

        Args:
            schema: Schema name to create

        Raises:
            RuntimeError: If schema creation fails after retries
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        logger.debug(f"[Thread {thread_id}] create_schema('{schema}') START")
        print(f"[{thread_id}] create_schema('{schema}') START", flush=True)

        try:
            # Check if schema already exists in both caches and database
            print(f"[{thread_id}] Checking if schema already exists...", flush=True)
            thread_cache = _get_thread_schema_cache()

            # Only return early if schema exists in BOTH thread cache AND self.schemas dict
            # This ensures the schema object is actually created
            if schema in thread_cache and schema in self.schemas:
                logger.debug(
                    f"[Thread {thread_id}] create_schema('{schema}') END (already exists, {time.time() - start_time:.3f}s)"
                )
                print(
                    f"[{thread_id}] create_schema('{schema}') END (already exists, {time.time() - start_time:.3f}s)",
                    flush=True,
                )
                return

            # If in process cache but not thread cache, update thread cache
            if schema in self.schemas:
                thread_cache.add(schema)
                logger.debug(
                    f"[Thread {thread_id}] create_schema('{schema}') END (in process cache, {time.time() - start_time:.3f}s)"
                )
                print(
                    f"[{thread_id}] create_schema('{schema}') END (in process cache, {time.time() - start_time:.3f}s)",
                    flush=True,
                )
                return

            # Create schema using main connection
            # For in-memory databases, all operations use the same connection
            # Thread-local operations are handled by ensuring schemas exist before parallel execution
            print(f"[{thread_id}] Calling _ensure_schema_in_connection...", flush=True)
            ensure_start = time.time()
            success = self._ensure_schema_in_connection(schema, self.connection)
            ensure_duration = time.time() - ensure_start
            print(
                f"[{thread_id}] _ensure_schema_in_connection completed in {ensure_duration:.3f}s",
                flush=True,
            )
            if ensure_duration > 1.0:
                print(
                    f"[{thread_id}] WARNING: _ensure_schema_in_connection took {ensure_duration:.3f}s (>1s)",
                    flush=True,
                )

            if not success:
                raise RuntimeError(f"Failed to create schema '{schema}' after retries.")

            # Update both caches on success
            print(f"[{thread_id}] Updating caches...", flush=True)
            if schema not in self.schemas:
                self.schemas[schema] = DuckDBSchema(
                    schema, self.connection, None, self.engine
                )
            thread_cache.add(schema)

            logger.debug(
                f"[Thread {thread_id}] create_schema('{schema}') END (success, {time.time() - start_time:.3f}s)"
            )
            print(
                f"[{thread_id}] create_schema('{schema}') END (success, {time.time() - start_time:.3f}s)",
                flush=True,
            )
        except Exception as e:
            logger.error(f"[Thread {thread_id}] create_schema('{schema}') ERROR: {e}")
            print(f"[{thread_id}] create_schema('{schema}') ERROR: {e}", flush=True)
            raise

    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists, with thread-local awareness.

        This method checks multiple levels to determine if a schema exists:
        1. Thread-local cache (fastest path)
        2. Process-level cache (self.schemas dict)
        3. Database via thread-local connection (for persistent databases)

        Args:
            schema: Schema name to check

        Returns:
            True if schema exists in current thread's context, False otherwise
        """
        thread_id = threading.current_thread().ident
        start_time = time.time()
        logger.debug(f"[Thread {thread_id}] schema_exists('{schema}') START")
        print(f"[{thread_id}] schema_exists('{schema}') START", flush=True)

        try:
            # Fast path: check if schema exists in process-level cache
            # Thread cache alone is not sufficient - we need the actual schema object
            print(f"[{thread_id}] Checking process-level cache...", flush=True)
            thread_cache = _get_thread_schema_cache()
            if schema in self.schemas:
                thread_cache.add(schema)  # Update thread cache
                logger.debug(
                    f"[Thread {thread_id}] schema_exists('{schema}') END (in process cache, {time.time() - start_time:.3f}s)"
                )
                print(
                    f"[{thread_id}] schema_exists('{schema}') END (in process cache, {time.time() - start_time:.3f}s)",
                    flush=True,
                )
                return True

            # Check thread-local cache (but only if process cache also has it)
            print(f"[{thread_id}] Checking thread-local cache...", flush=True)
            if schema in thread_cache and schema in self.schemas:
                logger.debug(
                    f"[Thread {thread_id}] schema_exists('{schema}') END (in thread cache, {time.time() - start_time:.3f}s)"
                )
                print(
                    f"[{thread_id}] schema_exists('{schema}') END (in thread cache, {time.time() - start_time:.3f}s)",
                    flush=True,
                )
                return True

            # Check database - use main connection for simplicity
            # For in-memory databases, all threads share the same connection context
            print(f"[{thread_id}] Checking database...", flush=True)
            try:
                db_check_start = time.time()
                # Use lock to protect DuckDB connection access (not thread-safe)
                with _connection_lock:
                    print(
                        f"[{thread_id}] Acquired connection lock for database check",
                        flush=True,
                    )
                    result = self.connection.execute(
                        f"SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '{schema}'"
                    ).fetchone()
                    print(
                        f"[{thread_id}] Released connection lock after database check",
                        flush=True,
                    )
                db_check_duration = time.time() - db_check_start
                print(
                    f"[{thread_id}] Database check completed in {db_check_duration:.3f}s",
                    flush=True,
                )
                if db_check_duration > 1.0:
                    print(
                        f"[{thread_id}] WARNING: Database check took {db_check_duration:.3f}s (>1s)",
                        flush=True,
                    )
                if db_check_duration > 5.0:
                    print(
                        f"[{thread_id}] ERROR: Database check took {db_check_duration:.3f}s (>5s) - possible hang!",
                        flush=True,
                    )
                if result and result[0] > 0:
                    # Schema exists in database but not in our caches - update both
                    if schema not in self.schemas:
                        self.schemas[schema] = DuckDBSchema(
                            schema, self.connection, None, self.engine
                        )
                    thread_cache.add(schema)
                    logger.debug(
                        f"[Thread {thread_id}] schema_exists('{schema}') END (found in DB, {time.time() - start_time:.3f}s)"
                    )
                    print(
                        f"[{thread_id}] schema_exists('{schema}') END (found in DB, {time.time() - start_time:.3f}s)",
                        flush=True,
                    )
                    return True
            except Exception as e:
                print(f"[{thread_id}] Database check exception: {e}", flush=True)
                logger.debug(f"[Thread {thread_id}] Database check exception: {e}")

            logger.debug(
                f"[Thread {thread_id}] schema_exists('{schema}') END (not found, {time.time() - start_time:.3f}s)"
            )
            print(
                f"[{thread_id}] schema_exists('{schema}') END (not found, {time.time() - start_time:.3f}s)",
                flush=True,
            )
            return False
        except Exception as e:
            logger.error(f"[Thread {thread_id}] schema_exists('{schema}') ERROR: {e}")
            print(f"[{thread_id}] schema_exists('{schema}') ERROR: {e}", flush=True)
            raise

    def drop_schema(self, schema_name: str, cascade: bool = False) -> None:
        """Drop a schema."""
        if schema_name in self.schemas and schema_name != "default":
            del self.schemas[schema_name]

    def list_schemas(self) -> List[str]:
        """List all schemas."""
        return list(self.schemas.keys())

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        # Get schema object atomically to avoid race conditions
        # where schema might be created/removed between check and access
        schema_obj = self.schemas.get(schema)
        if schema_obj is None:
            return False

        # Use the schema object to check for table existence
        # This ensures we're using the same schema object that was used for table creation
        return schema_obj.table_exists(table)

    def create_table(
        self,
        schema_name: str,
        table_name: str,
        fields: Union[List[MockStructField], MockStructType],
    ) -> Optional[DuckDBTable]:
        """Create a new table with type safety."""
        if schema_name not in self.schemas:
            self.create_schema(schema_name)

        # Create the table - it will be automatically registered in the schema's
        # table registry with proper locking in create_table()
        return self.schemas[schema_name].create_table(table_name, fields)

    def drop_table(self, schema_name: str, table_name: str) -> None:
        """Drop a table."""
        if schema_name in self.schemas:
            self.schemas[schema_name].drop_table(table_name)

    def insert_data(
        self, schema_name: str, table_name: str, data: List[Dict[str, Any]]
    ) -> None:
        """Insert data with type safety."""
        # Check in-memory registry first (fast path)
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            self.schemas[schema_name].tables[table_name].insert_data(data)
            return

        # For persistent tables not in registry, insert directly into DuckDB
        if not data:
            return

        try:
            # Ensure schema exists
            if schema_name not in self.schemas:
                self.create_schema(schema_name)

            # Get column names from first row
            first_row = data[0]
            columns = list(first_row.keys())

            # Build INSERT statement
            schema_quoted = (
                f'"{schema_name}"' if not schema_name.isalnum() else schema_name
            )
            table_quoted = f'"{table_name}"' if not table_name.isalnum() else table_name

            # Insert each row
            for row in data:
                values = []
                for col in columns:
                    value = row.get(col)
                    if value is None:
                        values.append("NULL")
                    elif isinstance(value, str):
                        # Escape single quotes in strings
                        escaped_value = value.replace("'", "''")
                        values.append(f"'{escaped_value}'")
                    else:
                        values.append(str(value))

                # Build column list with quoted names
                quoted_cols = [f'"{col}"' for col in columns]
                cols_str = ", ".join(quoted_cols)
                values_str = ", ".join(values)
                insert_sql = f"INSERT INTO {schema_quoted}.{table_quoted} ({cols_str}) VALUES ({values_str})"
                self.connection.execute(insert_sql)
        except Exception:
            # If insert fails, raise or handle appropriately
            pass

    def query_data(
        self, schema_name: str, table_name: str, **filters: Any
    ) -> List[Dict[str, Any]]:
        """Query data from table."""
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            return self.schemas[schema_name].tables[table_name].query_data()
        return []

    def query_table(
        self, schema: str, table: str, filter_expr: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query data with type safety."""
        # Check in-memory registry first (fast path)
        if schema in self.schemas and table in self.schemas[schema].tables:
            return self.schemas[schema].tables[table].query_data(filter_expr)

        # For persistent tables not in registry, query DuckDB directly
        try:
            # Try different query formats - DuckDB might store tables differently
            # First try with schema qualification (standard format)
            schema_quoted = f'"{schema}"' if not schema.isalnum() else schema
            table_quoted = f'"{table}"' if not table.isalnum() else table

            queries_to_try = [
                f"SELECT * FROM {schema_quoted}.{table_quoted}",
                f'SELECT * FROM {schema_quoted}."{table}"',
                f'SELECT * FROM "{schema}"."{table}"',
            ]

            if filter_expr:
                # Add WHERE clause to each query variant
                queries_to_try = [f"{q} WHERE {filter_expr}" for q in queries_to_try]

            for query in queries_to_try:
                try:
                    result = self.connection.execute(query).fetchall()
                    description = self.connection.description
                    columns = [desc[0] for desc in description] if description else []
                    return [dict(zip(columns, row)) for row in result]
                except Exception:
                    # Try next query format
                    continue

            # If all queries failed, return empty
            return []
        except Exception:
            # If query fails, return empty list
            return []

    def get_table_schema(self, schema_name: str, table_name: str) -> MockStructType:
        """Get table schema."""
        from ...spark_types import MockStructField, MockStructType
        from ...spark_types import BooleanType, IntegerType, DoubleType, StringType

        # Check in-memory registry first (fast path)
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            return self.schemas[schema_name].tables[table_name].get_schema()

        # For persistent tables not in registry, query DuckDB directly
        try:
            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            # Try to get table columns using SQLAlchemy inspector
            columns = inspector.get_columns(table_name, schema=schema_name)

            if columns:
                # Convert SQLAlchemy column info to MockStructType
                fields = []
                for col in columns:
                    col_name = col["name"]
                    col_type = col["type"]

                    # Map SQLAlchemy types to MockSpark types
                    if hasattr(col_type, "python_type"):
                        py_type = col_type.python_type
                        if py_type is bool:
                            mock_type = BooleanType(nullable=col.get("nullable", True))
                        elif py_type is int:
                            mock_type = IntegerType(nullable=col.get("nullable", True))
                        elif py_type is float:
                            mock_type = DoubleType(nullable=col.get("nullable", True))
                        else:
                            mock_type = StringType(nullable=col.get("nullable", True))
                    else:
                        mock_type = StringType(nullable=col.get("nullable", True))

                    fields.append(
                        MockStructField(
                            col_name, mock_type, nullable=col.get("nullable", True)
                        )
                    )

                return MockStructType(fields)
        except Exception:
            pass

        # Return empty schema if table doesn't exist
        return MockStructType([])

    def get_data(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get all data from table."""
        return self.query_table(schema, table)

    def create_temp_view(self, name: str, dataframe: Any) -> None:
        """Create temporary view with type safety."""
        schema = "default"
        self.create_schema(schema)

        # Convert DataFrame data to table format
        data = dataframe.data
        schema_obj = dataframe.schema

        # Create the table
        self.create_table(schema, name, schema_obj)

        # Insert the data
        self.insert_data(schema, name, data)

    def get_table(self, schema: str, table: str) -> Optional[DuckDBTable]:
        """Get an existing table."""
        if schema not in self.schemas:
            return None
        return self.schemas[schema].tables.get(table)

    def list_tables(self, schema_name: Optional[str] = None) -> List[str]:
        """List tables in schema."""
        if schema_name is None:
            # List tables from all schemas
            all_tables = []
            for schema in self.schemas.values():
                all_tables.extend(schema.list_tables())
            return all_tables

        if schema_name not in self.schemas:
            return []
        return self.schemas[schema_name].list_tables()

    def get_table_metadata(self, schema_name: str, table_name: str) -> Dict[str, Any]:
        """Get table metadata including Delta-specific fields."""
        if schema_name not in self.schemas:
            return {}
        if table_name not in self.schemas[schema_name].tables:
            return {}
        return self.schemas[schema_name].tables[table_name].get_metadata()

    def update_table_metadata(
        self, schema_name: str, table_name: str, metadata_updates: Dict[str, Any]
    ) -> None:
        """Update table metadata fields."""
        if (
            schema_name in self.schemas
            and table_name in self.schemas[schema_name].tables
        ):
            table_obj = self.schemas[schema_name].tables[table_name]
            table_obj._metadata.update(metadata_updates)

    def get_database_info(self) -> Dict[str, Any]:
        """Get database information with type safety."""
        tables = {}
        total_tables = 0

        for schema_name, schema in self.schemas.items():
            schema_tables = schema.list_tables()
            tables.update({f"{schema_name}.{table}": table for table in schema_tables})
            total_tables += len(schema_tables)

        return {
            "database_path": self.db_path,
            "tables": tables,
            "total_tables": total_tables,
            "schemas": list(self.schemas.keys()),
            "storage_engine": "DuckDB",
            "type_safety": "SQLAlchemy + Dataclasses",
        }

    def execute_analytical_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute complex analytical queries with DuckDB's optimizer."""
        start_time = time.time()

        try:
            result = self.connection.execute(query).fetchall()
            description = self.connection.description
            columns = [desc[0] for desc in description] if description else []
            data = [dict(zip(columns, row)) for row in result]

            (time.time() - start_time) * 1000

            return data

        except Exception as e:
            raise ValueError(f"Analytical query failed: {e}") from e

    def close(self) -> None:
        """Close connections with proper cleanup."""
        if self.connection:
            try:
                # For persistent databases, don't drop schemas - preserve data
                # Only clean up if in-memory
                if self.is_in_memory:
                    # Clean up all schemas and tables for in-memory storage
                    for schema_name in list(self.schemas.keys()):
                        if schema_name != "default":
                            try:
                                self.drop_schema(schema_name)
                            except:  # noqa: E722
                                pass

                # Close SQLAlchemy engine connections FIRST (they may reference the connection)
                # Dispose all connections in the pool to ensure they're fully closed
                if hasattr(self, "engine") and self.engine:
                    self.engine.dispose(close=True)  # Close all connections in the pool

                # Close the direct DuckDB connection
                self.connection.close()
                self.connection = None  # type: ignore[assignment]
            except Exception:
                pass  # Ignore errors during cleanup

        # Clean up unique temp directory if it exists
        if self._temp_dir:
            try:
                import os
                import shutil

                if os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None
            except:  # noqa: E722
                pass  # Ignore cleanup errors

    def __del__(self) -> None:
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:  # noqa: E722
            pass

    def __enter__(self) -> "DuckDBStorageManager":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()

    def cleanup_temp_tables(self) -> None:
        """Clean up temporary tables to free memory."""
        try:
            # Drop all temporary tables
            result = self.connection.execute("SHOW TABLES").fetchall()
            for table_info in result:
                table_name = table_info[0]
                if table_name.startswith("temp_"):
                    self.connection.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    def optimize_storage(self) -> None:
        """Optimize storage by cleaning up and compacting data."""
        try:
            # Analyze tables for better query planning
            result = self.connection.execute("SHOW TABLES").fetchall()
            for table_info in result:
                table_name = table_info[0]
                self.connection.execute(f"ANALYZE {table_name}")
        except Exception:
            pass

    def get_memory_usage(self) -> Dict[str, Any]:
        """Get current memory usage statistics."""
        try:
            import psutil
            import os

            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            return {
                "rss": memory_info.rss,
                "vms": memory_info.vms,
                "percent": process.memory_percent(),
                "available": psutil.virtual_memory().available,
                "total": psutil.virtual_memory().total,
            }
        except Exception:
            return {"rss": 0, "vms": 0, "percent": 0, "available": 0, "total": 0}

    def force_garbage_collection(self) -> None:
        """Force garbage collection to free memory."""
        import gc

        gc.collect()

    def get_table_sizes(self) -> Dict[str, int]:
        """Get estimated sizes of all tables."""
        sizes = {}
        try:
            result = self.connection.execute("SHOW TABLES").fetchall()
            for table_info in result:
                table_name = table_info[0]
                # Get row count as size estimate
                count_result = self.connection.execute(
                    f"SELECT COUNT(*) FROM {table_name}"
                ).fetchone()
                sizes[table_name] = count_result[0] if count_result else 0
        except Exception:
            pass
        return sizes

    def cleanup_old_tables(self, max_age_hours: int = 24) -> int:
        """Clean up tables older than specified age."""
        # For in-memory storage, this is not applicable
        # But we can clean up temporary tables
        if self.is_in_memory:
            self.cleanup_temp_tables()
            return 0

        # For persistent storage, we could implement age-based cleanup
        # For now, just clean up temp tables
        self.cleanup_temp_tables()
        return 0

    def set_current_schema(self, schema: str) -> None:
        """Set the current schema."""
        if schema not in self.schemas:
            raise ValueError(f"Schema '{schema}' does not exist")
        self._current_schema = schema

    def get_current_schema(self) -> str:
        """Get the current schema."""
        return self._current_schema

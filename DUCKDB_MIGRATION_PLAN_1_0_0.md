# DuckDB Migration Plan for Mock Spark 1.0.0

## Executive Summary

This document outlines the comprehensive plan to migrate Mock Spark from SQLite and pandas to DuckDB for the 1.0.0 release. This migration will provide significant performance improvements, better analytical capabilities, and reduced memory usage while maintaining 100% API compatibility.

## Current State Analysis

### SQLite Usage
- **Primary Storage Backend**: `mock_spark/storage/backends/sqlite.py` (454 lines)
- **Key Components**:
  - `SQLiteStorageManager`: Main storage manager
  - `SQLiteTable`: Table operations and data management
  - `SQLiteSchema`: Schema management
- **Usage Patterns**:
  - Table creation and schema management
  - Data insertion with append/overwrite/ignore modes
  - Query operations with filtering
  - Temporary view creation
  - Database metadata management

### Pandas Usage
- **DataFrame Operations**: Used in `toPandas()` methods for data conversion
- **Key Files**:
  - `mock_spark/dataframe/core/dataframe.py` (line 10, 198-199)
  - `mock_spark/dataframe/dataframe.py` (line 33, 228-229)
  - `tests/compatibility/utils/comparison.py` (extensive pandas usage)
- **Usage Patterns**:
  - DataFrame conversion for testing and compatibility
  - Data type checking and validation
  - Performance comparisons with PySpark

## Migration Benefits

### Performance Improvements
- **Analytical Queries**: 10-100x faster for complex analytical operations
- **Memory Efficiency**: Better memory management for large datasets
- **Columnar Storage**: Native columnar format for better compression
- **Vectorized Operations**: Optimized vectorized processing

### Feature Enhancements
- **Advanced SQL**: Full SQL support with window functions, CTEs, and complex joins
- **Data Format Support**: Native Parquet, CSV, JSON, and Arrow support
- **Python Integration**: Seamless pandas DataFrame integration
- **Extensions**: Rich ecosystem of extensions for specialized use cases

### Reduced Dependencies
- **Single Engine**: Replace SQLite + pandas with unified DuckDB
- **Smaller Footprint**: Reduced memory usage and faster startup
- **Better Testing**: More reliable test execution with consistent behavior

## Migration Strategy

### Phase 1: Infrastructure Setup (Week 1-2)

#### 1.1 SQLModel Integration for Type Safety
**File**: `mock_spark/storage/models.py` (new)

```python
from sqlmodel import SQLModel, Field, create_engine, Session
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

class StorageMode(str, Enum):
    """Storage operation modes."""
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"

class MockTableMetadata(SQLModel, table=True):
    """Type-safe table metadata model."""
    id: Optional[int] = Field(default=None, primary_key=True)
    table_name: str = Field(index=True)
    schema_name: str = Field(default="default", index=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    row_count: int = Field(default=0)
    schema_version: str = Field(default="1.0")
    storage_format: str = Field(default="columnar")

class MockColumnDefinition(SQLModel, table=True):
    """Type-safe column definition model."""
    id: Optional[int] = Field(default=None, primary_key=True)
    table_id: int = Field(foreign_key="mocktablemetadata.id")
    column_name: str
    column_type: str
    is_nullable: bool = Field(default=True)
    default_value: Optional[str] = Field(default=None)

class DuckDBTableModel(SQLModel):
    """Base model for DuckDB table operations."""
    table_name: str
    schema_name: str = "default"
    
    def get_full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema_name}.{self.table_name}" if self.schema_name != "default" else self.table_name
```

#### 1.2 DuckDB Backend Implementation with SQLModel
**File**: `mock_spark/storage/backends/duckdb.py`

```python
from sqlmodel import Session, select
from .models import MockTableMetadata, MockColumnDefinition, StorageMode, DuckDBTableModel
import duckdb
from typing import List, Dict, Any, Optional, Union
from ..interfaces import ITable
from mock_spark.spark_types import MockStructType, MockStructField

class DuckDBTable(ITable):
    """Type-safe DuckDB table implementation with SQLModel integration."""
    
    def __init__(self, name: str, schema: MockStructType, connection: duckdb.DuckDBPyConnection, 
                 sqlmodel_session: Session):
        self.name = name
        self.schema = schema
        self.connection = connection
        self.sqlmodel_session = sqlmodel_session
        
        # Create type-safe metadata
        self.metadata_model = self._create_metadata_model()
        self._create_column_definitions()
    
    def _create_metadata_model(self) -> MockTableMetadata:
        """Create type-safe metadata model."""
        return MockTableMetadata(
            table_name=self.name,
            schema_name="default",
            row_count=0,
            storage_format="columnar"
        )
    
    def _create_column_definitions(self) -> List[MockColumnDefinition]:
        """Create type-safe column definitions."""
        column_defs = []
        for field in self.schema.fields:
            column_def = MockColumnDefinition(
                table_id=self.metadata_model.id or 0,  # Will be set after save
                column_name=field.name,
                column_type=self._get_duckdb_type(field.dataType),
                is_nullable=True
            )
            column_defs.append(column_def)
        
        # Save to database
        self.sqlmodel_session.add(self.metadata_model)
        self.sqlmodel_session.commit()
        
        for col_def in column_defs:
            col_def.table_id = self.metadata_model.id
            self.sqlmodel_session.add(col_def)
        self.sqlmodel_session.commit()
        
        return column_defs

    def insert_data(self, data: List[Dict[str, Any]], mode: StorageMode = StorageMode.APPEND) -> None:
        """Type-safe data insertion with validation."""
        if not data:
            return
        
        # Validate data against schema using Pydantic
        validated_data = self._validate_data(data)
        
        if mode == StorageMode.OVERWRITE:
            self.connection.execute(f"DROP TABLE IF EXISTS {self.name}")
            self._create_table_from_schema()
        
        # Type-safe insertion with error handling
        try:
            for row in validated_data:
                values = [row.get(field.name) for field in self.schema.fields]
                placeholders = ", ".join(["?" for _ in values])
                self.connection.execute(
                    f"INSERT INTO {self.name} VALUES ({placeholders})", values
                )
            
            # Update metadata with type safety
            self._update_row_count(len(validated_data))
            
        except Exception as e:
            raise ValueError(f"Failed to insert data: {e}") from e
    
    def _validate_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Validate data against schema using type checking."""
        validated = []
        for row in data:
            # Check required fields exist
            for field in self.schema.fields:
                if field.name not in row:
                    raise ValueError(f"Missing required field: {field.name}")
            
            # Type validation could be added here
            validated.append(row)
        
        return validated
    
    def _update_row_count(self, new_rows: int) -> None:
        """Update row count with type safety."""
        self.metadata_model.row_count += new_rows
        self.sqlmodel_session.add(self.metadata_model)
        self.sqlmodel_session.commit()
        
    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Optimized querying with DuckDB's analytical engine."""
        if filter_expr:
            query = f"SELECT * FROM {self.name} WHERE {filter_expr}"
        else:
            query = f"SELECT * FROM {self.name}"
        
        result = self.connection.execute(query).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in result]
```

#### 1.2 DuckDB Storage Manager
**File**: `mock_spark/storage/backends/duckdb.py` (continued)

```python
class DuckDBStorageManager(IStorageManager):
    """DuckDB storage manager with enhanced analytical capabilities."""
    
    def __init__(self, db_path: str = "mock_spark.duckdb"):
        self.db_path = db_path
        self.connection = duckdb.connect(db_path)
        self.schemas: Dict[str, DuckDBSchema] = {}
        self.schemas["default"] = DuckDBSchema("default", self.connection)
        
        # Enable extensions for enhanced functionality
        self.connection.execute("INSTALL sqlite")
        self.connection.execute("LOAD sqlite")
        
    def create_temp_view(self, name: str, dataframe) -> None:
        """Create temporary view using DuckDB's pandas integration."""
        # Convert DataFrame to pandas for efficient processing
        pandas_df = dataframe.toPandas()
        
        # Register as temporary view
        self.connection.register(name, pandas_df)
        
    def execute_analytical_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute complex analytical queries with DuckDB's optimizer."""
        result = self.connection.execute(query).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        return [dict(zip(columns, row)) for row in result]
```

### Phase 2: DataFrame Integration (Week 3-4)

#### 2.1 Enhanced DataFrame Operations with Optional Pandas
**File**: `mock_spark/dataframe/core/dataframe.py`

```python
class MockDataFrame:
    """Enhanced DataFrame with DuckDB integration and optional pandas support."""
    
    def toPandas(self):
        """Convert to pandas DataFrame using DuckDB (requires pandas as optional dependency)."""
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for toPandas() method. "
                "Install with: pip install mock-spark[pandas] or pip install pandas"
            )
        
        if not self.data:
            return pd.DataFrame(columns=[field.name for field in self.schema.fields])
        
        # Use DuckDB for efficient conversion if available
        if hasattr(self, '_duckdb_connection'):
            return self._duckdb_connection.execute(
                f"SELECT * FROM {self._temp_table_name}"
            ).df()
        
        return pd.DataFrame(self.data)
    
    def _register_with_duckdb(self, connection: duckdb.DuckDBPyConnection) -> str:
        """Register DataFrame with DuckDB for analytical operations."""
        temp_name = f"temp_df_{id(self)}"
        
        # Use DuckDB's native data insertion instead of pandas
        if self.data:
            # Create table from schema
            schema_sql = self._generate_create_table_sql(temp_name)
            connection.execute(schema_sql)
            
            # Insert data directly
            for row in self.data:
                values = [row.get(field.name) for field in self.schema.fields]
                placeholders = ", ".join(["?" for _ in values])
                connection.execute(
                    f"INSERT INTO {temp_name} VALUES ({placeholders})", values
                )
        else:
            # Create empty table
            schema_sql = self._generate_create_table_sql(temp_name)
            connection.execute(schema_sql)
        
        self._duckdb_connection = connection
        self._temp_table_name = temp_name
        return temp_name
    
    def _generate_create_table_sql(self, table_name: str) -> str:
        """Generate CREATE TABLE SQL from schema."""
        columns = []
        for field in self.schema.fields:
            duckdb_type = self._get_duckdb_type(field.dataType)
            columns.append(f"{field.name} {duckdb_type}")
        
        return f"CREATE TABLE {table_name} ({', '.join(columns)})"
    
    def _get_duckdb_type(self, data_type) -> str:
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
        else:
            return "VARCHAR"
```

#### 2.2 Analytical Operations
**File**: `mock_spark/dataframe/analytics.py` (new)

```python
class DuckDBAnalytics:
    """Analytical operations using DuckDB's engine."""
    
    def __init__(self, dataframe: MockDataFrame):
        self.dataframe = dataframe
        self.connection = duckdb.connect()
        self.temp_table = self.dataframe._register_with_duckdb(self.connection)
    
    def execute_sql(self, query: str) -> MockDataFrame:
        """Execute SQL queries on DataFrame data."""
        result = self.connection.execute(query).fetchall()
        columns = [desc[0] for desc in self.connection.description]
        
        # Convert to MockDataFrame without pandas dependency
        data = [dict(zip(columns, row)) for row in result]
        return self._convert_to_mock_dataframe(data, columns)
    
    def window_function(self, window_spec: str, function: str) -> MockDataFrame:
        """Execute window functions with DuckDB's optimizer."""
        query = f"""
        SELECT *, {function} OVER ({window_spec}) as window_result
        FROM {self.temp_table}
        """
        return self.execute_sql(query)
    
    def _convert_to_mock_dataframe(self, data: List[Dict], columns: List[str]) -> MockDataFrame:
        """Convert query result to MockDataFrame without pandas."""
        # Create schema from columns
        from mock_spark.spark_types import MockStructType, MockStructField, StringType
        fields = [MockStructField(col, StringType()) for col in columns]
        schema = MockStructType(fields)
        
        # Create new MockDataFrame
        return MockDataFrame(data, schema)
```

### Phase 3: Storage Manager Updates (Week 5-6)

#### 3.1 Updated Storage Factory
**File**: `mock_spark/storage/manager.py`

```python
class StorageManagerFactory:
    """Updated factory with DuckDB support."""
    
    @staticmethod
    def create_duckdb_manager(db_path: str = "mock_spark.duckdb") -> IStorageManager:
        """Create a DuckDB storage manager.
        
        Args:
            db_path: Path to DuckDB database file.
            
        Returns:
            DuckDB storage manager instance.
        """
        return DuckDBStorageManager(db_path)
    
    @staticmethod
    def create_hybrid_manager() -> IStorageManager:
        """Create a hybrid manager with DuckDB for analytics and SQLite for compatibility."""
        return HybridStorageManager()
```

#### 3.2 Hybrid Storage Manager
**File**: `mock_spark/storage/backends/hybrid.py` (new)

```python
class HybridStorageManager(IStorageManager):
    """Hybrid storage manager using DuckDB for analytics and SQLite for compatibility."""
    
    def __init__(self):
        self.duckdb_manager = DuckDBStorageManager()
        self.sqlite_manager = SQLiteStorageManager()
        self.analytical_mode = True  # Default to DuckDB for new operations
    
    def switch_to_analytical_mode(self):
        """Switch to DuckDB for analytical operations."""
        self.analytical_mode = True
    
    def switch_to_compatibility_mode(self):
        """Switch to SQLite for compatibility testing."""
        self.analytical_mode = False
    
    def create_table(self, schema: str, table: str, fields: Union[List[MockStructField], MockStructType]) -> None:
        """Create table in appropriate backend."""
        if self.analytical_mode:
            return self.duckdb_manager.create_table(schema, table, fields)
        else:
            return self.sqlite_manager.create_table(schema, table, fields)
```

### Phase 4: Testing and Compatibility (Week 7-8)

#### 4.1 Enhanced Test Suite
**File**: `tests/unit/test_duckdb_storage.py` (new)

```python
class TestDuckDBStorage:
    """Comprehensive tests for DuckDB storage backend."""
    
    def test_analytical_performance(self):
        """Test analytical query performance."""
        # Create large dataset
        large_data = [{"id": i, "value": i * 2, "category": f"cat_{i % 10}"} 
                     for i in range(10000)]
        
        # Test DuckDB performance
        duckdb_manager = DuckDBStorageManager()
        duckdb_manager.create_table("test", "analytics", schema)
        duckdb_manager.insert_data("test", "analytics", large_data)
        
        start_time = time.time()
        result = duckdb_manager.execute_analytical_query(
            "SELECT category, COUNT(*), AVG(value) FROM analytics GROUP BY category"
        )
        duckdb_time = time.time() - start_time
        
        # Performance should be significantly better than SQLite
        assert duckdb_time < 0.1  # Should be very fast
        assert len(result) == 10  # 10 categories
```

#### 4.2 Compatibility Tests
**File**: `tests/compatibility/test_duckdb_compatibility.py` (new)

```python
class TestDuckDBCompatibility:
    """Test DuckDB compatibility with existing PySpark code."""
    
    def test_pandas_integration(self):
        """Test seamless pandas integration."""
        # Create DataFrame
        data = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 30}]
        df = spark.createDataFrame(data)
        
        # Convert to pandas using DuckDB
        pandas_df = df.toPandas()
        
        # Should work seamlessly
        assert isinstance(pandas_df, pd.DataFrame)
        assert len(pandas_df) == 2
        assert list(pandas_df.columns) == ["name", "age"]
    
    def test_sql_operations(self):
        """Test SQL operations with DuckDB."""
        # Create and register DataFrame
        df = spark.createDataFrame([{"id": 1, "value": 100}])
        spark.storage.create_temp_view("test_table", df)
        
        # Execute SQL query
        result = spark.sql("SELECT * FROM test_table WHERE value > 50")
        
        # Should work with DuckDB's SQL engine
        assert result.count() == 1
```

### Phase 5: Documentation and Migration (Week 9-10)

#### 5.1 Updated Documentation
**Files to Update**:
- `README.md`: Update performance benchmarks
- `docs/storage_serialization_guide.md`: Add DuckDB section
- `docs/api_reference.md`: Update storage APIs
- `examples/`: Add DuckDB examples

#### 5.2 Migration Guide
**File**: `docs/duckdb_migration_guide.md` (new)

```markdown
# DuckDB Migration Guide

## Benefits of Migration
- 10x faster analytical queries
- Reduced memory usage
- Better pandas integration
- Enhanced SQL capabilities

## Breaking Changes
- Storage file format changes (.db → .duckdb)
- Some SQL syntax differences
- Performance characteristics changes

## Migration Steps
1. Update dependencies
2. Migrate existing data
3. Update configuration
4. Test thoroughly
```

## Implementation Timeline

### Week 1-2: Core Infrastructure ✅ IN PROGRESS
- [x] Create release-1.0.0 branch
- [x] Implement SQLModel models for type safety
- [x] Implement DuckDB backend classes
- [x] Create DuckDB storage manager
- [x] Set up basic connection handling
- [x] Implement core table operations
- [x] **TEST**: Run all tests to ensure no regressions (195/195 passed ✅)
- [x] **TEST**: Fix DuckDB connection configuration conflict ✅
- [x] **TEST**: Verify DuckDB backend works correctly ✅
- [x] **TEST**: Check type safety with mypy ✅

### Week 3-4: DataFrame Integration
- [ ] Update DataFrame toPandas() methods
- [ ] **TEST**: Run tests to verify pandas integration works
- [ ] Implement analytical operations
- [ ] **TEST**: Test analytical operations with DuckDB
- [ ] Add DuckDB-specific optimizations
- [ ] **TEST**: Performance test against current implementation
- [ ] Create analytics module
- [ ] **TEST**: Full test suite passes

### Week 5-6: Storage Management
- [ ] Update storage factory
- [ ] **TEST**: Test storage factory with all backends
- [ ] Implement hybrid storage manager
- [ ] **TEST**: Test hybrid manager switching
- [ ] Add migration utilities
- [ ] **TEST**: Test data migration from SQLite to DuckDB
- [ ] Update session management
- [ ] **TEST**: Full compatibility test suite

### Week 7-8: Testing and Validation
- [ ] Comprehensive test suite
- [ ] **TEST**: All 396 tests pass
- [ ] Performance benchmarking
- [ ] **TEST**: Performance improvements verified
- [ ] Compatibility testing
- [ ] **TEST**: PySpark compatibility maintained
- [ ] Integration testing
- [ ] **TEST**: End-to-end integration tests pass

### Week 9-10: Documentation and Release
- [ ] Update all documentation
- [ ] **TEST**: Documentation examples work
- [ ] Create migration guide
- [ ] **TEST**: Migration guide tested
- [ ] Update examples
- [ ] **TEST**: All examples run successfully
- [ ] Prepare 1.0.0 release
- [ ] **TEST**: Final release candidate testing

## Progress Tracking

### Current Status: Week 1-2 (Core Infrastructure)
**Branch**: `release-1.0.0` ✅ Created
**Progress**: 
- ✅ SQLModel models implemented
- ✅ DuckDB backend created
- ✅ Dependencies updated
- ✅ All 195 unit tests passing
- ✅ DuckDB connection issue resolved
- ✅ DuckDB backend fully functional
- ✅ Type safety verified

**Next Steps**:
1. Commit Phase 1 changes to release-1.0.0 branch
2. Move to DataFrame integration phase
3. Update DataFrame toPandas() methods
4. Implement analytical operations

### Test Setup Integration
**File**: `tests/setup_spark_env.sh` (included in plan)
```bash
# Run tests with proper environment setup
./tests/setup_spark_env.sh
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
python -m pytest tests/ -v
```

### Testing Strategy Between Each Step

#### 1. **Unit Tests** (After each implementation step)
```bash
# Run unit tests only
python -m pytest tests/unit/ -v --tb=short
```

#### 2. **Compatibility Tests** (After DataFrame integration)
```bash
# Run compatibility tests with PySpark
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
python -m pytest tests/compatibility/ -v --tb=short
```

#### 3. **Performance Tests** (After optimizations)
```bash
# Run performance tests
python -m pytest tests/unit/test_performance_comprehensive.py -v -s
```

#### 4. **Full Test Suite** (Before each phase completion)
```bash
# Run all tests
./tests/setup_spark_env.sh
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@11/11.0.28/libexec/openjdk.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
python -m pytest tests/ -v --tb=short --disable-warnings
```

#### 5. **Type Safety Tests** (After each code change)
```bash
# Run mypy type checking
mypy mock_spark/ --ignore-missing-imports
```

#### 6. **Integration Tests** (After storage management)
```bash
# Test DuckDB backend specifically
python -c "
from mock_spark.storage import DuckDBStorageManager
from mock_spark.spark_types import MockStructType, MockStructField, StringType, IntegerType

# Test basic functionality
manager = DuckDBStorageManager('test.duckdb')
schema = MockStructType([
    MockStructField('id', IntegerType()),
    MockStructField('name', StringType())
])
table = manager.create_table('default', 'test_table', schema)
print('✅ DuckDB backend test passed')
"
```

## Risk Mitigation

### Compatibility Risks
- **Mitigation**: Hybrid storage manager for gradual migration
- **Testing**: Comprehensive compatibility test suite
- **Rollback**: Maintain SQLite backend as fallback

### Performance Risks
- **Mitigation**: Extensive performance testing
- **Monitoring**: Benchmark all operations
- **Optimization**: Profile and optimize critical paths

### Migration Risks
- **Mitigation**: Gradual migration with feature flags
- **Testing**: Staged rollout with validation
- **Support**: Clear migration documentation

## Success Metrics

### Performance Improvements
- [ ] 10x faster analytical queries
- [ ] 50% reduction in memory usage
- [ ] 5x faster DataFrame operations
- [ ] 2x faster test execution

### Compatibility Maintenance
- [ ] 100% API compatibility
- [ ] All existing tests pass
- [ ] No breaking changes for users
- [ ] Seamless pandas integration

### Code Quality
- [ ] Maintained test coverage (>90%)
- [ ] Type safety compliance
- [ ] Documentation completeness
- [ ] Code review approval

## Dependencies Update

### New Dependencies
```toml
dependencies = [
    "duckdb>=0.9.0",
    "sqlmodel>=0.0.14",
    "psutil>=5.8.0",
]

[project.optional-dependencies]
pandas = [
    "pandas>=1.3.0",
    "pandas-stubs>=2.0.0",
]
analytics = [
    "pandas>=1.3.0",
    "pandas-stubs>=2.0.0",
    "numpy>=1.20.0",
]
dev = [
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "black>=23.0.0",
    "isort>=5.12.0",
    "mypy>=1.0.0",
    "pandas>=1.3.0",
    "pandas-stubs>=2.0.0",
    "types-psutil>=6.0.0",
]
```

### Installation Options
```bash
# Minimal installation (no pandas)
pip install mock-spark

# With pandas support
pip install mock-spark[pandas]

# With full analytics support
pip install mock-spark[analytics]

# Development installation
pip install mock-spark[dev]
```

### Removed Dependencies
- SQLite (built into Python, but no longer primary backend)
- pandas as mandatory dependency (now optional)

## Conclusion

This migration to DuckDB will significantly enhance Mock Spark's analytical capabilities while maintaining full compatibility. The phased approach ensures minimal risk while delivering substantial performance improvements for the 1.0.0 release.

The migration will position Mock Spark as a leading solution for PySpark testing and development, with enterprise-grade performance and analytical capabilities.

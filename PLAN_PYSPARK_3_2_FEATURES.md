# PySpark 3.2 Features Implementation Plan

**Created:** 2025-01-14  
**Target Version:** mock-spark 2.5.0  
**PySpark Version:** 3.2.0  

---

## Executive Summary

This plan outlines the implementation strategy for PySpark 3.2 features in mock-spark. The features are prioritized by impact and implementation complexity, with a focus on the most valuable additions for users.

### Implementation Phases

- **Phase 1 (Quick Wins):** Low complexity, high value features (2-3 weeks)
- **Phase 2 (Core Features):** Medium complexity, high usage features (4-6 weeks)
- **Phase 3 (Advanced Features):** High complexity, specialized features (6-8 weeks)

---

## Phase 1: Quick Wins (2-3 Weeks)

### 1.1 TIMESTAMPADD Function

**Priority:** High  
**Complexity:** Low  
**Estimated Effort:** 2-3 days  

#### Description
Add time units to a timestamp. Part of PySpark 3.3 but commonly used.

#### Implementation Details

**File:** `mock_spark/functions/datetime.py`

```python
@staticmethod
def timestampadd(unit: str, quantity: Union[int, MockColumn], timestamp: Union[str, MockColumn]) -> MockColumn:
    """
    Add time units to a timestamp.
    
    Args:
        unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND)
        quantity: Number of units to add
        timestamp: Timestamp column or literal
    
    Returns:
        MockColumn with timestamp added
    
    Example:
        >>> df.select(F.timestampadd("DAY", 7, F.col("created_at")))
    """
    # Implementation will use DuckDB's DATEADD function
    pass
```

**SQL Translation:**
```sql
-- PySpark
F.timestampadd("DAY", 7, "created_at")

-- DuckDB equivalent
DATEADD('DAY', 7, created_at)
```

**Tests:** `tests/unit/test_datetime_functions.py`
- Test all time units (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND)
- Test with positive and negative quantities
- Test with column references and literals
- Test edge cases (leap years, month boundaries)

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/datetime_functions.md`

---

### 1.2 TIMESTAMPDIFF Function

**Priority:** High  
**Complexity:** Low  
**Estimated Effort:** 2-3 days  

#### Description
Calculate the difference between two timestamps in specified units.

#### Implementation Details

**File:** `mock_spark/functions/datetime.py`

```python
@staticmethod
def timestampdiff(unit: str, start: Union[str, MockColumn], end: Union[str, MockColumn]) -> MockColumn:
    """
    Calculate difference between two timestamps.
    
    Args:
        unit: Time unit (YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND)
        start: Start timestamp column or literal
        end: End timestamp column or literal
    
    Returns:
        MockColumn with difference in specified units
    
    Example:
        >>> df.select(F.timestampdiff("DAY", F.col("start_date"), F.col("end_date")))
    """
    # Implementation will use DuckDB's DATEDIFF function
    pass
```

**SQL Translation:**
```sql
-- PySpark
F.timestampdiff("DAY", "start_date", "end_date")

-- DuckDB equivalent
DATEDIFF('DAY', start_date, end_date)
```

**Tests:** `tests/unit/test_datetime_functions.py`
- Test all time units
- Test with various date ranges
- Test edge cases (negative differences, same timestamps)
- Test with different time zones

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/datetime_functions.md`

---

### 1.3 Enhanced Error Messages

**Priority:** Medium  
**Complexity:** Low  
**Estimated Effort:** 3-4 days  

#### Description
Improve error messages to be more informative and match PySpark 3.3+ format.

#### Implementation Details

**Files to Modify:**
- `mock_spark/core/exceptions/analysis.py`
- `mock_spark/core/exceptions/__init__.py`
- `mock_spark/dataframe/dataframe.py`

**Enhancements:**
1. Add context to error messages (column names, table names, SQL query)
2. Include suggestions for common errors
3. Add error codes for programmatic handling
4. Improve formatting for readability

**Example Improvements:**

```python
# Before
raise AnalysisException("Column not found")

# After
raise AnalysisException(
    f"Column 'user_id' not found in table 'users'. "
    f"Available columns: {', '.join(available_columns)}. "
    f"Did you mean 'id' or 'user_name'?"
)
```

**Tests:** `tests/unit/test_exceptions.py`
- Test error message format
- Test error context inclusion
- Test error code consistency

**Documentation:**
- Add error handling guide to `docs/guides/error_handling.md`

---

### 1.4 Additional String Functions

**Priority:** Medium  
**Complexity:** Low  
**Estimated Effort:** 3-4 days  

#### Functions to Add

1. **regexp_extract_all** - Extract all matches of a regex pattern
2. **array_join** - Join array elements with a delimiter
3. **repeat** - Repeat a string N times
4. **initcap** - Capitalize first letter of each word
5. **soundex** - Soundex encoding for phonetic matching

#### Implementation Details

**File:** `mock_spark/functions/string.py`

```python
@staticmethod
def regexp_extract_all(col: Union[str, MockColumn], pattern: str, idx: int = 0) -> MockColumn:
    """Extract all matches of a regex pattern."""
    pass

@staticmethod
def array_join(col: Union[str, MockColumn], delimiter: str, null_replacement: str = None) -> MockColumn:
    """Join array elements with a delimiter."""
    pass

@staticmethod
def repeat(col: Union[str, MockColumn], n: int) -> MockColumn:
    """Repeat a string N times."""
    pass

@staticmethod
def initcap(col: Union[str, MockColumn]) -> MockColumn:
    """Capitalize first letter of each word."""
    pass

@staticmethod
def soundex(col: Union[str, MockColumn]) -> MockColumn:
    """Soundex encoding for phonetic matching."""
    pass
```

**SQL Translation:**
```sql
-- regexp_extract_all
regexp_extract_all(column, 'pattern', 0)

-- array_join
array_to_string(array_column, delimiter, null_replacement)

-- repeat
repeat(column, n)

-- initcap
initcap(column)

-- soundex
soundex(column)
```

**Tests:** `tests/unit/test_string_functions.py`
- Test each function with various inputs
- Test edge cases (empty strings, null values)
- Test with unicode characters
- Test performance with large strings

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/string_functions.md`

---

## Phase 2: Core Features (4-6 Weeks)

### 2.1 Pandas API on Spark - mapInPandas

**Priority:** High  
**Complexity:** High  
**Estimated Effort:** 1-2 weeks  

#### Description
Apply a Python iterator of pandas DataFrames to each partition.

#### Implementation Details

**File:** `mock_spark/dataframe/dataframe.py`

```python
def mapInPandas(
    self,
    func: Callable[[pd.DataFrame], Iterator[pd.DataFrame]],
    schema: Union[StructType, str]
) -> "MockDataFrame":
    """
    Map an iterator of pandas DataFrames to another iterator of pandas DataFrames.
    
    Args:
        func: Function that takes an iterator of pandas DataFrames and returns
              an iterator of pandas DataFrames
        schema: Output schema
    
    Returns:
        MockDataFrame with transformed data
    
    Example:
        >>> def multiply_by_two(iterator):
        ...     for df in iterator:
        ...         yield df * 2
        >>> df.mapInPandas(multiply_by_two, schema="value int")
    """
    # Implementation strategy:
    # 1. Convert MockDataFrame to pandas
    # 2. Create iterator of DataFrames per partition
    # 3. Apply function to iterator
    # 4. Convert result back to MockDataFrame
    pass
```

**Implementation Strategy:**

1. **Partition Handling:**
   - For mock-spark, partitions are conceptual
   - Process entire DataFrame as single partition
   - Or split into logical partitions based on config

2. **Schema Handling:**
   - Parse schema (StructType or DDL string)
   - Validate output schema matches
   - Handle schema inference if not provided

3. **Pandas Conversion:**
   - Convert MockDataFrame to pandas DataFrame
   - Apply function
   - Convert result back to MockDataFrame
   - Preserve schema information

4. **Error Handling:**
   - Validate function signature
   - Handle pandas conversion errors
   - Provide helpful error messages

**Tests:** `tests/unit/test_pandas_api.py`
- Test basic mapInPandas operations
- Test with different schemas
- Test with multi-column DataFrames
- Test error handling (invalid function, schema mismatch)
- Test with empty DataFrames
- Test with large DataFrames

**Documentation:**
- Add comprehensive guide to `docs/guides/pandas_api.md`
- Add examples to `docs/api_reference.md`
- Add migration guide from pandas to PySpark

---

### 2.2 Pandas API on Spark - applyInPandas

**Priority:** High  
**Complexity:** High  
**Estimated Effort:** 1-2 weeks  

#### Description
Apply a Python function to each group of a GroupedData.

#### Implementation Details

**File:** `mock_spark/dataframe/grouped/base.py`

```python
class MockGroupedData:
    def applyInPandas(
        self,
        func: Callable[[pd.DataFrame], pd.DataFrame],
        schema: Union[StructType, str]
    ) -> "MockDataFrame":
        """
        Apply a Python function to each group.
        
        Args:
            func: Function that takes a pandas DataFrame and returns a pandas DataFrame
            schema: Output schema
        
        Returns:
            MockDataFrame with transformed data
        
        Example:
            >>> def normalize_group(df):
            ...     df['value'] = (df['value'] - df['value'].mean()) / df['value'].std()
            ...     return df
            >>> df.groupBy("category").applyInPandas(normalize_group, schema=df.schema)
        """
        # Implementation strategy:
        # 1. Group DataFrame by grouping columns
        # 2. Convert each group to pandas DataFrame
        # 3. Apply function to each group
        # 4. Combine results back into MockDataFrame
        pass
```

**Implementation Strategy:**

1. **Grouping:**
   - Extract groups from MockGroupedData
   - Each group becomes a separate pandas DataFrame

2. **Function Application:**
   - Apply function to each group
   - Validate output schema for each group
   - Handle empty groups

3. **Result Combination:**
   - Concatenate all group results
   - Preserve grouping column information
   - Maintain schema consistency

4. **Performance:**
   - Optimize for small to medium-sized groups
   - Add progress tracking for large datasets
   - Consider parallel processing for large groups

**Tests:** `tests/unit/test_pandas_api.py`
- Test basic applyInPandas operations
- Test with different group sizes
- Test with single-column groups
- Test with multi-column groups
- Test schema validation
- Test error handling
- Test with empty groups
- Test with null values in grouping columns

**Documentation:**
- Add to `docs/guides/pandas_api.md`
- Add examples to `docs/api_reference.md`
- Add performance considerations

---

### 2.3 Pandas API on Spark - transform

**Priority:** High  
**Complexity:** Medium  
**Estimated Effort:** 1 week  

#### Description
Apply a function to each group and return a DataFrame with the same schema.

#### Implementation Details

**File:** `mock_spark/dataframe/grouped/base.py`

```python
class MockGroupedData:
    def transform(
        self,
        func: Callable[[pd.DataFrame], pd.DataFrame]
    ) -> "MockDataFrame":
        """
        Apply a function to each group and return a DataFrame with the same schema.
        
        Args:
            func: Function that takes a pandas DataFrame and returns a pandas DataFrame
                  with the same schema
        
        Returns:
            MockDataFrame with transformed data
        
        Example:
            >>> def add_mean(df):
            ...     df['mean_value'] = df['value'].mean()
            ...     return df
            >>> df.groupBy("category").transform(add_mean)
        """
        # Implementation strategy:
        # 1. Group DataFrame by grouping columns
        # 2. Apply function to each group
        # 3. Validate schema matches input
        # 4. Combine results maintaining row order
        pass
```

**Implementation Strategy:**

1. **Schema Preservation:**
   - Validate output schema matches input
   - Handle additional columns (optional)
   - Maintain column order

2. **Row Ordering:**
   - Preserve original row order within groups
   - Maintain group order

3. **Performance:**
   - Optimize for in-place transformations
   - Minimize data copying

**Tests:** `tests/unit/test_pandas_api.py`
- Test basic transform operations
- Test schema validation
- Test row ordering preservation
- Test with different group sizes
- Test error handling
- Test with null values

**Documentation:**
- Add to `docs/guides/pandas_api.md`
- Add examples to `docs/api_reference.md`

---

### 2.4 DataFrame.transform()

**Priority:** High  
**Complexity:** Medium  
**Estimated Effort:** 1 week  

#### Description
Apply a function to transform a DataFrame (functional programming style).

#### Implementation Details

**File:** `mock_spark/dataframe/dataframe.py`

```python
def transform(
    self,
    func: Callable[["MockDataFrame"], "MockDataFrame"]
) -> "MockDataFrame":
    """
    Apply a function to transform a DataFrame.
    
    Args:
        func: Function that takes a MockDataFrame and returns a MockDataFrame
    
    Returns:
        MockDataFrame with transformed data
    
    Example:
        >>> def add_columns(df):
        ...     return df.withColumn("sum", F.col("a") + F.col("b"))
        >>> df.transform(add_columns)
    """
    return func(self)
```

**Implementation Strategy:**

1. **Simple Implementation:**
   - Just call the function with self
   - Validate return type is MockDataFrame
   - Handle errors gracefully

2. **Type Hints:**
   - Use Callable type hints
   - Add generic type support for chaining

3. **Chaining Support:**
   - Enable method chaining
   - Support multiple transforms

**Tests:** `tests/unit/test_dataframe_operations.py`
- Test basic transform operations
- Test method chaining
- Test with different transformation functions
- Test error handling
- Test with lazy evaluation

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples showing chaining patterns

---

### 2.5 DataFrame.unpivot() / melt()

**Priority:** Medium  
**Complexity:** Medium  
**Estimated Effort:** 1 week  

#### Description
Unpivot columns into rows (opposite of pivot).

#### Implementation Details

**File:** `mock_spark/dataframe/dataframe.py`

```python
def unpivot(
    self,
    ids: Union[str, List[str]],
    values: Union[str, List[str]],
    variableColumnName: str = "variable",
    valueColumnName: str = "value"
) -> "MockDataFrame":
    """
    Unpivot columns into rows.
    
    Args:
        ids: Column(s) to keep as identifiers
        values: Column(s) to unpivot
        variableColumnName: Name for the variable column
        valueColumnName: Name for the value column
    
    Returns:
        MockDataFrame with unpivoted data
    
    Example:
        >>> df.unpivot(
        ...     ids=["id", "name"],
        ...     values=["Q1", "Q2", "Q3", "Q4"],
        ...     variableColumnName="quarter",
        ...     valueColumnName="sales"
        ... )
    """
    # Implementation strategy:
    # 1. Identify id and value columns
    # 2. Create rows for each value column
    # 3. Add variable column with column names
    # 4. Add value column with values
    pass
```

**Implementation Strategy:**

1. **Column Identification:**
   - Validate id columns exist
   - Validate value columns exist
   - Handle column name conflicts

2. **Row Generation:**
   - For each row, create N rows (one per value column)
   - Preserve id columns
   - Add variable column with column name
   - Add value column with value

3. **Schema Creation:**
   - Combine id columns
   - Add variable column (string)
   - Add value column (infer type from value columns)

**Tests:** `tests/unit/test_dataframe_operations.py`
- Test basic unpivot operations
- Test with single id column
- Test with multiple id columns
- Test with single value column
- Test with multiple value columns
- Test with custom column names
- Test with null values
- Test with different data types

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/dataframe_operations.md`

---

### 2.6 DEFAULT Column Values

**Priority:** High  
**Complexity:** Medium  
**Estimated Effort:** 1-2 weeks  

#### Description
Support for default values in table column definitions.

#### Implementation Details

**Files to Modify:**
- `mock_spark/core/ddl_adapter.py`
- `mock_spark/session/catalog.py`
- `mock_spark/spark_types.py`

**Schema Enhancement:**

```python
class MockStructField:
    def __init__(
        self,
        name: str,
        dataType: MockDataType,
        nullable: bool = True,
        metadata: Optional[Dict] = None,
        default_value: Optional[Any] = None  # NEW
    ):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable
        self.metadata = metadata or {}
        self.default_value = default_value
```

**DDL Parser Enhancement:**

```python
# Support in CREATE TABLE statements
CREATE TABLE users (
    id INT,
    name STRING,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status STRING DEFAULT 'active'
)
```

**Implementation Strategy:**

1. **Schema Storage:**
   - Store default values in MockStructField
   - Support literal values and expressions (CURRENT_TIMESTAMP, etc.)

2. **DDL Parsing:**
   - Parse DEFAULT clause in CREATE TABLE
   - Validate default value type matches column type
   - Support common default expressions

3. **Data Insertion:**
   - Apply defaults when inserting rows
   - Apply defaults when columns are missing
   - Allow explicit NULL to override defaults

4. **Catalog Integration:**
   - Store defaults in catalog
   - Retrieve defaults for table operations

**Tests:** `tests/unit/test_ddl_parser.py`
- Test CREATE TABLE with defaults
- Test literal default values
- Test expression defaults (CURRENT_TIMESTAMP)
- Test type validation
- Test INSERT with missing columns
- Test INSERT with explicit NULL
- Test ALTER TABLE ADD COLUMN with default

**Documentation:**
- Add to `docs/guides/ddl_operations.md`
- Add examples to `docs/api_reference.md`

---

## Phase 3: Advanced Features (6-8 Weeks)

### 3.1 Parameterized SQL Queries

**Priority:** High  
**Complexity:** High  
**Estimated Effort:** 2-3 weeks  

#### Description
Support for parameterized SQL queries with placeholders.

#### Implementation Details

**File:** `mock_spark/session/sql/query_executor.py`

```python
def sql(
    self,
    sqlQuery: str,
    *args: Any,
    **kwargs: Any
) -> "MockDataFrame":
    """
    Execute a SQL query with optional parameters.
    
    Args:
        sqlQuery: SQL query string with placeholders
        *args: Positional parameters
        **kwargs: Named parameters
    
    Returns:
        MockDataFrame with query results
    
    Example:
        >>> spark.sql("SELECT * FROM users WHERE age > ?", 18)
        >>> spark.sql("SELECT * FROM users WHERE age > :age", age=18)
    """
    # Implementation strategy:
    # 1. Parse SQL query
    # 2. Identify placeholders (? or :name)
    # 3. Replace placeholders with parameters
    # 4. Execute modified query
    pass
```

**Implementation Strategy:**

1. **Placeholder Support:**
   - Support `?` for positional parameters
   - Support `:name` for named parameters
   - Support `%(name)s` for named parameters (alternative syntax)

2. **Parameter Binding:**
   - Validate parameter count matches placeholders
   - Type checking and conversion
   - SQL injection prevention

3. **Query Execution:**
   - Replace placeholders with safe values
   - Execute modified query
   - Return results

4. **Performance:**
   - Query caching for repeated queries
   - Prepared statement simulation

**Tests:** `tests/unit/test_sql_queries.py`
- Test positional parameters
- Test named parameters
- Test mixed parameters
- Test parameter validation
- Test SQL injection prevention
- Test with different data types
- Test with complex queries (joins, subqueries)
- Test error handling

**Documentation:**
- Add to `docs/guides/sql_operations.md`
- Add security best practices
- Add examples to `docs/api_reference.md`

---

### 3.2 ORDER BY ALL / GROUP BY ALL

**Priority:** Medium  
**Complexity:** Medium  
**Estimated Effort:** 1-2 weeks  

#### Description
Support for ORDER BY ALL and GROUP BY ALL SQL syntax.

#### Implementation Details

**Files to Modify:**
- `mock_spark/storage/sql_translator.py`
- `mock_spark/storage/sqlalchemy_helpers.py`

**ORDER BY ALL:**

```sql
-- PySpark
SELECT * FROM users ORDER BY ALL

-- Equivalent to
SELECT * FROM users ORDER BY col1, col2, col3, ...
```

**GROUP BY ALL:**

```sql
-- PySpark
SELECT department, SUM(salary) FROM employees GROUP BY ALL

-- Equivalent to
SELECT department, SUM(salary) FROM employees GROUP BY department
```

**Implementation Strategy:**

1. **SQL Parser Enhancement:**
   - Recognize `ORDER BY ALL` and `GROUP BY ALL`
   - Expand to explicit column lists

2. **ORDER BY ALL:**
   - Get all columns from SELECT clause
   - Expand to explicit ORDER BY with all columns
   - Handle SELECT * separately

3. **GROUP BY ALL:**
   - Identify non-aggregated columns
   - Expand to explicit GROUP BY with those columns
   - Validate no non-aggregated columns without GROUP BY

4. **Query Translation:**
   - Translate to standard SQL
   - Execute with expanded clauses

**Tests:** `tests/unit/test_sql_queries.py`
- Test ORDER BY ALL with explicit columns
- Test ORDER BY ALL with SELECT *
- Test GROUP BY ALL with aggregates
- Test GROUP BY ALL error cases
- Test with complex queries
- Test with joins and subqueries

**Documentation:**
- Add to `docs/guides/sql_operations.md`
- Add examples to `docs/api_reference.md`

---

### 3.3 DataFrame.mapPartitions()

**Priority:** Medium  
**Complexity:** Medium  
**Estimated Effort:** 1-2 weeks  

#### Description
Apply a function to each partition and return a new DataFrame.

#### Implementation Details

**File:** `mock_spark/dataframe/dataframe.py`

```python
def mapPartitions(
    self,
    func: Callable[[Iterator[Row]], Iterator[Row]],
    preservesPartitioning: bool = False
) -> "MockDataFrame":
    """
    Apply a function to each partition.
    
    Args:
        func: Function that takes an iterator of Rows and returns an iterator of Rows
        preservesPartitioning: Whether the function preserves partitioning
    
    Returns:
        MockDataFrame with transformed data
    
    Example:
        >>> def add_index(iterator):
        ...     for i, row in enumerate(iterator):
        ...         yield Row(row.id, row.name, i)
        >>> df.mapPartitions(add_index)
    """
    # Implementation strategy:
    # 1. Partition DataFrame (conceptual)
    # 2. Apply function to each partition
    # 3. Combine results
    # 4. Preserve schema
    pass
```

**Implementation Strategy:**

1. **Partitioning:**
   - For mock-spark, partitions are conceptual
   - Split DataFrame into logical partitions
   - Configurable partition size

2. **Function Application:**
   - Apply function to each partition
   - Handle empty partitions
   - Validate output schema

3. **Result Combination:**
   - Concatenate partition results
   - Maintain row order if preservesPartitioning=True
   - Preserve schema information

**Tests:** `tests/unit/test_dataframe_operations.py`
- Test basic mapPartitions operations
- Test with different partition sizes
- Test with empty partitions
- Test schema preservation
- Test preservesPartitioning flag
- Test with large DataFrames
- Test error handling

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/dataframe_operations.md`

---

### 3.4 Enhanced Numeric/Timestamp Casting

**Priority:** Medium  
**Complexity:** Medium  
**Estimated Effort:** 1 week  

#### Description
Direct casting between numeric types and timestamps.

#### Implementation Details

**File:** `mock_spark/functions/core/column.py`

```python
def cast(self, dataType: Union[str, MockDataType]) -> MockColumn:
    """
    Cast column to a different data type.
    
    Enhanced to support:
    - Numeric to Timestamp (epoch time)
    - Timestamp to Numeric (epoch time)
    """
    # Implementation strategy:
    # 1. Detect source and target types
    # 2. Handle numeric <-> timestamp conversions
    # 3. Use appropriate conversion functions
    pass
```

**Implementation Strategy:**

1. **Type Detection:**
   - Identify source type
   - Identify target type
   - Determine conversion path

2. **Numeric to Timestamp:**
   - Interpret as Unix epoch (seconds since 1970-01-01)
   - Convert to timestamp
   - Handle milliseconds vs seconds

3. **Timestamp to Numeric:**
   - Extract Unix epoch
   - Convert to numeric type
   - Handle precision (seconds, milliseconds, microseconds)

4. **SQL Translation:**
   - Use appropriate SQL functions
   - Handle type-specific conversions

**Tests:** `tests/unit/test_type_casting.py`
- Test numeric to timestamp
- Test timestamp to numeric
- Test different numeric types (int, long, double)
- Test different timestamp precisions
- Test edge cases (epoch 0, future dates)
- Test null handling

**Documentation:**
- Add to `docs/guides/type_casting.md`
- Add examples to `docs/api_reference.md`

---

### 3.5 Array Functions Enhancement

**Priority:** Medium  
**Complexity:** Medium  
**Estimated Effort:** 1-2 weeks  

#### Functions to Add

1. **array_distinct** - Remove duplicate elements
2. **array_intersect** - Intersection of two arrays
3. **array_union** - Union of two arrays
4. **array_except** - Elements in first array but not second
5. **array_position** - Position of element in array
6. **array_remove** - Remove all occurrences of element

#### Implementation Details

**File:** `mock_spark/functions/array.py` (new file)

```python
class ArrayFunctions:
    @staticmethod
    def array_distinct(col: Union[str, MockColumn]) -> MockColumn:
        """Remove duplicate elements from array."""
        pass
    
    @staticmethod
    def array_intersect(col1: Union[str, MockColumn], col2: Union[str, MockColumn]) -> MockColumn:
        """Intersection of two arrays."""
        pass
    
    @staticmethod
    def array_union(col1: Union[str, MockColumn], col2: Union[str, MockColumn]) -> MockColumn:
        """Union of two arrays."""
        pass
    
    @staticmethod
    def array_except(col1: Union[str, MockColumn], col2: Union[str, MockColumn]) -> MockColumn:
        """Elements in first array but not second."""
        pass
    
    @staticmethod
    def array_position(col: Union[str, MockColumn], value: Any) -> MockColumn:
        """Position of element in array."""
        pass
    
    @staticmethod
    def array_remove(col: Union[str, MockColumn], value: Any) -> MockColumn:
        """Remove all occurrences of element."""
        pass
```

**SQL Translation:**
```sql
-- array_distinct
array_distinct(array_column)

-- array_intersect
array_intersect(array1, array2)

-- array_union
array_union(array1, array2)

-- array_except
array_except(array1, array2)

-- array_position
array_position(array_column, value)

-- array_remove
array_remove(array_column, value)
```

**Tests:** `tests/unit/test_array_functions.py`
- Test each function with various inputs
- Test with different data types
- Test with null values
- Test with empty arrays
- Test with nested arrays

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/array_functions.md`

---

### 3.6 Map Functions Enhancement

**Priority:** Low  
**Complexity:** Medium  
**Estimated Effort:** 1 week  

#### Functions to Add

1. **map_keys** - Get all keys from map
2. **map_values** - Get all values from map
3. **map_entries** - Get key-value pairs as array of structs
4. **map_concat** - Concatenate two maps
5. **map_from_arrays** - Create map from key and value arrays

#### Implementation Details

**File:** `mock_spark/functions/map.py` (new file)

```python
class MapFunctions:
    @staticmethod
    def map_keys(col: Union[str, MockColumn]) -> MockColumn:
        """Get all keys from map."""
        pass
    
    @staticmethod
    def map_values(col: Union[str, MockColumn]) -> MockColumn:
        """Get all values from map."""
        pass
    
    @staticmethod
    def map_entries(col: Union[str, MockColumn]) -> MockColumn:
        """Get key-value pairs as array of structs."""
        pass
    
    @staticmethod
    def map_concat(*cols: Union[str, MockColumn]) -> MockColumn:
        """Concatenate multiple maps."""
        pass
    
    @staticmethod
    def map_from_arrays(keys: Union[str, MockColumn], values: Union[str, MockColumn]) -> MockColumn:
        """Create map from key and value arrays."""
        pass
```

**SQL Translation:**
```sql
-- map_keys
map_keys(map_column)

-- map_values
map_values(map_column)

-- map_entries
map_entries(map_column)

-- map_concat
map_concat(map1, map2, map3)

-- map_from_arrays
map_from_arrays(keys_array, values_array)
```

**Tests:** `tests/unit/test_map_functions.py`
- Test each function with various inputs
- Test with different key/value types
- Test with null values
- Test with empty maps
- Test with duplicate keys

**Documentation:**
- Add to `docs/api_reference.md`
- Add examples to `docs/guides/map_functions.md`

---

## Implementation Timeline

### Phase 1: Quick Wins (Weeks 1-3)
- Week 1: TIMESTAMPADD, TIMESTAMPDIFF, Enhanced Error Messages
- Week 2: Additional String Functions
- Week 3: Testing, Documentation, Bug Fixes

### Phase 2: Core Features (Weeks 4-9)
- Week 4-5: Pandas API on Spark (mapInPandas, applyInPandas, transform)
- Week 6: DataFrame.transform(), DataFrame.unpivot()
- Week 7: DEFAULT Column Values
- Week 8-9: Testing, Documentation, Integration

### Phase 3: Advanced Features (Weeks 10-17)
- Week 10-12: Parameterized SQL Queries
- Week 13: ORDER BY ALL / GROUP BY ALL
- Week 14: DataFrame.mapPartitions()
- Week 15: Enhanced Numeric/Timestamp Casting
- Week 16: Array Functions Enhancement
- Week 17: Map Functions Enhancement, Final Testing

### Buffer (Weeks 18-20)
- Week 18: Integration Testing
- Week 19: Performance Testing
- Week 20: Documentation, Release Preparation

---

## Testing Strategy

### Unit Tests
- Each feature gets dedicated test file
- Test all code paths
- Test edge cases
- Test error handling
- Target: 90%+ code coverage

### Integration Tests
- Test feature interactions
- Test with real-world scenarios
- Test performance
- Test backward compatibility

### Compatibility Tests
- Test against PySpark 3.2
- Test against PySpark 3.3
- Verify API compatibility
- Test migration scenarios

---

## Documentation Strategy

### API Reference
- Add all new functions to `docs/api_reference.md`
- Include examples for each function
- Document parameters and return types
- Add type hints

### Guides
- Create `docs/guides/pandas_api.md` for Pandas API
- Update `docs/guides/datetime_functions.md`
- Update `docs/guides/sql_operations.md`
- Add migration guides

### Examples
- Add examples to `examples/` directory
- Create comprehensive usage examples
- Add performance benchmarks
- Add best practices

---

## Risk Mitigation

### Technical Risks
1. **Pandas API Complexity:** Mitigate with thorough testing and documentation
2. **SQL Parser Changes:** Mitigate with incremental changes and extensive testing
3. **Performance Impact:** Mitigate with profiling and optimization
4. **Backward Compatibility:** Mitigate with comprehensive compatibility tests

### Timeline Risks
1. **Scope Creep:** Mitigate with strict feature prioritization
2. **Unexpected Complexity:** Mitigate with buffer time
3. **Integration Issues:** Mitigate with early integration testing

---

## Success Criteria

### Functional
- All Phase 1 features implemented and tested
- All Phase 2 features implemented and tested
- All Phase 3 features implemented and tested
- 90%+ test coverage for new code
- All tests passing

### Quality
- Code review completed
- Documentation complete
- Examples working
- Performance benchmarks acceptable

### Release
- Version 2.5.0 released
- Release notes complete
- Migration guide available
- User feedback positive

---

## Conclusion

This implementation plan provides a comprehensive roadmap for adding PySpark 3.2 features to mock-spark. The phased approach ensures quick wins in Phase 1, core functionality in Phase 2, and advanced features in Phase 3.

The plan balances:
- **User Value:** High-impact features prioritized
- **Implementation Complexity:** Realistic time estimates
- **Quality:** Comprehensive testing and documentation
- **Risk Management:** Mitigation strategies for common risks

With this plan, mock-spark will achieve even better PySpark 3.2 compatibility while maintaining its focus on fast, reliable testing.

---

**Plan Version:** 1.0  
**Created:** 2025-01-14  
**Target Release:** mock-spark 2.5.0 (Q2 2025)


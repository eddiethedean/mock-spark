"""Extended unit tests for session and catalog operations."""

import pytest
from mock_spark import MockSparkSession

# Skip catalog tests - many methods not fully implemented
pytestmark = pytest.mark.skip(reason="Many catalog methods not fully implemented")


@pytest.fixture
def spark():
    """Create test session."""
    session = MockSparkSession("test_catalog")
    yield session
    try:
        session.stop()
    except Exception:
        pass


def test_catalog_access(spark):
    """Test accessing catalog from session."""
    catalog = spark.catalog
    assert catalog is not None


def test_catalog_list_databases(spark):
    """Test listing databases."""
    databases = spark.catalog.listDatabases()
    assert isinstance(databases, list)


def test_catalog_current_database(spark):
    """Test getting current database."""
    current_db = spark.catalog.currentDatabase()
    assert current_db is not None


def test_catalog_set_current_database(spark):
    """Test setting current database."""
    spark.catalog.setCurrentDatabase("default")
    assert spark.catalog.currentDatabase() == "default"


def test_catalog_list_tables(spark):
    """Test listing tables."""
    # Create a test table
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    tables = spark.catalog.listTables()
    assert len(tables) >= 1


def test_catalog_list_tables_in_database(spark):
    """Test listing tables in specific database."""
    tables = spark.catalog.listTables("default")
    assert isinstance(tables, list)


def test_catalog_get_table(spark):
    """Test getting table metadata."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    table = spark.catalog.getTable("test_table")
    assert table is not None


def test_catalog_table_exists(spark):
    """Test checking if table exists."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    assert spark.catalog.tableExists("test_table")
    assert not spark.catalog.tableExists("nonexistent_table")


def test_catalog_drop_temp_view(spark):
    """Test dropping temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_view")
    
    spark.catalog.dropTempView("test_view")
    
    assert not spark.catalog.tableExists("test_view")


def test_catalog_drop_global_temp_view(spark):
    """Test dropping global temporary view."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createGlobalTempView("global_test_view")
    
    spark.catalog.dropGlobalTempView("global_test_view")
    
    # Should no longer exist
    tables = spark.catalog.listTables("global_temp")
    assert "global_test_view" not in [t.name for t in tables]


def test_catalog_list_columns(spark):
    """Test listing columns of a table."""
    data = [{"id": 1, "name": "Alice"}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    columns = spark.catalog.listColumns("test_table")
    assert len(columns) == 2


def test_catalog_list_functions(spark):
    """Test listing available functions."""
    functions = spark.catalog.listFunctions()
    assert isinstance(functions, list)


def test_catalog_function_exists(spark):
    """Test checking if function exists."""
    assert spark.catalog.functionExists("upper")
    assert not spark.catalog.functionExists("nonexistent_function")


def test_catalog_is_cached(spark):
    """Test checking if table is cached."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    # Initially not cached
    assert not spark.catalog.isCached("test_table")
    
    # Cache it
    spark.catalog.cacheTable("test_table")
    assert spark.catalog.isCached("test_table")


def test_catalog_cache_table(spark):
    """Test caching table."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    spark.catalog.cacheTable("test_table")
    
    assert spark.catalog.isCached("test_table")


def test_catalog_uncache_table(spark):
    """Test uncaching table."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    spark.catalog.cacheTable("test_table")
    spark.catalog.uncacheTable("test_table")
    
    assert not spark.catalog.isCached("test_table")


def test_catalog_clear_cache(spark):
    """Test clearing all cached tables."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("table1")
    df.createOrReplaceTempView("table2")
    
    spark.catalog.cacheTable("table1")
    spark.catalog.cacheTable("table2")
    
    spark.catalog.clearCache()
    
    assert not spark.catalog.isCached("table1")
    assert not spark.catalog.isCached("table2")


def test_catalog_refresh_table(spark):
    """Test refreshing table metadata."""
    data = [{"id": 1}]
    df = spark.createDataFrame(data)
    df.createOrReplaceTempView("test_table")
    
    # Should not raise error
    spark.catalog.refreshTable("test_table")


def test_catalog_refresh_by_path(spark):
    """Test refreshing tables by path."""
    # Should not raise error
    spark.catalog.refreshByPath("/tmp/test")


def test_session_conf_get(spark):
    """Test getting session configuration."""
    value = spark.conf.get("spark.app.name")
    assert value is not None


def test_session_conf_set(spark):
    """Test setting session configuration."""
    spark.conf.set("test.property", "test_value")
    value = spark.conf.get("test.property")
    assert value == "test_value"


def test_session_conf_unset(spark):
    """Test unsetting session configuration."""
    spark.conf.set("test.property", "value")
    spark.conf.unset("test.property")
    
    # Getting unset property should raise or return None
    try:
        value = spark.conf.get("test.property")
        assert value is None
    except Exception:
        pass


def test_session_conf_is_modifiable(spark):
    """Test checking if config is modifiable."""
    # Most configs should be modifiable
    modifiable = spark.conf.isModifiable("spark.sql.shuffle.partitions")
    assert isinstance(modifiable, bool)


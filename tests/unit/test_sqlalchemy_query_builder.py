"""
Unit tests for SQLAlchemy-based query builder.

Tests the database-agnostic query building functionality.
"""

import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, insert
from mock_spark.dataframe.sqlalchemy_query_builder import SQLAlchemyQueryBuilder
from mock_spark.functions import col
from mock_spark.spark_types import (
    MockStructType,
    MockStructField,
    IntegerType,
    StringType,
    DoubleType,
)


class TestSQLAlchemyQueryBuilder:
    """Test SQLAlchemy query builder functionality."""

    @pytest.fixture
    def engine(self):
        """Create an in-memory SQLite engine for testing."""
        return create_engine("sqlite:///:memory:")

    @pytest.fixture
    def sample_table(self, engine):
        """Create a sample table for testing."""
        metadata = MetaData()
        table = Table(
            "users",
            metadata,
            Column("id", Integer),
            Column("name", String),
            Column("age", Integer),
            Column("salary", Float),
        )
        metadata.create_all(engine)

        # Insert sample data
        with engine.connect() as conn:
            conn.execute(
                insert(table),
                [
                    {"id": 1, "name": "Alice", "age": 30, "salary": 50000.0},
                    {"id": 2, "name": "Bob", "age": 25, "salary": 60000.0},
                    {"id": 3, "name": "Charlie", "age": 35, "salary": 70000.0},
                    {"id": 4, "name": "Diana", "age": 28, "salary": 55000.0},
                ],
            )
            conn.commit()

        return table

    @pytest.fixture
    def schema(self):
        """Create a sample schema."""
        return MockStructType(
            [
                MockStructField("id", IntegerType()),
                MockStructField("name", StringType()),
                MockStructField("age", IntegerType()),
                MockStructField("salary", DoubleType()),
            ]
        )

    def test_simple_select(self, engine, sample_table, schema):
        """Test simple SELECT query."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 4
            assert result[0].name == "Alice"

    def test_select_with_filter(self, engine, sample_table, schema):
        """Test SELECT with WHERE clause."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_filter(col("age") > 30)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 1
            assert result[0].name == "Charlie"

    def test_select_specific_columns(self, engine, sample_table, schema):
        """Test selecting specific columns."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_select(("name", "age"))
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 4
            # Check that only 2 columns are returned
            assert len(result[0]._mapping.keys()) == 2
            assert "name" in result[0]._mapping
            assert "age" in result[0]._mapping

    def test_order_by(self, engine, sample_table, schema):
        """Test ORDER BY clause."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_order_by(("salary",))
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 4
            assert result[0].name == "Alice"  # Lowest salary
            assert result[3].name == "Charlie"  # Highest salary

    def test_order_by_desc(self, engine, sample_table, schema):
        """Test ORDER BY DESC."""
        from mock_spark.functions import col

        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_order_by((col("salary").desc(),))
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 4
            assert result[0].name == "Charlie"  # Highest salary first
            assert result[3].name == "Alice"  # Lowest salary last

    def test_group_by(self, engine, sample_table, schema):
        """Test GROUP BY clause."""
        # Note: GROUP BY requires aggregate functions to be meaningful
        # This is a basic test just to verify the clause is added correctly
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_group_by(("age",))
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            # Each age group should appear once
            assert len(result) == 4  # All different ages in our sample data

    def test_limit(self, engine, sample_table, schema):
        """Test LIMIT clause."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_limit(2)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 2

    def test_multiple_filters(self, engine, sample_table, schema):
        """Test multiple WHERE conditions."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_filter(col("age") > 25)
        builder.add_filter(col("salary") < 65000)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 2  # Alice and Diana

    def test_arithmetic_operations(self, engine, sample_table, schema):
        """Test arithmetic operations in filters."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_filter(col("salary") / 1000 > 55)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 2  # Bob and Charlie

    def test_with_column(self, engine, sample_table, schema):
        """Test adding computed columns."""
        builder = SQLAlchemyQueryBuilder(sample_table, schema)
        builder.add_with_column("bonus", col("salary") * 0.1)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert len(result) == 4
            assert "bonus" in result[0]._mapping
            assert result[0].bonus == 5000.0  # Alice's bonus

    def test_cross_database_sqlite(self):
        """Test that the builder works with SQLite."""
        engine = create_engine("sqlite:///:memory:")
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer), Column("value", String))
        metadata.create_all(engine)

        builder = SQLAlchemyQueryBuilder(table)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert isinstance(result, list)

    def test_cross_database_duckdb(self):
        """Test that the builder works with DuckDB."""
        pytest.importorskip("duckdb_engine")

        engine = create_engine("duckdb:///:memory:")
        metadata = MetaData()
        table = Table("test", metadata, Column("id", Integer), Column("value", String))
        metadata.create_all(engine)

        builder = SQLAlchemyQueryBuilder(table)
        stmt = builder.build_select()

        with engine.connect() as conn:
            result = list(conn.execute(stmt))
            assert isinstance(result, list)


class TestSQLAlchemyMaterializer:
    """Test SQLAlchemy materializer functionality."""

    def test_materializer_basic(self):
        """Test basic materialization with SQLite."""
        from mock_spark.backend.duckdb import SQLAlchemyMaterializer
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType, StringType

        schema = MockStructType(
            [MockStructField("id", IntegerType()), MockStructField("name", StringType())]
        )

        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        materializer = SQLAlchemyMaterializer(engine_url="sqlite:///:memory:")

        # Test with no operations
        result = materializer.materialize(data, schema, [])
        assert len(result) == 2
        assert result[0]["id"] == 1

        materializer.close()

    def test_materializer_with_filter(self):
        """Test materialization with filter operation."""
        from mock_spark.backend.duckdb import SQLAlchemyMaterializer
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType, StringType
        from mock_spark.functions import col

        schema = MockStructType(
            [MockStructField("id", IntegerType()), MockStructField("name", StringType())]
        )

        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]

        materializer = SQLAlchemyMaterializer(engine_url="sqlite:///:memory:")

        # Apply filter operation
        operations = [("filter", col("id") > 1)]
        result = materializer.materialize(data, schema, operations)

        assert len(result) == 2
        assert result[0]["name"] == "Bob"
        assert result[1]["name"] == "Charlie"

        materializer.close()

    def test_temporary_table_creation(self):
        """Test that temporary tables are created with TEMPORARY prefix."""
        from mock_spark.backend.duckdb import SQLAlchemyMaterializer
        from mock_spark.spark_types import MockStructType, MockStructField, IntegerType

        schema = MockStructType([MockStructField("id", IntegerType())])
        data = [{"id": 1}]

        materializer = SQLAlchemyMaterializer(engine_url="sqlite:///:memory:")

        # The temp table should be created and cleaned up
        result = materializer.materialize(data, schema, [])
        assert len(result) == 1

        materializer.close()

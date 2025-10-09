"""
Unit tests for SQL to SQLAlchemy translator.

Tests the translation of Spark SQL to SQLAlchemy statements.
"""

import pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float
from mock_spark.storage.sql_translator import SQLToSQLAlchemyTranslator, SQLTranslationError


class TestSQLTranslator:
    """Test SQL translation functionality."""

    @pytest.fixture
    def engine(self):
        """Create test engine."""
        engine = create_engine("duckdb:///:memory:")

        # Create test table
        metadata = MetaData()
        users = Table(
            "users",
            metadata,
            Column("id", Integer),
            Column("name", String),
            Column("age", Integer),
            Column("salary", Float),
        )
        users.create(engine)

        # Insert test data
        with engine.begin() as conn:
            conn.execute(
                users.insert(),
                [
                    {"id": 1, "name": "Alice", "age": 25, "salary": 75000.0},
                    {"id": 2, "name": "Bob", "age": 30, "salary": 85000.0},
                    {"id": 3, "name": "Charlie", "age": 35, "salary": 95000.0},
                ],
            )

        return engine

    def test_simple_select(self, engine):
        """Test simple SELECT translation."""
        translator = SQLToSQLAlchemyTranslator(engine)

        # Translate query
        stmt = translator.translate("SELECT * FROM users")

        # Execute and verify
        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
            assert len(result) == 3

    def test_select_with_where(self, engine):
        """Test SELECT with WHERE clause."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("SELECT name, age FROM users WHERE age > 25")

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
            assert len(result) == 2
            assert result[0][0] in ("Bob", "Charlie")

    def test_select_with_order_by(self, engine):
        """Test SELECT with ORDER BY."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("SELECT name FROM users ORDER BY age DESC")

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
            assert len(result) == 3
            assert result[0][0] == "Charlie"  # Oldest first

    def test_select_with_limit(self, engine):
        """Test SELECT with LIMIT."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("SELECT * FROM users LIMIT 2")

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
            assert len(result) == 2

    def test_aggregate_functions(self, engine):
        """Test aggregate functions."""
        translator = SQLToSQLAlchemyTranslator(engine)

        # COUNT
        stmt = translator.translate("SELECT COUNT(*) FROM users")
        with engine.connect() as conn:
            result = conn.execute(stmt).fetchone()
            assert result[0] == 3

        # AVG
        stmt = translator.translate("SELECT AVG(age) FROM users")
        with engine.connect() as conn:
            result = conn.execute(stmt).fetchone()
            assert result[0] == 30.0

    def test_string_functions(self, engine):
        """Test string functions."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("SELECT UPPER(name) FROM users WHERE id = 1")

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchone()
            assert result[0] == "ALICE"

    def test_group_by(self, engine):
        """Test GROUP BY."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("SELECT age, COUNT(*) FROM users GROUP BY age")

        with engine.connect() as conn:
            result = conn.execute(stmt).fetchall()
            assert len(result) == 3

    def test_unsupported_query_raises_error(self, engine):
        """Test that unsupported SQL raises clear error."""
        translator = SQLToSQLAlchemyTranslator(engine)

        with pytest.raises(SQLTranslationError):
            translator.translate("INVALID SQL SYNTAX")

    def test_insert_translation(self, engine):
        """Test INSERT translation."""
        translator = SQLToSQLAlchemyTranslator(engine)

        stmt = translator.translate("INSERT INTO users VALUES (4, 'Diana', 28, 80000.0)")

        with engine.begin() as conn:
            conn.execute(stmt)

        # Verify insert worked using translator
        select_stmt = translator.translate("SELECT * FROM users WHERE id = 4")
        with engine.connect() as conn:
            result = conn.execute(select_stmt).fetchone()
            assert result is not None
            assert result[1] == "Diana"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

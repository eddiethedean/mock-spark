"""
Integration tests for safe expression evaluator in SQL executor.

Tests that SQL UPDATE and DELETE operations use the safe evaluator
instead of eval(), ensuring security and proper functionality.
"""

import pytest


@pytest.mark.unit
class TestSafeEvaluatorInSQLExecutor:
    """Test safe evaluator integration in SQL executor."""

    def test_sql_update_with_where_condition(self, spark):
        """Test SQL UPDATE with WHERE condition uses safe evaluator."""
        data = [{"id": 1, "age": 25}, {"id": 2, "age": 30}, {"id": 3, "age": 35}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.users")

        # Update with WHERE condition
        spark.sql("UPDATE test_schema.users SET age = age + 1 WHERE age > 27")

        # Verify update
        result = spark.table("test_schema.users").collect()
        # Rows with age > 27 should be updated
        row2 = [r for r in result if r["id"] == 2][0]
        assert row2["age"] == 31
        row3 = [r for r in result if r["id"] == 3][0]
        assert row3["age"] == 36

    def test_sql_update_with_complex_condition(self, spark):
        """Test SQL UPDATE with complex WHERE condition."""
        data = [{"id": 1, "status": "active", "score": 50}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.scores")

        # Update with complex condition
        spark.sql(
            "UPDATE test_schema.scores SET score = 100 "
            "WHERE status = 'active' AND score < 60"
        )

        # Verify update
        result = spark.table("test_schema.scores").collect()
        assert result[0]["score"] == 100

    def test_sql_delete_with_where_condition(self, spark):
        """Test SQL DELETE with WHERE condition uses safe evaluator."""
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 20},
            {"id": 3, "value": 30},
        ]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.items")

        # Delete with WHERE condition
        spark.sql("DELETE FROM test_schema.items WHERE value < 15")

        # Verify delete
        result = spark.table("test_schema.items").collect()
        assert len(result) == 2
        assert all(r["value"] >= 15 for r in result)

    def test_sql_delete_with_comparison(self, spark):
        """Test SQL DELETE with comparison condition."""
        data = [{"id": 1, "age": 25}, {"id": 2, "age": 30}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.people")

        # Delete with comparison
        spark.sql("DELETE FROM test_schema.people WHERE age >= 30")

        # Verify delete
        result = spark.table("test_schema.people").collect()
        assert len(result) == 1
        assert result[0]["age"] == 25

    def test_sql_update_set_with_expression(self, spark):
        """Test SQL UPDATE SET with expression evaluation."""
        data = [{"id": 1, "value": 5}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.values")

        # Update SET with expression
        spark.sql("UPDATE test_schema.values SET value = value * 2 WHERE id = 1")

        # Verify update
        result = spark.table("test_schema.values").collect()
        assert result[0]["value"] == 10

    def test_sql_update_with_null_handling(self, spark):
        """Test SQL UPDATE handles null values correctly."""
        data = [{"id": 1, "value": None}, {"id": 2, "value": 10}]
        df = spark.createDataFrame(data)
        df.write.mode("overwrite").saveAsTable("test_schema.nulls_table")

        # Update should handle nulls gracefully
        spark.sql(
            "UPDATE test_schema.nulls_table SET value = 20 WHERE value IS NOT NULL"
        )

        # Verify update
        result = spark.table("test_schema.nulls_table").collect()
        row1 = [r for r in result if r["id"] == 1][0]
        row2 = [r for r in result if r["id"] == 2][0]
        assert row1["value"] is None  # Null should remain null
        assert row2["value"] == 20  # Non-null should be updated

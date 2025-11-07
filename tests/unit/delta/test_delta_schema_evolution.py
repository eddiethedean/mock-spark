"""Regression tests for Delta schema evolution with Polars backend."""

import pytest

from mock_spark import SparkSession, StringType


@pytest.mark.unit
class TestDeltaSchemaEvolution:
    """Ensure Delta mergeSchema appends remain compatible under Polars."""

    @pytest.fixture
    def spark(self):
        session = SparkSession("delta-schema-evolution-tests")
        yield session
        table_name = "default.delta_schema_evolution_regression"
        if session.catalog.tableExists(table_name):
            session.catalog.dropTable(table_name)

    def test_append_with_merge_schema_adds_new_column(self, spark):
        table_name = "default.delta_schema_evolution_regression"

        base_rows = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        spark.createDataFrame(base_rows).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table_name)

        new_rows = [
            {"id": 3, "name": "Charlie", "favorite_color": "green"},
        ]

        spark.createDataFrame(new_rows).write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(table_name)

        result_df = spark.table(table_name)
        schema = result_df.schema

        assert schema.fieldNames() == ["id", "name", "favorite_color"]
        favorite_color_field = schema.get_field_by_name("favorite_color")
        assert favorite_color_field is not None
        assert isinstance(favorite_color_field.dataType, StringType)

        rows = result_df.collect()
        row_by_id = {row["id"]: row for row in rows}

        assert row_by_id[1]["favorite_color"] is None
        assert row_by_id[2]["favorite_color"] is None
        assert row_by_id[3]["favorite_color"] == "green"

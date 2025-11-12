import pytest

from mock_spark import SparkSession
from mock_spark.delta import DeltaTable


class TestDeltaOperations:
    @pytest.fixture
    def spark(self):
        session = SparkSession("delta_operations_suite")
        yield session
        session.stop()

    def test_delete_rows(self, spark: SparkSession) -> None:
        table_name = "delta_ops_delete"
        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "inactive"},
        ]
        spark.createDataFrame(data).write.format("delta").mode("overwrite").saveAsTable(
            table_name
        )

        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.delete("status = 'inactive'")

        remaining = spark.table(table_name).orderBy("id").collect()
        assert [row.id for row in remaining] == [1]
        assert remaining[0].status == "active"

    def test_update_rows(self, spark: SparkSession) -> None:
        table_name = "delta_ops_update"
        data = [
            {"id": 1, "status": "active"},
            {"id": 2, "status": "pending"},
        ]
        spark.createDataFrame(data).write.format("delta").mode("overwrite").saveAsTable(
            table_name
        )

        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.update("id = 2", {"status": "'processed'"})

        results = spark.table(table_name).orderBy("id").collect()
        values = {row.id: row.status for row in results}
        assert values == {1: "active", 2: "processed"}

    def test_merge_updates_and_inserts(self, spark: SparkSession) -> None:
        table_name = "delta_ops_merge"
        target_data = [
            {"id": 1, "name": "Alice", "score": 100},
            {"id": 2, "name": "Bob", "score": 150},
        ]
        source_data = [
            {"id": 1, "name": "Alice Updated", "score": 175},
            {"id": 3, "name": "Charlie", "score": 200},
        ]

        spark.createDataFrame(target_data).write.format("delta").mode(
            "overwrite"
        ).saveAsTable(table_name)

        delta_table = DeltaTable.forName(spark, table_name).alias("t")
        source_df = spark.createDataFrame(source_data).alias("s")

        (
            delta_table.merge(source_df, "t.id = s.id")
            .whenMatchedUpdate({"name": "s.name", "score": "s.score"})
            .whenNotMatchedInsertAll()
            .execute()
        )

        results = spark.table(table_name).orderBy("id").collect()
        assert len(results) == 3

        by_id = {row.id: row for row in results}
        assert by_id[1].name == "Alice Updated"
        assert by_id[1].score == 175
        assert by_id[2].name == "Bob"
        assert by_id[3].name == "Charlie"


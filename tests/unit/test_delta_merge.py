"""
Unit tests for Delta Lake MERGE INTO operations.

Based on exploration/delta_merge.py findings:
- MERGE requires target to be Delta format
- WHEN MATCHED THEN UPDATE updates matching rows
- WHEN NOT MATCHED THEN INSERT inserts new rows
- Can have either or both WHEN clauses
- WHEN MATCHED THEN DELETE removes matched rows
- Condition can be complex (multiple columns, AND/OR)
"""

from mock_spark import MockSparkSession


class TestDeltaMerge:
    """Test Delta Lake MERGE INTO operations."""

    def test_basic_merge_update_and_insert(self):
        """Test basic MERGE with both UPDATE and INSERT clauses."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Create target Delta table
        target_data = [
            {"id": 1, "name": "Alice", "score": 100},
            {"id": 2, "name": "Bob", "score": 200},
        ]
        target_df = spark.createDataFrame(target_data)
        target_df.write.format("delta").saveAsTable("test.target")

        # Create source table
        source_data = [
            {"id": 1, "name": "Alice Updated", "score": 150},  # Will update
            {"id": 3, "name": "Charlie", "score": 300},  # Will insert
        ]
        source_df = spark.createDataFrame(source_data)
        source_df.createOrReplaceTempView("source")

        # Execute MERGE
        spark.sql(
            """
            MERGE INTO test.target AS t
            USING source AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.name = s.name, t.score = s.score
            WHEN NOT MATCHED THEN INSERT (id, name, score) VALUES (s.id, s.name, s.score)
        """
        )

        # Verify results
        result = spark.table("test.target")
        assert result.count() == 3

        rows_by_id = {row["id"]: row for row in result.collect()}

        # Alice should be updated
        assert rows_by_id[1]["name"] == "Alice Updated"
        assert rows_by_id[1]["score"] == 150

        # Bob should be unchanged
        assert rows_by_id[2]["name"] == "Bob"
        assert rows_by_id[2]["score"] == 200

        # Charlie should be inserted
        assert 3 in rows_by_id
        assert rows_by_id[3]["name"] == "Charlie"
        assert rows_by_id[3]["score"] == 300

        spark.stop()

    def test_merge_matched_only(self):
        """Test MERGE with only WHEN MATCHED clause."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Target
        target = spark.createDataFrame(
            [{"id": 1, "value": 10}, {"id": 2, "value": 20}, {"id": 3, "value": 30}]
        )
        target.write.format("delta").saveAsTable("test.target2")

        # Source
        source = spark.createDataFrame(
            [
                {"id": 1, "value": 100},
                {"id": 3, "value": 300},
                {"id": 99, "value": 9900},  # Won't be inserted
            ]
        )
        source.createOrReplaceTempView("source2")

        # MERGE with matched only
        spark.sql(
            """
            MERGE INTO test.target2 AS t
            USING source2 AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET t.value = s.value
        """
        )

        result = spark.table("test.target2")
        assert result.count() == 3  # Still 3 rows (no inserts)

        rows_by_id = {row["id"]: row for row in result.collect()}
        assert rows_by_id[1]["value"] == 100  # Updated
        assert rows_by_id[2]["value"] == 20  # Unchanged
        assert rows_by_id[3]["value"] == 300  # Updated
        assert 99 not in rows_by_id  # Not inserted

        spark.stop()

    def test_merge_not_matched_only(self):
        """Test MERGE with only WHEN NOT MATCHED clause."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Target
        target = spark.createDataFrame([{"id": 1, "value": 10}, {"id": 2, "value": 20}])
        target.write.format("delta").saveAsTable("test.target3")

        # Source
        source = spark.createDataFrame(
            [
                {"id": 1, "value": 999},  # Matches but won't update
                {"id": 3, "value": 30},  # Will insert
                {"id": 4, "value": 40},  # Will insert
            ]
        )
        source.createOrReplaceTempView("source3")

        # MERGE with not matched only
        spark.sql(
            """
            MERGE INTO test.target3 AS t
            USING source3 AS s
            ON t.id = s.id
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
        """
        )

        result = spark.table("test.target3")
        assert result.count() == 4  # 2 original + 2 inserted

        rows_by_id = {row["id"]: row for row in result.collect()}
        assert rows_by_id[1]["value"] == 10  # Unchanged (no UPDATE clause)
        assert rows_by_id[2]["value"] == 20  # Unchanged
        assert rows_by_id[3]["value"] == 30  # Inserted
        assert rows_by_id[4]["value"] == 40  # Inserted

        spark.stop()

    def test_merge_with_delete(self):
        """Test MERGE with DELETE clause."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Target
        target = spark.createDataFrame(
            [
                {"id": 1, "active": True},
                {"id": 2, "active": True},
                {"id": 3, "active": True},
            ]
        )
        target.write.format("delta").saveAsTable("test.target5")

        # Source - IDs to delete
        source = spark.createDataFrame([{"id": 2}])
        source.createOrReplaceTempView("source5")

        # MERGE with DELETE
        spark.sql(
            """
            MERGE INTO test.target5 AS t
            USING source5 AS s
            ON t.id = s.id
            WHEN MATCHED THEN DELETE
        """
        )

        result = spark.table("test.target5")
        assert result.count() == 2  # One deleted

        remaining_ids = {row["id"] for row in result.collect()}
        assert 1 in remaining_ids
        assert 2 not in remaining_ids  # Deleted
        assert 3 in remaining_ids

        spark.stop()

    def test_merge_complex_condition(self):
        """Test MERGE with complex ON condition."""
        spark = MockSparkSession.builder.appName("test").getOrCreate()

        # Target
        target = spark.createDataFrame(
            [
                {"id": 1, "category": "A", "value": 10},
                {"id": 2, "category": "B", "value": 20},
                {"id": 3, "category": "A", "value": 30},
            ]
        )
        target.write.format("delta").saveAsTable("test.target4")

        # Source
        source = spark.createDataFrame(
            [
                {
                    "id": 1,
                    "category": "A",
                    "value": 100,
                },  # Matches (id=1 AND category=A)
                {
                    "id": 2,
                    "category": "B",
                    "value": 200,
                },  # Matches (id=2 AND category=B)
                {"id": 4, "category": "C", "value": 400},  # No match
            ]
        )
        source.createOrReplaceTempView("source4")

        # MERGE with complex condition (must match both id AND category)
        spark.sql(
            """
            MERGE INTO test.target4 AS t
            USING source4 AS s
            ON t.id = s.id AND t.category = s.category
            WHEN MATCHED THEN UPDATE SET t.value = s.value
            WHEN NOT MATCHED THEN INSERT (id, category, value) VALUES (s.id, s.category, s.value)
        """
        )

        result = spark.table("test.target4")
        rows_by_id_cat = {(row["id"], row["category"]): row for row in result.collect()}

        # Matched rows updated
        assert rows_by_id_cat[(1, "A")]["value"] == 100
        assert rows_by_id_cat[(2, "B")]["value"] == 200

        # Unmatched target row unchanged
        assert rows_by_id_cat[(3, "A")]["value"] == 30

        # New row inserted
        assert (4, "C") in rows_by_id_cat
        assert rows_by_id_cat[(4, "C")]["value"] == 400

        spark.stop()

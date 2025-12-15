from sparkless.sql import SparkSession


def test_create_table_as_select_basic() -> None:
    """BUG-011 regression: CREATE TABLE AS SELECT should create a table from a query."""
    spark = SparkSession("Bug011CTAS")
    try:
        # Create source table
        df = spark.createDataFrame(
            [
                {"name": "Alice", "age": 30, "dept": "IT"},
                {"name": "Bob", "age": 25, "dept": "HR"},
            ]
        )
        df.write.mode("overwrite").saveAsTable("employees_ctas")

        # CTAS: create new table from a select query
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS it_employees_ctas AS
            SELECT name, age FROM employees_ctas WHERE dept = 'IT'
            """
        )

        result = spark.sql("SELECT * FROM it_employees_ctas")
        rows = result.collect()

        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == 30
    finally:
        spark.sql("DROP TABLE IF EXISTS employees_ctas")
        spark.sql("DROP TABLE IF EXISTS it_employees_ctas")
        spark.stop()



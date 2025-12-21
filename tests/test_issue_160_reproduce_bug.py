"""
Test to reproduce issue #160: cannot resolve error when execution plan references dropped columns.

This test should FAIL without the fix and PASS with the fix.

The bug occurs when:
1. An expression is translated and cached (referencing a column)
2. That column is dropped via select()
3. A subsequent operation tries to reuse the cached expression
4. The cached expression references the dropped column, causing an error

Based on the issue description, the problem is that the execution plan still contains
references to the dropped column from earlier operations, and when the plan is evaluated,
sparkless tries to resolve ALL column references, including the dropped column.
"""

import pytest
from sparkless import SparkSession, functions as F
from sparkless.core.exceptions.operation import SparkColumnNotFoundError


def test_reproduce_bug_exact_scenario_from_issue():
    """
    Reproduce the exact bug scenario from issue #160.

    The issue states:
    - Transform uses a column in operations (e.g., F.col("impression_date"))
    - Then drops that column via .select() (excluding it from the final column list)
    - sparkless's execution plan still contains references to the dropped column
    - When the plan is evaluated, sparkless tries to resolve ALL column references,
      including the dropped column, causing a "cannot resolve" error
    """
    spark = SparkSession.builder.appName("bug_reproduction").getOrCreate()

    # Create test data exactly as in the issue
    data = [
        (
            "imp_001",
            "2024-01-15T10:30:45.123456",
            "campaign_1",
            "customer_1",
            "web",
            "ad_1",
            "mobile",
            0.05,
        ),
        (
            "imp_002",
            "2024-01-16T14:20:30.789012",
            "campaign_2",
            "customer_2",
            "mobile",
            "ad_2",
            "mobile",
            0.03,
        ),
    ]

    bronze_df = spark.createDataFrame(
        data,
        [
            "impression_id",
            "impression_date",  # This column will be dropped
            "campaign_id",
            "customer_id",
            "channel",
            "ad_id",
            "device_type",
            "cost_per_impression",
        ],
    )

    # Apply transform that uses impression_date then drops it
    # This matches the exact scenario from the issue
    silver_df = (
        bronze_df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
        .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
        .withColumn(
            "is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False)
        )
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",  # New column
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
            # impression_date is DROPPED - not in select list
        )
    )

    # Verify column was dropped
    assert "impression_date" not in silver_df.columns
    assert "impression_date_parsed" in silver_df.columns

    # This is where the bug occurs - when trying to materialize/evaluate the DataFrame
    # The execution plan still contains references to 'impression_date' from the earlier
    # F.regexp_replace(F.col("impression_date"), ...) operation
    # Without the fix, this should raise an error about 'impression_date' not being found
    try:
        count = silver_df.count()  # This triggers materialization
        # If we get here without an error, the bug might not be reproduced
        # But we should still verify it works
        assert count == 2
    except SparkColumnNotFoundError as e:
        # Check if the error is about the dropped column
        error_msg = str(e).lower()
        if "impression_date" in error_msg and "cannot resolve" in error_msg:
            # Bug reproduced! The error is about the dropped column
            pytest.fail(
                f"Bug reproduced! Got SparkColumnNotFoundError for dropped column 'impression_date': {e}\n"
                f"This error should not occur - the execution plan should not reference dropped columns."
            )
        else:
            # Different error, re-raise it
            raise

    spark.stop()


def test_reproduce_bug_with_150_plus_rows():
    """
    Reproduce the bug with 150+ rows as mentioned in the issue comment.
    This might trigger different cache behavior.
    """
    spark = SparkSession.builder.appName("bug_reproduction_150_rows").getOrCreate()

    # Create test data with 150+ rows to trigger cache behavior
    data = [
        (
            f"imp_{i:03d}",
            f"2024-01-15T10:30:45.{i:06d}",
            f"campaign_{i}",
            f"customer_{i}",
            "web",
            f"ad_{i}",
            "mobile",
            0.05,
        )
        for i in range(200)
    ]

    bronze_df = spark.createDataFrame(
        data,
        [
            "impression_id",
            "impression_date",  # This column will be dropped
            "campaign_id",
            "customer_id",
            "channel",
            "ad_id",
            "device_type",
            "cost_per_impression",
        ],
    )

    # Apply transform that uses impression_date then drops it
    silver_df = (
        bronze_df.withColumn(
            "impression_date_parsed",
            F.to_timestamp(
                F.regexp_replace(F.col("impression_date"), r"\.\d+", "").cast("string"),
                "yyyy-MM-dd'T'HH:mm:ss",
            ),
        )
        .withColumn("hour_of_day", F.hour(F.col("impression_date_parsed")))
        .withColumn("day_of_week", F.dayofweek(F.col("impression_date_parsed")))
        .withColumn(
            "is_mobile", F.when(F.col("device_type") == "mobile", True).otherwise(False)
        )
        .select(
            "impression_id",
            "campaign_id",
            "customer_id",
            "impression_date_parsed",
            "hour_of_day",
            "day_of_week",
            "channel",
            "ad_id",
            "cost_per_impression",
            "device_type",
            "is_mobile",
            # impression_date is DROPPED
        )
    )

    # Verify column was dropped
    assert "impression_date" not in silver_df.columns

    # Try to materialize - this is where the bug would occur with 150+ rows
    try:
        count = silver_df.count()
        assert count == 200
    except SparkColumnNotFoundError as e:
        error_msg = str(e).lower()
        if "impression_date" in error_msg and "cannot resolve" in error_msg:
            pytest.fail(
                f"Bug reproduced with 150+ rows! Got SparkColumnNotFoundError for dropped column 'impression_date': {e}"
            )
        raise

    spark.stop()

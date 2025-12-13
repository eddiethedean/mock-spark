#!/usr/bin/env python3
"""
Benchmark script to compare Polars and DuckDB backend performance.

This script runs the same operations with both backends and compares
execution times to help identify performance differences.
"""

import time
import statistics
from typing import Any, Callable
from sparkless import MockSparkSession, F


def benchmark_operation(
    spark: MockSparkSession,
    operation_name: str,
    operation: Callable,
    warmup_runs: int = 2,
    benchmark_runs: int = 5,
) -> dict[str, Any]:
    """Benchmark an operation multiple times and return statistics.

    Args:
        spark: MockSparkSession instance
        operation_name: Name of the operation
        operation: Callable that performs the operation
        warmup_runs: Number of warmup runs to perform
        benchmark_runs: Number of benchmark runs to perform

    Returns:
        Dictionary with timing statistics
    """
    # Warmup runs
    for _ in range(warmup_runs):
        try:
            operation()
        except Exception as e:
            print(f"Warning: Operation {operation_name} failed during warmup: {e}")
            return {"error": str(e)}

    # Benchmark runs
    times = []
    for _ in range(benchmark_runs):
        start = time.perf_counter()
        try:
            result = operation()
            # Force evaluation if it's a DataFrame
            if hasattr(result, "count"):
                result.count()
            elif hasattr(result, "collect"):
                result.collect()
        except Exception as e:
            print(f"Warning: Operation {operation_name} failed: {e}")
            return {"error": str(e)}
        end = time.perf_counter()
        times.append(end - start)

    return {
        "mean": statistics.mean(times),
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times),
        "stdev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "runs": benchmark_runs,
    }


def create_test_dataframe(spark: MockSparkSession, size: int = 10000):
    """Create a test DataFrame with sample data.

    Args:
        spark: MockSparkSession instance
        size: Number of rows to create

    Returns:
        MockDataFrame instance
    """
    data = [
        {
            "id": i,
            "name": f"user_{i}",
            "age": 20 + (i % 50),
            "score": 50 + (i % 100),
            "category": f"cat_{i % 10}",
            "value": i * 1.5,
        }
        for i in range(size)
    ]
    return spark.createDataFrame(data)


def run_benchmarks(backend_type: str, size: int = 10000) -> dict[str, Any]:
    """Run all benchmarks for a specific backend.

    Args:
        backend_type: "polars" or "duckdb"
        size: Size of test dataset

    Returns:
        Dictionary of benchmark results
    """
    print(f"\n{'=' * 60}")
    print(f"Running benchmarks with {backend_type.upper()} backend")
    print(f"{'=' * 60}")

    # Create session with specified backend
    spark = MockSparkSession(
        app_name=f"Benchmark_{backend_type}",
        backend_type=backend_type,
        max_memory="2GB",
        allow_disk_spillover=False,
    )

    results = {}

    try:
        # Create test DataFrame
        print(f"\nCreating test DataFrame with {size:,} rows...")
        df = create_test_dataframe(spark, size)
        print(f"DataFrame created: {df.count():,} rows")

        # Benchmark 1: Simple filter
        print("\n1. Simple filter operation...")
        results["filter"] = benchmark_operation(
            spark,
            "filter",
            lambda: df.filter(F.col("age") > 30),
        )

        # Benchmark 2: Complex filter with multiple conditions
        print("2. Complex filter operation...")
        results["complex_filter"] = benchmark_operation(
            spark,
            "complex_filter",
            lambda: df.filter((F.col("age") > 30) & (F.col("score") > 70)),
        )

        # Benchmark 3: Select specific columns
        print("3. Select columns...")
        results["select"] = benchmark_operation(
            spark,
            "select",
            lambda: df.select("id", "name", "score"),
        )

        # Benchmark 4: Aggregation - count
        print("4. Count aggregation...")
        results["count"] = benchmark_operation(
            spark,
            "count",
            lambda: df.count(),
        )

        # Benchmark 5: Aggregation - groupBy + count
        print("5. GroupBy + count...")
        results["groupby_count"] = benchmark_operation(
            spark,
            "groupby_count",
            lambda: df.groupBy("category").count(),
        )

        # Benchmark 6: Aggregation - groupBy + avg
        print("6. GroupBy + avg...")
        results["groupby_avg"] = benchmark_operation(
            spark,
            "groupby_avg",
            lambda: df.groupBy("category").agg(F.avg("score")),
        )

        # Benchmark 7: Multiple aggregations
        print("7. Multiple aggregations...")
        results["multiple_agg"] = benchmark_operation(
            spark,
            "multiple_agg",
            lambda: df.groupBy("category").agg(
                F.avg("score"),
                F.max("age"),
                F.min("value"),
                F.count("*"),
            ),
        )

        # Benchmark 8: OrderBy
        print("8. OrderBy operation...")
        results["orderby"] = benchmark_operation(
            spark,
            "orderby",
            lambda: df.orderBy(F.col("score").desc()),
        )

        # Benchmark 9: Join operation (self-join)
        print("9. Join operation...")
        df2 = df.select("id", "category").withColumnRenamed("category", "cat2")
        results["join"] = benchmark_operation(
            spark,
            "join",
            lambda: df.join(df2, on="id", how="inner"),
        )

        # Benchmark 10: Window function
        print("10. Window function...")
        from sparkless import Window

        window_spec = Window.partitionBy("category").orderBy(F.col("score").desc())
        results["window_function"] = benchmark_operation(
            spark,
            "window_function",
            lambda: df.withColumn("rank", F.rank().over(window_spec)),
        )

        # Benchmark 11: Complex transformation chain
        print("11. Complex transformation chain...")
        results["complex_chain"] = benchmark_operation(
            spark,
            "complex_chain",
            lambda: (
                df.filter(F.col("age") > 25)
                .select("id", "name", "score", "category")
                .groupBy("category")
                .agg(F.avg("score").alias("avg_score"))
                .filter(F.col("avg_score") > 60)
                .orderBy(F.col("avg_score").desc())
            ),
        )

        # Benchmark 12: DataFrame creation
        print("12. DataFrame creation...")
        test_data = [{"x": i, "y": i * 2} for i in range(1000)]
        results["create_dataframe"] = benchmark_operation(
            spark,
            "create_dataframe",
            lambda: spark.createDataFrame(test_data),
        )

    finally:
        spark.stop()

    return results


def print_results(results: dict[str, dict[str, Any]], backend_name: str):
    """Print benchmark results in a formatted table.

    Args:
        results: Dictionary of benchmark results
        backend_name: Name of the backend
    """
    print(f"\n{'=' * 60}")
    print(f"Benchmark Results: {backend_name.upper()}")
    print(f"{'=' * 60}")
    print(
        f"{'Operation':<25} {'Mean (s)':<12} {'Median (s)':<12} {'Min (s)':<12} {'Max (s)':<12}"
    )
    print("-" * 60)

    for op_name, stats in results.items():
        if "error" in stats:
            print(f"{op_name:<25} ERROR: {stats['error']}")
        else:
            print(
                f"{op_name:<25} "
                f"{stats['mean']:<12.6f} "
                f"{stats['median']:<12.6f} "
                f"{stats['min']:<12.6f} "
                f"{stats['max']:<12.6f}"
            )


def compare_results(
    polars_results: dict[str, dict[str, Any]],
    duckdb_results: dict[str, dict[str, Any]],
):
    """Compare results from both backends and show speedup.

    Args:
        polars_results: Results from Polars backend
        duckdb_results: Results from DuckDB backend
    """
    print(f"\n{'=' * 60}")
    print("Performance Comparison: Polars vs DuckDB")
    print(f"{'=' * 60}")
    print(
        f"{'Operation':<25} "
        f"{'Polars (s)':<15} "
        f"{'DuckDB (s)':<15} "
        f"{'Speedup':<15} "
        f"{'Winner':<10}"
    )
    print("-" * 80)

    all_ops = set(polars_results.keys()) | set(duckdb_results.keys())

    for op_name in sorted(all_ops):
        polars_stats = polars_results.get(op_name, {})
        duckdb_stats = duckdb_results.get(op_name, {})

        if "error" in polars_stats or "error" in duckdb_stats:
            polars_err = polars_stats.get("error", "N/A")
            duckdb_err = duckdb_stats.get("error", "N/A")
            print(f"{op_name:<25} ERROR: Polars={polars_err}, DuckDB={duckdb_err}")
            continue

        polars_time = polars_stats.get("mean", 0)
        duckdb_time = duckdb_stats.get("mean", 0)

        if polars_time == 0 or duckdb_time == 0:
            print(f"{op_name:<25} Skipped (zero time)")
            continue

        # Calculate speedup (higher is better for the faster one)
        if polars_time < duckdb_time:
            speedup = duckdb_time / polars_time
            winner = "Polars"
        else:
            speedup = polars_time / duckdb_time
            winner = "DuckDB"

        print(
            f"{op_name:<25} "
            f"{polars_time:<15.6f} "
            f"{duckdb_time:<15.6f} "
            f"{speedup:.2f}x ({winner}){'':<8}"
        )

    # Summary statistics
    print(f"\n{'=' * 60}")
    print("Summary Statistics")
    print(f"{'=' * 60}")

    # Calculate geometric mean of speedups
    speedups = []
    for op_name in all_ops:
        polars_stats = polars_results.get(op_name, {})
        duckdb_stats = duckdb_results.get(op_name, {})

        if "error" in polars_stats or "error" in duckdb_stats:
            continue

        polars_time = polars_stats.get("mean", 0)
        duckdb_time = duckdb_stats.get("mean", 0)

        if polars_time > 0 and duckdb_time > 0:
            if polars_time < duckdb_time:
                speedups.append(duckdb_time / polars_time)
            else:
                speedups.append(polars_time / duckdb_time)

    if speedups:
        avg_speedup = statistics.mean(speedups)
        print(f"Average speedup: {avg_speedup:.2f}x")

        polars_wins = sum(
            1
            for s in speedups
            if s > 1
            and polars_results.get(list(all_ops)[speedups.index(s)], {}).get("mean", 0)
            < duckdb_results.get(list(all_ops)[speedups.index(s)], {}).get("mean", 0)
        )
        # Actually, let's count properly
        polars_wins = 0
        duckdb_wins = 0
        for op_name in all_ops:
            polars_stats = polars_results.get(op_name, {})
            duckdb_stats = duckdb_results.get(op_name, {})
            if "error" not in polars_stats and "error" not in duckdb_stats:
                polars_time = polars_stats.get("mean", 0)
                duckdb_time = duckdb_stats.get("mean", 0)
                if polars_time > 0 and duckdb_time > 0:
                    if polars_time < duckdb_time:
                        polars_wins += 1
                    else:
                        duckdb_wins += 1

        print(f"Polars faster: {polars_wins} operations")
        print(f"DuckDB faster: {duckdb_wins} operations")


def main():
    """Main benchmark function."""
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark Polars vs DuckDB backends")
    parser.add_argument(
        "--size",
        type=int,
        default=10000,
        help="Number of rows in test dataset (default: 10000)",
    )
    parser.add_argument(
        "--polars-only",
        action="store_true",
        help="Only run Polars benchmarks",
    )
    parser.add_argument(
        "--duckdb-only",
        action="store_true",
        help="Only run DuckDB benchmarks",
    )

    args = parser.parse_args()

    print("Mock Spark Backend Performance Benchmark")
    print("=" * 60)
    print(f"Test dataset size: {args.size:,} rows")
    print("Warmup runs: 2, Benchmark runs: 5 per operation")

    polars_results = {}
    duckdb_results = {}

    # Run Polars benchmarks
    if not args.duckdb_only:
        try:
            polars_results = run_benchmarks("polars", args.size)
            print_results(polars_results, "Polars")
        except Exception as e:
            print(f"\nError running Polars benchmarks: {e}")
            import traceback

            traceback.print_exc()

    # Run DuckDB benchmarks
    if not args.polars_only:
        try:
            duckdb_results = run_benchmarks("duckdb", args.size)
            print_results(duckdb_results, "DuckDB")
        except Exception as e:
            print(f"\nError running DuckDB benchmarks: {e}")
            import traceback

            traceback.print_exc()

    # Compare results if both were run
    if polars_results and duckdb_results:
        compare_results(polars_results, duckdb_results)

    print("\nBenchmark complete!")


if __name__ == "__main__":
    main()

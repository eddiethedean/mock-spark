"""
Unit tests for performance mode in SparkSession.

Tests the performance_mode parameter and JVM overhead simulation.
"""

import time
from sparkless import SparkSession


class TestPerformanceMode:
    """Test performance mode functionality."""

    def test_default_performance_mode_is_fast(self):
        """Test that default performance mode is 'fast'."""
        spark = SparkSession("test")
        try:
            assert spark.performance_mode == "fast"
        finally:
            spark.stop()

    def test_set_performance_mode_realistic(self):
        """Test setting performance mode to 'realistic'."""
        spark = SparkSession("test", performance_mode="realistic")
        try:
            assert spark.performance_mode == "realistic"
        finally:
            spark.stop()

    def test_fast_mode_has_low_overhead(self):
        """Test that fast mode has very low overhead."""
        spark = SparkSession("test", performance_mode="fast")
        try:
            assert spark._jvm_overhead < 0.0001
        finally:
            spark.stop()

    def test_realistic_mode_has_higher_overhead(self):
        """Test that realistic mode has higher overhead."""
        spark = SparkSession("test", performance_mode="realistic")
        try:
            assert spark._jvm_overhead > 0.0001
        finally:
            spark.stop()

    def test_simulate_jvm_overhead_fast_mode(self):
        """Test that fast mode overhead is minimal."""
        spark = SparkSession("test", performance_mode="fast")
        try:
            start = time.time()
            for _ in range(1000):
                spark._simulate_jvm_overhead()
            duration = time.time() - start

            # Should be very fast (< 0.1 seconds for 1000 operations)
            assert duration < 0.1
        finally:
            spark.stop()

    def test_simulate_jvm_overhead_realistic_mode(self):
        """Test that realistic mode adds noticeable overhead."""
        spark = SparkSession("test", performance_mode="realistic")
        try:
            start = time.time()
            for _ in range(100):
                spark._simulate_jvm_overhead()
            duration = time.time() - start

            # Should be slower than fast mode
            assert duration > 0.01
        finally:
            spark.stop()

    def test_simulate_jvm_overhead_with_operations_count(self):
        """Test that overhead scales with operations count."""
        spark = SparkSession("test", performance_mode="realistic")
        try:
            start = time.time()
            spark._simulate_jvm_overhead(operations=100)
            duration_single = time.time() - start

            start = time.time()
            for _ in range(100):
                spark._simulate_jvm_overhead(operations=1)
            duration_multiple = time.time() - start

            # Should be roughly similar (within 2x)
            assert abs(duration_single - duration_multiple) < duration_multiple
        finally:
            spark.stop()

    def test_fast_mode_no_overhead_simulation(self):
        """Test that fast mode doesn't add overhead."""
        spark = SparkSession("test", performance_mode="fast")
        try:
            start = time.time()
            for _ in range(10000):
                spark._simulate_jvm_overhead()
            duration = time.time() - start

            # Should be extremely fast (allow some variance in CI environments)
            assert duration < 0.05
        finally:
            spark.stop()

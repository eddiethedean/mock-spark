"""
Test that all example scripts are runnable.

Ensures documentation examples work correctly and produce expected outputs.
"""

import pytest
import subprocess
import os
from pathlib import Path


class TestExampleScripts:
    """Validate that all example scripts run without errors."""

    def test_basic_usage_runs(self):
        """Test that basic_usage.py runs successfully."""
        result = subprocess.run(
            ["python3", "examples/basic_usage.py"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode == 0, f"basic_usage.py failed: {result.stderr}"
        assert "Mock Spark" in result.stdout
        assert "Basic Usage Example" in result.stdout

    def test_comprehensive_usage_runs(self):
        """Test that comprehensive_usage.py runs successfully."""
        result = subprocess.run(
            ["python3", "examples/comprehensive_usage.py"],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=Path(__file__).parent.parent.parent,
        )
        assert result.returncode == 0, f"comprehensive_usage.py failed: {result.stderr}"
        assert "Comprehensive Feature Showcase" in result.stdout

    def test_examples_show_v2_features(self):
        """Test that examples mention v2.0.0 features."""
        examples_dir = Path(__file__).parent.parent.parent / "examples"

        # Check basic_usage.py
        basic_content = (examples_dir / "basic_usage.py").read_text()
        assert "515 tests" in basic_content
        assert "2.0.0" in basic_content

        # Check comprehensive_usage.py
        comp_content = (examples_dir / "comprehensive_usage.py").read_text()
        assert "515 tests" in comp_content
        assert "2.0.0" in comp_content

    def test_example_outputs_captured(self):
        """Test that example outputs are saved."""
        outputs_dir = Path(__file__).parent.parent.parent / "outputs"

        # Check that outputs directory exists and has files
        assert outputs_dir.exists(), "outputs/ directory should exist"
        assert (outputs_dir / "basic_usage_output.txt").exists()
        assert (outputs_dir / "comprehensive_output.txt").exists()

        # Verify outputs contain expected content
        basic_output = (outputs_dir / "basic_usage_output.txt").read_text()
        assert "Mock Spark" in basic_output
        assert "DataFrame" in basic_output

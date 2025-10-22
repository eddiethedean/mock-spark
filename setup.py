#!/usr/bin/env python3
"""
Setup script for Mock Spark package with PySpark version compatibility support.

This extends the standard install command to create version marker files
when installing with version-specific extras like [pyspark-3-0].
"""

import os
import sys
from pathlib import Path
from setuptools import setup
from setuptools.command.install import install
from setuptools.command.develop import develop


class PostInstallCommand(install):
    """Post-installation command to create version marker files."""

    def run(self) -> None:
        # Run standard install first
        install.run(self)

        # Create version marker if version extra was used
        self._create_version_marker()

    def _create_version_marker(self) -> None:
        """Create .pyspark_X_Y_compat marker file based on install command."""

        # Check if a version extra was specified via environment variable
        # Users can set: MOCK_SPARK_INSTALL_VERSION=3.0 pip install mock-spark
        env_version = os.environ.get("MOCK_SPARK_INSTALL_VERSION")

        if env_version:
            self._create_marker_for_version(env_version)
            return

        # Try to detect from install_requires or command line
        # This is a best-effort detection
        install_args = " ".join(sys.argv)

        version_keywords = [
            ("pyspark-3-0", "3.0"),
            ("pyspark-3-1", "3.1"),
            ("pyspark-3-2", "3.2"),
            ("pyspark-3-3", "3.3"),
            ("pyspark-3-4", "3.4"),
            ("pyspark-3-5", "3.5"),
        ]

        for keyword, version in version_keywords:
            if keyword in install_args.lower():
                self._create_marker_for_version(version)
                return

        # No version extra detected - print info message
        print("\n" + "=" * 70)
        print("ℹ️  mock-spark installed with full API (all PySpark versions)")
        print("\nTo enable version-specific API compatibility, either:")
        print("  1. Reinstall with: MOCK_SPARK_INSTALL_VERSION=3.0 pip install .")
        print("  2. Set environment variable: export MOCK_SPARK_PYSPARK_VERSION=3.0")
        print(
            "  3. In code: from mock_spark._version_compat import set_pyspark_version"
        )
        print("              set_pyspark_version('3.0')")
        print("=" * 70 + "\n")

    def _create_marker_for_version(self, version: str) -> None:
        """Create marker file for specific version."""
        try:
            import mock_spark

            package_dir = Path(mock_spark.__file__).parent

            marker_filename = f".pyspark_{version.replace('.', '_')}_compat"
            marker_path = package_dir / marker_filename

            # Create marker file
            marker_path.touch(exist_ok=True)

            print("\n" + "=" * 70)
            print(f"✅ PySpark {version} API compatibility enabled")
            print(f"   Marker file created: {marker_path}")
            print(f"\n   Only PySpark {version} APIs will be available.")
            print("   Functions from later versions will raise AttributeError.")
            print("=" * 70 + "\n")

        except Exception as e:
            print(f"⚠️  Warning: Could not create version marker: {e}")
            print("   You can manually set version with:")
            print(f"   export MOCK_SPARK_PYSPARK_VERSION={version}")


class PostDevelopCommand(develop):
    """Post-develop command for editable installs."""

    def run(self) -> None:
        develop.run(self)

        # Same marker creation logic
        env_version = os.environ.get("MOCK_SPARK_INSTALL_VERSION")
        if env_version:
            try:
                import mock_spark

                package_dir = Path(mock_spark.__file__).parent
                marker_filename = f".pyspark_{env_version.replace('.', '_')}_compat"
                marker_path = package_dir / marker_filename
                marker_path.touch(exist_ok=True)
                print(f"✅ PySpark {env_version} API compatibility enabled (dev mode)")
            except Exception:
                pass


if __name__ == "__main__":
    setup(
        cmdclass={
            "install": PostInstallCommand,
            "develop": PostDevelopCommand,
        }
    )

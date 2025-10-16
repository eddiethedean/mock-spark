#!/usr/bin/env python3
"""
Post-installation script for mock-spark version compatibility.

This script runs after pip install to set up PySpark version compatibility
based on which extra was requested during installation.

Usage:
    pip install mock-spark[pyspark-3-0]  # Creates .pyspark_3_0_compat marker
    pip install mock-spark[pyspark-3-1]  # Creates .pyspark_3_1_compat marker
"""

import os
import sys
from pathlib import Path


def create_version_marker():
    """Create version compatibility marker file based on installation extras."""
    
    # Find the mock_spark package directory
    try:
        import mock_spark
        package_dir = Path(mock_spark.__file__).parent
    except ImportError:
        print("Warning: mock-spark not yet installed, skipping version marker creation")
        return
    
    # Check sys.argv for which extra was requested
    # During pip install, sys.argv contains the install command args
    install_args = ' '.join(sys.argv)
    
    # Map extras to marker files
    version_extras = {
        'pyspark-3-0': '.pyspark_3_0_compat',
        'pyspark-3-1': '.pyspark_3_1_compat',
        'pyspark-3-2': '.pyspark_3_2_compat',
        'pyspark-3-3': '.pyspark_3_3_compat',
        'pyspark-3-4': '.pyspark_3_4_compat',
        'pyspark-3-5': '.pyspark_3_5_compat',
    }
    
    # Check environment variable as alternative
    env_version = os.environ.get('MOCK_SPARK_INSTALL_VERSION')
    
    marker_created = False
    
    # Try to detect from environment first
    if env_version:
        for extra, marker_file in version_extras.items():
            if env_version in extra:
                marker_path = package_dir / marker_file
                marker_path.touch(exist_ok=True)
                print(f"✅ Created version marker: {marker_path}")
                print(f"   PySpark {env_version.replace('pyspark-', '').replace('-', '.')} API compatibility enabled")
                marker_created = True
                break
    
    # Try to detect from install args
    if not marker_created:
        for extra, marker_file in version_extras.items():
            if f'[{extra}]' in install_args or f"'{extra}'" in install_args or f'"{extra}"' in install_args:
                marker_path = package_dir / marker_file
                marker_path.touch(exist_ok=True)
                print(f"✅ Created version marker: {marker_path}")
                version_str = extra.replace('pyspark-', '').replace('-', '.')
                print(f"   PySpark {version_str} API compatibility enabled")
                marker_created = True
                break
    
    if not marker_created:
        print("ℹ️  No PySpark version extra detected - all features will be available")
        print("   To enable version gating, use: pip install mock-spark[pyspark-3-0]")
        print("   Or set: export MOCK_SPARK_PYSPARK_VERSION=3.0")


if __name__ == '__main__':
    create_version_marker()


#!/usr/bin/env python3
"""
Update the Mock-Spark column in PYSPARK_FUNCTION_MATRIX.md based on actual implementation.

This script scans the mock_spark codebase to identify which functions are actually implemented
and updates the matrix to reflect the actual implementation status.
"""

import re
import sys
from pathlib import Path


# Get the list of implemented functions from __init__.py
def get_implemented_functions():
    """Read the __all__ list from mock_spark/functions/__init__.py"""
    init_file = (
        Path(__file__).parent.parent / "mock_spark" / "functions" / "__init__.py"
    )

    with open(init_file) as f:
        content = f.read()

    # Extract the __all__ list
    match = re.search(r"__all__\s*=\s*\[(.*?)\]", content, re.DOTALL)
    if not match:
        return set()

    all_str = match.group(1)
    # Extract quoted strings
    functions = re.findall(r'"([^"]+)"', all_str)
    return set(functions)


def update_matrix_file(functions, matrix_file):
    """Update the matrix file with implementation status"""
    with open(matrix_file, "r") as f:
        lines = f.readlines()

    # Track changes
    updated_count = 0

    # Iterate through each line
    for i, line in enumerate(lines):
        # Check if this line is a function row
        # Format: | `function_name` | 3.0.3 | 3.1.3 | 3.2.4 | 3.3.4 | 3.4.3 | 3.5.2 | Mock-Spark |
        match = re.match(r"^\|\s+`([^`]+)`\s+\|.*$", line)
        if match:
            func_name = match.group(1)

            # Check if this function is in our implemented list
            if func_name in functions:
                # Parse the line to get the Mock-Spark column
                parts = line.split("|")
                if len(parts) >= 8:
                    # Mock-Spark column is index 7
                    current_status = parts[7].strip()

                    # Only update if it's not already ✅
                    if current_status != "✅" and current_status != "🔷":
                        parts[7] = " ✅ "
                        lines[i] = "|".join(parts)
                        updated_count += 1
                        print(f"Updated: {func_name} (was: '{current_status}')")

    # Write back
    with open(matrix_file, "w") as f:
        f.writelines(lines)

    return updated_count


def main():
    """Main entry point"""
    repo_root = Path(__file__).parent.parent
    matrix_file = repo_root / "PYSPARK_FUNCTION_MATRIX.md"

    print("Scanning mock_spark functions...")
    implemented = get_implemented_functions()
    print(f"Found {len(implemented)} implemented functions")

    print(f"Updating {matrix_file}...")
    updated = update_matrix_file(implemented, matrix_file)
    print(f"Updated {updated} entries")

    return 0


if __name__ == "__main__":
    sys.exit(main())

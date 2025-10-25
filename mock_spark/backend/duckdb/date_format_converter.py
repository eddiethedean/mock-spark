"""
Date format conversion utilities for Mock Spark.

This module provides conversion between Java SimpleDateFormat patterns
and DuckDB strptime format patterns.
"""


class DateFormatConverter:
    """Converts Java SimpleDateFormat patterns to DuckDB strptime format patterns."""

    @staticmethod
    def convert_java_to_duckdb_format(java_format: str) -> str:
        """Convert Java SimpleDateFormat to DuckDB strptime format.

        Args:
            java_format: Java SimpleDateFormat pattern (e.g., "yyyy-MM-dd HH:mm:ss")

        Returns:
            DuckDB strptime format pattern (e.g., "%Y-%m-%d %H:%M:%S")
        """
        # Common Java to DuckDB format conversions
        format_map = {
            "yyyy": "%Y",  # 4-digit year
            "yy": "%y",  # 2-digit year
            "MM": "%m",  # Month (01-12)
            "M": "%m",  # Month (1-12)
            "dd": "%d",  # Day (01-31)
            "d": "%d",  # Day (1-31)
            "HH": "%H",  # Hour (00-23)
            "H": "%H",  # Hour (0-23)
            "hh": "%I",  # Hour (01-12)
            "h": "%I",  # Hour (1-12)
            "mm": "%M",  # Minute (00-59)
            "m": "%M",  # Minute (0-59)
            "ss": "%S",  # Second (00-59)
            "s": "%S",  # Second (0-59)
            "SSS": "%f",  # Millisecond (000-999)
            "a": "%p",  # AM/PM
            "E": "%a",  # Day of week (abbreviated)
            "EEEE": "%A",  # Day of week (full)
            "z": "%Z",  # Timezone
            "Z": "%z",  # Timezone offset
        }

        # Replace Java format patterns with DuckDB equivalents
        # Use placeholders to avoid conflicts during replacement
        # Sort patterns by length (descending) to process longest matches first
        sorted_patterns = sorted(
            format_map.items(), key=lambda x: len(x[0]), reverse=True
        )

        # First pass: replace with unique placeholders using special unicode characters
        duckdb_format = java_format
        replacements = {}
        for i, (java_pattern, duckdb_pattern) in enumerate(sorted_patterns):
            # Use unicode placeholder that won't conflict with format patterns
            placeholder = f"\ue000{i}\ue001"
            duckdb_format = duckdb_format.replace(java_pattern, placeholder)
            replacements[placeholder] = duckdb_pattern

        # Second pass: replace placeholders with actual patterns
        for placeholder, duckdb_pattern in replacements.items():
            duckdb_format = duckdb_format.replace(placeholder, duckdb_pattern)

        return duckdb_format

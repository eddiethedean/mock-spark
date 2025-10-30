"""
Date format conversion utilities for Mock Spark.

This module provides conversion between Java SimpleDateFormat patterns
and DuckDB strptime format patterns.
"""

import re
from typing import Tuple, Optional


class DateFormatConverter:
    """Converts Java SimpleDateFormat patterns to DuckDB strptime format patterns."""

    @staticmethod
    def convert_java_to_duckdb_format(java_format: str) -> str:
        """Convert Java SimpleDateFormat to DuckDB strptime format.

        Args:
            java_format: Java SimpleDateFormat pattern (e.g., "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]")

        Returns:
            DuckDB strptime format pattern (e.g., "%Y-%m-%d %H:%M:%S" or "%Y-%m-%dT%H:%M:%S")
            Also handles optional fractional seconds patterns like [.SSSSSS]
        """
        # Handle optional fractional seconds pattern [.SSSSSS] or [.SSS]
        # Extract it first before processing the rest
        optional_fractional_pattern = None
        has_optional_fractional = False
        
        # Match patterns like [.SSSSSS] or [.SSS] (brackets indicate optional)
        fractional_match = re.search(r'\[\.(S+)\]', java_format)
        if fractional_match:
            has_optional_fractional = True
            optional_fractional_pattern = fractional_match.group(1)
            # Remove the optional pattern from format string for now
            java_format = java_format.replace(fractional_match.group(0), '')
        
        # Handle single-quoted literal characters in Java format (e.g., 'T' in yyyy-MM-dd'T'HH:mm:ss)
        # In Java SimpleDateFormat, single quotes escape literal characters
        # Extract and preserve them, then replace quotes with nothing (or handle specially)
        # Pattern to match quoted literals: 'X' or 'XX' etc.
        def replace_quoted_literals(text):
            """Replace quoted literals like 'T' with just the literal character."""
            # Match single-quoted characters: 'X' where X can be any character(s)
            # Use regex to find and replace '...' with just the content
            result = re.sub(r"'([^']*)'", r'\1', text)
            return result
        
        # Remove quoted literals (they'll be preserved as literal characters)
        java_format = replace_quoted_literals(java_format)
        
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
            "SSS": "%f",  # Millisecond (000-999) - note: DuckDB %f is microseconds (6 digits)
            "SSSSSS": "%f",  # Microseconds (6 digits) - maps to DuckDB %f
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
        
        # If we had optional fractional seconds, we need to handle them in SQL generation
        # For now, return the base format - the actual handling will be in SQL generation
        # The presence of optional fractional seconds will be handled by checking the original format
        return duckdb_format
    
    @staticmethod
    def extract_optional_fractional_seconds(java_format: str) -> Optional[Tuple[str, str]]:
        """Extract optional fractional seconds pattern from format string.
        
        Args:
            java_format: Java SimpleDateFormat pattern
            
        Returns:
            Tuple of (full_pattern_with_brackets, fractional_pattern) if found, None otherwise
            Example: ("[.SSSSSS]", "SSSSSS") or ("[.SSS]", "SSS")
        """
        fractional_match = re.search(r'\[\.(S+)\]', java_format)
        if fractional_match:
            return (fractional_match.group(0), fractional_match.group(1))
        return None

"""
DuckDB-based materialization for lazy evaluation.

This module provides DuckDB-specific optimizations on top of the generic SQLAlchemy materializer.
"""

from .query_executor import SQLAlchemyMaterializer


class DuckDBMaterializer(SQLAlchemyMaterializer):
    """Materializes lazy DataFrames using DuckDB with SQLAlchemy."""

    def __init__(self, max_memory: str = "1GB", allow_disk_spillover: bool = False):
        """Initialize DuckDB materializer.

        Args:
            max_memory: Maximum memory for DuckDB to use (e.g., '1GB', '4GB', '8GB').
            allow_disk_spillover: If True, allows DuckDB to spill to disk when memory is full.
        """
        # Initialize with DuckDB engine
        super().__init__(engine_url="duckdb:///:memory:")

        self._temp_dir = None

        # Configure DuckDB-specific settings using raw connection
        try:
            raw_conn = self.engine.raw_connection()
            raw_conn.execute(f"SET max_memory='{max_memory}'")

            if allow_disk_spillover:
                # Create unique temp directory for this materializer
                import tempfile
                import uuid

                self._temp_dir = tempfile.mkdtemp(
                    prefix=f"duckdb_mat_{uuid.uuid4().hex[:8]}_"
                )
                raw_conn.execute(f"SET temp_directory='{self._temp_dir}'")
            else:
                # Disable disk spillover for test isolation
                raw_conn.execute("SET temp_directory=''")
        except:  # noqa: E722
            pass  # Ignore if settings not supported

    def close(self) -> None:
        """Close the DuckDB connection and clean up temp directory."""
        # Clean up unique temp directory if it exists
        if self._temp_dir:
            try:
                import os
                import shutil

                if os.path.exists(self._temp_dir):
                    shutil.rmtree(self._temp_dir, ignore_errors=True)
                self._temp_dir = None
            except:  # noqa: E722
                pass  # Ignore cleanup errors

        # Call parent close to dispose engine
        super().close()

    def __del__(self) -> None:
        """Cleanup on deletion to prevent resource leaks."""
        try:
            self.close()
        except:  # noqa: E722
            pass

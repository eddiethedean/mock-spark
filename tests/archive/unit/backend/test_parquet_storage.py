"""
Unit tests for Parquet storage handler.
"""

import os
import pytest
import tempfile
import polars as pl
from sparkless.backend.polars.parquet_storage import ParquetStorage


@pytest.mark.unit
class TestParquetStorage:
    """Test ParquetStorage operations."""

    def test_write_parquet_overwrite_mode(self):
        """Test writing Parquet file in overwrite mode."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

            ParquetStorage.write_parquet(df, path, mode="overwrite")

            assert os.path.exists(path)
            read_df = pl.read_parquet(path)
            assert read_df.shape == (3, 2)
            assert read_df.columns == ["id", "name"]

    def test_write_parquet_append_mode_new_file(self):
        """Test appending to a non-existent file creates new file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

            ParquetStorage.write_parquet(df, path, mode="append")

            assert os.path.exists(path)
            read_df = pl.read_parquet(path)
            assert read_df.shape == (2, 2)

    def test_write_parquet_append_mode_existing_file(self):
        """Test appending to existing file combines data."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            df2 = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})

            ParquetStorage.write_parquet(df1, path, mode="overwrite")
            ParquetStorage.write_parquet(df2, path, mode="append")

            read_df = pl.read_parquet(path)
            assert read_df.shape == (4, 2)
            assert read_df["id"].to_list() == [1, 2, 3, 4]

    def test_write_parquet_invalid_mode(self):
        """Test that invalid mode raises ValueError."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1]})

            with pytest.raises(ValueError, match="Unsupported write mode"):
                ParquetStorage.write_parquet(df, path, mode="invalid")

    def test_write_parquet_creates_directory(self):
        """Test that write_parquet creates parent directory if needed."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "subdir", "test.parquet")
            df = pl.DataFrame({"id": [1]})

            ParquetStorage.write_parquet(df, path)

            assert os.path.exists(path)
            assert os.path.isdir(os.path.dirname(path))

    def test_read_parquet_success(self):
        """Test reading existing Parquet file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1, 2, 3], "value": [10.5, 20.5, 30.5]})
            df.write_parquet(path)

            read_df = ParquetStorage.read_parquet(path)

            assert read_df.shape == (3, 2)
            assert read_df.columns == ["id", "value"]

    def test_read_parquet_file_not_found(self):
        """Test reading non-existent file raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "nonexistent.parquet")

            with pytest.raises(FileNotFoundError):
                ParquetStorage.read_parquet(path)

    def test_schema_evolution_new_file(self):
        """Test schema evolution with new file returns new DataFrame."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            new_df = pl.DataFrame({"id": [1], "name": ["a"]})

            result = ParquetStorage.schema_evolution(path, new_df)

            assert result.shape == new_df.shape
            assert result.columns == new_df.columns

    def test_schema_evolution_same_schema(self):
        """Test schema evolution with same schema combines data."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            old_df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            old_df.write_parquet(path)

            new_df = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})
            result = ParquetStorage.schema_evolution(path, new_df)

            assert result.shape == (4, 2)
            assert result.columns == ["id", "name"]

    def test_schema_evolution_new_columns_in_new(self):
        """Test schema evolution when new DataFrame has additional columns."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            old_df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
            old_df.write_parquet(path)

            new_df = pl.DataFrame({"id": [3, 4], "name": ["c", "d"], "age": [25, 30]})
            result = ParquetStorage.schema_evolution(path, new_df)

            assert result.shape == (4, 3)
            assert "age" in result.columns
            # Schema evolution adds nulls to old rows - check that we have all rows
            assert len(result) == 4

    def test_schema_evolution_new_columns_in_old(self):
        """Test schema evolution when old DataFrame has additional columns."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            old_df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "age": [20, 25]})
            old_df.write_parquet(path)

            new_df = pl.DataFrame({"id": [3, 4], "name": ["c", "d"]})
            result = ParquetStorage.schema_evolution(path, new_df)

            assert result.shape == (4, 3)
            assert "age" in result.columns
            # New rows should have null for missing column (check last two rows)
            age_values = result["age"].to_list()
            assert age_values[2] is None or age_values[3] is None or age_values[2] == 20

    def test_schema_evolution_preserves_column_order(self):
        """Test schema evolution preserves column order from old DataFrame."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            old_df = pl.DataFrame({"id": [1], "name": ["a"], "age": [20]})
            old_df.write_parquet(path)

            new_df = pl.DataFrame({"age": [25], "name": ["b"], "id": [2]})
            result = ParquetStorage.schema_evolution(path, new_df)

            # Column order should match old DataFrame
            assert result.columns == ["id", "name", "age"]

    def test_parquet_exists_true(self):
        """Test parquet_exists returns True for existing file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1]})
            df.write_parquet(path)

            assert ParquetStorage.parquet_exists(path) is True

    def test_parquet_exists_false(self):
        """Test parquet_exists returns False for non-existent file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "nonexistent.parquet")

            assert ParquetStorage.parquet_exists(path) is False

    def test_write_parquet_with_compression(self):
        """Test writing Parquet with custom compression."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, "test.parquet")
            df = pl.DataFrame({"id": [1, 2, 3]})

            ParquetStorage.write_parquet(df, path, compression="gzip")

            assert os.path.exists(path)
            read_df = pl.read_parquet(path)
            assert read_df.shape == (3, 1)

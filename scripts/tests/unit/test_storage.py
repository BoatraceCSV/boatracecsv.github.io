"""Unit tests for storage module."""

import pytest
from pathlib import Path
from boatrace import storage


def test_write_csv_success(tmp_path):
    """Test successful CSV write."""
    csv_file = tmp_path / "test.csv"
    csv_content = "header1,header2\nvalue1,value2\n"

    success = storage.write_csv(str(csv_file), csv_content)

    assert success
    assert csv_file.exists()
    assert csv_file.read_text() == csv_content


def test_write_csv_creates_directories(tmp_path):
    """Test that write_csv creates parent directories."""
    csv_file = tmp_path / "a" / "b" / "c" / "test.csv"
    csv_content = "test,data\n"

    success = storage.write_csv(str(csv_file), csv_content)

    assert success
    assert csv_file.parent.exists()
    assert csv_file.exists()


def test_write_csv_skip_existing(tmp_path):
    """Test skipping existing file without force."""
    csv_file = tmp_path / "test.csv"
    original_content = "original,data\n"
    csv_file.write_text(original_content)

    new_content = "new,data\n"
    success = storage.write_csv(str(csv_file), new_content, force_overwrite=False)

    assert not success
    assert csv_file.read_text() == original_content


def test_write_csv_force_overwrite(tmp_path):
    """Test forcing overwrite of existing file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("original,data\n")

    new_content = "new,data\n"
    success = storage.write_csv(str(csv_file), new_content, force_overwrite=True)

    assert success
    assert csv_file.read_text() == new_content


def test_read_csv_success(tmp_path):
    """Test successful CSV read."""
    csv_file = tmp_path / "test.csv"
    csv_content = "header1,header2\nvalue1,value2\n"
    csv_file.write_text(csv_content)

    content = storage.read_csv(str(csv_file))

    assert content == csv_content


def test_read_csv_not_found(tmp_path):
    """Test reading non-existent file."""
    csv_file = tmp_path / "nonexistent.csv"

    content = storage.read_csv(str(csv_file))

    assert content is None


def test_file_exists_true(tmp_path):
    """Test file_exists returns True for existing file."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("test")

    exists = storage.file_exists(str(csv_file))

    assert exists


def test_file_exists_false(tmp_path):
    """Test file_exists returns False for non-existent file."""
    csv_file = tmp_path / "nonexistent.csv"

    exists = storage.file_exists(str(csv_file))

    assert not exists


def test_delete_csv_success(tmp_path):
    """Test successful file deletion."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("test")

    success = storage.delete_csv(str(csv_file))

    assert success
    assert not csv_file.exists()


def test_delete_csv_not_found(tmp_path):
    """Test deleting non-existent file."""
    csv_file = tmp_path / "nonexistent.csv"

    success = storage.delete_csv(str(csv_file))

    assert not success

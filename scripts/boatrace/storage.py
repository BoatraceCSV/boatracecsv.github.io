"""File I/O operations for CSV storage."""

from pathlib import Path
from typing import Optional
from . import logger as logging_module


def write_csv(
    file_path: str,
    csv_content: str,
    force_overwrite: bool = False,
) -> bool:
    """Write CSV content to file.

    Args:
        file_path: Path to output CSV file
        csv_content: CSV content (header + rows)
        force_overwrite: Whether to overwrite existing file

    Returns:
        True if write successful, False otherwise
    """
    try:
        path = Path(file_path)

        # Check if file exists
        if path.exists() and not force_overwrite:
            logging_module.info(
                "file_skipped",
                reason="already_exists",
                file_path=str(path),
            )
            return False

        # Create parent directories
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write file
        with open(path, "w", encoding="utf-8") as f:
            f.write(csv_content)

        # Verify file was written
        if not path.exists() or path.stat().st_size == 0:
            logging_module.error(
                "write_failed",
                reason="file_empty_or_missing",
                file_path=str(path),
            )
            return False

        logging_module.info(
            "file_written",
            file_path=str(path),
            size_bytes=path.stat().st_size,
        )
        return True

    except Exception as e:
        logging_module.error(
            "write_failed",
            file_path=file_path,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False


def read_csv(file_path: str) -> Optional[str]:
    """Read CSV content from file.

    Args:
        file_path: Path to CSV file

    Returns:
        CSV content if successful, None otherwise
    """
    try:
        path = Path(file_path)

        if not path.exists():
            logging_module.warning(
                "file_not_found",
                file_path=str(path),
            )
            return None

        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

        if not content:
            logging_module.warning(
                "file_empty",
                file_path=str(path),
            )
            return None

        return content

    except Exception as e:
        logging_module.error(
            "read_failed",
            file_path=file_path,
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


def file_exists(file_path: str) -> bool:
    """Check if CSV file exists.

    Args:
        file_path: Path to CSV file

    Returns:
        True if file exists, False otherwise
    """
    return Path(file_path).exists()


def delete_csv(file_path: str) -> bool:
    """Delete CSV file.

    Args:
        file_path: Path to CSV file

    Returns:
        True if delete successful, False otherwise
    """
    try:
        path = Path(file_path)

        if not path.exists():
            logging_module.warning(
                "delete_skipped",
                reason="file_not_found",
                file_path=str(path),
            )
            return False

        path.unlink()

        logging_module.info(
            "file_deleted",
            file_path=str(path),
        )
        return True

    except Exception as e:
        logging_module.error(
            "delete_failed",
            file_path=file_path,
            error=str(e),
            error_type=type(e).__name__,
        )
        return False

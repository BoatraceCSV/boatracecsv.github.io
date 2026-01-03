"""Extract LZH-compressed files."""

from typing import Dict, Optional
from io import BytesIO
from . import logger as logging_module

try:
    import lhafile
except ImportError:
    lhafile = None


class ExtractionError(Exception):
    """LZH extraction failed."""

    pass


def extract_lzh(lzh_bytes: bytes) -> Optional[Dict[str, str]]:
    """Extract LZH file and return contents.

    Args:
        lzh_bytes: LZH file content as bytes

    Returns:
        Dictionary of {filename: text_content} or None on failure
    """
    if lhafile is None:
        logging_module.error(
            "lhafile_not_installed",
            message="lhafile library not found. Install with: pip install lhafile",
        )
        return None

    try:
        # Open LZH archive from bytes using BytesIO
        lzh_file = BytesIO(lzh_bytes)
        lha = lhafile.LhaFile(lzh_file)
        result = {}

        # Extract each file
        for item in lha.infolist():
            # Skip directories
            if item.filename.endswith("/"):
                continue

            try:
                # Read file content
                file_content = lha.read(item.filename)

                # Decode from Shift-JIS to UTF-8
                # Shift-JIS is the encoding used by boatrace files
                text_content = file_content.decode("shift-jis")

                result[item.filename] = text_content

                logging_module.debug(
                    "file_extracted",
                    filename=item.filename,
                    size_bytes=len(file_content),
                )

            except UnicodeDecodeError as e:
                logging_module.warning(
                    "decode_error",
                    filename=item.filename,
                    error=str(e),
                )
            except Exception as e:
                logging_module.warning(
                    "extract_file_error",
                    filename=item.filename,
                    error=str(e),
                )

        if not result:
            logging_module.error(
                "extraction_empty",
                reason="No files extracted from archive",
            )
            return None

        logging_module.info(
            "lzh_extracted",
            files_count=len(result),
        )
        return result

    except Exception as e:
        logging_module.error(
            "extraction_failed",
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


def extract_k_file(lzh_bytes: bytes) -> Optional[str]:
    """Extract K-file (results) from LZH archive.

    Args:
        lzh_bytes: LZH file content

    Returns:
        K-file text content or None on failure
    """
    files = extract_lzh(lzh_bytes)
    if not files:
        return None

    # Find K-file (typically K??????.TXT)
    for filename, content in files.items():
        if filename.startswith("K") and filename.endswith(".TXT"):
            logging_module.info(
                "k_file_found",
                filename=filename,
            )
            return content

    logging_module.error(
        "k_file_not_found",
        available_files=list(files.keys()),
    )
    return None


def extract_b_file(lzh_bytes: bytes) -> Optional[str]:
    """Extract B-file (program) from LZH archive.

    Args:
        lzh_bytes: LZH file content

    Returns:
        B-file text content or None on failure
    """
    files = extract_lzh(lzh_bytes)
    if not files:
        return None

    # Find B-file (typically B??????.TXT)
    for filename, content in files.items():
        if filename.startswith("B") and filename.endswith(".TXT"):
            logging_module.info(
                "b_file_found",
                filename=filename,
            )
            return content

    logging_module.error(
        "b_file_not_found",
        available_files=list(files.keys()),
    )
    return None

"""Download K-files and B-files from boatrace official server."""

import time
import requests
from typing import Optional, Tuple
from . import logger as logging_module


class DownloadError(Exception):
    """Download operation failed."""

    pass


class RateLimiter:
    """Rate limiter to respect server limits."""

    def __init__(self, interval_seconds: float = 3.0):
        """Initialize rate limiter.

        Args:
            interval_seconds: Minimum seconds between requests
        """
        self.interval_seconds = interval_seconds
        self.last_request_time: float = 0.0

    def wait(self) -> None:
        """Wait if necessary to maintain rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.interval_seconds:
            time.sleep(self.interval_seconds - elapsed)
        self.last_request_time = time.time()


class ExponentialBackoff:
    """Exponential backoff strategy for retries."""

    def __init__(
        self,
        initial_seconds: float = 5.0,
        max_seconds: float = 30.0,
    ):
        """Initialize backoff strategy.

        Args:
            initial_seconds: Initial backoff interval
            max_seconds: Maximum backoff interval
        """
        self.initial_seconds = initial_seconds
        self.max_seconds = max_seconds
        self.current_attempt = 0

    def get_wait_time(self) -> float:
        """Get wait time for current attempt."""
        # Exponential backoff: initial * 2^attempt, capped at max
        wait_time = self.initial_seconds * (2 ** self.current_attempt)
        return min(wait_time, self.max_seconds)

    def reset(self) -> None:
        """Reset backoff state."""
        self.current_attempt = 0

    def increment(self) -> None:
        """Increment attempt counter."""
        self.current_attempt += 1


def download_file(
    url: str,
    max_retries: int = 3,
    timeout_seconds: int = 30,
    rate_limiter: Optional[RateLimiter] = None,
) -> Tuple[Optional[bytes], int]:
    """Download file from URL with retry logic.

    Args:
        url: URL to download
        max_retries: Maximum retry attempts
        timeout_seconds: Request timeout
        rate_limiter: Optional RateLimiter instance

    Returns:
        Tuple of (file_content, status_code) or (None, error_code) on failure
    """
    if rate_limiter is None:
        rate_limiter = RateLimiter()

    backoff = ExponentialBackoff()
    last_error: Optional[Exception] = None
    last_status_code: int = 0

    logging_module.info(
        "download_start",
        url=url,
        max_retries=max_retries,
    )

    for attempt in range(max_retries + 1):
        try:
            # Apply rate limiting
            rate_limiter.wait()

            # Make request
            response = requests.get(url, timeout=timeout_seconds)
            last_status_code = response.status_code

            if response.status_code == 200:
                logging_module.info(
                    "download_success",
                    url=url,
                    size_bytes=len(response.content),
                    attempt=attempt + 1,
                )
                return response.content, 200

            elif response.status_code == 404:
                # Not found - don't retry
                logging_module.info(
                    "download_skipped",
                    url=url,
                    reason="not_found",
                    status_code=404,
                )
                return None, 404

            elif response.status_code == 403:
                # Forbidden - don't retry
                logging_module.warning(
                    "download_forbidden",
                    url=url,
                    status_code=403,
                )
                return None, 403

            else:
                # Server error - retry
                last_error = DownloadError(
                    f"HTTP {response.status_code}"
                )
                if attempt < max_retries:
                    wait_time = backoff.get_wait_time()
                    logging_module.warning(
                        "download_retry",
                        url=url,
                        attempt=attempt + 1,
                        max_retries=max_retries,
                        status_code=response.status_code,
                        wait_seconds=wait_time,
                    )
                    backoff.increment()
                    time.sleep(wait_time)

        except requests.Timeout:
            last_error = DownloadError("Request timeout")
            if attempt < max_retries:
                wait_time = backoff.get_wait_time()
                logging_module.warning(
                    "download_timeout",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    wait_seconds=wait_time,
                )
                backoff.increment()
                time.sleep(wait_time)

        except requests.ConnectionError as e:
            last_error = DownloadError(f"Connection error: {str(e)}")
            if attempt < max_retries:
                wait_time = backoff.get_wait_time()
                logging_module.warning(
                    "download_connection_error",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    wait_seconds=wait_time,
                )
                backoff.increment()
                time.sleep(wait_time)

        except Exception as e:
            last_error = DownloadError(f"Unexpected error: {str(e)}")
            if attempt < max_retries:
                wait_time = backoff.get_wait_time()
                logging_module.warning(
                    "download_error",
                    url=url,
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(e),
                    wait_seconds=wait_time,
                )
                backoff.increment()
                time.sleep(wait_time)

    # All retries exhausted
    logging_module.error(
        "download_failed",
        url=url,
        reason=str(last_error) if last_error else "Unknown error",
        attempts=max_retries + 1,
    )
    return None, last_status_code


def download_boatrace_files(
    date: str,
    rate_limiter: Optional[RateLimiter] = None,
    max_retries: int = 3,
) -> Tuple[Optional[bytes], Optional[bytes]]:
    """Download K-file (results) and B-file (program) for a date.

    Args:
        date: Date string (YYYY-MM-DD format)
        rate_limiter: Optional RateLimiter instance
        max_retries: Maximum retry attempts per file

    Returns:
        Tuple of (k_file_content, b_file_content) or (None, None) if both fail
    """
    if rate_limiter is None:
        rate_limiter = RateLimiter()

    # Convert date to K-file and B-file format
    # e.g., 2025-12-01 -> K251201
    date_parts = date.split("-")
    year = date_parts[0][2:]  # Last 2 digits of year
    month = date_parts[1]
    day = date_parts[2]
    file_date = f"{year}{month}{day}"

    base_url = "http://www1.mbrace.or.jp/od2"
    k_file_url = f"{base_url}/K{file_date}.LZH"
    b_file_url = f"{base_url}/B{file_date}.LZH"

    # Download K-file
    k_content, k_status = download_file(
        k_file_url,
        max_retries=max_retries,
        rate_limiter=rate_limiter,
    )

    # Download B-file
    b_content, b_status = download_file(
        b_file_url,
        max_retries=max_retries,
        rate_limiter=rate_limiter,
    )

    # Both 404 means no races scheduled for this date
    if k_status == 404 and b_status == 404:
        logging_module.info(
            "no_races_scheduled",
            date=date,
        )

    return k_content, b_content

"""Scrape the monthly holding schedule (月間開催日程) from race.boatcast.jp.

Source (one file per stadium, keyed by the *current* month):

* ``/hp_txt/{jo}/bc_mon_2_{YYYYMM}_{jo}.txt``
    One row per 節 (race series):
    ``開始日(YYYYMMDD) \\t 終了日(YYYYMMDD) \\t グレード \\t タイトル \\t レース数``.
    The file spans well beyond its own month (empirically ~3 months of
    future 節), so fetching the current-month key is sufficient. Keys for
    future months return 403 until that month begins.

Unlike the ``bc_j_*`` race files this file has **no** ``data=`` marker —
the first line is the status (``"1"``) directly. Missing files return the
usual CloudFront 403 / HTML SPA fallback.
"""

from __future__ import annotations

from typing import List, Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import ScheduleEntry


def _format_yyyymmdd_to_iso(raw: str) -> Optional[str]:
    """``20260628`` -> ``2026-06-28``. Returns ``None`` for malformed input."""
    cleaned = (raw or "").strip()
    if len(cleaned) != 8 or not cleaned.isdigit():
        return None
    return f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]}"


def parse_mon2(body: str, stadium_code: int) -> Optional[List[ScheduleEntry]]:
    """Parse a ``bc_mon_2`` body into schedule entries.

    Returns ``None`` when the body is empty or the status line is not
    ``1``; rows without a parseable 開始日 are skipped.
    """
    if not body:
        return None
    lines = body.splitlines()
    if not lines or lines[0].split("\t")[0].strip() != "1":
        return None

    entries: List[ScheduleEntry] = []
    for raw in lines[1:]:
        if not raw.strip():
            continue
        cols = raw.split("\t")
        if len(cols) < 5:
            continue
        start_date = _format_yyyymmdd_to_iso(cols[0])
        if start_date is None:
            continue
        entries.append(
            ScheduleEntry(
                stadium_code=f"{stadium_code:02d}",
                start_date=start_date,
                end_date=_format_yyyymmdd_to_iso(cols[1]),
                grade=cols[2].strip() or None,
                title=cols[3].strip() or None,
                races=cols[4].strip() or None,
            )
        )
    return entries


class MonthlyScheduleScraper:
    """Fetch ``bc_mon_2`` for a stadium and parse into schedule entries."""

    def __init__(
        self,
        base_url: str = "https://race.boatcast.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36"
                )
            }
        )

    def _build_url(self, year_month: str, stadium_code: int) -> str:
        jo = f"{stadium_code:02d}"
        return f"{self.base_url}/hp_txt/{jo}/bc_mon_2_{year_month}_{jo}.txt"

    def _fetch_body(self, url: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)
        except requests.Timeout:
            logging_module.warning("monthly_schedule_timeout", url=url)
            return None
        except requests.ConnectionError as exc:
            logging_module.warning(
                "monthly_schedule_connection_error", url=url, error=str(exc)
            )
            return None

        if response.status_code in (403, 404):
            logging_module.debug(
                "monthly_schedule_not_found",
                url=url,
                status_code=response.status_code,
            )
            return None
        if response.status_code != 200:
            logging_module.warning(
                "monthly_schedule_http_error",
                url=url,
                status_code=response.status_code,
            )
            return None

        response.encoding = "utf-8"
        body = response.text
        if body.lstrip().startswith("<"):
            # CloudFront SPA fallback for missing files
            logging_module.debug("monthly_schedule_body_is_html", url=url)
            return None
        return body

    def scrape_stadium(
        self,
        year_month: str,
        stadium_code: int,
    ) -> Optional[List[ScheduleEntry]]:
        """Fetch + parse one stadium's monthly schedule.

        Args:
            year_month: ``YYYYMM`` — should be the current month (future
                months 403 on the server side).
            stadium_code: 1..24.

        Returns:
            List of :class:`ScheduleEntry` (possibly spanning several
            months ahead), or ``None`` when the file is missing /
            unparseable.
        """
        body = self._fetch_body(self._build_url(year_month, stadium_code))
        if body is None:
            return None
        return parse_mon2(body, stadium_code)


__all__ = [
    "MonthlyScheduleScraper",
    "parse_mon2",
]

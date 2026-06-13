"""Scrape recent-form (近況成績) data from race.boatcast.jp.

Two TSV variants are served per stadium per day:

* ``/hp_txt/{jo}/bc_zensou_{ymd}_{jo}.txt``         全国近況5節
* ``/hp_txt/{jo}/bc_zensou_touchi_{ymd}_{jo}.txt``  当地近況5節

Both share the same row layout (32 tab-separated columns):

    [0]  登録番号
    [1]  選手名 (full-width-space padded)
    [2..7]   前1節: 開始日 / 終了日 / 場コード / 場名 / グレード / 着順列
    [8..13]  前2節 (same 6-field layout)
    [14..19] 前3節
    [20..25] 前4節
    [26..31] 前5節

A non-existent file returns HTTP 403 with the CloudFront SPA fallback
HTML (mirroring the other boatcast endpoints), so we detect validity by
inspecting the body rather than the status code.

The scraper exposes a single ``scrape_stadium_day`` entry point that
returns a ``{registration_number: list[RecentFormSession]}`` mapping plus
each racer's name. Callers (e.g. ``scripts/recent-form.py``) do the
race-x-boat join with the B-file's ``registration_number`` to build per-
race ``RecentForm`` objects.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional, Tuple

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import RecentFormSession
from .original_exhibition_scraper import _normalize_name


# Each session block is exactly 6 fields, repeated 5 times after the 2-field
# identity prefix (登録番号, 選手名). 2 + 5*6 = 32 columns total.
_SESSIONS_PER_ROW = 5
_FIELDS_PER_SESSION = 6
_IDENTITY_PREFIX = 2


class RecentFormScraperError(Exception):
    """Recent-form scraping failed."""

    pass


# Public per-racer record returned by ``scrape_stadium_day``: registration
# number → (racer_name, list[RecentFormSession]). Sessions are ordered most
# recent first (index 0 = 前1節).
RecentFormRow = Tuple[Optional[str], List[RecentFormSession]]


class RecentFormScraper:
    """Scraper for recent-form data (national + local variants)."""

    VARIANT_NATIONAL = "national"
    VARIANT_LOCAL = "local"

    _VARIANT_TO_BASENAME = {
        VARIANT_NATIONAL: "bc_zensou",
        VARIANT_LOCAL: "bc_zensou_touchi",
    }

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

    # ---- Public API -----------------------------------------------------

    def scrape_stadium_day(
        self,
        date: str,
        stadium_code: int,
        variant: str,
    ) -> Optional[Dict[str, RecentFormRow]]:
        """Fetch and parse one stadium-day TSV for a chosen variant.

        Args:
            date: ``YYYY-MM-DD``.
            stadium_code: 1..24.
            variant: ``"national"`` (bc_zensou) or ``"local"`` (bc_zensou_touchi).

        Returns:
            ``{registration_number: (racer_name, sessions)}`` mapping, or
            ``None`` when the file is missing / unparseable.
        """
        if variant not in self._VARIANT_TO_BASENAME:
            raise ValueError(f"Unknown variant: {variant!r}")

        url = self._build_url(date, stadium_code, variant)

        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code in (403, 404):
                logging_module.debug(
                    "recent_form_not_found",
                    url=url,
                    variant=variant,
                    status_code=response.status_code,
                )
                return None
            if response.status_code != 200:
                logging_module.warning(
                    "recent_form_http_error",
                    url=url,
                    variant=variant,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text

            # CloudFront sometimes returns 200 + HTML SPA fallback. Detect
            # by leading "<".
            if body.lstrip().startswith("<"):
                logging_module.debug(
                    "recent_form_body_is_html",
                    url=url,
                    variant=variant,
                )
                return None

            return self._parse_tsv(body)

        except requests.Timeout:
            logging_module.warning(
                "recent_form_timeout",
                url=url,
                variant=variant,
                date=date,
                stadium=stadium_code,
            )
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "recent_form_connection_error",
                url=url,
                variant=variant,
                error=str(e),
            )
            return None
        except Exception as e:  # pragma: no cover - defensive
            logging_module.warning(
                "recent_form_unexpected_error",
                url=url,
                variant=variant,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    # ---- URL ------------------------------------------------------------

    def _build_url(self, date: str, stadium_code: int, variant: str) -> str:
        date_yyyymmdd = date.replace("-", "")
        jo = f"{stadium_code:02d}"
        basename = self._VARIANT_TO_BASENAME[variant]
        return f"{self.base_url}/hp_txt/{jo}/{basename}_{date_yyyymmdd}_{jo}.txt"

    # ---- Parser ---------------------------------------------------------

    @staticmethod
    def _parse_tsv(body: str) -> Dict[str, RecentFormRow]:
        """Parse the TSV body into ``{registration_number: (name, sessions)}``.

        Skips rows whose 登録番号 (col[0]) is missing. Rows shorter than 32
        fields are accepted (trailing sessions become empty).
        """
        result: Dict[str, RecentFormRow] = {}
        for raw in body.splitlines():
            if not raw.strip():
                continue
            cols = raw.split("\t")
            if len(cols) < _IDENTITY_PREFIX + _FIELDS_PER_SESSION:
                # Need at least one full session block for the row to be useful.
                continue
            reg = (cols[0] or "").strip()
            if not reg:
                continue
            name = _normalize_name(cols[1]) if len(cols) > 1 else None
            sessions = _parse_sessions(cols[_IDENTITY_PREFIX:])
            result[reg] = (name, sessions)
        return result


# ---------------------------------------------------------------------------
# Module-level helpers (also used by tests)
# ---------------------------------------------------------------------------


def _parse_sessions(payload_cols: List[str]) -> List[RecentFormSession]:
    """Parse the trailing ``5 * 6 = 30`` columns into 5 RecentFormSession.

    The list is always exactly 5 long. Missing trailing columns produce
    empty sessions so downstream consumers can iterate without bounds
    checks.
    """
    sessions: List[RecentFormSession] = []
    for slot in range(_SESSIONS_PER_ROW):
        base = slot * _FIELDS_PER_SESSION
        block = payload_cols[base : base + _FIELDS_PER_SESSION]
        if len(block) < _FIELDS_PER_SESSION:
            block = list(block) + [""] * (_FIELDS_PER_SESSION - len(block))
        sessions.append(_parse_session_block(block))
    return sessions


def _parse_session_block(block: List[str]) -> RecentFormSession:
    """Parse one 6-field session block into a :class:`RecentFormSession`.

    All-blank blocks (newer racers with fewer than 5 historic 節) return
    an empty session.
    """
    raw_start = (block[0] or "").strip()
    raw_end = (block[1] or "").strip()
    raw_stadium_code = (block[2] or "").strip()
    raw_stadium_name = block[3] or ""
    raw_grade = (block[4] or "").strip()
    raw_finish = block[5] or ""

    if not any([raw_start, raw_end, raw_stadium_code, raw_grade, raw_finish.strip()]):
        return RecentFormSession()

    return RecentFormSession(
        start_date=_format_yyyymmdd_to_iso(raw_start) or None,
        end_date=_format_yyyymmdd_to_iso(raw_end) or None,
        stadium_code=raw_stadium_code or None,
        stadium_name=_normalize_stadium_name(raw_stadium_name) or None,
        grade=raw_grade or None,
        finish_sequence=_normalize_finish_sequence(raw_finish) or None,
    )


def _format_yyyymmdd_to_iso(raw: str) -> str:
    """``20260411`` -> ``2026-04-11``. Returns ``""`` for malformed input."""
    if not raw:
        return ""
    cleaned = raw.strip()
    if len(cleaned) != 8 or not cleaned.isdigit():
        return ""
    return f"{cleaned[0:4]}-{cleaned[4:6]}-{cleaned[6:8]}"


def _normalize_stadium_name(raw: str) -> str:
    """Strip + collapse internal full-width spaces (used as visual padding)."""
    if raw is None:
        return ""
    return re.sub(r"　+", "", raw).strip()


# Full-width F / L flags emitted by ``bc_zensou`` are normalised to their
# half-width forms. Full-width digits ``１-６`` are *kept* because the
# README documents them as full-width (it is the convention used to
# disambiguate "race finish" from any other tokens in downstream tooling).
_FINISH_SEQUENCE_TRANSLATIONS = str.maketrans({
    "Ｆ": "F",  # フライング
    "Ｌ": "L",  # 出遅れ
})


def _normalize_finish_sequence(raw: str) -> str:
    """Trim trailing full-width-space padding and normalise letter tokens.

    The boatcast source emits full-width F / L flags (``Ｆ`` / ``Ｌ``) but
    documents them as half-width in CSV output. Internal full-width spaces
    are *kept* because they are meaningful (they separate sub-races within
    a session — typically day-to-day boundaries within a 節).

    Other token characters (``欠``/``転``/``妨``/``落``/``エ``/``不``/
    ``沈``/``失``) and full-width digits ``１-６`` pass through unchanged.
    """
    if raw is None:
        return ""
    trimmed = raw.rstrip("　 \t\r\n")
    return trimmed.translate(_FINISH_SEQUENCE_TRANSLATIONS)


__all__ = [
    "RecentFormScraper",
    "RecentFormScraperError",
    "RecentFormRow",
    "_parse_session_block",
    "_parse_sessions",
    "_format_yyyymmdd_to_iso",
    "_normalize_stadium_name",
    "_normalize_finish_sequence",
]

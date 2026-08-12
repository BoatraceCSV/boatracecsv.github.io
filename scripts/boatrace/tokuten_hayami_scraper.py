"""Scrape 得点率早見 (score-rate quick reference) from race.boatcast.jp.

The site exposes per-race TSV files at:
    https://race.boatcast.jp/hp_txt/{jo:02d}/bc_j_tokuten_hayami_{YYYYMMDD}_{jo:02d}_{race:02d}.txt

Format (tab-separated, UTF-8)::

    line 1: "data="                 (literal marker)
    line 2: "1"                     (ready flag; anything else = not published)
    line 3..8: one row per boat     (see below)
    line 9: placement points        "10\t08\t06\t04\t02\t01"
    line 10..: border rank          "18"

Boat row columns (index)::

    0  枠 (1..6)
    1  級別 (A1 / A2 / B1 / B2)
    2  登録番号
    3  選手名 (full-width padded)
    4  ボーダー状態 ("00" / "01"; last digit 1 = 順位がボーダー以内)
    5  得点率 (numeric, or 賞除 / 欠場 / 帰郷 / 追配)
    6  順位 (within the series)
    7..18  6 pairs of (状態, その着順を取った場合の得点率), 1着 -> 6着
    20 早見 (the racer's other race number today; blank when only one race)

得点率 is the average placement point (得点の合計 ÷ 出走数), so the "if 1着"
cell is ``(現在の得点合計 + 1着点) / (出走数 + 1)``.

The table only exists up to 予選最終日 and is not published for every series
(the SPA consults ``/mu_txt/noScoringRate.json``). A missing race returns
HTTP 403 with an HTML body (CloudFront error page), so validity is detected
by the "data=" prefix rather than the status code — same as ``bc_oriten``.
"""

import re
from typing import List, Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import TokutenHayamiData, TokutenHayamiRacer

# Boat rows occupy lines 3..8 (0-indexed 2..7); line 9 (index 8) holds the
# placement points and the border rank follows it.
_BOAT_ROW_START = 2
_BOAT_ROW_END = 8
_RANK_POINT_ROW = 8
_BORDER_RANK_ROW = 9

# Column indices inside a boat row (see module docstring).
_COL_CLASS = 1
_COL_REGISTRATION = 2
_COL_NAME = 3
_COL_BORDER_STATUS = 4
_COL_SCORE_RATE = 5
_COL_RANK = 6
_COL_IF_RANK_START = 7
_COL_OTHER_RACE = 20

RANK_COUNT = 6


class TokutenHayamiScraperError(Exception):
    """得点率早見 scraping failed."""

    pass


class TokutenHayamiScraper:
    """Scraper for 得点率早見 (score-rate quick reference)."""

    def __init__(
        self,
        base_url: str = "https://race.boatcast.jp",
        timeout_seconds: int = 30,
        rate_limiter: Optional[RateLimiter] = None,
    ):
        """Initialize scraper.

        Args:
            base_url: Base URL for the boatcast site.
            timeout_seconds: HTTP request timeout.
            rate_limiter: Optional shared RateLimiter.
        """
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.rate_limiter = rate_limiter or RateLimiter()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    # ---- Public API -----------------------------------------------------

    def scrape_race(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[TokutenHayamiData]:
        """Fetch and parse one race.

        Args:
            date: YYYY-MM-DD
            stadium_code: 1..24
            race_number: 1..12

        Returns:
            TokutenHayamiData, or None when the file does not exist (series
            without a score-rate table / after 予選最終日 / race not held) or
            fetch/parse failed.
        """
        url = self._build_url(date, stadium_code, race_number)

        try:
            logging_module.debug(
                "tokuten_hayami_fetch_start",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )

            self.rate_limiter.wait()

            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code in (403, 404):
                # File does not exist (no race held, no score-rate table).
                logging_module.debug(
                    "tokuten_hayami_not_found",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            if response.status_code != 200:
                logging_module.warning(
                    "tokuten_hayami_http_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text

            # CloudFront sometimes returns 200 with an HTML body when the race
            # does not exist (SPA fallback). Detect via the "data=" marker.
            if not body.lstrip().startswith("data="):
                logging_module.debug("tokuten_hayami_body_not_tsv", url=url)
                return None

            return self._parse_tsv(body, date, stadium_code, race_number)

        except requests.Timeout:
            logging_module.warning(
                "tokuten_hayami_timeout",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "tokuten_hayami_connection_error",
                url=url,
                error=str(e),
            )
            return None
        except Exception as e:  # pragma: no cover - defensive
            logging_module.warning(
                "tokuten_hayami_unexpected_error",
                url=url,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    # ---- Helpers --------------------------------------------------------

    def _build_url(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> str:
        """Compose the TSV URL for a single race."""
        date_yyyymmdd = date.replace("-", "")
        jo = f"{stadium_code:02d}"
        rno = f"{race_number:02d}"
        return (
            f"{self.base_url}/hp_txt/{jo}/"
            f"bc_j_tokuten_hayami_{date_yyyymmdd}_{jo}_{rno}.txt"
        )

    def _parse_tsv(
        self,
        body: str,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[TokutenHayamiData]:
        """Parse the TSV body into TokutenHayamiData.

        Returns None when the body is malformed.
        """
        try:
            lines = body.splitlines()
            if len(lines) < 2 or not lines[0].startswith("data="):
                return None

            data = TokutenHayamiData(
                date=date,
                stadium_number=stadium_code,
                race_number=race_number,
                race_code=self._race_code(date, stadium_code, race_number),
                status=_cell(lines[1].split("\t"), 0),
            )

            # Not published yet: the file carries only the marker + flag.
            if not data.is_ready():
                return data

            for raw in lines[_BOAT_ROW_START:_BOAT_ROW_END]:
                racer = _parse_boat_row(raw)
                if racer is not None:
                    data.racers.append(racer)

            if len(lines) > _RANK_POINT_ROW:
                points = lines[_RANK_POINT_ROW].split("\t")
                data.rank_points = [_cell(points, i) for i in range(RANK_COUNT)]

            if len(lines) > _BORDER_RANK_ROW:
                data.border_rank = _cell(lines[_BORDER_RANK_ROW].split("\t"), 0)

            return data

        except Exception as e:
            logging_module.debug(
                "tokuten_hayami_parse_error",
                date=date,
                stadium=stadium_code,
                race=race_number,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    @staticmethod
    def _race_code(date: str, stadium_code: int, race_number: int) -> str:
        return f"{date.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _parse_boat_row(raw: str) -> Optional[TokutenHayamiRacer]:
    """Parse one boat row. Returns None when the row is not a boat row."""
    if not raw.strip():
        return None
    parts = raw.split("\t")
    try:
        boat_no = int(parts[0].strip())
    except (IndexError, ValueError):
        return None
    if boat_no < 1 or boat_no > 6:
        return None

    if_rank_score_rates: List[Optional[str]] = []
    if_rank_statuses: List[Optional[str]] = []
    for k in range(RANK_COUNT):
        base = _COL_IF_RANK_START + k * 2
        if_rank_statuses.append(_cell(parts, base))
        if_rank_score_rates.append(_cell(parts, base + 1))

    return TokutenHayamiRacer(
        boat_number=boat_no,
        class_grade=_cell(parts, _COL_CLASS),
        registration_number=_cell(parts, _COL_REGISTRATION),
        racer_name=_normalize_name(parts[_COL_NAME])
        if len(parts) > _COL_NAME
        else None,
        border_status=_cell(parts, _COL_BORDER_STATUS),
        score_rate=_cell(parts, _COL_SCORE_RATE),
        rank=_cell(parts, _COL_RANK),
        other_race_number=_cell(parts, _COL_OTHER_RACE),
        if_rank_score_rates=if_rank_score_rates,
        if_rank_statuses=if_rank_statuses,
    )


def _cell(parts: List[str], index: int) -> Optional[str]:
    """Return the trimmed cell at *index*, or None when absent / empty."""
    if index >= len(parts):
        return None
    value = parts[index].strip()
    return value if value else None


def _normalize_name(name: str) -> str:
    """Collapse the full-width padding spaces the source uses in names."""
    if name is None:
        return ""
    stripped = name.strip().strip("　")
    return re.sub(r"　+", " ", stripped)


__all__ = [
    "TokutenHayamiScraper",
    "TokutenHayamiScraperError",
    "RANK_COUNT",
]

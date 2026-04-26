"""Scrape race-card detail (出走表詳細) data from race.boatcast.jp.

The site exposes per-race TSV files at:
    https://race.boatcast.jp/hp_txt/{jo:02d}/bc_j_str3_{YYYYMMDD}_{jo:02d}_{race:02d}.txt

Format (tab-separated, UTF-8):
    line 1: "data="                (literal marker)
    line 2: "{status}\t{ncols}"    (status: "1" normal, "2" not held / data
                                     unavailable. ncols: typically "6")
    line 3..8: one row per boat (6 rows). 39 columns each.

Column mapping is reverse-engineered from ``RacerPerformance.js`` and
``SectionPerformance.js`` on race.boatcast.jp:

    [0]  登録番号
    [1]  選手名 (full-width-space padded)
    [2]  期別 (e.g. "81期")
    [3]  支部:出身地 (full-width spaces inside; ":" separator)
    [4]  年齢
    [5]  級別 ("A1"/"A2"/"B1"/"B2")
    [6]  (unused)
    [7]  F本数
    [8]  L本数
    [9]  全国平均ST
    [10] 全国勝率
    [11] 全国2連対率
    [12] 全国3連対率
    [13] 当地勝率
    [14] 当地2連対率
    [15] 当地3連対率
    [16] モーターフラグ ("1" = special state, else "0")
    [17] モーター番号
    [18] モーター2連対率
    [19] モーター3連対率
    [20] ボートフラグ
    [21] ボート番号
    [22] ボート2連対率
    [23] ボート3連対率
    [24] 早見 (other race number same day; blank when only one race)
    [25..38] 節間14スロット, each "{R番号},{進入},{枠},{ST},{着順}"

A non-existent race returns HTTP 403 with an HTML SPA fallback body, so we
detect validity by checking the "data=" prefix rather than the status code
(matching ``original_exhibition_scraper.py``).
"""

from __future__ import annotations

import re
from typing import List, Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .models import RaceCard, RaceCardBoat, RaceCardSession
from .original_exhibition_scraper import _normalize_name, _to_float


class RaceCardScraperError(Exception):
    """Race card scraping failed."""

    pass


# Number of session slots in a bc_j_str3 row (cols [25..38]).
_SESSION_SLOTS = 14
_FIRST_SESSION_COL = 25

# Tokens used inside ``着順`` of a session quintuple. We canonicalise full-width
# digits to half-width strings but pass through letter tokens unchanged.
_FULLWIDTH_DIGITS = {
    "１": "1",
    "２": "2",
    "３": "3",
    "４": "4",
    "５": "5",
    "６": "6",
    "７": "7",  # safety; should not appear
    "８": "8",
    "９": "9",
}


class RaceCardScraper:
    """Scraper for race-card detail (出走表詳細) data."""

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

    def scrape_race(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RaceCard]:
        """Fetch and parse one race's bc_j_str3 TSV.

        Args:
            date: ``YYYY-MM-DD``.
            stadium_code: 1..24.
            race_number: 1..12.

        Returns:
            :class:`RaceCard` (with up to 6 boats), or ``None`` when the file
            does not exist or cannot be parsed.
        """
        url = self._build_url(date, stadium_code, race_number)

        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)

            if response.status_code in (403, 404):
                logging_module.debug(
                    "race_card_not_found",
                    url=url,
                    status_code=response.status_code,
                )
                return None
            if response.status_code != 200:
                logging_module.warning(
                    "race_card_http_error",
                    url=url,
                    status_code=response.status_code,
                )
                return None

            response.encoding = "utf-8"
            body = response.text

            if not body.lstrip().startswith("data="):
                # CloudFront SPA fallback, treated as "not found".
                logging_module.debug("race_card_body_not_tsv", url=url)
                return None

            return self._parse_tsv(body, date, stadium_code, race_number)

        except requests.Timeout:
            logging_module.warning(
                "race_card_timeout",
                url=url,
                date=date,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        except requests.ConnectionError as e:
            logging_module.warning(
                "race_card_connection_error",
                url=url,
                error=str(e),
            )
            return None
        except Exception as e:  # pragma: no cover - defensive
            logging_module.warning(
                "race_card_unexpected_error",
                url=url,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    # ---- URL construction ----------------------------------------------

    def _build_url(
        self,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> str:
        date_yyyymmdd = date.replace("-", "")
        jo = f"{stadium_code:02d}"
        rno = f"{race_number:02d}"
        return f"{self.base_url}/hp_txt/{jo}/bc_j_str3_{date_yyyymmdd}_{jo}_{rno}.txt"

    # ---- Parser ---------------------------------------------------------

    def _parse_tsv(
        self,
        body: str,
        date: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RaceCard]:
        try:
            lines = body.splitlines()
            if len(lines) < 2:
                return None
            if not lines[0].startswith("data="):
                return None

            meta_parts = lines[1].split("\t")
            status = meta_parts[0].strip() if meta_parts and meta_parts[0] else None
            ncols: Optional[int] = None
            if len(meta_parts) >= 2 and meta_parts[1].strip().isdigit():
                ncols = int(meta_parts[1].strip())

            card = RaceCard(
                date=date,
                stadium_number=stadium_code,
                race_number=race_number,
                race_code=self._race_code(date, stadium_code, race_number),
                status=status if status else None,
                ncols=ncols,
            )

            # Status "2" = race not held; meta lines only.
            if status == "2":
                return card
            if len(lines) < 3:
                return card

            for boat_index, raw in enumerate(lines[2:], start=1):
                if boat_index > 6:
                    break
                if not raw.strip():
                    continue
                boat = self._parse_boat_row(raw, boat_index)
                if boat is not None:
                    card.boats.append(boat)

            return card

        except Exception as e:
            logging_module.debug(
                "race_card_parse_error",
                date=date,
                stadium=stadium_code,
                race=race_number,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None

    @staticmethod
    def _parse_boat_row(raw: str, boat_number: int) -> Optional[RaceCardBoat]:
        cols = raw.split("\t")
        if len(cols) < 25:
            # Need at least the racer profile + stat block.
            return None

        boat = RaceCardBoat(
            boat_number=boat_number,
            registration_number=_strip(cols[0]) or None,
            racer_name=_normalize_name(cols[1]) if len(cols) > 1 else None,
            period=_strip(cols[2]) or None,
            grade=_strip(cols[5]) or None,
            f_count=_to_int(cols[7]) if len(cols) > 7 else None,
            l_count=_to_int(cols[8]) if len(cols) > 8 else None,
            national_avg_st=_to_float(cols[9]),
            national_win_rate=_to_float(cols[10]),
            national_double_rate=_to_float(cols[11]),
            national_triple_rate=_to_float(cols[12]),
            local_win_rate=_to_float(cols[13]),
            local_double_rate=_to_float(cols[14]),
            local_triple_rate=_to_float(cols[15]),
            motor_flag=_to_int(cols[16]),
            motor_number=_to_int(cols[17]),
            motor_double_rate=_to_float(cols[18]),
            motor_triple_rate=_to_float(cols[19]),
            boat_flag=_to_int(cols[20]),
            boat_id=_to_int(cols[21]),
            boat_double_rate=_to_float(cols[22]),
            boat_triple_rate=_to_float(cols[23]),
            hayami=_to_int(cols[24]) if len(cols) > 24 else None,
            age=_to_int(cols[4]) if len(cols) > 4 else None,
        )

        # 支部:出身地 (col[3]) — full-width spaces inside, separated by ASCII ":".
        if len(cols) > 3:
            branch_birth = _collapse_fullwidth(cols[3])
            if branch_birth:
                if ":" in branch_birth:
                    branch, _, birth = branch_birth.partition(":")
                    boat.branch = branch.strip() or None
                    boat.birthplace = birth.strip() or None
                else:
                    boat.branch = branch_birth or None

        # Sessions: cols[25..38]. Always emit 14 entries; missing or empty
        # quintuples become an empty RaceCardSession (all fields None).
        for slot in range(_SESSION_SLOTS):
            ci = _FIRST_SESSION_COL + slot
            raw_quintuple = cols[ci] if ci < len(cols) else ""
            boat.sessions.append(_parse_session_quintuple(raw_quintuple))

        return boat

    # ---- Helpers --------------------------------------------------------

    @staticmethod
    def _race_code(date: str, stadium_code: int, race_number: int) -> str:
        return f"{date.replace('-', '')}{stadium_code:02d}{race_number:02d}"


# ---------------------------------------------------------------------------
# Module-level helpers (also used by tests)
# ---------------------------------------------------------------------------


def _parse_session_quintuple(raw: str) -> RaceCardSession:
    """Parse one ``"R,進入,枠,ST,着順"`` quintuple into a :class:`RaceCardSession`.

    Empty / placeholder strings return an all-``None`` session.
    """
    session = RaceCardSession()
    if raw is None:
        return session
    cleaned = raw.strip()
    if not cleaned:
        return session
    parts = [p.strip() for p in cleaned.split(",")]
    if len(parts) < 5:
        # Defensive: treat malformed as empty.
        return session
    # Detect the placeholder "-,-,-,-,-" (any subset of dashes also).
    if all(p in ("", "-") for p in parts[:5]):
        return session

    session.race_number = _to_int(parts[0])
    session.entry_course = _to_int(parts[1])
    session.waku = _to_int(parts[2])
    session.start_timing = _parse_session_st(parts[3])
    session.finish_position = _normalize_finish_position(parts[4])
    return session


def _parse_session_st(raw: str) -> Optional[float]:
    """Parse ST values like ``".10"`` -> 0.10, ``"0.13"`` -> 0.13.

    Returns ``None`` for empty / dash / non-numeric inputs.
    """
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned or cleaned == "-":
        return None
    if cleaned.startswith("."):
        cleaned = "0" + cleaned
    elif cleaned.startswith("-."):
        cleaned = "-0" + cleaned[1:]
    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_finish_position(raw: str) -> Optional[str]:
    """Convert ``"１"-"６"`` (full-width) to ``"1"-"6"``; pass through letter
    tokens (``"F"``/``"L"``/``"欠"``/``"転"``/``"妨"``/``"落"``).
    """
    if raw is None:
        return None
    cleaned = raw.strip()
    if not cleaned or cleaned == "-":
        return None
    # Map a single full-width digit if present.
    if len(cleaned) == 1 and cleaned in _FULLWIDTH_DIGITS:
        return _FULLWIDTH_DIGITS[cleaned]
    return cleaned


def _to_int(raw) -> Optional[int]:
    if raw is None:
        return None
    cleaned = str(raw).strip()
    if not cleaned or cleaned == "-":
        return None
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return None


def _strip(raw) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def _collapse_fullwidth(raw: str) -> str:
    """Collapse internal full-width spaces (used for visual padding) and trim."""
    if raw is None:
        return ""
    return re.sub(r"　+", "", raw).strip()


__all__ = [
    "RaceCardScraper",
    "RaceCardScraperError",
    "_parse_session_quintuple",
    "_parse_session_st",
    "_normalize_finish_position",
]

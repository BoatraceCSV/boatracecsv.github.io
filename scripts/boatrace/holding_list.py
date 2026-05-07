"""Fetch and parse race.boatcast.jp ``getHoldingList2`` JSON.

This is the data source the SPA at https://race.boatcast.jp/ uses to render
"today's open venues" and per-race deadline times. We use it in
``preview-realtime.py`` to decide which races to scrape at any given minute,
without needing a B-file download.

Endpoint::

    https://race.boatcast.jp/api_txt/getHoldingList2_{YYYYMMDD}.json

Response shape (subset we care about, per ``return_info[]`` entry)::

    RaceStudiumNo:    "01" .. "24"          # 会場コード
    RecentRace:       "01" .. "12"          # 直近のレース番号 (進行中)
    HoldingTitle:     str                   # 開催タイトル
    DailyTitle:       str                   # 開催日数 ("3日目" など)
    RaceTitleAll:     [str x 12]            # 各レースタイトル (1R..12R)
    DeadlineTimeAll:  ["HH:MM" x 12]        # 各レース締切時刻
    CancelStatusAll:  [str x 12]            # "" | "順延" | "中止" | "途中中止"
    EntryFixedAll:    ["0"|"1" x 12]        # 出走確定フラグ

A race is *eligible* for realtime preview scraping when its
``CancelStatusAll[i]`` is the empty string. Anything else (順延 / 中止 /
途中中止) means there is no preview to fetch.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import requests

from . import logger as logging_module
from .downloader import RateLimiter


HOLDING_LIST_BASE_URL = "https://race.boatcast.jp/api_txt"


class HoldingListError(Exception):
    """Failed to fetch / parse the holding list."""


@dataclass
class HoldingRace:
    """One race entry resolved from getHoldingList2."""

    stadium_code: int          # 1..24
    race_number: int           # 1..12
    deadline_time: str         # "HH:MM" (JST)
    cancel_status: str         # "" | "順延" | "中止" | "途中中止"
    title: Optional[str]       # 当該レースのタイトル

    @property
    def is_open(self) -> bool:
        """True when the race is not cancelled / postponed."""
        return self.cancel_status == ""

    @property
    def race_code(self) -> str:
        """``YYYYMMDD`` not included; supply via :func:`build_race_code`."""
        return f"{self.stadium_code:02d}{self.race_number:02d}"


def build_race_code(date_str: str, stadium_code: int, race_number: int) -> str:
    """Compose the standard race_code (``YYYYMMDDjjrr``)."""
    return f"{date_str.replace('-', '')}{stadium_code:02d}{race_number:02d}"


def _holding_list_url(date_str: str) -> str:
    """``2026-05-03`` -> ``.../getHoldingList2_20260503.json``."""
    return f"{HOLDING_LIST_BASE_URL}/getHoldingList2_{date_str.replace('-', '')}.json"


def fetch_holding_list(
    date_str: str,
    timeout_seconds: int = 30,
    rate_limiter: Optional[RateLimiter] = None,
    session: Optional[requests.Session] = None,
) -> List[HoldingRace]:
    """Download and flatten getHoldingList2 into a list of races.

    Args:
        date_str: ``YYYY-MM-DD``.
        timeout_seconds: HTTP timeout.
        rate_limiter: Optional shared rate limiter.
        session: Optional ``requests.Session``.

    Returns:
        List of :class:`HoldingRace`. Empty list when no venues are open
        today.

    Raises:
        HoldingListError: when the HTTP request itself fails or the JSON
            cannot be parsed. A 404 (no holdings on this date) is treated
            as an empty list rather than an error.
    """
    url = _holding_list_url(date_str)
    sess = session or requests.Session()
    if rate_limiter:
        rate_limiter.wait()

    logging_module.debug("holding_list_fetch_start", url=url, date=date_str)

    try:
        response = sess.get(url, timeout=timeout_seconds)
    except requests.RequestException as exc:
        logging_module.error(
            "holding_list_fetch_error",
            url=url,
            error=str(exc),
            error_type=type(exc).__name__,
        )
        raise HoldingListError(f"failed to fetch holding list: {exc}") from exc

    if response.status_code == 404:
        logging_module.info(
            "holding_list_no_holdings", date=date_str, url=url
        )
        return []
    if response.status_code != 200:
        logging_module.error(
            "holding_list_http_error",
            url=url,
            status_code=response.status_code,
        )
        raise HoldingListError(
            f"holding list HTTP {response.status_code}"
        )

    try:
        payload = response.json()
    except ValueError as exc:
        logging_module.error(
            "holding_list_parse_error", url=url, error=str(exc)
        )
        raise HoldingListError(f"holding list invalid JSON: {exc}") from exc

    return _parse_holding_payload(payload, date_str)


def _parse_holding_payload(payload: dict, date_str: str) -> List[HoldingRace]:
    """Flatten the JSON response into per-race :class:`HoldingRace`."""
    races: List[HoldingRace] = []
    return_info = payload.get("return_info") if isinstance(payload, dict) else None
    if not isinstance(return_info, list):
        logging_module.warning(
            "holding_list_missing_return_info", date=date_str
        )
        return races

    for venue in return_info:
        if not isinstance(venue, dict):
            continue
        try:
            stadium_code = int(venue.get("RaceStudiumNo", ""))
        except (TypeError, ValueError):
            continue
        if stadium_code < 1 or stadium_code > 24:
            continue

        deadlines = venue.get("DeadlineTimeAll") or []
        cancels = venue.get("CancelStatusAll") or []
        titles = venue.get("RaceTitleAll") or []

        for i in range(min(12, len(deadlines))):
            deadline = (deadlines[i] or "").strip()
            if not deadline:
                continue
            cancel = (cancels[i] if i < len(cancels) else "") or ""
            title = (titles[i] if i < len(titles) else None)
            if isinstance(title, str):
                # boatcast pads titles with full-width spaces (例: "予選　　　　　").
                title = title.strip("　 ").strip() or None

            races.append(
                HoldingRace(
                    stadium_code=stadium_code,
                    race_number=i + 1,
                    deadline_time=deadline,
                    cancel_status=cancel,
                    title=title if isinstance(title, str) else None,
                )
            )

    logging_module.info(
        "holding_list_parsed",
        date=date_str,
        venues=len(return_info),
        races=len(races),
    )
    return races


def load_holding_from_title_csv(
    project_root: Path,
    date_str: str,
) -> List[HoldingRace]:
    """Load :class:`HoldingRace` list from ``data/programs/title/.../DD.csv``.

    The live ``getHoldingList2`` API rewrites a race's ``DeadlineTimeAll[i]``
    entry from ``"HH:MM"`` to ``"締切"`` (closing) and finally ``"確定"``
    (finalised) once the race progresses past its deadline. After that
    point :func:`fetch_holding_list` cannot tell us *when* a finished
    race's deadline was, so :func:`select_finished_races` in
    ``preview-realtime.py`` would silently drop every result candidate.

    The daily-sync workflow snapshots all 12 races' scheduled deadlines
    at JST 08:30 (before any race starts) into
    ``data/programs/title/{YYYY}/{MM}/{DD}.csv`` (column
    ``電話投票締切予定``). That file is therefore a stable truth source
    for "what was the deadline of race N at stadium S today".

    Returns an empty list when the file is missing or unreadable; the
    caller may fall back to :func:`fetch_holding_list` in that case.
    """
    year, month, day = date_str.split("-")
    path = (
        project_root
        / "data"
        / "programs"
        / "title"
        / year
        / month
        / f"{day}.csv"
    )
    if not path.exists():
        logging_module.info(
            "title_csv_missing",
            path=str(path),
            date=date_str,
        )
        return []

    races: List[HoldingRace] = []
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    stadium_code = int((row.get("レース場コード") or "").strip())
                except (TypeError, ValueError):
                    continue
                if stadium_code < 1 or stadium_code > 24:
                    continue

                race_str = (row.get("レース回") or "").strip().rstrip("R")
                try:
                    race_number = int(race_str)
                except ValueError:
                    continue
                if race_number < 1 or race_number > 12:
                    continue

                deadline = (row.get("電話投票締切予定") or "").strip()
                cancel = (row.get("中止状態") or "").strip()
                title = (row.get("タイトル") or "").strip() or None

                races.append(
                    HoldingRace(
                        stadium_code=stadium_code,
                        race_number=race_number,
                        deadline_time=deadline,
                        cancel_status=cancel,
                        title=title,
                    )
                )
    except OSError as exc:
        logging_module.warning(
            "title_csv_read_error",
            path=str(path),
            error=str(exc),
        )
        return []

    logging_module.info(
        "title_csv_loaded",
        path=str(path),
        date=date_str,
        races=len(races),
    )
    return races


__all__ = [
    "HoldingListError",
    "HoldingRace",
    "HOLDING_LIST_BASE_URL",
    "build_race_code",
    "fetch_holding_list",
    "load_holding_from_title_csv",
]

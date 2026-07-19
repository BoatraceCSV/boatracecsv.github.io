"""Realtime aggregating-odds (集計中オッズ) fetcher.

Sources (all plain TSV on race.boatcast.jp, same CloudFront semantics as
the other realtime files — HTTP 403 / HTML fallback for missing files):

* ``/txt/{jo}/bc_smt_od3_{ymd}_{jo}_{rno}.txt``
    3連単 120 通り。6 行(1着艇ごと、行頭は選手名)× 20 値 + 末尾パディング。
    列順は「2着艇を残り 5 艇から昇順 → 3着艇を残り 4 艇から昇順」。
    ``bc_smt_best_worst20``(人気順上位/下位 20)との突合で検証済み。
* ``/txt/{jo}/bc_smt_od2_{ymd}_{jo}_{rno}.txt``
    2連単 30 通り(6 行 × 5 値、列順は相手艇昇順)+ 最終行に
    2連複 15 通り(組番昇順: 1-2, 1-3, .., 5-6)。
* ``/txt/{jo}/bc_smt_od1_{ymd}_{jo}_{rno}.txt``
    行順に 3連複 20 通り(組番昇順)/ 拡連複 15 通り(``min ～ max``)/
    単勝 6 値 / 複勝下限 6 値(複勝レンジと重複のため捨てる)/ 選手名 /
    複勝レンジ 6 組(``min ～ max``)。
    2026-07-19 に平和島 1R の画面表示と突合して列順を検証済み。

いずれも「集計中」= 締切前に随時更新されるスナップショットであり、
確定オッズ(``bc_kakutei_od*``)とは別物。preview-realtime の取得窓
(締切 1〜10 分前)で 1 レースにつき 1 回だけ記録する。

Values are kept verbatim as decimal strings (e.g. ``"11.7"``). ``0.0``
means "no votes on this combination yet", not missing data.

CSV layout: ``data/previews/{od1,od2,od3}/{YYYY}/{MM}/{DD}.csv`` with the
same six common identifier columns as the other preview sources, so all
files join on ``レースコード``. Path / dedupe / append helpers are shared
with :mod:`preview_csv`.
"""

from __future__ import annotations

from itertools import combinations
from typing import Dict, List, Optional, Tuple

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .preview_csv import COMMON_HEADERS


ODDS_SOURCES: Tuple[str, ...] = ("od1", "od2", "od3")


# ---------------------------------------------------------------------------
# Combination enumerations (canonical column orders)
# ---------------------------------------------------------------------------


def trifecta_combos() -> List[Tuple[int, int, int]]:
    """3連単 120 通りをファイル/CSV の列順で返す。

    1着艇 1..6(= od3 の行順)ごとに、2着艇を残りから昇順、3着艇を
    さらに残りから昇順で列挙する。
    """
    combos: List[Tuple[int, int, int]] = []
    for first in range(1, 7):
        for second in range(1, 7):
            if second == first:
                continue
            for third in range(1, 7):
                if third in (first, second):
                    continue
                combos.append((first, second, third))
    return combos


def exacta_combos() -> List[Tuple[int, int]]:
    """2連単 30 通り(1着艇ごとに相手艇昇順)。"""
    return [
        (first, second)
        for first in range(1, 7)
        for second in range(1, 7)
        if second != first
    ]


def pair_combos() -> List[Tuple[int, int]]:
    """2連複 / 拡連複 15 通り(組番昇順: 1-2, 1-3, .., 5-6)。"""
    return list(combinations(range(1, 7), 2))


def trio_combos() -> List[Tuple[int, int, int]]:
    """3連複 20 通り(組番昇順: 1-2-3, 1-2-4, .., 4-5-6)。"""
    return list(combinations(range(1, 7), 3))


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def _od3_headers() -> List[str]:
    headers = list(COMMON_HEADERS)
    headers.extend(
        f"3連単_{f}-{s}-{t}" for f, s, t in trifecta_combos()
    )
    return headers


def _od2_headers() -> List[str]:
    headers = list(COMMON_HEADERS)
    headers.extend(f"2連単_{f}-{s}" for f, s in exacta_combos())
    headers.extend(f"2連複_{a}={b}" for a, b in pair_combos())
    return headers


def _od1_headers() -> List[str]:
    headers = list(COMMON_HEADERS)
    headers.extend(f"3連複_{a}={b}={c}" for a, b, c in trio_combos())
    for a, b in pair_combos():
        headers.extend([f"拡連複_{a}={b}_下限", f"拡連複_{a}={b}_上限"])
    headers.extend(f"単勝_{n}" for n in range(1, 7))
    for n in range(1, 7):
        headers.extend([f"複勝_{n}_下限", f"複勝_{n}_上限"])
    return headers


OD3_HEADERS: List[str] = _od3_headers()
OD2_HEADERS: List[str] = _od2_headers()
OD1_HEADERS: List[str] = _od1_headers()

ODDS_HEADERS: Dict[str, List[str]] = {
    "od1": OD1_HEADERS,
    "od2": OD2_HEADERS,
    "od3": OD3_HEADERS,
}

# Value-column counts (headers minus the six common identifiers).
_N_OD3_VALUES = 120
_N_OD2_VALUES = 30 + 15
_N_OD1_VALUES = 20 + 30 + 6 + 12


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _body_lines(body: str) -> Optional[List[str]]:
    """Strip the ``data=`` header and require status ``1`` (集計中/有効).

    Returns the remaining lines, or ``None`` when the body is not a valid
    odds file (missing header, or status other than ``1`` — e.g. 発売前).
    """
    if not body:
        return None
    lines = body.splitlines()
    if not lines or not lines[0].lstrip().startswith("data="):
        return None
    if len(lines) < 2 or lines[1].split("\t")[0].strip() != "1":
        return None
    return lines[2:]


def _clean(cell: str) -> str:
    return (cell or "").strip()


def _numeric_cells(line: str, expected: int) -> Optional[List[str]]:
    """Take the first *expected* non-name cells of a tab-split line.

    Returns ``None`` when the line has fewer than *expected* cells.
    """
    cells = [_clean(c) for c in line.split("\t")]
    if len(cells) < expected:
        return None
    return cells[:expected]


def _range_cells(line: str, expected_pairs: int) -> Optional[List[str]]:
    """Parse ``min ～ max`` triplet groups into a flat [min, max, ...] list.

    The wire format is ``v \\t ～ \\t v`` repeated; we keep positions 0 and 2
    of each group.
    """
    cells = [_clean(c) for c in line.split("\t")]
    if len(cells) < expected_pairs * 3:
        return None
    flat: List[str] = []
    for i in range(expected_pairs):
        flat.extend([cells[i * 3], cells[i * 3 + 2]])
    return flat


def parse_od3(body: str) -> Optional[List[str]]:
    """Parse ``bc_smt_od3`` into 120 3連単 values (canonical order).

    Each of the six data rows is ``選手名 \\t v*20 \\t 0*5``; the racer name
    is dropped and the 20 odds are taken per row (row = 1着艇 1..6).
    """
    lines = _body_lines(body)
    if lines is None or len(lines) < 6:
        return None
    values: List[str] = []
    for row in lines[:6]:
        cells = row.split("\t")
        numeric = _numeric_cells("\t".join(cells[1:]), 20)
        if numeric is None:
            return None
        values.extend(numeric)
    if len(values) != _N_OD3_VALUES:
        return None
    return values


def parse_od2(body: str) -> Optional[List[str]]:
    """Parse ``bc_smt_od2`` into 30 2連単 + 15 2連複 values."""
    lines = _body_lines(body)
    if lines is None or len(lines) < 7:
        return None
    values: List[str] = []
    for row in lines[:6]:
        cells = row.split("\t")
        numeric = _numeric_cells("\t".join(cells[1:]), 5)
        if numeric is None:
            return None
        values.extend(numeric)
    quinella = _numeric_cells(lines[6], 15)
    if quinella is None:
        return None
    values.extend(quinella)
    if len(values) != _N_OD2_VALUES:
        return None
    return values


def parse_od1(body: str) -> Optional[List[str]]:
    """Parse ``bc_smt_od1`` into 3連複 / 拡連複 / 単勝 / 複勝 values.

    Wire line order (after ``data=`` + status): 3連複 20 値 / 拡連複
    15 レンジ / 単勝 6 値 / 複勝下限 6 値(捨てる) / 選手名 / 複勝
    6 レンジ。
    """
    lines = _body_lines(body)
    if lines is None or len(lines) < 6:
        return None
    trio = _numeric_cells(lines[0], 20)
    wide = _range_cells(lines[1], 15)
    win = _numeric_cells(lines[2], 6)
    # lines[3] = 複勝下限のみ(レンジと重複) / lines[4] = 選手名 — both skipped.
    place = _range_cells(lines[5], 6)
    if trio is None or wide is None or win is None or place is None:
        return None
    values = trio + wide + win + place
    if len(values) != _N_OD1_VALUES:
        return None
    return values


_PARSERS = {
    "od1": parse_od1,
    "od2": parse_od2,
    "od3": parse_od3,
}


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class OddsRealtimeFetcher:
    """Fetch and parse ``bc_smt_od{1,2,3}`` for one race.

    Shares HTTP / fallback semantics with the other realtime fetchers:
    HTTP 403/404 or an HTML body (CloudFront SPA fallback) is treated as
    a missing file and returns ``None``.
    """

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

    def _build_url(
        self, source: str, date_str: str, stadium_code: int, race_number: int
    ) -> str:
        ymd = date_str.replace("-", "")
        jo = f"{stadium_code:02d}"
        rno = f"{race_number:02d}"
        return f"{self.base_url}/txt/{jo}/bc_smt_{source}_{ymd}_{jo}_{rno}.txt"

    def _fetch_body(self, url: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)
        except requests.Timeout:
            logging_module.warning("odds_realtime_timeout", url=url)
            return None
        except requests.ConnectionError as exc:
            logging_module.warning(
                "odds_realtime_connection_error", url=url, error=str(exc)
            )
            return None

        if response.status_code in (403, 404):
            logging_module.debug(
                "odds_realtime_not_found",
                url=url,
                status_code=response.status_code,
            )
            return None
        if response.status_code != 200:
            logging_module.warning(
                "odds_realtime_http_error",
                url=url,
                status_code=response.status_code,
            )
            return None

        response.encoding = "utf-8"
        body = response.text
        if body.lstrip().startswith("<"):
            # CloudFront SPA fallback for missing files
            logging_module.debug("odds_realtime_body_is_html", url=url)
            return None
        return body

    def fetch_values(
        self,
        source: str,
        date_str: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[List[str]]:
        """Fetch + parse one odds file.

        Returns the flat value list matching ``ODDS_HEADERS[source]``
        (minus the common columns), or ``None`` when the file is missing
        or unparseable.
        """
        if source not in _PARSERS:
            raise ValueError(f"unknown odds source: {source}")
        body = self._fetch_body(
            self._build_url(source, date_str, stadium_code, race_number)
        )
        if body is None:
            return None
        values = _PARSERS[source](body)
        if values is None:
            logging_module.debug(
                "odds_realtime_unparseable",
                source=source,
                date=date_str,
                stadium=stadium_code,
                race=race_number,
            )
        return values


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def build_odds_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    values: List[str],
) -> List[str]:
    """Compose one odds CSV row: common identifiers + parsed values."""
    return [
        race_code,
        date_str,
        f"{stadium_code:02d}",
        f"{race_number:02d}R",
        deadline_time,
        fetched_at_iso,
    ] + list(values)


__all__ = [
    "ODDS_SOURCES",
    "ODDS_HEADERS",
    "OD1_HEADERS",
    "OD2_HEADERS",
    "OD3_HEADERS",
    "trifecta_combos",
    "exacta_combos",
    "pair_combos",
    "trio_combos",
    "parse_od1",
    "parse_od2",
    "parse_od3",
    "build_odds_row",
    "OddsRealtimeFetcher",
]

"""Realtime race-payout fetcher.

Source: ``https://race.boatcast.jp/m_txt/{jo}/bc_rs2_{ymd}_{jo}_{rno}.txt``

This is the payout (払戻金) counterpart to :mod:`result_realtime` (which
consumes ``bc_rs1_2`` for finishes / ST / weather). The two files are
published by boatcast right after a race finalises and are independent —
we fetch them in parallel from ``preview-realtime.py`` and append to
distinct daily CSVs.

The ``bc_rs2`` body is plain TSV split into 7 sections by blank lines.
Each section's row count is variable but its position is fixed::

    line 1                   status (e.g. ``"1\\t0\\t"``) - skipped
    [blank line]
    §1 2連単 (1 row)         boat1, ``-``, boat2, payout, ``円``, popularity
    [blank]
    §2 2連複 (1 row)         boatA, ``<`` or ``>``, boatB, payout, ``円``, popularity
                              (the ``<``/``>`` orientation points to the
                               1着 boat; we just normalize to "small=big")
    [blank]
    §3 3連単 (1 row)         boat1, boat2, boat3, payout, ``円``, popularity
    [blank]
    §4 3連複 (1 row)         sort1, sort2, sort3 (ascending), payout, ``円``, popularity
    [blank]
    §5 拡連複 (0-3 rows)     boatA, ``=``, boatB, payout, ``円``, popularity
                              Row order is invariant:
                                row 1 = 1-2着 pair
                                row 2 = 1-3着 pair
                                row 3 = 2-3着 pair
    [blank, sometimes multiple]
    §6 単勝 (1 row)          boat, payout, ``円``
    [blank]
    §7 複勝 (1-3 rows)       boat, payout, ``円``
                              Order: 1着, 2着, (3着 if same-time)

The flat row is written to ``data/results/payouts/{YYYY}/{MM}/{DD}.csv``
with one row per race, containing 単勝 / 複勝 / 2連単 / 2連複 / 拡連複 /
3連単 / 3連複 の払戻金・組番・人気。

Idempotency is the same model as :mod:`result_realtime`: the caller
looks up :func:`existing_race_codes` first and only fetches races whose
``レースコード`` isn't yet in the payouts CSV.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Iterable, List, Optional, Set

import requests

from . import logger as logging_module
from .downloader import RateLimiter


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


COMMON_HEADERS: List[str] = [
    "レースコード",
    "レース日",
    "レース場",
    "レース回",
    "締切時刻",
    "取得日時",
]


def _payout_headers() -> List[str]:
    headers = list(COMMON_HEADERS)
    # 単勝 (win): boat, payout
    headers.extend(["単勝_艇番", "単勝_払戻金"])
    # 複勝 (place): 1着 / 2着 / 3着 (3着 only on same-time)
    for rank in (1, 2, 3):
        headers.extend([f"複勝_{rank}着_艇番", f"複勝_{rank}着_払戻金"])
    # 2連単 (exacta)
    headers.extend(["2連単_組番", "2連単_払戻金", "2連単_人気"])
    # 2連複 (quinella)
    headers.extend(["2連複_組番", "2連複_払戻金", "2連複_人気"])
    # 拡連複 (wide) — fixed row order 1-2着 / 1-3着 / 2-3着
    for label in ("1-2着", "1-3着", "2-3着"):
        headers.extend(
            [
                f"拡連複_{label}_組番",
                f"拡連複_{label}_払戻金",
                f"拡連複_{label}_人気",
            ]
        )
    # 3連単 (trifecta)
    headers.extend(["3連単_組番", "3連単_払戻金", "3連単_人気"])
    # 3連複 (trio)
    headers.extend(["3連複_組番", "3連複_払戻金", "3連複_人気"])
    return headers


PAYOUT_HEADERS: List[str] = _payout_headers()


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def csv_path_for(project_root: Path, date_str: str) -> Path:
    """Resolve ``data/results/payouts/{YYYY}/{MM}/{DD}.csv``."""
    year, month, day = date_str.split("-")
    return (
        project_root
        / "data"
        / "results"
        / "payouts"
        / year
        / month
        / f"{day}.csv"
    )


def existing_race_codes(path: Path) -> Set[str]:
    """Return the set of ``レースコード`` already present in *path*.

    Returns an empty set if the file does not exist or cannot be read.
    """
    if not path.exists():
        return set()
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                next(reader)  # skip header
            except StopIteration:
                return set()
            return {row[0] for row in reader if row}
    except OSError as exc:
        logging_module.warning(
            "payout_realtime_existing_read_failed",
            path=str(path),
            error=str(exc),
        )
        return set()


def append_rows(
    path: Path,
    headers: List[str],
    rows: Iterable[List[str]],
) -> int:
    """Append ``rows`` to *path*, writing the header first if needed.

    Returns the number of rows actually written. Dedup-by-レースコード is
    the caller's responsibility (use :func:`existing_race_codes`).
    """
    rows = list(rows)
    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not path.exists() or path.stat().st_size == 0

    buf = StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    if new_file:
        writer.writerow(headers)
    for row in rows:
        writer.writerow(row)

    with open(path, "a", encoding="utf-8") as f:
        f.write(buf.getvalue())

    logging_module.info(
        "payout_realtime_csv_appended",
        path=str(path),
        rows=len(rows),
        new_file=new_file,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Payout:
    """One payout entry."""

    combination: str  # "1-4" / "1=4" / "1-4-2" / "1" (for 単勝/複勝)
    payout: Optional[int]  # 円 (None when un-parseable / 特払い)
    popularity: Optional[int] = None  # 人気 (None for 単勝/複勝)


@dataclass(frozen=True)
class RacePayouts:
    """Parsed payout block for one race.

    Each field is None / empty when the corresponding section is missing
    or unsold (e.g. 拡連複 is not sold on 5 艇立て or below).
    """

    tansho: Optional[Payout] = None  # 単勝
    fukusho: List[Payout] = field(default_factory=list)  # 複勝, len 0-3 (rank order)
    nirentan: Optional[Payout] = None  # 2連単
    nirenpuku: Optional[Payout] = None  # 2連複
    sanrentan: Optional[Payout] = None  # 3連単
    sanrenpuku: Optional[Payout] = None  # 3連複
    # 拡連複 in file order: index 0 = 1-2着, 1 = 1-3着, 2 = 2-3着.
    # Missing entries are None to preserve positional invariants.
    kakurenfuku: List[Optional[Payout]] = field(
        default_factory=lambda: [None, None, None]
    )

    @property
    def is_complete(self) -> bool:
        """Treat the row as "ready to write" when 3連単 is parsed.

        3連単 is the canonical headline payout and the last one to be
        finalised; once it's there, the rest of the body has typically
        been populated as well.
        """
        return self.sanrentan is not None and self.sanrentan.payout is not None


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def _to_int(raw: str) -> Optional[int]:
    """Parse a payout / popularity string. Strips commas + non-digits."""
    if raw is None:
        return None
    cleaned = re.sub(r"[^\d]", "", raw)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _split_sections(body: str) -> List[List[List[str]]]:
    """Split a bc_rs2 body into sections separated by blank lines.

    Returns a list of sections, each section being a list of TSV-split
    rows (themselves lists of cells). The leading status line (``"1\\t0"``)
    is **dropped** before splitting.
    """
    lines = (body or "").splitlines()
    # Drop the leading status line (always present), but keep blanks for
    # section splitting.
    if lines and lines[0].split("\t")[0:1] in (["1"], ["0"]):
        lines = lines[1:]

    sections: List[List[List[str]]] = []
    current: List[List[str]] = []
    for ln in lines:
        if ln.strip() == "":
            if current:
                sections.append(current)
                current = []
            continue
        current.append(ln.split("\t"))
    if current:
        sections.append(current)
    return sections


def _parse_two_boat_row(row: List[str]) -> Optional[Payout]:
    """Parse a row of shape ``boat1\\tSEP\\tboat2\\tpayout\\t円\\tpopularity``.

    Used for 2連単 / 2連複 / 拡連複. The separator is preserved as part
    of the rendered combination string (``-`` for 2連単, ``=`` for
    2連複 / 拡連複).
    """
    if len(row) < 4:
        return None
    boat1 = (row[0] or "").strip()
    sep = (row[1] or "").strip()
    boat2 = (row[2] or "").strip()
    payout = _to_int(row[3])
    popularity = _to_int(row[5]) if len(row) > 5 else None
    if not boat1 or not boat2:
        return None
    # Map boatcast's ``<`` / ``>`` (2連複 orientation pointing at 1着)
    # to the canonical ``=`` separator and normalize to ascending order.
    if sep in ("<", ">"):
        sep = "="
    if sep == "=" and boat1.isdigit() and boat2.isdigit() and int(boat1) > int(boat2):
        boat1, boat2 = boat2, boat1
    return Payout(
        combination=f"{boat1}{sep}{boat2}",
        payout=payout,
        popularity=popularity,
    )


def _parse_three_boat_row(row: List[str], sep: str) -> Optional[Payout]:
    """Parse a row of shape ``boat1\\tboat2\\tboat3\\tpayout\\t円\\tpopularity``.

    Used for 3連単 (sep ``-``) and 3連複 (sep ``=``). The boats are
    rendered verbatim — bc_rs2 already orders 3連単 as 1着→2着→3着 and
    3連複 ascending, so no normalization is needed.
    """
    if len(row) < 4:
        return None
    boat1 = (row[0] or "").strip()
    boat2 = (row[1] or "").strip()
    boat3 = (row[2] or "").strip()
    payout = _to_int(row[3])
    popularity = _to_int(row[5]) if len(row) > 5 else None
    if not (boat1 and boat2 and boat3):
        return None
    return Payout(
        combination=f"{boat1}{sep}{boat2}{sep}{boat3}",
        payout=payout,
        popularity=popularity,
    )


def _parse_single_boat_row(row: List[str]) -> Optional[Payout]:
    """Parse a row of shape ``boat\\tpayout\\t円``.

    Used for 単勝 and each 複勝 row. No popularity column.
    """
    if len(row) < 2:
        return None
    boat = (row[0] or "").strip()
    payout = _to_int(row[1])
    if not boat:
        return None
    return Payout(combination=boat, payout=payout, popularity=None)


def parse_rs2(body: str) -> Optional[RacePayouts]:
    """Parse a ``bc_rs2`` body into a :class:`RacePayouts`.

    Returns ``None`` if the body is empty / not a finished race.

    The result may be partially populated; callers should consult
    :attr:`RacePayouts.is_complete` before persisting.
    """
    if not body or not body.strip():
        return None

    sections = _split_sections(body)
    if not sections:
        return None

    payouts = {
        "nirentan": None,
        "nirenpuku": None,
        "sanrentan": None,
        "sanrenpuku": None,
        "tansho": None,
        "fukusho": [],
        "kakurenfuku": [None, None, None],
    }

    # Sections are positionally indexed. Missing sections (e.g. on
    # 中止 / 不成立) will simply leave the corresponding payout None.
    # Order on the wire is:
    #   0=2連単, 1=2連複, 2=3連単, 3=3連複, 4=拡連複, 5=単勝, 6=複勝
    if len(sections) >= 1 and sections[0]:
        payouts["nirentan"] = _parse_two_boat_row(sections[0][0])
    if len(sections) >= 2 and sections[1]:
        payouts["nirenpuku"] = _parse_two_boat_row(sections[1][0])
    if len(sections) >= 3 and sections[2]:
        payouts["sanrentan"] = _parse_three_boat_row(sections[2][0], "-")
    if len(sections) >= 4 and sections[3]:
        payouts["sanrenpuku"] = _parse_three_boat_row(sections[3][0], "=")
    if len(sections) >= 5:
        # 拡連複: keep positional slots (1-2着 / 1-3着 / 2-3着).
        for i, row in enumerate(sections[4][:3]):
            payouts["kakurenfuku"][i] = _parse_two_boat_row(row)
    if len(sections) >= 6 and sections[5]:
        payouts["tansho"] = _parse_single_boat_row(sections[5][0])
    if len(sections) >= 7:
        payouts["fukusho"] = [
            p for p in (_parse_single_boat_row(r) for r in sections[6][:3]) if p
        ]

    return RacePayouts(
        tansho=payouts["tansho"],
        fukusho=payouts["fukusho"],
        nirentan=payouts["nirentan"],
        nirenpuku=payouts["nirenpuku"],
        sanrentan=payouts["sanrentan"],
        sanrenpuku=payouts["sanrenpuku"],
        kakurenfuku=payouts["kakurenfuku"],
    )


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class PayoutRealtimeFetcher:
    """Fetch and parse ``bc_rs2`` for one race.

    Shares HTTP / fallback semantics with :class:`ResultRealtimeFetcher`:
    HTTP 200 + body that starts with ``<`` (CloudFront SPA fallback) is
    treated as a missing file and returns ``None``.
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

    def _build_url(self, date_str: str, stadium_code: int, race_number: int) -> str:
        ymd = date_str.replace("-", "")
        jo = f"{stadium_code:02d}"
        rno = f"{race_number:02d}"
        return f"{self.base_url}/m_txt/{jo}/bc_rs2_{ymd}_{jo}_{rno}.txt"

    def _fetch_body(self, url: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)
        except requests.Timeout:
            logging_module.warning("payout_realtime_timeout", url=url)
            return None
        except requests.ConnectionError as exc:
            logging_module.warning(
                "payout_realtime_connection_error",
                url=url,
                error=str(exc),
            )
            return None

        if response.status_code in (403, 404):
            logging_module.debug(
                "payout_realtime_not_found",
                url=url,
                status_code=response.status_code,
            )
            return None
        if response.status_code != 200:
            logging_module.warning(
                "payout_realtime_http_error",
                url=url,
                status_code=response.status_code,
            )
            return None

        response.encoding = "utf-8"
        body = response.text
        if body.lstrip().startswith("<"):
            # CloudFront SPA fallback for missing files
            logging_module.debug("payout_realtime_body_is_html", url=url)
            return None
        return body

    def fetch_race_payouts(
        self,
        date_str: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RacePayouts]:
        """Fetch + parse one race. ``None`` on missing / unparseable / partial."""
        body = self._fetch_body(
            self._build_url(date_str, stadium_code, race_number)
        )
        if body is None:
            return None
        payouts = parse_rs2(body)
        if payouts is None:
            return None
        if not payouts.is_complete:
            logging_module.debug(
                "payout_realtime_incomplete",
                date=date_str,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        return payouts


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def _fmt(value) -> str:
    return "" if value is None else str(value)


def _fmt_combo(payout: Optional[Payout]) -> str:
    return "" if payout is None else payout.combination


def _fmt_payout(payout: Optional[Payout]) -> str:
    if payout is None or payout.payout is None:
        return ""
    return str(payout.payout)


def _fmt_popularity(payout: Optional[Payout]) -> str:
    if payout is None or payout.popularity is None:
        return ""
    return str(payout.popularity)


def build_payout_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    payouts: RacePayouts,
) -> List[str]:
    """Compose a flat CSV row for one race's payouts."""
    row: List[str] = [
        race_code,
        date_str,
        f"{stadium_code:02d}",
        f"{race_number:02d}R",
        deadline_time,
        fetched_at_iso,
    ]
    # 単勝
    row.extend([_fmt_combo(payouts.tansho), _fmt_payout(payouts.tansho)])
    # 複勝 (3 slots, fill with empty strings when fewer rows)
    for i in range(3):
        f = payouts.fukusho[i] if i < len(payouts.fukusho) else None
        row.extend([_fmt_combo(f), _fmt_payout(f)])
    # 2連単
    row.extend(
        [
            _fmt_combo(payouts.nirentan),
            _fmt_payout(payouts.nirentan),
            _fmt_popularity(payouts.nirentan),
        ]
    )
    # 2連複
    row.extend(
        [
            _fmt_combo(payouts.nirenpuku),
            _fmt_payout(payouts.nirenpuku),
            _fmt_popularity(payouts.nirenpuku),
        ]
    )
    # 拡連複 (3 positional slots: 1-2着 / 1-3着 / 2-3着)
    for slot in payouts.kakurenfuku[:3]:
        row.extend(
            [
                _fmt_combo(slot),
                _fmt_payout(slot),
                _fmt_popularity(slot),
            ]
        )
    # Pad to 3 slots if somehow shorter (defensive — dataclass default
    # always yields 3 entries).
    pad = 3 - min(len(payouts.kakurenfuku), 3)
    for _ in range(pad):
        row.extend(["", "", ""])
    # 3連単
    row.extend(
        [
            _fmt_combo(payouts.sanrentan),
            _fmt_payout(payouts.sanrentan),
            _fmt_popularity(payouts.sanrentan),
        ]
    )
    # 3連複
    row.extend(
        [
            _fmt_combo(payouts.sanrenpuku),
            _fmt_payout(payouts.sanrenpuku),
            _fmt_popularity(payouts.sanrenpuku),
        ]
    )
    return row


__all__ = [
    "PAYOUT_HEADERS",
    "csv_path_for",
    "existing_race_codes",
    "append_rows",
    "parse_rs2",
    "build_payout_row",
    "Payout",
    "RacePayouts",
    "PayoutRealtimeFetcher",
]

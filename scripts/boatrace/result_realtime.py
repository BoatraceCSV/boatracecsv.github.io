"""Realtime race-result fetcher.

Source: ``https://race.boatcast.jp/m_txt/{jo}/bc_rs1_2_{ymd}_{jo}_{rno}.txt``

This is the same TSV file that :mod:`preview_tsv_scraper` already consumes
to back-fill weather for finished races. After a race finalises, the file
contains three sections::

    line 1   ST  per lane (boat<TAB><F-flag><TAB>ST x 6, in 進入 order)
    line 2-7 着順 / 艇番 / 選手名 / レースタイム / 決まり手 (1st place only)
    line 8   weather (HHMM<TAB>weather<TAB>wave<TAB>wind_dir<TAB>wind_speed
             <TAB>air_temp<TAB>water_temp)

Per-lane and per-finish data is rendered into a flat row keyed by
``レースコード`` and appended (idempotently) to::

    data/results/realtime/{YYYY}/{MM}/{DD}.csv

Idempotency is handled by the same approach as ``preview_csv``: the
caller looks up :func:`existing_race_codes` before fetching and only
fetches/parses races that aren't yet present.

The K-file pipeline (``scripts/result.py``) writes the canonical results
the next day to ``data/results/daily/{YYYY}/{MM}/{DD}.csv`` (a different path),
so the two never collide.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import requests

from . import logger as logging_module
from .downloader import RateLimiter
from .preview_tsv_scraper import (
    PreviewTsvScraper,
    _VALID_WEATHER_CODES,
    _WIND_DIRECTION_TO_CODE,
)


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


def _result_headers() -> List[str]:
    headers = list(COMMON_HEADERS)
    headers.append("結果記録時刻")  # bc_rs1_2 weather-row HHMM
    headers.append("決まり手")
    # Per-finish (1着..6着)
    for rank in range(1, 7):
        headers.extend(
            [
                f"{rank}着_艇番",
                f"{rank}着_選手名",
                f"{rank}着_レースタイム",
            ]
        )
    # Per-course (1コース..6コース) — actual entry course in 進入 order
    for course in range(1, 7):
        headers.extend(
            [
                f"{course}コース_艇番",
                f"{course}コース_スタートタイミング",
                f"{course}コース_F",
            ]
        )
    # Weather
    headers.extend(
        [
            "天候",
            "風向",
            "風速(m)",
            "波の高さ(cm)",
            "気温(℃)",
            "水温(℃)",
        ]
    )
    return headers


RESULT_HEADERS: List[str] = _result_headers()


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def csv_path_for(project_root: Path, date_str: str) -> Path:
    """Resolve ``data/results/realtime/{YYYY}/{MM}/{DD}.csv``."""
    year, month, day = date_str.split("-")
    return (
        project_root
        / "data"
        / "results"
        / "realtime"
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
            "result_realtime_existing_read_failed",
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
        "result_realtime_csv_appended",
        path=str(path),
        rows=len(rows),
        new_file=new_file,
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


# Map full-width digit ("１"-"６") -> int. Anything else returns None.
_FULLWIDTH_DIGIT_MAP = {
    "０": 0, "１": 1, "２": 2, "３": 3, "４": 4,
    "５": 5, "６": 6, "７": 7, "８": 8, "９": 9,
}


def _parse_rank(raw: str) -> Optional[int]:
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    # Try full-width first
    if cleaned[0] in _FULLWIDTH_DIGIT_MAP:
        return _FULLWIDTH_DIGIT_MAP[cleaned[0]] or None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _normalize_name(raw: str) -> str:
    """Strip stray whitespace/full-width spaces, but keep canonical interior.

    The TSV pads names to a fixed 6-character full-width width using
    full-width spaces. The existing K-file CSV preserves these as readable
    spacing (e.g., ``"上 瀧 和 則"``), so we keep them.
    """
    if raw is None:
        return ""
    return raw.strip()


def _parse_st_value(raw: str) -> Optional[float]:
    """``".18"`` -> 0.18; ``""`` -> None."""
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("."):
        cleaned = "0" + cleaned
    try:
        return float(cleaned)
    except ValueError:
        return None


@dataclass
class FinishEntry:
    rank: int               # 1..6
    boat_number: int        # 1..6 (艇番)
    racer_name: str
    race_time: str          # ``"1'49\"3"`` raw — kept as-is
    kimari_te: str = ""     # only on 1st place row


@dataclass
class CourseEntry:
    course_number: int      # 1..6 (進入コース)
    boat_number: Optional[int]   # 1..6 — None on rare blank rows
    start_timing: Optional[float]
    is_flying: bool         # True when the F flag is set


@dataclass
class RaceWeather:
    observed_at: Optional[str] = None     # "HHMM"
    weather: Optional[int] = None
    wave_height: Optional[float] = None
    wind_direction: Optional[int] = None
    wind_speed: Optional[float] = None
    air_temperature: Optional[float] = None
    water_temperature: Optional[float] = None


@dataclass
class RaceResult:
    finishes: List[FinishEntry] = field(default_factory=list)
    courses: List[CourseEntry] = field(default_factory=list)
    weather: RaceWeather = field(default_factory=RaceWeather)
    kimari_te: str = ""

    @property
    def is_complete(self) -> bool:
        """True when at least one finish row has both rank and boat number.

        Some bc_rs1_2 files appear pre-finalisation with the ST line only
        and empty placement rows. We treat those as incomplete.
        """
        return any(f.boat_number for f in self.finishes)


def parse_rs1_2(body: str) -> Optional[RaceResult]:
    """Parse a ``bc_rs1_2`` body into a :class:`RaceResult`.

    Returns ``None`` if the body is unparseable (wrong shape / not a
    finished race). Returns a partially-populated :class:`RaceResult`
    when only some rows are present — callers should consult
    :attr:`RaceResult.is_complete`.
    """
    lines = [ln for ln in (body or "").splitlines() if ln.strip()]
    if not lines:
        return None

    # Parse ST line (line 1) — 6 triplets of (boat, flag, ST)
    courses = _parse_st_line(lines[0])

    # Placement rows: from line 2 onward, but the last line is weather.
    weather_line: Optional[str] = None
    placement_lines: List[str] = []
    for ln in lines[1:]:
        cols = ln.split("\t")
        if (
            len(cols) >= 7
            and re.fullmatch(r"\d{4}", cols[0].strip())
        ):
            weather_line = ln
            continue
        placement_lines.append(ln)

    finishes = _parse_placement_lines(placement_lines)

    weather = _parse_weather_line(weather_line) if weather_line else RaceWeather()

    kimari_te = ""
    for f in finishes:
        if f.kimari_te:
            kimari_te = f.kimari_te
            break

    return RaceResult(
        finishes=finishes,
        courses=courses,
        weather=weather,
        kimari_te=kimari_te,
    )


def _parse_st_line(line: str) -> List[CourseEntry]:
    """Split the ST line into 6 :class:`CourseEntry`.

    The line has 18 tab-separated fields (6 triplets of ``boat``, ``flag``,
    ``ST``). ``flag`` is empty for normal, ``F`` for 飛び (flying), ``L``
    for 出遅れ. Course number is implicit (the position of the triplet —
    1コース is the first one, etc.).
    """
    cols = line.split("\t")
    courses: List[CourseEntry] = []
    for course_num in range(1, 7):
        base = (course_num - 1) * 3
        if base + 2 >= len(cols):
            courses.append(
                CourseEntry(
                    course_number=course_num,
                    boat_number=None,
                    start_timing=None,
                    is_flying=False,
                )
            )
            continue
        boat_raw = cols[base].strip()
        flag = cols[base + 1].strip().upper()
        st_raw = cols[base + 2].strip()

        try:
            boat_number = int(boat_raw) if boat_raw else None
        except ValueError:
            boat_number = None

        is_flying = flag == "F"
        is_late = flag == "L"
        st_value = _parse_st_value(st_raw)
        if st_value is not None and is_flying:
            # Convention shared with ``preview_tsv_scraper``: F-flagged ST
            # is rendered as a negative number.
            st_value = -st_value
        if is_late:
            st_value = None

        courses.append(
            CourseEntry(
                course_number=course_num,
                boat_number=boat_number,
                start_timing=st_value,
                is_flying=is_flying,
            )
        )
    return courses


def _parse_placement_lines(lines: List[str]) -> List[FinishEntry]:
    """Read up to 6 placement rows.

    Layout per row (tab-separated):
        [0] 着順 (full-width digit)
        [1] 艇番 (1..6, ascii digit)
        [2] 選手名 (full-width-space padded)
        [3] レースタイム (e.g. ``"1'49\\\"3"``); empty if F/失格/転覆 etc.
        [4] 決まり手 (only on 1着 row; e.g. ``"逃　げ"``)
    """
    finishes: List[FinishEntry] = []
    for raw in lines[:6]:
        cols = raw.split("\t")
        if len(cols) < 3:
            continue
        rank = _parse_rank(cols[0])
        if rank is None:
            continue
        boat_raw = cols[1].strip()
        try:
            boat_number = int(boat_raw)
        except ValueError:
            continue
        racer_name = _normalize_name(cols[2] if len(cols) > 2 else "")
        race_time = (cols[3].strip() if len(cols) > 3 else "")
        kimari_te = (cols[4].strip() if len(cols) > 4 else "")
        finishes.append(
            FinishEntry(
                rank=rank,
                boat_number=boat_number,
                racer_name=racer_name,
                race_time=race_time,
                kimari_te=kimari_te,
            )
        )
    finishes.sort(key=lambda f: f.rank)
    return finishes


def _parse_weather_line(line: str) -> RaceWeather:
    """Parse the trailing weather row.

    Layout (tab-separated):
        [0] HHMM
        [1] 天候 (1..9)
        [2] 波高 (cm)
        [3] 風向(風質) full-width — e.g. ``"東　　(向い風)"``
        [4] 風速 (m)
        [5] 気温 ``"+21.0"`` / ``"-1.0"``
        [6] 水温 ``"+13.0"``
    """
    cols = line.split("\t")
    if len(cols) < 7:
        return RaceWeather()
    return RaceWeather(
        observed_at=cols[0].strip() or None,
        weather=_parse_weather_code(cols[1]),
        wave_height=_to_optional_float(cols[2]),
        wind_direction=_parse_wind_direction(cols[3]),
        wind_speed=_to_optional_float(cols[4]),
        air_temperature=_parse_temperature(cols[5]),
        water_temperature=_parse_temperature(cols[6]),
    )


def _parse_weather_code(raw: str) -> Optional[int]:
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    try:
        code = int(cleaned)
    except ValueError:
        return None
    return code if code in _VALID_WEATHER_CODES else None


def _parse_wind_direction(raw: str) -> Optional[int]:
    if raw is None:
        return None
    primary = raw.split("　(")[0].split("(")[0]
    primary = re.sub(r"[\s　]+", "", primary)
    if not primary:
        return None
    for token, code in _WIND_DIRECTION_TO_CODE:
        if token in primary:
            return code
    return None


def _parse_temperature(raw: str) -> Optional[float]:
    cleaned = (raw or "").strip().lstrip("+")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def _to_optional_float(raw: str) -> Optional[float]:
    cleaned = (raw or "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Fetcher
# ---------------------------------------------------------------------------


class ResultRealtimeFetcher:
    """Fetch and parse ``bc_rs1_2`` for one race.

    The on-disk URL pattern is shared with :class:`PreviewTsvScraper`'s
    weather lookup, so we re-use the same HTTP session semantics:
    HTTP 200 + body that starts with ``<`` (CloudFront fallback) is
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
        return f"{self.base_url}/m_txt/{jo}/bc_rs1_2_{ymd}_{jo}_{rno}.txt"

    def _fetch_body(self, url: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            response = self.session.get(url, timeout=self.timeout_seconds)
        except requests.Timeout:
            logging_module.warning("result_realtime_timeout", url=url)
            return None
        except requests.ConnectionError as exc:
            logging_module.warning(
                "result_realtime_connection_error",
                url=url,
                error=str(exc),
            )
            return None

        if response.status_code in (403, 404):
            logging_module.debug(
                "result_realtime_not_found",
                url=url,
                status_code=response.status_code,
            )
            return None
        if response.status_code != 200:
            logging_module.warning(
                "result_realtime_http_error",
                url=url,
                status_code=response.status_code,
            )
            return None

        response.encoding = "utf-8"
        body = response.text
        if body.lstrip().startswith("<"):
            # CloudFront SPA fallback for missing files
            logging_module.debug("result_realtime_body_is_html", url=url)
            return None
        return body

    def fetch_race_result(
        self,
        date_str: str,
        stadium_code: int,
        race_number: int,
    ) -> Optional[RaceResult]:
        """Fetch + parse one race. ``None`` on missing / unparseable / partial."""
        body = self._fetch_body(
            self._build_url(date_str, stadium_code, race_number)
        )
        if body is None:
            return None
        result = parse_rs1_2(body)
        if result is None:
            return None
        if not result.is_complete:
            logging_module.debug(
                "result_realtime_incomplete",
                date=date_str,
                stadium=stadium_code,
                race=race_number,
            )
            return None
        return result


# ---------------------------------------------------------------------------
# Row builder
# ---------------------------------------------------------------------------


def _fmt(value) -> str:
    return "" if value is None else str(value)


def build_result_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    result: RaceResult,
) -> List[str]:
    """Compose a flat CSV row for one race."""
    row: List[str] = [
        race_code,
        date_str,
        f"{stadium_code:02d}",
        f"{race_number:02d}R",
        deadline_time,
        fetched_at_iso,
    ]
    row.append(_fmt(result.weather.observed_at))
    row.append(_fmt(result.kimari_te))

    finishes_by_rank = {f.rank: f for f in result.finishes}
    for rank in range(1, 7):
        f = finishes_by_rank.get(rank)
        if f is None:
            row.extend(["", "", ""])
        else:
            row.extend(
                [
                    _fmt(f.boat_number),
                    _fmt(f.racer_name),
                    _fmt(f.race_time),
                ]
            )

    courses_by_num = {c.course_number: c for c in result.courses}
    for course in range(1, 7):
        c = courses_by_num.get(course)
        if c is None:
            row.extend(["", "", ""])
        else:
            row.extend(
                [
                    _fmt(c.boat_number),
                    _fmt(c.start_timing),
                    "F" if c.is_flying else "",
                ]
            )

    row.extend(
        [
            _fmt(result.weather.weather),
            _fmt(result.weather.wind_direction),
            _fmt(result.weather.wind_speed),
            _fmt(result.weather.wave_height),
            _fmt(result.weather.air_temperature),
            _fmt(result.weather.water_temperature),
        ]
    )
    return row


__all__ = [
    "RESULT_HEADERS",
    "csv_path_for",
    "existing_race_codes",
    "append_rows",
    "parse_rs1_2",
    "build_result_row",
    "RaceResult",
    "FinishEntry",
    "CourseEntry",
    "RaceWeather",
    "ResultRealtimeFetcher",
]

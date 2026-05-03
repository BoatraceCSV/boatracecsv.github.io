"""CSV writers for the realtime preview pipeline.

Four sibling files, all append-only and idempotent per ``レースコード``:

* ``data/previews/tkz/{YYYY}/{MM}/{DD}.csv`` - bc_j_tkz (体重 / 展示タイム / チルト)
* ``data/previews/stt/{YYYY}/{MM}/{DD}.csv`` - bc_j_stt (進入コース / ST 展示)
* ``data/previews/sui/{YYYY}/{MM}/{DD}.csv`` - bc_sui  (水面気象スナップショット)
* ``data/previews/original_exhibition/{YYYY}/{MM}/{DD}.csv`` - bc_oriten
  (場ごとに2〜3項目のオリジナル展示計測。一周/まわり足/直線 等)

Each file shares the same first six columns (the *common* identifiers) so the
three CSVs can be joined on ``レースコード`` after the fact:

    レースコード, レース日, レース場, レース回, 締切時刻, 取得日時

``取得日時`` is the JST ISO8601 timestamp (e.g. ``2026-05-03T20:25:03+09:00``)
of the moment the realtime scheduler fired the fetch for that race.

Idempotency: the append helpers refuse to add a row whose ``レースコード`` is
already present in the file. This makes the realtime job safe to re-run for
the same minute, and tolerant of cron drift that causes the same race to
fall inside the eligibility window for two consecutive minutes.
"""

from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

from . import logger as logging_module


# --- Common identifiers -----------------------------------------------------

COMMON_HEADERS: List[str] = [
    "レースコード",
    "レース日",
    "レース場",
    "レース回",
    "締切時刻",
    "取得日時",
]


# --- Per-source headers -----------------------------------------------------

TKZ_HEADERS: List[str] = list(COMMON_HEADERS) + ["状態"]
for _n in range(1, 7):
    TKZ_HEADERS.extend(
        [
            f"艇{_n}_体重(kg)",
            f"艇{_n}_体重調整(kg)",
            f"艇{_n}_展示タイム",
            f"艇{_n}_チルト",
        ]
    )

STT_HEADERS: List[str] = list(COMMON_HEADERS)
for _n in range(1, 7):
    STT_HEADERS.extend([f"艇{_n}_コース", f"艇{_n}_スタート展示"])

SUI_HEADERS: List[str] = list(COMMON_HEADERS) + [
    "気象観測時刻",
    "風速(m)",
    "風向",
    "波の高さ(cm)",
    "天候",
    "気温(℃)",
    "水温(℃)",
]

# Original exhibition (オリジナル展示データ) — variable column semantics.
# ``計測項目1`` / ``計測項目2`` / ``計測項目3`` carry the per-stadium label of
# the corresponding ``艇N_値1`` / ``艇N_値2`` / ``艇N_値3`` columns. 2-項目場
# leaves ``計測項目3`` and every ``艇N_値3`` blank. Realtime mode only ever
# writes rows with status == ``"1"`` (status ``"0"`` / ``"2"`` are skipped),
# so a status column is intentionally not present.
OEX_HEADERS: List[str] = list(COMMON_HEADERS) + [
    "計測数",
    "計測項目1",
    "計測項目2",
    "計測項目3",
]
for _n in range(1, 7):
    OEX_HEADERS.extend(
        [
            f"艇{_n}_選手名",
            f"艇{_n}_値1",
            f"艇{_n}_値2",
            f"艇{_n}_値3",
        ]
    )


# --- Path helpers -----------------------------------------------------------

def csv_path_for(project_root: Path, source: str, date_str: str) -> Path:
    """Resolve ``data/previews/{source}/{YYYY}/{MM}/{DD}.csv``."""
    year, month, day = date_str.split("-")
    return project_root / "data" / "previews" / source / year / month / f"{day}.csv"


# --- Row builders -----------------------------------------------------------

def _fmt(value) -> str:
    """Render a possibly-``None`` cell. ``None`` -> empty string."""
    if value is None:
        return ""
    return str(value)


def _common_cells(
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
) -> List[str]:
    return [
        race_code,
        date_str,
        f"{stadium_code:02d}",
        f"{race_number:02d}R",
        deadline_time,
        fetched_at_iso,
    ]


def build_tkz_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    status: Optional[str],
    boats: Dict[int, Dict[str, Optional[float]]],
) -> List[str]:
    """Compose one tkz CSV row.

    ``boats`` is the dict returned by
    :meth:`PreviewTsvScraper.fetch_tkz_raw` (keyed 1..6).
    """
    row = _common_cells(
        race_code, date_str, stadium_code, race_number,
        deadline_time, fetched_at_iso,
    )
    row.append(_fmt(status))
    for boat_num in range(1, 7):
        info = boats.get(boat_num) or {}
        row.extend(
            [
                _fmt(info.get("weight")),
                _fmt(info.get("weight_adjustment")),
                _fmt(info.get("exhibition_time")),
                _fmt(info.get("tilt_adjustment")),
            ]
        )
    return row


def build_stt_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    boats: Dict[int, Dict[str, Optional[float]]],
) -> List[str]:
    """Compose one stt CSV row.

    ``boats`` is the dict returned by
    :meth:`PreviewTsvScraper.fetch_stt_raw` (keyed 1..6).
    Start timing follows the existing CSV convention: F is already negated
    and L is rendered as an empty cell.
    """
    row = _common_cells(
        race_code, date_str, stadium_code, race_number,
        deadline_time, fetched_at_iso,
    )
    for boat_num in range(1, 7):
        info = boats.get(boat_num) or {}
        row.extend(
            [
                _fmt(info.get("course_number")),
                _fmt(info.get("start_timing")),
            ]
        )
    return row


def build_oex_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    measure_count: Optional[int],
    measure_labels: List[str],
    boats,
) -> List[str]:
    """Compose one original-exhibition CSV row.

    ``boats`` is an iterable of :class:`OriginalExhibitionBoat` returned
    by :meth:`OriginalExhibitionScraper.scrape_race` (always 6 entries
    when valid, indexed by ``boat_number``).
    """
    row = _common_cells(
        race_code, date_str, stadium_code, race_number,
        deadline_time, fetched_at_iso,
    )
    labels = list(measure_labels) + [""] * 3
    row.extend(
        [
            _fmt(measure_count),
            labels[0],
            labels[1],
            labels[2],
        ]
    )
    boats_by_number = {b.boat_number: b for b in boats}
    for boat_num in range(1, 7):
        b = boats_by_number.get(boat_num)
        if b is None:
            row.extend(["", "", "", ""])
        else:
            row.extend(
                [
                    _fmt(b.racer_name),
                    _fmt(b.value1),
                    _fmt(b.value2),
                    _fmt(b.value3),
                ]
            )
    return row


def build_sui_row(
    *,
    race_code: str,
    date_str: str,
    stadium_code: int,
    race_number: int,
    deadline_time: str,
    fetched_at_iso: str,
    weather: Dict[str, Optional[float]],
) -> List[str]:
    """Compose one sui CSV row.

    ``weather`` is the dict returned by
    :meth:`PreviewTsvScraper.fetch_sui_raw`.
    """
    row = _common_cells(
        race_code, date_str, stadium_code, race_number,
        deadline_time, fetched_at_iso,
    )
    row.extend(
        [
            _fmt(weather.get("observed_at")),
            _fmt(weather.get("wind_speed")),
            _fmt(weather.get("wind_direction")),
            _fmt(weather.get("wave_height")),
            _fmt(weather.get("weather")),
            _fmt(weather.get("air_temperature")),
            _fmt(weather.get("water_temperature")),
        ]
    )
    return row


# --- File I/O helpers -------------------------------------------------------

def existing_race_codes(path: Path) -> Set[str]:
    """Return the set of ``レースコード`` already in *path* (empty if absent).

    The header row is skipped. Used by the realtime scheduler to dedupe
    re-runs for the same race within a single day.
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
            "preview_csv_existing_read_failed",
            path=str(path),
            error=str(exc),
        )
        return set()


def append_rows(
    path: Path,
    headers: List[str],
    rows: Iterable[List[str]],
) -> int:
    """Append ``rows`` to ``path``, writing the header first if needed.

    Returns the number of rows actually written. The caller is responsible
    for deduping by ``レースコード`` *before* calling this (use
    :func:`existing_race_codes`).
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
        "preview_csv_appended",
        path=str(path),
        rows=len(rows),
        new_file=new_file,
    )
    return len(rows)


__all__ = [
    "COMMON_HEADERS",
    "TKZ_HEADERS",
    "STT_HEADERS",
    "SUI_HEADERS",
    "OEX_HEADERS",
    "csv_path_for",
    "build_tkz_row",
    "build_stt_row",
    "build_sui_row",
    "build_oex_row",
    "existing_race_codes",
    "append_rows",
]

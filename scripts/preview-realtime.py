#!/usr/bin/env python3
"""Realtime preview scraper.

Run every minute by GitHub Actions between JST 08:30 and 23:00. Each
invocation:

1. Fetches ``getHoldingList2`` for today (JST) to discover open venues and
   per-race deadline times. Nothing is persisted from this response.
2. Computes the *eligibility window* — races whose deadline falls roughly
   five minutes from now (default: ``[now+1min, now+10min]``).
3. For each eligible race that has not yet been recorded today, scrapes
   ``bc_j_tkz`` / ``bc_j_stt`` / ``bc_sui`` / ``bc_oriten`` and appends one
   row per source to the matching daily CSV under
   ``data/previews/{tkz,stt,sui,original_exhibition}/``.
4. Commits and pushes the changes (one commit per invocation, only when
   rows were actually appended).

The eligibility window is intentionally wider than five minutes because
GitHub Actions cron is best-effort and may fire several minutes late.
Idempotency is guaranteed by deduping new rows against existing
``レースコード`` already present in each daily CSV.

If no race in the window remains to be fetched, the script exits 0
without touching any file.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Make the boatrace package importable regardless of CWD.
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module  # noqa: E402
from boatrace import git_operations  # noqa: E402
from boatrace.downloader import RateLimiter  # noqa: E402
from boatrace.holding_list import (  # noqa: E402
    HoldingListError,
    HoldingRace,
    build_race_code,
    fetch_holding_list,
)
from boatrace.preview_tsv_scraper import PreviewTsvScraper  # noqa: E402
from boatrace.original_exhibition_scraper import (  # noqa: E402
    OriginalExhibitionScraper,
)
from boatrace.preview_csv import (  # noqa: E402
    OEX_HEADERS,
    STT_HEADERS,
    SUI_HEADERS,
    TKZ_HEADERS,
    append_rows,
    build_oex_row,
    build_stt_row,
    build_sui_row,
    build_tkz_row,
    csv_path_for,
    existing_race_codes,
)


SOURCES = ("tkz", "stt", "sui", "original_exhibition")


JST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_config(config_path: str = ".boatrace/config.json") -> dict:
    """Load configuration. Returns empty dict if not found."""
    config_file = Path(config_path)
    if not config_file.is_absolute() and not config_file.exists():
        config_file = PROJECT_ROOT / config_path
    if not config_file.exists():
        return {}
    try:
        with open(config_file) as f:
            return json.load(f)
    except (OSError, ValueError) as exc:
        logging_module.error("config_load_error", error=str(exc))
        return {}


def parse_hhmm(value: str) -> Optional[Tuple[int, int]]:
    """``"15:18"`` -> ``(15, 18)`` (or ``None`` if malformed)."""
    if not value or ":" not in value:
        return None
    hh_s, mm_s = value.split(":", 1)
    try:
        hh, mm = int(hh_s), int(mm_s)
    except ValueError:
        return None
    if not (0 <= hh < 24 and 0 <= mm < 60):
        return None
    return hh, mm


def deadline_to_jst_datetime(date_str: str, deadline: str) -> Optional[datetime]:
    """Combine ``YYYY-MM-DD`` + ``HH:MM`` into a JST datetime."""
    parts = parse_hhmm(deadline)
    if not parts:
        return None
    hh, mm = parts
    base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=JST)
    return base.replace(hour=hh, minute=mm, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def select_eligible_races(
    races: List[HoldingRace],
    now_jst: datetime,
    date_str: str,
    window_min: int,
    window_max: int,
    already_recorded: Dict[str, Set[str]],
) -> List[HoldingRace]:
    """Filter the holding-list to races we should fetch right now.

    A race is eligible when:
      * ``cancel_status`` is empty (open / not 順延 / not 中止 / not 途中中止)
      * ``deadline_time`` falls in ``[now + window_min, now + window_max]`` minutes
      * its ``race_code`` is not yet present in **every** source CSV

    The third condition uses an intersection — if any source still misses
    this race we re-include it so subsequent runs can complete the
    partial success (each source's append step does its own per-source
    dedupe).
    """
    lower = now_jst + timedelta(minutes=window_min)
    upper = now_jst + timedelta(minutes=window_max)

    eligible: List[HoldingRace] = []
    for race in races:
        if not race.is_open:
            continue
        deadline_dt = deadline_to_jst_datetime(date_str, race.deadline_time)
        if deadline_dt is None:
            continue
        if not (lower <= deadline_dt <= upper):
            continue
        race_code = build_race_code(date_str, race.stadium_code, race.race_number)
        # Skip only when already in *every* source's CSV — that way a
        # partial success last run can be completed this run.
        recorded_in_all = all(
            race_code in already_recorded.get(src, set())
            for src in SOURCES
        )
        if recorded_in_all:
            continue
        eligible.append(race)
    return eligible


def fetch_and_build_rows(
    scraper: PreviewTsvScraper,
    oex_scraper: OriginalExhibitionScraper,
    eligible: List[HoldingRace],
    date_str: str,
    fetched_at_iso: str,
    already_recorded: Dict[str, Set[str]],
) -> Tuple[List[List[str]], List[List[str]], List[List[str]], List[List[str]]]:
    """Scrape each eligible race and build per-source CSV rows.

    Returns ``(tkz_rows, stt_rows, sui_rows, oex_rows)`` containing only
    rows for races whose source data could be fetched & parsed and which
    are not already present in that source's CSV.

    Per-source skip rules:
      * tkz: ``status`` of ``"0"`` (計測中) or ``"2"`` (計測不能) -> skip
      * stt / sui: source file absent -> skip
      * original_exhibition (oex): ``status`` not ``"1"`` (i.e. measuring
        or unmeasurable) or source file absent -> skip
    """
    tkz_rows: List[List[str]] = []
    stt_rows: List[List[str]] = []
    sui_rows: List[List[str]] = []
    oex_rows: List[List[str]] = []

    # Per-invocation cache: bc_sui content is the same for all races at
    # the same stadium at any given moment. Within one minute's run we
    # dedupe the HTTP call per stadium.
    sui_cache: Dict[int, Optional[Dict]] = {}

    for race in eligible:
        race_code = build_race_code(date_str, race.stadium_code, race.race_number)
        common = dict(
            race_code=race_code,
            date_str=date_str,
            stadium_code=race.stadium_code,
            race_number=race.race_number,
            deadline_time=race.deadline_time,
            fetched_at_iso=fetched_at_iso,
        )

        # --- tkz ---
        if race_code not in already_recorded.get("tkz", set()):
            tkz_result = scraper.fetch_tkz_raw(
                date_str, race.stadium_code, race.race_number
            )
            if tkz_result is not None:
                status, boats = tkz_result
                if status == "1":
                    tkz_rows.append(
                        build_tkz_row(status=status, boats=boats, **common)
                    )
                else:
                    logging_module.info(
                        "preview_realtime_tkz_skipped",
                        race_code=race_code,
                        status=status,
                        reason="not_ready",
                    )
            else:
                logging_module.info(
                    "preview_realtime_tkz_skipped",
                    race_code=race_code,
                    reason="file_missing",
                )

        # --- stt ---
        if race_code not in already_recorded.get("stt", set()):
            stt_data = scraper.fetch_stt_raw(
                date_str, race.stadium_code, race.race_number
            )
            if stt_data:
                stt_rows.append(build_stt_row(boats=stt_data, **common))
            else:
                logging_module.info(
                    "preview_realtime_stt_skipped",
                    race_code=race_code,
                    reason="file_missing",
                )

        # --- sui ---
        if race_code not in already_recorded.get("sui", set()):
            if race.stadium_code not in sui_cache:
                sui_cache[race.stadium_code] = scraper.fetch_sui_raw(
                    date_str, race.stadium_code
                )
            weather = sui_cache[race.stadium_code]
            if weather:
                sui_rows.append(build_sui_row(weather=weather, **common))
            else:
                logging_module.info(
                    "preview_realtime_sui_skipped",
                    race_code=race_code,
                    reason="file_missing",
                )

        # --- original_exhibition (bc_oriten) ---
        if race_code not in already_recorded.get("original_exhibition", set()):
            oex_data = oex_scraper.scrape_race(
                date_str, race.stadium_code, race.race_number
            )
            if oex_data is None:
                logging_module.info(
                    "preview_realtime_oex_skipped",
                    race_code=race_code,
                    reason="file_missing",
                )
            elif oex_data.status != "1":
                logging_module.info(
                    "preview_realtime_oex_skipped",
                    race_code=race_code,
                    status=oex_data.status,
                    reason="not_ready",
                )
            elif not oex_data.is_valid():
                logging_module.info(
                    "preview_realtime_oex_skipped",
                    race_code=race_code,
                    reason="invalid_boat_count",
                )
            else:
                oex_rows.append(
                    build_oex_row(
                        measure_count=oex_data.measure_count,
                        measure_labels=oex_data.measure_labels,
                        boats=oex_data.boats,
                        **common,
                    )
                )

    return tkz_rows, stt_rows, sui_rows, oex_rows


def write_all(
    date_str: str,
    tkz_rows: List[List[str]],
    stt_rows: List[List[str]],
    sui_rows: List[List[str]],
    oex_rows: List[List[str]],
    dry_run: bool,
) -> Tuple[List[Path], int]:
    """Append rows to each daily CSV.

    Returns ``(written_paths, total_rows)``. In dry-run mode nothing is
    written but the same return shape is produced for logging.
    """
    paths_written: List[Path] = []
    total_rows = 0

    for source, headers, rows in (
        ("tkz", TKZ_HEADERS, tkz_rows),
        ("stt", STT_HEADERS, stt_rows),
        ("sui", SUI_HEADERS, sui_rows),
        ("original_exhibition", OEX_HEADERS, oex_rows),
    ):
        if not rows:
            continue
        path = csv_path_for(PROJECT_ROOT, source, date_str)
        if dry_run:
            logging_module.info(
                "preview_realtime_csv_dry_run",
                source=source,
                path=str(path),
                rows=len(rows),
            )
            paths_written.append(path)
            total_rows += len(rows)
            continue
        written = append_rows(path, headers, rows)
        if written > 0:
            paths_written.append(path)
            total_rows += written
    return paths_written, total_rows


def commit_changes(
    paths: List[Path],
    date_str: str,
    eligible: List[HoldingRace],
) -> bool:
    """Stage paths, commit, push. Returns True on success / no-op."""
    if not paths:
        return True

    rel_paths = [str(p.relative_to(PROJECT_ROOT)) for p in paths]
    summary = ",".join(
        f"{r.stadium_code:02d}-{r.race_number:02d}"
        for r in sorted(eligible, key=lambda r: (r.stadium_code, r.race_number))
    )
    message = f"Update preview realtime: {date_str} [{summary}]"
    return git_operations.commit_and_push(rel_paths, message)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Realtime preview scraper. Designed to be run every minute "
            "from GitHub Actions."
        )
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override the target date (YYYY-MM-DD). Default: today (JST).",
    )
    parser.add_argument(
        "--now",
        type=str,
        default=None,
        help=(
            "Override the reference 'current time' as ``HH:MM`` (JST), "
            "useful for backfill / testing."
        ),
    )
    parser.add_argument(
        "--window-min",
        type=int,
        default=1,
        help="Lower bound (minutes ahead of now). Default: 1.",
    )
    parser.add_argument(
        "--window-max",
        type=int,
        default=10,
        help="Upper bound (minutes ahead of now). Default: 10.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve eligible races and log the plan, but write nothing.",
    )
    parser.add_argument(
        "--no-commit",
        action="store_true",
        help="Write CSVs but skip the git commit & push step.",
    )
    return parser.parse_args()


def resolve_now(args_now: Optional[str], date_str: str) -> datetime:
    if args_now:
        parts = parse_hhmm(args_now)
        if not parts:
            raise SystemExit(f"--now must be HH:MM, got: {args_now}")
        hh, mm = parts
        base = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=JST)
        return base.replace(hour=hh, minute=mm)
    return datetime.now(JST)


def main() -> int:
    args = parse_args()

    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    today_jst = datetime.now(JST).strftime("%Y-%m-%d")
    date_str = args.date or today_jst
    now_jst = resolve_now(args.now, date_str)
    fetched_at_iso = now_jst.isoformat()

    logging_module.info(
        "preview_realtime_start",
        date=date_str,
        now=now_jst.isoformat(),
        window=f"[+{args.window_min}, +{args.window_max}] min",
        dry_run=args.dry_run,
    )

    rate_limiter = RateLimiter(
        interval_seconds=config.get("rate_limit_interval_seconds", 0.1)
    )

    # --- 1. Fetch holding list ----------------------------------------
    try:
        races = fetch_holding_list(
            date_str,
            timeout_seconds=config.get("request_timeout_seconds", 30),
            rate_limiter=rate_limiter,
        )
    except HoldingListError as exc:
        logging_module.error(
            "preview_realtime_holding_list_failed", error=str(exc)
        )
        return 1

    if not races:
        logging_module.info("preview_realtime_no_holdings", date=date_str)
        return 0

    # --- 2. Existing race_codes per source (for idempotency) ----------
    already_recorded: Dict[str, Set[str]] = {
        source: existing_race_codes(csv_path_for(PROJECT_ROOT, source, date_str))
        for source in SOURCES
    }

    # --- 3. Eligibility window ----------------------------------------
    eligible = select_eligible_races(
        races,
        now_jst,
        date_str,
        args.window_min,
        args.window_max,
        already_recorded,
    )

    logging_module.info(
        "preview_realtime_eligible",
        date=date_str,
        eligible_count=len(eligible),
        eligible=[
            f"{r.stadium_code:02d}-{r.race_number:02d}@{r.deadline_time}"
            for r in eligible
        ],
    )

    if not eligible:
        logging_module.info(
            "preview_realtime_nothing_to_do",
            date=date_str,
            now=now_jst.isoformat(),
        )
        return 0

    # --- 4. Fetch & build rows ----------------------------------------
    scraper = PreviewTsvScraper(
        timeout_seconds=config.get("request_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )
    oex_scraper = OriginalExhibitionScraper(
        timeout_seconds=config.get("request_timeout_seconds", 30),
        rate_limiter=rate_limiter,
    )
    tkz_rows, stt_rows, sui_rows, oex_rows = fetch_and_build_rows(
        scraper,
        oex_scraper,
        eligible,
        date_str,
        fetched_at_iso,
        already_recorded,
    )

    # --- 5. Write CSVs -------------------------------------------------
    paths, total = write_all(
        date_str, tkz_rows, stt_rows, sui_rows, oex_rows, args.dry_run
    )

    logging_module.info(
        "preview_realtime_write_complete",
        date=date_str,
        paths=[str(p.relative_to(PROJECT_ROOT)) for p in paths],
        total_rows=total,
        tkz=len(tkz_rows),
        stt=len(stt_rows),
        sui=len(sui_rows),
        original_exhibition=len(oex_rows),
    )

    # --- 6. Commit & push ---------------------------------------------
    if args.dry_run or args.no_commit or not paths:
        return 0

    if not commit_changes(paths, date_str, eligible):
        logging_module.error("preview_realtime_commit_failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

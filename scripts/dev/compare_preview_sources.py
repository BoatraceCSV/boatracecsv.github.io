#!/usr/bin/env python3
"""Compare TSV-source vs HTML-source preview CSVs for parity validation.

Phase 4 helper: takes a date whose committed
``data/previews/daily/YYYY/MM/DD.csv`` was produced by the legacy HTML scraper,
re-runs the new :class:`PreviewTsvScraper` for the same set of races,
and prints a column-by-column diff report.

Usage:
    python3 scripts/dev/compare_preview_sources.py --date 2026-04-24
    python3 scripts/dev/compare_preview_sources.py --date 2026-04-24 --limit 12
    python3 scripts/dev/compare_preview_sources.py --date 2026-04-24 --rate 1.0
    python3 scripts/dev/compare_preview_sources.py --date 2026-04-24 --details

The script does NOT write any files. It only fetches TSVs from
race.boatcast.jp and emits a textual report on stdout. Numeric fields
are compared with a ±0.01 tolerance; everything else uses exact string
equality.
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import Counter, defaultdict
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Make the boatrace package importable regardless of where this is run.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

from boatrace.converter import (  # noqa: E402  (sys.path set above)
    PREVIEWS_HEADERS,
    race_preview_to_row,
)
from boatrace.downloader import RateLimiter  # noqa: E402
from boatrace.preview_tsv_scraper import PreviewTsvScraper  # noqa: E402

# Columns whose semantics are known to differ between sources. The HTML
# source captures weather AT EXHIBITION TIME, while the TSV source uses
# the bc_rs1_2 row (race-terminal). We still report the diff for these,
# but flag them so the parity verdict isn't blocked by them.
_EXPECTED_DRIFT_COLUMNS = {
    "風速(m)",
    "風向",
    "波の高さ(cm)",
    "天候",
    "気温(℃)",
    "水温(℃)",
}

# Numeric tolerance for floating-point comparisons.
_NUMERIC_TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _values_equal(a: str, b: str) -> bool:
    """Return ``True`` when two CSV cells should be considered equivalent.

    Empty strings on both sides count as equal. If both values look
    numeric, they're compared with ``_NUMERIC_TOLERANCE``. Otherwise we
    fall back to literal string equality.
    """
    if a == b:
        return True
    if not a and not b:
        return True
    try:
        return abs(float(a) - float(b)) <= _NUMERIC_TOLERANCE
    except (TypeError, ValueError):
        return False


def _classify_diff(a: str, b: str) -> str:
    """Bucket how two cells differ for the per-column tally."""
    if _values_equal(a, b):
        return "equal"
    if a and not b:
        return "tsv_blank"
    if not a and b:
        return "html_blank"
    return "mismatch"


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------


def _load_existing_csv(date: str) -> List[Dict[str, str]]:
    y, m, d = date.split("-")
    path = _REPO_ROOT / "data" / "previews" / "daily" / y / m / f"{d}.csv"
    if not path.exists():
        raise FileNotFoundError(
            f"Committed CSV not found at {path}. Run a previous "
            f"day's data through the legacy pipeline first."
        )
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _race_key(row: Dict[str, str]) -> Tuple[int, int]:
    """Return ``(stadium_number, race_number)`` from a CSV row."""
    stadium_number = int(row["レース場"])
    race_round = row["レース回"].rstrip("R")
    return stadium_number, int(race_round)


def _scrape_tsv_row(
    scraper: PreviewTsvScraper,
    date: str,
    stadium_number: int,
    race_number: int,
    title: Optional[str],
) -> Optional[Dict[str, str]]:
    """Scrape a single race via TSV and turn it into a row dict.

    Returns ``None`` when the scraper couldn't produce a valid preview.
    """
    preview = scraper.scrape_race_preview(date, stadium_number, race_number)
    if preview is None:
        return None
    if title and not preview.title:
        preview.title = title
    row_values = race_preview_to_row(preview)
    return dict(zip(PREVIEWS_HEADERS, row_values))


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def _format_count(count: int, total: int) -> str:
    if total == 0:
        return f"{count}"
    pct = (count / total) * 100
    return f"{count}/{total} ({pct:.1f}%)"


def _print_report(
    date: str,
    column_stats: Dict[str, Counter],
    sample_diffs: Dict[str, List[Tuple[str, str, str]]],
    missing_in_tsv: List[str],
    missing_in_html: List[str],
    races_compared: int,
    races_skipped: int,
    show_details: bool,
) -> None:
    print()
    print(f"Preview source parity report — {date}")
    print("=" * 72)
    print(f"Races compared:  {races_compared}")
    print(f"Races skipped:   {races_skipped} (TSV scraper returned None)")
    if missing_in_tsv:
        print(f"Race codes in HTML but missing from TSV ({len(missing_in_tsv)}):")
        for code in missing_in_tsv[:20]:
            print(f"  {code}")
        if len(missing_in_tsv) > 20:
            print(f"  ... and {len(missing_in_tsv) - 20} more")
    if missing_in_html:
        print(f"Race codes in TSV but missing from HTML ({len(missing_in_html)}):")
        for code in missing_in_html[:20]:
            print(f"  {code}")
    print()

    # Per-column summary table.
    print("Per-column tally")
    print("-" * 72)
    fmt = "{:<24} {:>14} {:>10} {:>10} {:>10}  {}"
    print(fmt.format("Column", "equal", "mismatch", "tsv-blank", "html-blank", "Note"))
    column_blocking_diffs: List[Tuple[str, int]] = []
    for column in PREVIEWS_HEADERS:
        c = column_stats[column]
        total = sum(c.values())
        flag = "(weather drift expected)" if column in _EXPECTED_DRIFT_COLUMNS else ""
        if (
            c["mismatch"] + c["tsv_blank"] + c["html_blank"] > 0
            and column not in _EXPECTED_DRIFT_COLUMNS
        ):
            column_blocking_diffs.append((column, c["mismatch"] + c["tsv_blank"] + c["html_blank"]))
        print(
            fmt.format(
                column[:24],
                _format_count(c["equal"], total),
                str(c["mismatch"]),
                str(c["tsv_blank"]),
                str(c["html_blank"]),
                flag,
            )
        )

    print()
    print("Verdict")
    print("-" * 72)
    if not column_blocking_diffs:
        print("PASS — all non-weather columns agree within tolerance.")
    else:
        print("DIFFS in non-weather columns (review needed):")
        for col, n in sorted(column_blocking_diffs, key=lambda x: -x[1]):
            print(f"  {col}: {n} differing cells")

    if show_details:
        print()
        print("Sample differences (up to 5 per column)")
        print("-" * 72)
        for column in PREVIEWS_HEADERS:
            samples = sample_diffs.get(column) or []
            if not samples:
                continue
            tag = " (drift expected)" if column in _EXPECTED_DRIFT_COLUMNS else ""
            print(f"\n{column}{tag}")
            for race_code, html_val, tsv_val in samples[:5]:
                print(f"  {race_code}: HTML={html_val!r:<12} TSV={tsv_val!r}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        required=True,
        help="Date to compare (YYYY-MM-DD). The committed CSV at "
        "data/previews/daily/YYYY/MM/DD.csv must already exist.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N races (handy for smoke tests).",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=1.0,
        help="Seconds between TSV fetches (default 1.0). boatcast.jp is "
        "served by CloudFront so 1.0s is gentle but functional.",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Print up to 5 sample differing rows per column.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        existing_rows = _load_existing_csv(args.date)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    if args.limit is not None:
        existing_rows = existing_rows[: args.limit]

    rate_limiter = RateLimiter(interval_seconds=args.rate)
    scraper = PreviewTsvScraper(rate_limiter=rate_limiter)

    column_stats: Dict[str, Counter] = defaultdict(Counter)
    sample_diffs: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    missing_in_tsv: List[str] = []
    races_compared = 0
    races_skipped = 0

    print(
        f"Comparing {len(existing_rows)} race(s) for {args.date} "
        f"(rate={args.rate}s) ...",
        file=sys.stderr,
    )

    for index, html_row in enumerate(existing_rows, start=1):
        try:
            stadium_number, race_number = _race_key(html_row)
        except (KeyError, ValueError):
            continue

        title = html_row.get("タイトル") or None
        tsv_row = _scrape_tsv_row(
            scraper, args.date, stadium_number, race_number, title
        )
        if tsv_row is None:
            races_skipped += 1
            missing_in_tsv.append(html_row.get("レースコード", "?"))
            print(
                f"  [{index:3d}/{len(existing_rows)}] "
                f"jo={stadium_number:02d} race={race_number:02d}  TSV: MISSING",
                file=sys.stderr,
            )
            continue

        races_compared += 1
        for column in PREVIEWS_HEADERS:
            html_val = html_row.get(column, "")
            tsv_val = tsv_row.get(column, "")
            bucket = _classify_diff(html_val, tsv_val)
            column_stats[column][bucket] += 1
            if bucket != "equal" and len(sample_diffs[column]) < 5:
                sample_diffs[column].append(
                    (html_row.get("レースコード", "?"), html_val, tsv_val)
                )

        if index % 10 == 0:
            print(
                f"  [{index:3d}/{len(existing_rows)}] processed",
                file=sys.stderr,
            )

    _print_report(
        date=args.date,
        column_stats=column_stats,
        sample_diffs=sample_diffs,
        missing_in_tsv=missing_in_tsv,
        missing_in_html=[],  # we drive iteration from HTML, so this is always empty
        races_compared=races_compared,
        races_skipped=races_skipped,
        show_details=args.details,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

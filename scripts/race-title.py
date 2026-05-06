#!/usr/bin/env python3
"""Scrape race title (レース名) data from race.boatcast.jp.

For a given date, this script fetches the boatcast holding-list endpoint
``/api_txt/getHoldingList2_YYYYMMDD.json`` (the same JSON the
https://race.boatcast.jp/?jo=XX SPA consumes) and writes a flat per-race
CSV to ``data/programs/title/YYYY/MM/DD.csv``.

The motivation: the per-race title (e.g. "ドラドキ目玉", "予選特別",
"優勝戦") only appears in the ``data/programs/YYYY/MM/DD.csv`` program
files — it is not present in ``race_cards``, ``recent_local``,
``recent_national`` or ``motor_stats``. This script produces a small
sidecar CSV keyed by ``レースコード`` that downstream consumers can join
to the other tables without loading the full program file.

Output columns:
    レースコード, レース日, レース場コード, レース場, レース回,
    タイトル, 日次, グレード, ナイター, レース名,
    電話投票締切予定, 中止状態

One row per scheduled race per open stadium.
"""

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

# Add boatrace package to path
sys.path.insert(0, str(Path(__file__).parent))

from boatrace import logger as logging_module
from boatrace import git_operations
from boatrace.downloader import RateLimiter
from boatrace.storage import write_csv


OUTPUT_DIR = "data/programs/title"
BOATCAST_BASE = "https://race.boatcast.jp"

# Stadium code (zero-padded 2-digit, "01"-"24") -> 場名 (without prefix).
# Mirrors VENUE_CODES in boatrace.converter but inverted.
STADIUM_NAMES = {
    "01": "桐生",
    "02": "戸田",
    "03": "江戸川",
    "04": "平和島",
    "05": "多摩川",
    "06": "浜名湖",
    "07": "蒲郡",
    "08": "常滑",
    "09": "津",
    "10": "三国",
    "11": "びわこ",
    "12": "住之江",
    "13": "尼崎",
    "14": "鳴門",
    "15": "丸亀",
    "16": "児島",
    "17": "宮島",
    "18": "徳山",
    "19": "下関",
    "20": "若松",
    "21": "芦屋",
    "22": "福岡",
    "23": "唐津",
    "24": "大村",
}

CSV_HEADER = [
    "レースコード",
    "レース日",
    "レース場コード",
    "レース場",
    "レース回",
    "タイトル",
    "日次",
    "グレード",
    "ナイター",
    "レース名",
    "電話投票締切予定",
    "中止状態",
]


def fetch_holding_list(
    date_str: str,
    timeout_seconds: int = 30,
    rate_limiter: Optional[RateLimiter] = None,
) -> Optional[dict]:
    """Fetch ``/api_txt/getHoldingList2_YYYYMMDD.json`` for the given date.

    Args:
        date_str: Date in YYYY-MM-DD format.
        timeout_seconds: HTTP timeout.
        rate_limiter: Optional rate limiter; ``wait()`` is called before
            the request.

    Returns:
        Parsed JSON dict on success, ``None`` if the API was reachable
        but returned non-success / invalid payload. Raises on network
        errors.
    """
    yyyymmdd = date_str.replace("-", "")
    url = f"{BOATCAST_BASE}/api_txt/getHoldingList2_{yyyymmdd}.json"

    if rate_limiter is not None:
        rate_limiter.wait()

    logging_module.info(
        "race_title_fetch_start",
        date=date_str,
        url=url,
    )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Referer": f"{BOATCAST_BASE}/",
    }

    resp = requests.get(url, headers=headers, timeout=timeout_seconds)
    if resp.status_code != 200:
        logging_module.warning(
            "race_title_fetch_non_200",
            date=date_str,
            url=url,
            status=resp.status_code,
        )
        return None

    try:
        payload = resp.json()
    except ValueError as e:
        logging_module.warning(
            "race_title_fetch_invalid_json",
            date=date_str,
            url=url,
            error=str(e),
        )
        return None

    if not isinstance(payload, dict) or payload.get("res_cd") != 0:
        logging_module.warning(
            "race_title_fetch_bad_res_cd",
            date=date_str,
            url=url,
            res_cd=payload.get("res_cd") if isinstance(payload, dict) else None,
        )
        return None

    return payload


def _csv_escape(value: str) -> str:
    """Minimal CSV-field escape compatible with the rest of the repo."""
    if value is None:
        return ""
    s = str(value)
    if any(c in s for c in (",", '"', "\n", "\r")):
        return '"' + s.replace('"', '""') + '"'
    return s


def build_csv(date_str: str, payload: dict) -> tuple[str, int]:
    """Flatten the boatcast holding-list payload into per-race CSV rows.

    Returns:
        ``(csv_text, row_count)``. ``csv_text`` ends with ``\\n`` and is
        empty (``""``) with row_count ``0`` if no races were emitted.
    """
    yyyymmdd = date_str.replace("-", "")
    rows: list[list[str]] = []

    for venue in payload.get("return_info", []) or []:
        jo = str(venue.get("RaceStudiumNo") or "").zfill(2)
        if not jo or jo == "00":
            continue

        holding_title = (venue.get("HoldingTitle") or "").strip()
        daily_title = (venue.get("DailyTitle") or "").strip()
        race_grade = (venue.get("RaceGrade") or "").strip()
        nighter = (venue.get("NighterFlag") or "").strip()
        title_all = venue.get("RaceTitleAll") or []
        deadline_all = venue.get("DeadlineTimeAll") or []
        cancel_all = venue.get("CancelStatusAll") or []

        # RaceTitleAll is the authoritative per-race list. Fall back to
        # the singular RaceTitle if necessary.
        if not title_all and venue.get("RaceTitle"):
            title_all = [venue["RaceTitle"]]

        for idx, raw_title in enumerate(title_all, start=1):
            race_no = idx
            race_title = (raw_title or "").strip()
            # Boatcast pads race titles with full-width spaces; trim them.
            race_title = race_title.strip("　 ").strip()

            deadline = ""
            if idx - 1 < len(deadline_all):
                deadline = (deadline_all[idx - 1] or "").strip()

            cancel = ""
            if idx - 1 < len(cancel_all):
                cancel = (cancel_all[idx - 1] or "").strip()

            race_code = f"{yyyymmdd}{jo}{race_no:02d}"
            stadium_name = STADIUM_NAMES.get(jo, "")

            rows.append(
                [
                    race_code,
                    date_str,
                    jo,
                    stadium_name,
                    f"{race_no}R",
                    holding_title,
                    daily_title,
                    race_grade,
                    nighter,
                    race_title,
                    deadline,
                    cancel,
                ]
            )

    if not rows:
        return "", 0

    # Stable ordering: by stadium code, then race number.
    rows.sort(key=lambda r: (r[2], int(r[4].rstrip("R"))))

    out_lines = [",".join(_csv_escape(c) for c in CSV_HEADER)]
    out_lines.extend(",".join(_csv_escape(c) for c in row) for row in rows)
    return "\n".join(out_lines) + "\n", len(rows)


def process_race_title(
    date_str: str,
    config: dict,
    rate_limiter: RateLimiter,
    force_overwrite: bool = False,
    dry_run: bool = False,
) -> dict:
    """Fetch boatcast holding list for one day and write title CSV."""
    stats = {
        "stadiums_open": 0,
        "races_written": 0,
        "csv_files_created": 0,
        "csv_files_skipped": 0,
        "errors": [],
    }

    logging_module.info("race_title_processing_start", date=date_str)

    try:
        payload = fetch_holding_list(
            date_str,
            timeout_seconds=config.get("race_title_timeout_seconds", 30),
            rate_limiter=rate_limiter,
        )
    except Exception as e:
        stats["errors"].append(
            {
                "date": date_str,
                "error_type": "race_title_fetch_error",
                "message": str(e),
            }
        )
        logging_module.error(
            "race_title_fetch_error",
            date=date_str,
            error=str(e),
            error_type=type(e).__name__,
        )
        return stats

    if not payload:
        return stats

    return_info = payload.get("return_info") or []
    stats["stadiums_open"] = len(return_info)

    csv_content, row_count = build_csv(date_str, payload)
    stats["races_written"] = row_count

    if not csv_content:
        logging_module.info(
            "race_title_no_rows",
            date=date_str,
        )
        return stats

    if dry_run:
        stats["csv_files_created"] += 1
        logging_module.info(
            "race_title_csv_dry_run",
            date=date_str,
            row_count=row_count,
        )
        return stats

    year, month, day = date_str.split("-")
    project_root = Path(__file__).parent.parent
    csv_path = project_root / f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"

    logging_module.info(
        "race_title_csv_write_start",
        date=date_str,
        path=str(csv_path),
        row_count=row_count,
    )

    if write_csv(str(csv_path), csv_content, force_overwrite):
        stats["csv_files_created"] += 1
        logging_module.info(
            "race_title_csv_write_success",
            date=date_str,
            path=str(csv_path),
        )
    else:
        stats["csv_files_skipped"] += 1
        logging_module.warning(
            "race_title_csv_write_skipped",
            date=date_str,
            path=str(csv_path),
        )

    return stats


def load_config(config_path: str = ".boatrace/config.json") -> dict:
    try:
        config_file = Path(config_path)
        if not config_file.is_absolute() and not config_file.exists():
            config_file = Path(__file__).parent.parent / config_path
        if config_file.exists():
            with open(config_file) as f:
                return json.load(f)
    except Exception as e:
        logging_module.error("config_load_error", error=str(e))
    return {}


def parse_arguments():
    jst = timezone(timedelta(hours=9))
    today_jst = datetime.now(jst).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(
        description=(
            "Scrape per-race title (レース名) data from race.boatcast.jp. "
            "Writes data/programs/title/YYYY/MM/DD.csv (one row per "
            "scheduled race per open stadium). Boatcast only exposes the "
            "current/upcoming day reliably — backfill of distant past dates "
            "may return empty payloads."
        )
    )
    parser.add_argument(
        "--date",
        type=str,
        default=today_jst,
        help="Date to process (YYYY-MM-DD). Default: today (JST)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not write files or push to git",
    )
    parser.add_argument(
        "--no-push",
        action="store_true",
        help="Write CSV but skip git commit/push",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 1.0.0",
    )
    return parser.parse_args()


def validate_date_format(date_str: str) -> bool:
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def main():
    args = parse_arguments()

    if not validate_date_format(args.date):
        print(f"Error: Invalid date format: {args.date}. Expected YYYY-MM-DD")
        sys.exit(1)

    config = load_config()
    logging_module.initialize_logger(
        log_level=config.get("log_level", "INFO"),
        log_file=config.get("log_file", "logs/boatrace-{DATE}.json"),
    )

    logging_module.info(
        "race_title_cli_start",
        date=args.date,
        dry_run=args.dry_run,
        force=args.force,
    )

    try:
        rate_limiter = RateLimiter(
            interval_seconds=config.get("rate_limit_interval_seconds", 3)
        )

        stats = process_race_title(
            args.date,
            config,
            rate_limiter,
            force_overwrite=args.force,
            dry_run=args.dry_run,
        )

        print()
        print(f"Race Title Data Processing Complete for {args.date}")
        print(f"  Stadiums open: {stats['stadiums_open']}")
        print(f"  Races written: {stats['races_written']}")
        print(f"  CSV files created: {stats['csv_files_created']}")
        print(f"  CSV files skipped: {stats['csv_files_skipped']}")
        if stats["errors"]:
            print(f"  Errors: {len(stats['errors'])}")
            for error in stats["errors"]:
                print(f"    - {error['error_type']}: {error['message']}")
        print()

        if (
            stats["csv_files_created"] > 0
            and not args.dry_run
            and not args.no_push
        ):
            year, month, day = args.date.split("-")
            csv_file = f"{OUTPUT_DIR}/{year}/{month}/{day}.csv"
            message = f"Update boatrace race title data: {args.date}"
            if git_operations.commit_and_push([csv_file], message):
                print(f"Git commit and push successful for {csv_file}")
            else:
                print(f"Git commit and push failed for {csv_file}")

        sys.exit(
            0
            if stats["csv_files_created"] > 0 or stats["csv_files_skipped"] > 0
            else 1
        )

    except Exception as e:
        logging_module.critical(
            "race_title_cli_error",
            error=str(e),
            error_type=type(e).__name__,
        )
        print(f"Error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()

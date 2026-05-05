#!/usr/bin/env python3
"""
Build data/index/YYYY/MM/DD.csv — per-race strength components for each 枠番.

All output values use the 偏差値 scale (mean 50, std 10):
    pt    = 50 + 10 × z       where z = (raw − μ) / σ over 6mo training
    寄与   = w × pt            (= 50w + 10·w·z)
    強さpt = Σ 寄与 = 50 + 10 × Σ(w·z)   (since Σw = 1)

Per boat (1〜6枠) the script outputs:
    - 5 偏差値pt columns: 枠番pt / 選手pt / モーターpt / 展示pt / 気象pt
    - 5 contribution columns: 寄与_{label}  = w_i × 偏差値pt_i
    - 1 final 強さpt = sum of contributions (偏差値 scale, ~50 ± 10)
    - 1 状態 column at the race level: 'daily' / 'realtime'

Modes
-----
* ``--mode realtime`` (default) — full daily build using all available data.
  Used for back-fill of past dates and the post-race "final" version of
  today's CSV.
* ``--mode daily`` — daily-batch (early-morning) build. Forces 展示pt and
  気象pt to be 50.0 (mean) for every boat regardless of whether previews
  exist, and writes 状態=daily for every race.

Per-race update API
-------------------
The function ``update_index_for_races(repo, day, race_codes)`` is meant to
be called from ``preview-realtime.py``. It rebuilds *only* the listed
レースコード rows from current preview data, marks them 状態=realtime, and
atomically rewrites the daily CSV. Other races on the day keep whatever
status they previously had.

Usage:
    python scripts/build_index.py --date 2026-05-03                # realtime
    python scripts/build_index.py --date 2026-05-03 --mode daily   # daily batch
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

# Local import — shared feature builders
sys.path.insert(0, str(Path(__file__).parent))
from boatrace.index_features import (  # noqa: E402
    COMPONENT_KEYS, COMPONENT_LABELS, STADIUM_NAMES, compute_features_for_day,
)


STATE_DAILY = "daily"
STATE_REALTIME = "realtime"


# ─────────────────────────────────────────────────────────────────────
# Weights file lookup
# ─────────────────────────────────────────────────────────────────────
def find_weights_file(repo: Path, day: dt.date) -> Path | None:
    """Return the latest YYYY-MM.csv ≤ day's month. None if no file exists."""
    weights_dir = repo / "data" / "stadium" / "index_weights"
    if not weights_dir.exists():
        return None
    target_tag = f"{day:%Y-%m}"
    candidates = sorted(weights_dir.glob("????-??.csv"))
    candidates = [p for p in candidates if p.stem <= target_tag]
    return candidates[-1] if candidates else None


def load_weights(path: Path) -> dict:
    """Returns {stadium_name: {mu: {k:v}, sigma: {k:v}, w: {k:v}}}."""
    df = pd.read_csv(path)
    out = {}
    for _, r in df.iterrows():
        out[r["stadium"]] = {
            "mu":    {k: float(r[f"mu_{k}"])    for k in COMPONENT_KEYS},
            "sigma": {k: float(r[f"sigma_{k}"]) for k in COMPONENT_KEYS},
            "w":     {k: float(r[f"w_{k}"])     for k in COMPONENT_KEYS},
        }
    return out


def stadium_name_from_code(code2: str) -> str:
    try:
        n = int(code2)
        return STADIUM_NAMES.get(n, "")
    except (ValueError, TypeError):
        return ""


# ─────────────────────────────────────────────────────────────────────
# Index path & ordering helpers
# ─────────────────────────────────────────────────────────────────────
def index_csv_path(repo: Path, day: dt.date) -> Path:
    return repo / "data" / "index" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"


def index_columns() -> list[str]:
    """Canonical column order for the index CSV."""
    cols = ["レースコード", "レース日", "レース場コード", "レース回", "状態"]
    for w in range(1, 7):
        for k in COMPONENT_KEYS:
            cols.append(f"{w}枠_{COMPONENT_LABELS[k]}")
            cols.append(f"{w}枠_寄与_{COMPONENT_LABELS[k]}")
        cols.append(f"{w}枠_強さpt")
    return cols


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to ``path`` via temp file + os.replace (atomic)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ─────────────────────────────────────────────────────────────────────
# Per-race row builder
# ─────────────────────────────────────────────────────────────────────
def _build_one_race_row(
    code: str,
    meta_row: pd.Series,
    boats: pd.DataFrame,
    weights: dict,
    state: str,
    *,
    skip_preview: bool,
) -> dict:
    """Construct one CSV row from the long-format feature DataFrame.

    skip_preview=True forces 展示pt and 気象pt to 50.0 (mean), regardless of
    whatever was computed — used by the daily mode.
    """
    stadium_code2 = meta_row["レース場コード"]
    stadium = stadium_name_from_code(stadium_code2)
    params = weights.get(stadium) if weights else None

    out = {
        "レースコード": code,
        "レース日":    meta_row["レース日"],
        "レース場コード": stadium_code2,
        "レース回":    meta_row["レース回"],
        "状態":        state,
    }

    for waku in range(1, 7):
        sub = boats[boats["枠番"] == waku]
        if sub.empty or params is None:
            for k in COMPONENT_KEYS:
                out[f"{waku}枠_{COMPONENT_LABELS[k]}"] = float("nan")
                out[f"{waku}枠_寄与_{COMPONENT_LABELS[k]}"] = float("nan")
            out[f"{waku}枠_強さpt"] = float("nan")
            continue

        r = sub.iloc[0]
        raw = {k: r[k] for k in COMPONENT_KEYS}
        mu_st = params["mu"]
        sigma_st = params["sigma"]
        w_st = params["w"]

        total = 0.0
        for k in COMPONENT_KEYS:
            # Daily mode: force 展示 / 気象 to mean (50)
            if skip_preview and k in ("exhibit", "weather"):
                hensachi_pt = 50.0
            else:
                v = raw[k]
                if pd.isna(v):
                    hensachi_pt = 50.0
                else:
                    z = (float(v) - mu_st[k]) / sigma_st[k] if sigma_st[k] > 0 else 0.0
                    hensachi_pt = 50.0 + 10.0 * z
            out[f"{waku}枠_{COMPONENT_LABELS[k]}"] = round(hensachi_pt, 2)
            contrib = w_st[k] * hensachi_pt
            out[f"{waku}枠_寄与_{COMPONENT_LABELS[k]}"] = round(contrib, 2)
            total += contrib
        out[f"{waku}枠_強さpt"] = round(total, 2)

    return out


# ─────────────────────────────────────────────────────────────────────
# Whole-day builders
# ─────────────────────────────────────────────────────────────────────
def build_index_day(
    repo: Path, day: dt.date, *, mode: str = STATE_REALTIME,
) -> tuple[pd.DataFrame, Path | None]:
    """Build the full daily CSV from scratch (all races).

    mode == 'daily'   → 展示・気象を50で固定し、状態=daily を出力
    mode == 'realtime'→ 全要素を計算し、状態=realtime を出力(過去日backfillもこちら)
    """
    long_df = compute_features_for_day(repo, day)
    if long_df.empty:
        return pd.DataFrame(columns=index_columns()), None

    weights_path = find_weights_file(repo, day)
    weights = load_weights(weights_path) if weights_path else {}

    skip_preview = (mode == STATE_DAILY)
    state = STATE_DAILY if skip_preview else STATE_REALTIME

    rows = []
    for code, grp in long_df.sort_values(["レースコード", "枠番"]).groupby(
        "レースコード", sort=False
    ):
        rows.append(_build_one_race_row(
            code=code, meta_row=grp.iloc[0], boats=grp,
            weights=weights, state=state, skip_preview=skip_preview,
        ))

    df = pd.DataFrame(rows, columns=index_columns())
    return df, weights_path


def _csv_cell(v) -> str:
    """Convert a Python value to its CSV-cell string representation.

    Mirrors what ``pandas.DataFrame.to_csv`` would write: NaN/None → empty,
    everything else → ``str(v)``. Used when splicing fresh rows into a
    ``dtype=str`` DataFrame; recent pandas versions reject non-string
    assignments to string-typed columns with
    ``Invalid value 'X' for dtype 'str'``.
    """
    if v is None:
        return ""
    if isinstance(v, float) and np.isnan(v):
        return ""
    return str(v)


def update_index_for_races(
    repo: Path, day: dt.date, race_codes: list[str],
) -> int:
    """Rebuild only listed レースコード rows in the daily index CSV.

    - Reads the existing CSV (errors if missing — daily batch must run first)
    - Re-computes 5 features on-the-fly for those races (展示・気象を含む)
    - Marks 状態=realtime
    - Atomically rewrites the CSV preserving order and other rows verbatim

    Returns: the number of rows successfully updated.
    """
    if not race_codes:
        return 0
    csv_path = index_csv_path(repo, day)
    if not csv_path.exists():
        # No daily-batch CSV to update; nothing to do (caller decides whether
        # to invoke build_index_day --mode daily first).
        return 0

    # All columns read as object dtype so we can splice in fresh values
    # without pandas' strict "Invalid value 'X' for dtype 'str'" guard.
    existing = pd.read_csv(csv_path, dtype=object)

    # Normalize レースコード type for matching (CSV may have parsed it as int
    # in older files; we always treat it as string).
    existing["レースコード"] = existing["レースコード"].astype(str)

    # Compute fresh features for the whole day; cheap (~1s) and lets us reuse
    # the same code path. We only emit rows for the requested レースコード.
    long_df = compute_features_for_day(repo, day)
    if long_df.empty:
        return 0
    weights_path = find_weights_file(repo, day)
    weights = load_weights(weights_path) if weights_path else {}

    race_codes_str = [str(c) for c in race_codes]
    long_df = long_df[long_df["レースコード"].astype(str).isin(race_codes_str)]
    if long_df.empty:
        return 0

    new_rows = {}
    for code, grp in long_df.sort_values(["レースコード", "枠番"]).groupby(
        "レースコード", sort=False
    ):
        new_rows[str(code)] = _build_one_race_row(
            code=code, meta_row=grp.iloc[0], boats=grp,
            weights=weights, state=STATE_REALTIME, skip_preview=False,
        )

    # Splice updated rows back into the existing DataFrame.
    cols = index_columns()
    # Ensure existing CSV has all required columns (older daily files may
    # have been generated before 状態 was added).
    for c in cols:
        if c not in existing.columns:
            existing[c] = ""
    existing = existing[cols]

    updated = 0
    for idx, row in existing.iterrows():
        code = str(row["レースコード"])
        if code in new_rows:
            for k, v in new_rows[code].items():
                existing.at[idx, k] = _csv_cell(v)
            updated += 1

    atomic_write_csv(existing, csv_path)
    return updated


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(Path(__file__).parent.parent))
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--mode", choices=[STATE_DAILY, STATE_REALTIME],
                   default=STATE_REALTIME,
                   help="daily=展示/気象は50固定で日次バッチ用CSV、"
                        "realtime=全要素を計算(過去日backfillもこちら)")
    p.add_argument("--out", default=None, help="Override output path")
    p.add_argument("--update-races", default=None,
                   help="Comma-separated レースコード list. If set, only those "
                        "rows are updated in-place (state→realtime); the rest "
                        "of the day is left as-is. Requires the CSV to exist.")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    day = dt.date.fromisoformat(args.date)

    if args.update_races:
        codes = [c.strip() for c in args.update_races.split(",") if c.strip()]
        n = update_index_for_races(repo, day, codes)
        print(f"Updated {n} rows in {index_csv_path(repo, day)}")
        return

    df, weights_path = build_index_day(repo, day, mode=args.mode)
    out_path = Path(args.out) if args.out else index_csv_path(repo, day)
    atomic_write_csv(df, out_path)

    weight_msg = (f" (weights: {weights_path.name})" if weights_path
                  else " (no weights file → 強さpt is NaN)")
    print(f"Wrote {len(df)} rows → {out_path}  [mode={args.mode}]{weight_msg}")


if __name__ == "__main__":
    main()

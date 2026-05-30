#!/usr/bin/env python3
"""
Build data/estimate/index/YYYY/MM/DD.csv — per-race strength components for each 枠番.

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
be called from ``preview-realtime.py``. For each listed レースコード it
**adds (or upserts)** a 状態=realtime row computed from the latest preview
data, **leaving any 状態=daily row for that race intact**. Therefore a
single race can have up to two rows in the CSV:

    daily   行  — 朝バッチが書いた評価 (展示・気象は中立値 50)
    realtime 行 — preview-realtime 反映後の評価 (展示・気象を含む)

Other races on the day keep whatever rows they previously had. The
fun-site builder reads both rows separately by 状態 and shows them as
「当日買い目」/「直前買い目」, so the daily evaluation is preserved through
the day.

Usage:
    python scripts/build_index.py --date 2026-05-03                # realtime full rebuild
    python scripts/build_index.py --date 2026-05-03 --mode daily   # daily batch
    python scripts/build_index.py --date 2026-05-03 --mode realtime --force
        # force full rebuild even if existing CSV has 状態=daily rows
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

# Local import — shared feature builders & predictor registry
sys.path.insert(0, str(Path(__file__).parent))
from boatrace.index_features import (  # noqa: E402
    STADIUM_NAMES, compute_features_for_day,
)
from boatrace.predictors import (  # noqa: E402
    PredictorSpec, active_predictors, component_label,
    component_missing_fallback, predictor_by_id,
)


STATE_DAILY = "daily"
STATE_REALTIME = "realtime"

DEFAULT_PREDICTOR_ID = "v1_basic"


# ─────────────────────────────────────────────────────────────────────
# Weights file lookup
# ─────────────────────────────────────────────────────────────────────
def find_weights_file(repo: Path, predictor: PredictorSpec, day: dt.date) -> Path | None:
    """Return the latest YYYY-MM.csv ≤ day's month under the predictor's weights
    directory. None if no file exists.
    """
    weights_dir = predictor.weights_dir(repo)
    if not weights_dir.exists():
        return None
    target_tag = f"{day:%Y-%m}"
    candidates = sorted(weights_dir.glob("????-??.csv"))
    candidates = [p for p in candidates if p.stem <= target_tag]
    return candidates[-1] if candidates else None


def load_weights(path: Path, predictor: PredictorSpec) -> dict:
    """Returns {stadium_name: {mu: {k:v}, sigma: {k:v}, w: {k:v}}}.

    Reads only the columns relevant to the predictor's component_keys; ignores
    any extra columns. Fails fast if a required ``mu_{k}``/``sigma_{k}``/
    ``w_{k}`` column is missing — callers should re-run ``build_weights.py``
    for that predictor.
    """
    df = pd.read_csv(path)
    keys = predictor.component_keys
    required = {f"{prefix}_{k}" for prefix in ("mu", "sigma", "w") for k in keys}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(
            f"weights file {path} is missing columns {sorted(missing)} "
            f"required by predictor {predictor.predictor_id!r}. "
            f"Re-run build_weights.py --predictor {predictor.predictor_id} "
            f"to regenerate it."
        )
    out = {}
    for _, r in df.iterrows():
        out[r["stadium"]] = {
            "mu":    {k: float(r[f"mu_{k}"])    for k in keys},
            "sigma": {k: float(r[f"sigma_{k}"]) for k in keys},
            "w":     {k: float(r[f"w_{k}"])     for k in keys},
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
def index_csv_path(
    repo: Path, day: dt.date, predictor: PredictorSpec,
) -> Path:
    """``data/estimate/{predictor_id}/YYYY/MM/DD.csv``"""
    return predictor.index_csv_path(repo, day)


def index_columns(predictor: PredictorSpec) -> list[str]:
    """Canonical column order for the predictor's index CSV.

    Layout per boat (1..6 枠):
      - ``N枠_{label}``    for each ``component_keys[i]`` (素点)
      - ``N枠_寄与_{label}`` for each ``component_keys[i]`` (重み付き寄与)
      - ``N枠_強さpt``       (寄与の総和)
    """
    cols = ["レースコード", "レース日", "レース場コード", "レース回", "状態"]
    for w in range(1, 7):
        for k in predictor.component_keys:
            label = component_label(k)
            cols.append(f"{w}枠_{label}")
            cols.append(f"{w}枠_寄与_{label}")
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
# Daily モードで強制的に平均 50 に倒す成分。
# 朝バッチ時点では展示・気象・展開優位 (進入コース) データが揃わないため、
# これらは中立値で固定する。
DAILY_NEUTRAL_COMPONENTS: frozenset[str] = frozenset({"exhibit", "weather", "tenkai"})


def _build_one_race_row(
    code: str,
    meta_row: pd.Series,
    boats: pd.DataFrame,
    weights: dict,
    state: str,
    predictor: PredictorSpec,
    *,
    skip_preview: bool,
) -> dict:
    """Construct one CSV row from the long-format feature DataFrame.

    skip_preview=True forces ``DAILY_NEUTRAL_COMPONENTS`` (展示pt / 気象pt) to
    50.0 (mean) — used by the daily mode.

    Loops over ``predictor.component_keys`` so the column set adapts to the
    predictor's feature recipe.
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
            for k in predictor.component_keys:
                label = component_label(k)
                out[f"{waku}枠_{label}"] = float("nan")
                out[f"{waku}枠_寄与_{label}"] = float("nan")
            out[f"{waku}枠_強さpt"] = float("nan")
            continue

        r = sub.iloc[0]
        mu_st = params["mu"]
        sigma_st = params["sigma"]
        w_st = params["w"]

        total = 0.0
        for k in predictor.component_keys:
            label = component_label(k)
            # Daily mode: force preview-derived components to mean (50)
            if skip_preview and k in DAILY_NEUTRAL_COMPONENTS:
                hensachi_pt = 50.0
            else:
                # Raw feature column must be present in ``boats`` (long-format
                # output of compute_features_for_day). New components are
                # responsible for adding the column there.
                v = r[k] if k in r else float("nan")
                if pd.isna(v):
                    hensachi_pt = component_missing_fallback(k)
                else:
                    z = (float(v) - mu_st[k]) / sigma_st[k] if sigma_st[k] > 0 else 0.0
                    hensachi_pt = 50.0 + 10.0 * z
            out[f"{waku}枠_{label}"] = round(hensachi_pt, 2)
            contrib = w_st[k] * hensachi_pt
            out[f"{waku}枠_寄与_{label}"] = round(contrib, 2)
            total += contrib
        out[f"{waku}枠_強さpt"] = round(total, 2)

    return out


# ─────────────────────────────────────────────────────────────────────
# Whole-day builders
# ─────────────────────────────────────────────────────────────────────
def build_index_day(
    repo: Path, day: dt.date, predictor: PredictorSpec,
    *, mode: str = STATE_REALTIME,
) -> tuple[pd.DataFrame, Path | None]:
    """Build the full daily CSV from scratch (all races) for ``predictor``.

    mode == 'daily'   → 展示・気象を50で固定し、状態=daily を出力
    mode == 'realtime'→ 全要素を計算し、状態=realtime を出力(過去日backfillもこちら)
    """
    long_df = compute_features_for_day(repo, day)
    if long_df.empty:
        return pd.DataFrame(columns=index_columns(predictor)), None

    weights_path = find_weights_file(repo, predictor, day)
    weights = load_weights(weights_path, predictor) if weights_path else {}

    skip_preview = (mode == STATE_DAILY)
    state = STATE_DAILY if skip_preview else STATE_REALTIME

    rows = []
    for code, grp in long_df.sort_values(["レースコード", "枠番"]).groupby(
        "レースコード", sort=False
    ):
        rows.append(_build_one_race_row(
            code=code, meta_row=grp.iloc[0], boats=grp,
            weights=weights, state=state, predictor=predictor,
            skip_preview=skip_preview,
        ))

    df = pd.DataFrame(rows, columns=index_columns(predictor))
    return df, weights_path


def _normalize_state_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure ``状態`` column exists and has no empty/NaN cells.

    Older index CSVs were produced before the 状態 column existed, in
    which case every row is treated as ``daily``. NaN / empty cells are
    likewise treated as ``daily``. Returns the DataFrame with the column
    materialized (in-place modification of the input is also fine).
    """
    if "状態" not in df.columns:
        df["状態"] = STATE_DAILY
        return df
    df["状態"] = df["状態"].fillna(STATE_DAILY).replace("", STATE_DAILY)
    return df


def _sort_index_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Sort rows by (レースコード asc, 状態 = daily then realtime).

    This is purely for git-diff readability; downstream consumers
    (fun-site, gcs_publisher) don't depend on row order.
    """
    if df.empty:
        return df
    state_order = pd.Categorical(
        df["状態"], categories=[STATE_DAILY, STATE_REALTIME], ordered=True
    )
    return (
        df.assign(_state_order=state_order)
          .sort_values(["レースコード", "_state_order"], kind="stable")
          .drop(columns=["_state_order"])
          .reset_index(drop=True)
    )


def update_index_for_races(
    repo: Path, day: dt.date, race_codes: list[str],
    predictor: PredictorSpec,
) -> int:
    """Upsert ``状態=realtime`` rows for the listed レースコード under
    ``predictor``'s index CSV.

    Behaviour:
    - Reads the existing CSV. Returns 0 when the CSV is missing (the daily
      batch must have run first); the caller decides whether to bootstrap
      via ``build_index_day --mode daily``.
    - For each requested race, recomputes the predictor's features
      (展示・気象 含む) and constructs a fresh ``状態=realtime`` row.
    - Adds the realtime row alongside any existing ``状態=daily`` row for
      the same race (the daily row is **never overwritten or removed**).
    - If a ``状態=realtime`` row already exists for the race (e.g. a 2nd
      preview-realtime cycle), it is replaced with the freshly computed
      values (in-place upsert; row count stays the same).
    - Other races on the day are left byte-equivalent.
    - Atomically rewrites the CSV with rows sorted (race_code asc, daily
      before realtime).

    Returns: the number of realtime rows upserted (= len(race_codes) when
    every requested race has feature data).
    """
    if not race_codes:
        return 0
    csv_path = index_csv_path(repo, day, predictor)
    if not csv_path.exists():
        # No daily-batch CSV to update; nothing to do.
        return 0

    # All columns read as object dtype so subsequent concat preserves
    # mixed-type cells without pandas' strict "Invalid value 'X' for
    # dtype 'str'" guard.
    existing = pd.read_csv(csv_path, dtype=object)
    existing["レースコード"] = existing["レースコード"].astype(str)
    existing = _normalize_state_column(existing)

    # Pad missing columns and lock column order to the canonical schema.
    cols = index_columns(predictor)
    for c in cols:
        if c not in existing.columns:
            existing[c] = ""
    existing = existing[cols]

    # Compute fresh features for the whole day; cheap and reuses
    # build_index_day's code path. Filter down to the requested races only.
    long_df = compute_features_for_day(repo, day)
    if long_df.empty:
        return 0
    weights_path = find_weights_file(repo, predictor, day)
    weights = load_weights(weights_path, predictor) if weights_path else {}

    race_codes_str = {str(c) for c in race_codes}
    long_df = long_df[long_df["レースコード"].astype(str).isin(race_codes_str)]
    if long_df.empty:
        return 0

    new_realtime: dict[str, dict] = {}
    for code, grp in long_df.sort_values(["レースコード", "枠番"]).groupby(
        "レースコード", sort=False
    ):
        new_realtime[str(code)] = _build_one_race_row(
            code=code, meta_row=grp.iloc[0], boats=grp,
            weights=weights, state=STATE_REALTIME, predictor=predictor,
            skip_preview=False,
        )

    if not new_realtime:
        return 0

    # Partition existing rows: keep daily (and any other non-realtime states
    # that future versions may add) untouched; drop realtime rows that we're
    # about to upsert; keep realtime rows for races we're not touching.
    is_realtime = existing["状態"] == STATE_REALTIME
    daily_or_other = existing[~is_realtime]
    realtime_kept = existing[
        is_realtime & ~existing["レースコード"].isin(new_realtime.keys())
    ]
    realtime_new = pd.DataFrame(
        [new_realtime[c] for c in sorted(new_realtime.keys())],
        columns=cols,
    )

    result = pd.concat(
        [daily_or_other, realtime_kept, realtime_new],
        ignore_index=True,
    )
    result = _sort_index_rows(result)

    atomic_write_csv(result, csv_path)
    return len(new_realtime)


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────
def _resolve_predictors(
    args_predictor: str | None, all_active: bool,
) -> list[PredictorSpec]:
    """``--predictor`` / ``--all-active`` の組合せから対象予想者を解決する。"""
    if all_active:
        if args_predictor:
            sys.exit("--predictor and --all-active are mutually exclusive")
        actives = active_predictors()
        if not actives:
            sys.exit("No active predictors in registry; nothing to do.")
        return list(actives)
    return [predictor_by_id(args_predictor or DEFAULT_PREDICTOR_ID)]


def _run_one_predictor(
    repo: Path, day: dt.date, predictor: PredictorSpec,
    *, mode: str, out: str | None, update_races: list[str] | None,
    force: bool,
) -> None:
    if update_races:
        n = update_index_for_races(repo, day, update_races, predictor)
        print(
            f"[{predictor.predictor_id}] upserted {n} realtime rows in "
            f"{index_csv_path(repo, day, predictor)}"
        )
        return

    out_path = Path(out) if out else index_csv_path(repo, day, predictor)

    # Safety: --mode realtime full rebuild would clobber the morning batch's
    # 状態=daily rows. Require an explicit --force to proceed.
    if mode == STATE_REALTIME and out_path.exists() and not force:
        try:
            preview = pd.read_csv(out_path, dtype=object, usecols=["状態"])
        except (ValueError, KeyError):
            preview = None
        has_daily = (
            preview is not None
            and (preview["状態"].fillna(STATE_DAILY).replace("", STATE_DAILY) == STATE_DAILY).any()
        )
        if has_daily:
            sys.exit(
                f"refusing to overwrite {out_path}: it contains 状態=daily rows. "
                f"Re-run with --force if you really want a full realtime rebuild "
                f"(this discards the morning batch's daily evaluation)."
            )

    df, weights_path = build_index_day(repo, day, predictor, mode=mode)
    atomic_write_csv(df, out_path)

    weight_msg = (f" (weights: {weights_path.name})" if weights_path
                  else " (no weights file → 強さpt is NaN)")
    print(
        f"[{predictor.predictor_id}] wrote {len(df)} rows → {out_path}  "
        f"[mode={mode}]{weight_msg}"
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(Path(__file__).parent.parent))
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--mode", choices=[STATE_DAILY, STATE_REALTIME],
                   default=STATE_REALTIME,
                   help="daily=展示/気象は50固定で日次バッチ用CSV、"
                        "realtime=全要素を計算(過去日backfillもこちら)")
    p.add_argument("--out", default=None,
                   help="Override output path. Only valid with a single "
                        "--predictor (not with --all-active).")
    p.add_argument("--update-races", default=None,
                   help="Comma-separated レースコード list. If set, only those "
                        "rows are upserted as 状態=realtime; existing 状態=daily "
                        "rows for the same races are preserved. Requires the "
                        "CSV to exist.")
    p.add_argument("--force", action="store_true",
                   help="Allow --mode realtime to overwrite an existing CSV "
                        "that contains 状態=daily rows. Without --force, a "
                        "full realtime rebuild that would discard the morning "
                        "batch's daily evaluation is refused.")
    p.add_argument("--predictor", default=None,
                   help=f"Predictor ID to build (default: {DEFAULT_PREDICTOR_ID}). "
                        f"Mutually exclusive with --all-active.")
    p.add_argument("--all-active", action="store_true",
                   help="Loop over every active predictor in the registry. "
                        "Mutually exclusive with --predictor / --out.")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    day = dt.date.fromisoformat(args.date)

    predictors = _resolve_predictors(args.predictor, args.all_active)
    if args.out and len(predictors) > 1:
        sys.exit("--out cannot be combined with --all-active")

    update_races = (
        [c.strip() for c in args.update_races.split(",") if c.strip()]
        if args.update_races else None
    )

    for predictor in predictors:
        _run_one_predictor(
            repo, day, predictor,
            mode=args.mode, out=args.out, update_races=update_races,
            force=args.force,
        )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fit per-stadium 5-component weights from the last 6 months of data.

For each stadium:
    1. Compute (waku, racer, motor, exhibit, weather) raw pts on-the-fly
       for every race in [target_month - 6mo, target_month - 1day].
    2. Join with results to get the actual finish (1..6) per boat.
    3. Standardize each feature by stadium-wide (μ, σ) over that window.
    4. Solve the constrained optimization
            minimize  ‖ Z·w  −  y_std ‖²
            subject to  w ≥ 0,  Σ w = 1
       where y_std = standardized (7 − finish).

Output: data/estimate/stadium/index_weights/YYYY-MM.csv (one row per stadium).

Usage:
    python scripts/build_weights.py --month 2026-05
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

# Local import — reuse build_index feature builders
sys.path.insert(0, str(Path(__file__).parent))
from boatrace.index_features import (  # noqa: E402
    COMPONENT_KEYS, STADIUM_NAMES, compute_features_for_day,
)


# ─────────────────────────────────────────────────────────────────────
# 1. Pull historical results 着順 per (race, 枠番)
# ─────────────────────────────────────────────────────────────────────
def load_results_for_day(repo: Path, day: dt.date) -> pd.DataFrame:
    """Long-format (レースコード, 枠番, 着順) for one day."""
    p = repo / "data" / "results" / "daily" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    if not p.exists():
        return pd.DataFrame(columns=["レースコード", "枠番", "着順"])
    df = pd.read_csv(p, dtype=str)
    rows = []
    for _, r in df.iterrows():
        code = r["レースコード"]
        for rank in range(1, 7):
            try:
                waku = int(float(r[f"{rank}着_艇番"]))
            except (ValueError, TypeError, KeyError):
                continue
            if 1 <= waku <= 6:
                rows.append({"レースコード": code, "枠番": waku, "着順": rank})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────
# 2. Build the long training table for the date window
# ─────────────────────────────────────────────────────────────────────
def iter_dates(start: dt.date, end: dt.date):
    d = start
    while d <= end:
        yield d
        d += dt.timedelta(days=1)


def build_training_table(repo: Path, start: dt.date, end: dt.date) -> pd.DataFrame:
    parts = []
    n_days = (end - start).days + 1
    for i, day in enumerate(iter_dates(start, end)):
        feat = compute_features_for_day(repo, day)
        if feat.empty:
            continue
        res = load_results_for_day(repo, day)
        if res.empty:
            continue
        merged = feat.merge(res, on=["レースコード", "枠番"], how="inner")
        parts.append(merged)
        if (i + 1) % 30 == 0 or i + 1 == n_days:
            n_rows = sum(len(p) for p in parts)
            print(f"  loaded {i+1}/{n_days} days, {n_rows:,} boat-rows so far",
                  file=sys.stderr)
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────
# 3. Per-stadium fit: standardize then constrained NNLS
# ─────────────────────────────────────────────────────────────────────
def fit_one(df_st: pd.DataFrame) -> dict:
    """df_st has columns: waku, racer, motor, exhibit, weather, 着順.

    Returns dict with: mu, sigma per feature, w per feature, mu_y, sigma_y,
    n_samples, mse, r2, fallback (bool).

    Per-column μ, σ are computed using ONLY non-NaN rows of that column —
    this lets motor (whose data window is shorter) keep its scale even
    when the SLSQP fit can only use rows where all 5 features are present.
    """
    # Per-column statistics — keep each feature's μ/σ on its own valid window.
    # motor especially has a much shorter history (motor_stats backfill is not
    # possible), so other features' μ/σ should not be limited by motor's NaN rows.
    mus: dict[str, float] = {}
    sigmas: dict[str, float] = {}
    for k in COMPONENT_KEYS:
        col = df_st[k].dropna()
        if len(col) > 0:
            mus[k] = float(col.mean())
            sigmas[k] = max(float(col.std(ddof=0)), 1e-9)
        else:
            mus[k] = 0.0
            sigmas[k] = 1.0

    # For SLSQP fitting, require at minimum 着順 + the 4 features that backfill
    # the entire 6-month window. motor only goes back a handful of days; rather
    # than dropping those rows, impute its missing values with μ_motor (Z=0).
    needed = ["waku", "racer", "exhibit", "weather", "着順"]
    sub = df_st.dropna(subset=needed).copy()
    sub["着順"] = sub["着順"].astype(int)
    sub = sub[(sub["着順"] >= 1) & (sub["着順"] <= 6)]
    sub["motor"] = sub["motor"].fillna(mus["motor"])  # impute → standardised z=0
    n = len(sub)

    if n < 60:
        # Insufficient joint data — fall back to equal weights but keep
        # the per-column μ/σ we already computed
        w = {k: 0.2 for k in COMPONENT_KEYS}
        return dict(mu=mus, sigma=sigmas, w=w, mu_y=3.5, sigma_y=1.0,
                    n_samples=n, mse=float("nan"), r2=float("nan"), fallback=True)

    # Use the per-column μ, σ we already computed (above)
    Z = np.column_stack([
        (sub[k].values - mus[k]) / sigmas[k] for k in COMPONENT_KEYS
    ])

    # Target: 7 - 着順 (higher = better), then standardize
    y_raw = (7 - sub["着順"].values).astype(float)
    mu_y = float(y_raw.mean())
    sigma_y = max(float(y_raw.std(ddof=0)), 1e-9)
    y = (y_raw - mu_y) / sigma_y

    # Constrained optimization: w ≥ 0, sum(w) = 1
    def objective(w):
        return float(np.mean((Z @ w - y) ** 2))

    def grad(w):
        return 2.0 * Z.T @ (Z @ w - y) / len(y)

    constraints = ({"type": "eq", "fun": lambda w: float(np.sum(w) - 1.0)},)
    bounds = [(0.0, 1.0)] * len(COMPONENT_KEYS)
    w0 = np.full(len(COMPONENT_KEYS), 1.0 / len(COMPONENT_KEYS))

    res = minimize(objective, w0, jac=grad, method="SLSQP",
                   bounds=bounds, constraints=constraints,
                   options={"maxiter": 200, "ftol": 1e-9})
    w_arr = np.clip(res.x, 0.0, None)
    s = w_arr.sum()
    w_arr = w_arr / s if s > 0 else np.full_like(w_arr, 1.0 / len(COMPONENT_KEYS))

    # Metrics on standardized scale
    pred = Z @ w_arr
    mse = float(np.mean((pred - y) ** 2))
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    w = {k: float(v) for k, v in zip(COMPONENT_KEYS, w_arr)}
    return dict(mu=mus, sigma=sigmas, w=w, mu_y=mu_y, sigma_y=sigma_y,
                n_samples=n, mse=mse, r2=r2, fallback=False)


# ─────────────────────────────────────────────────────────────────────
# 4. Output schema
# ─────────────────────────────────────────────────────────────────────
def _flatten_row(stadium: str, fit: dict) -> dict:
    row = {"stadium": stadium, "n_samples": fit["n_samples"]}
    for k in COMPONENT_KEYS:
        row[f"mu_{k}"] = round(fit["mu"][k], 6)
        row[f"sigma_{k}"] = round(fit["sigma"][k], 6)
    for k in COMPONENT_KEYS:
        row[f"w_{k}"] = round(fit["w"][k], 6)
    row["mu_y"] = round(fit["mu_y"], 6)
    row["sigma_y"] = round(fit["sigma_y"], 6)
    row["mse"] = round(fit["mse"], 6) if not np.isnan(fit["mse"]) else float("nan")
    row["r2"] = round(fit["r2"], 6) if not np.isnan(fit["r2"]) else float("nan")
    row["fallback"] = int(fit["fallback"])
    return row


def save_weights(path: Path, results: dict):
    rows = []
    # Order by canonical stadium code
    for code, name in STADIUM_NAMES.items():
        if name in results:
            rows.append(_flatten_row(name, results[name]))
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


# ─────────────────────────────────────────────────────────────────────
# 5. CLI
# ─────────────────────────────────────────────────────────────────────
def parse_month(s: str) -> dt.date:
    """Accept YYYY-MM and return the first day of that month."""
    parts = s.split("-")
    if len(parts) != 2:
        raise ValueError(f"Bad month: {s}")
    return dt.date(int(parts[0]), int(parts[1]), 1)


def first_of_next_month(d: dt.date) -> dt.date:
    if d.month == 12:
        return dt.date(d.year + 1, 1, 1)
    return dt.date(d.year, d.month + 1, 1)


def six_months_before(d: dt.date) -> dt.date:
    y, m = d.year, d.month - 6
    while m <= 0:
        y -= 1
        m += 12
    return dt.date(y, m, 1)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(Path(__file__).parent.parent))
    p.add_argument("--month", required=True, help="Target month YYYY-MM. Training "
                                                   "window = [month-6mo, month-1day].")
    p.add_argument("--out", default=None,
                   help="Override output path (default: data/estimate/stadium/index_weights/YYYY-MM.csv)")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    target = parse_month(args.month)
    end = target - dt.timedelta(days=1)            # last day before target month
    start = six_months_before(target)              # 6 months back, first of month

    print(f"▼ Target month: {target:%Y-%m}", file=sys.stderr)
    print(f"  Training window: {start} → {end}", file=sys.stderr)

    print("\n▼ Building training table...", file=sys.stderr)
    df = build_training_table(repo, start, end)
    if df.empty:
        print("No training data; aborting.", file=sys.stderr)
        sys.exit(1)
    print(f"  Total rows: {len(df):,}", file=sys.stderr)

    print("\n▼ Fitting per stadium...", file=sys.stderr)
    results = {}
    for code, name in STADIUM_NAMES.items():
        sub = df[df["レース場コード"] == f"{code:02d}"]
        fit = fit_one(sub)
        results[name] = fit
        tag = " (FALLBACK)" if fit["fallback"] else ""
        ws = "  ".join(f"{k}={fit['w'][k]:.3f}" for k in COMPONENT_KEYS)
        r2_str = "nan" if np.isnan(fit["r2"]) else f"{fit['r2']:.3f}"
        print(f"  {name}: n={fit['n_samples']:>6,} R²={r2_str}  {ws}{tag}",
              file=sys.stderr)

    out_path = (Path(args.out) if args.out
                else repo / "data" / "estimate" / "stadium" / "index_weights"
                / f"{target:%Y-%m}.csv")
    save_weights(out_path, results)
    print(f"\n▼ Saved {len(results)} stadiums → {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

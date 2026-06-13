#!/usr/bin/env python3
"""
Fit per-stadium component weights from the last 6 months of data.

For each (predictor, stadium):
    1. Compute the predictor's raw component pts on-the-fly for every race
       in [target_month - 6mo, target_month - 1day].
    2. Join with results to get the actual finish (1..6) per boat.
    3. Standardize each feature by stadium-wide (μ, σ) over that window.
    4. Solve the constrained optimization
            minimize  ‖ Z·w  −  y_std ‖²
            subject to  w ≥ 0,  Σ w = 1
       where y_std = standardized (7 − finish).

Output: data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv
(one row per stadium, with mu_{k}/sigma_{k}/w_{k} for each k in the
predictor's ``component_keys``).

Usage:
    python scripts/build_weights.py --month 2026-05
    python scripts/build_weights.py --month 2026-05 --predictor v1_basic
    python scripts/build_weights.py --month 2026-05 --all-active
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize

# Local import — reuse build_index feature builders & predictor registry
sys.path.insert(0, str(Path(__file__).parent))
from boatrace.index_features import (  # noqa: E402
    STADIUM_NAMES, FeatureContext, compute_features_for_day,
)
from boatrace.predictors import (  # noqa: E402
    PredictorSpec, active_predictors, predictor_by_id,
)


DEFAULT_PREDICTOR_ID = "v1_basic"

# 6 ヶ月 backfill が揃わない (= 訓練ウィンドウ全体で値を取れない可能性が高い)
# 成分。fit_one での欠損処理を component_keys に対して動的に分岐する。
# motor は motor_stats CSV が当日開催のある場のみ収録するため期境界以前の
# データが手に入らず、長期 backfill が原理的にできない。
# 新しい "短期成分" を追加する場合はここに追記する。
SHORT_HISTORY_COMPONENTS: frozenset[str] = frozenset({"motor"})


# ─────────────────────────────────────────────────────────────────────
# 1. Pull historical results 着順 per (race, 枠番)
# ─────────────────────────────────────────────────────────────────────
def load_results_for_day(repo: Path, day: dt.date) -> pd.DataFrame:
    """Long-format (レースコード, 枠番, 着順) for one day.

    Reads from ``data/results/realtime/`` (race.boatcast.jp 由来の準リアルタイム
    結果)。
    """
    p = repo / "data" / "results" / "realtime" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
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
    # FeatureContext を window 全体で共有することで、static テーブル・race_cards
    # ・title・session_index・period_starts を amortize する。詳細は
    # docs/design/feature_context_refactor.md を参照。
    ctx = FeatureContext(repo, window_start=start, window_end=end)
    parts = []
    n_days = (end - start).days + 1
    for i, day in enumerate(iter_dates(start, end)):
        feat = compute_features_for_day(repo, day, ctx=ctx)
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
    # FeatureContext のキャッシュ統計を出力(本番デプロイ後の効果検証用)。
    # 期待値(6 ヶ月 window): race_cards≒270, title≒270, runs≒1,350,
    # period_starts≒181。これらから大きく乖離している場合は何かが
    # おかしい(キャッシュ無効化、window 不一致など)。
    print(
        f"  FeatureContext stats: "
        f"race_cards={len(ctx._race_cards_cache)} "
        f"title={len(ctx._title_cache)} "
        f"runs={len(ctx._runs_cache)} "
        f"period_starts={len(ctx._period_starts_cache)}",
        file=sys.stderr,
    )
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


# ─────────────────────────────────────────────────────────────────────
# 3. Per-stadium fit: standardize then constrained NNLS
# ─────────────────────────────────────────────────────────────────────
def fit_one(df_st: pd.DataFrame, component_keys: tuple[str, ...]) -> dict:
    """Per-stadium SLSQP weight fit for an arbitrary component list.

    ``df_st`` must contain raw feature columns for every ``component_keys`` plus
    the ``着順`` outcome column.

    Returns dict with: mu, sigma per feature, w per feature, mu_y, sigma_y,
    n_samples, mse, r2, fallback (bool).

    Per-column μ, σ are computed using ONLY non-NaN rows of that column —
    this lets short-history components (``SHORT_HISTORY_COMPONENTS``) keep
    their scale even when the SLSQP fit can only use rows where all features
    are present.
    """
    n_components = len(component_keys)
    fallback_weight = 1.0 / n_components

    # Per-column statistics — keep each feature's μ/σ on its own valid window.
    # Short-history components (e.g. motor: motor_stats only backfills a few
    # days) would otherwise truncate every other feature's training window.
    mus: dict[str, float] = {}
    sigmas: dict[str, float] = {}
    for k in component_keys:
        col = df_st[k].dropna() if k in df_st.columns else pd.Series(dtype=float)
        if len(col) > 0:
            mus[k] = float(col.mean())
            sigmas[k] = max(float(col.std(ddof=0)), 1e-9)
        else:
            mus[k] = 0.0
            sigmas[k] = 1.0

    # For SLSQP fitting, require at minimum 着順 + every long-history feature
    # (everything except SHORT_HISTORY_COMPONENTS) to be non-NaN. Short-history
    # features are imputed with their own μ (standardised z=0) so the row
    # contributes neutrally rather than being dropped.
    long_history = [k for k in component_keys if k not in SHORT_HISTORY_COMPONENTS]
    needed = long_history + ["着順"]
    sub = df_st.dropna(subset=needed).copy()
    sub["着順"] = sub["着順"].astype(int)
    sub = sub[(sub["着順"] >= 1) & (sub["着順"] <= 6)]
    for k in component_keys:
        if k in SHORT_HISTORY_COMPONENTS and k in sub.columns:
            sub[k] = sub[k].fillna(mus[k])  # impute → standardised z=0
    n = len(sub)

    if n < 60:
        # Insufficient joint data — fall back to equal weights but keep
        # the per-column μ/σ we already computed
        w = {k: fallback_weight for k in component_keys}
        return dict(mu=mus, sigma=sigmas, w=w, mu_y=3.5, sigma_y=1.0,
                    n_samples=n, mse=float("nan"), r2=float("nan"), fallback=True)

    # Use the per-column μ, σ we already computed (above)
    Z = np.column_stack([
        (sub[k].values - mus[k]) / sigmas[k] for k in component_keys
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
    bounds = [(0.0, 1.0)] * n_components
    w0 = np.full(n_components, fallback_weight)

    res = minimize(objective, w0, jac=grad, method="SLSQP",
                   bounds=bounds, constraints=constraints,
                   options={"maxiter": 200, "ftol": 1e-9})
    w_arr = np.clip(res.x, 0.0, None)
    s = w_arr.sum()
    w_arr = w_arr / s if s > 0 else np.full_like(w_arr, fallback_weight)

    # Metrics on standardized scale
    pred = Z @ w_arr
    mse = float(np.mean((pred - y) ** 2))
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")

    w = {k: float(v) for k, v in zip(component_keys, w_arr)}
    return dict(mu=mus, sigma=sigmas, w=w, mu_y=mu_y, sigma_y=sigma_y,
                n_samples=n, mse=mse, r2=r2, fallback=False)


# ─────────────────────────────────────────────────────────────────────
# 4. Output schema
# ─────────────────────────────────────────────────────────────────────
def _flatten_row(
    stadium: str, fit: dict, component_keys: tuple[str, ...],
) -> dict:
    row: dict = {"stadium": stadium, "n_samples": fit["n_samples"]}
    for k in component_keys:
        row[f"mu_{k}"] = round(fit["mu"][k], 6)
        row[f"sigma_{k}"] = round(fit["sigma"][k], 6)
    for k in component_keys:
        row[f"w_{k}"] = round(fit["w"][k], 6)
    row["mu_y"] = round(fit["mu_y"], 6)
    row["sigma_y"] = round(fit["sigma_y"], 6)
    row["mse"] = round(fit["mse"], 6) if not np.isnan(fit["mse"]) else float("nan")
    row["r2"] = round(fit["r2"], 6) if not np.isnan(fit["r2"]) else float("nan")
    row["fallback"] = int(fit["fallback"])
    return row


def save_weights(
    path: Path, results: dict, component_keys: tuple[str, ...],
) -> None:
    rows = []
    # Order by canonical stadium code
    for code, name in STADIUM_NAMES.items():
        if name in results:
            rows.append(_flatten_row(name, results[name], component_keys))
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


def _fit_predictor(
    repo: Path, training_df: pd.DataFrame, predictor: PredictorSpec,
    target: dt.date, out_override: Path | None,
) -> None:
    """1 予想者ぶんの per-stadium fit & save。"""
    print(
        f"\n▼ Fitting [{predictor.predictor_id}] "
        f"({len(predictor.component_keys)} components)...",
        file=sys.stderr,
    )
    component_keys = predictor.component_keys
    results: dict[str, dict] = {}
    for code, name in STADIUM_NAMES.items():
        sub = training_df[training_df["レース場コード"] == f"{code:02d}"]
        fit = fit_one(sub, component_keys)
        results[name] = fit
        tag = " (FALLBACK)" if fit["fallback"] else ""
        ws = "  ".join(f"{k}={fit['w'][k]:.3f}" for k in component_keys)
        r2_str = "nan" if np.isnan(fit["r2"]) else f"{fit['r2']:.3f}"
        print(f"  {name}: n={fit['n_samples']:>6,} R²={r2_str}  {ws}{tag}",
              file=sys.stderr)

    out_path = out_override or predictor.weights_csv_path(repo, target)
    save_weights(out_path, results, component_keys)
    print(
        f"  Saved {len(results)} stadiums → {out_path}",
        file=sys.stderr,
    )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo-root", default=str(Path(__file__).parent.parent))
    p.add_argument("--month", required=True, help="Target month YYYY-MM. Training "
                                                   "window = [month-6mo, month-1day].")
    p.add_argument("--out", default=None,
                   help="Override output path. Only valid with a single "
                        "--predictor (not with --all-active).")
    p.add_argument("--predictor", default=None,
                   help=f"Predictor ID to fit (default: {DEFAULT_PREDICTOR_ID}). "
                        f"Mutually exclusive with --all-active.")
    p.add_argument("--all-active", action="store_true",
                   help="Fit every active predictor in the registry. "
                        "Mutually exclusive with --predictor / --out.")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    target = parse_month(args.month)
    end = target - dt.timedelta(days=1)            # last day before target month
    start = six_months_before(target)              # 6 months back, first of month

    predictors = _resolve_predictors(args.predictor, args.all_active)
    if args.out and len(predictors) > 1:
        sys.exit("--out cannot be combined with --all-active")

    print(f"▼ Target month: {target:%Y-%m}", file=sys.stderr)
    print(f"  Training window: {start} → {end}", file=sys.stderr)
    print(
        f"  Predictors: {[p.predictor_id for p in predictors]}",
        file=sys.stderr,
    )

    # The training table is feature-agnostic: compute_features_for_day emits
    # every known component column, so a single table feeds all predictors.
    print("\n▼ Building training table (shared across predictors)...",
          file=sys.stderr)
    df = build_training_table(repo, start, end)
    if df.empty:
        print("No training data; aborting.", file=sys.stderr)
        sys.exit(1)
    print(f"  Total rows: {len(df):,}", file=sys.stderr)

    out_override = Path(args.out) if args.out else None
    for predictor in predictors:
        _fit_predictor(repo, df, predictor, target, out_override)


if __name__ == "__main__":
    main()

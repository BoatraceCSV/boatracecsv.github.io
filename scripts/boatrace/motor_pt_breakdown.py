"""モーターpt 素点の内訳 CSV ビルダー。

``data/estimate/motor_pt/{runs,motors,baseline}/YYYY/MM/DD.csv`` の 3 種を組み立てる。
生成 CLI は ``scripts/build_motor_pt_breakdown.py``、スキーマの説明は
``docs/data/motor_pt.md``。

素点 (= ``N枠_モーターpt`` の入力) は

    素点 = n_eff / (n_eff + k) × Σ(w × z) / Σw

で決まり、その計算には

  1. 当場の直近 6 節ぶんの全モーターの走 (``race_cards`` の節間成績 14 スロット)
  2. 開催グレード (``title``)
  3. モーター期起算日 (``motor_stats``)
  4. **全 24 場を横断した** コース補正ベースライン (級別 × グレード分類 × 進入 の μ/σ)

が要る。4 があるので 1 モーターぶんの素点でも全場のコーパスに依存し、当日ぶんの
CSV しか持たない下流 (fun-site) では再現できない。そこで ``build_index.py`` が
すでにメモリ上に持っているこれらを、そのまま明細として書き出すのが本モジュール。

3 ファイルの役割:

* ``runs``     — 1 走 1 行。素点に寄与した走の全明細 (生得点・セル μ/σ・残差 z・減衰重み)
* ``motors``   — 1 モーター 1 行。Σw / n_eff / 素点 などの集計値
* ``baseline`` — コース補正セルの μ/σ/サンプル数 (級別 × グレード分類 × 進入)

対象は **その日の race_cards に出てくるモーターだけ** に絞る (全場全モーターぶんは
下流が使わないため)。``baseline`` は絞り込み前の全場コーパスから算出した値をそのまま
出す (素点計算に使われたのがその値だから)。
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd

from .index_features import (
    SHRINKAGE_PRIOR_K,
    FeatureContext,
    MotorRun,
    motor_ability_breakdown,
    score_motor_run,
)

#: 出力ルート (リポジトリ相対)
OUTPUT_ROOT = "data/estimate/motor_pt"

#: 1 走 1 行。``記録日`` は他の日次 CSV (motor_stats) と揃えたキー列。
RUNS_COLUMNS: list[str] = [
    "記録日", "場コード", "モーター番号",
    "節", "節最終日", "走行日", "日次", "走",
    "級別", "グレード分類", "進入", "着順",
    "生得点", "セルμ", "セルσ", "残差z", "減衰重み",
]

#: 1 モーター 1 行の集計。``素点`` が ``N枠_モーターpt`` の入力そのもの。
MOTORS_COLUMNS: list[str] = [
    "記録日", "場コード", "モーター番号",
    "節数", "走数", "Σw", "Σw2", "n_eff", "加重平均残差", "素点",
]

#: コース補正セル。``進入=0`` は「級別 × グレード分類」のフォールバックセル。
BASELINE_COLUMNS: list[str] = [
    "記録日", "級別", "グレード分類", "進入", "μ", "σ", "サンプル数",
]

#: 小数の丸め桁。下流が同じ値を再計算しても一致する精度を確保しつつ、
#: 1 日 2 万行の runs が肥大しないところ。
_ROUND_CELL = 4      # セル μ/σ (生得点は 0〜125 のスケール)
_ROUND_WEIGHT = 6    # 残差 z / 減衰重み / 集計値


# ---------------------------------------------------------------------------
# 出力パス
# ---------------------------------------------------------------------------
def _ymd_path(repo: Path, kind: str, day: dt.date) -> Path:
    return repo / OUTPUT_ROOT / kind / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"


def runs_path(repo: Path, day: dt.date) -> Path:
    """``data/estimate/motor_pt/runs/YYYY/MM/DD.csv``"""
    return _ymd_path(repo, "runs", day)


def motors_path(repo: Path, day: dt.date) -> Path:
    """``data/estimate/motor_pt/motors/YYYY/MM/DD.csv``"""
    return _ymd_path(repo, "motors", day)


def baseline_path(repo: Path, day: dt.date) -> Path:
    """``data/estimate/motor_pt/baseline/YYYY/MM/DD.csv``"""
    return _ymd_path(repo, "baseline", day)


# ---------------------------------------------------------------------------
# 対象モーターの抽出
# ---------------------------------------------------------------------------
def target_motors(ctx: FeatureContext, day: dt.date) -> list[tuple[str, int]]:
    """その日の race_cards に出てくる ``(場コード, モーター番号)`` を昇順で返す。

    race_cards が無い日は空リスト (下流は 3 ファイルとも出力しない)。
    """
    prog = ctx.race_cards_for(day)
    if prog is None or prog.empty:
        return []
    found: set[tuple[str, int]] = set()
    for _, row in prog.iterrows():
        code = str(row.get("レースコード", ""))
        if len(code) < 10:
            continue
        stadium = code[8:10]
        for n in range(1, 7):
            try:
                motor_num = int(float(row.get(f"艇{n}_モーター番号")))
            except (TypeError, ValueError):
                continue
            found.add((stadium, motor_num))
    return sorted(found)


# ---------------------------------------------------------------------------
# ベースラインのサンプル数
# ---------------------------------------------------------------------------
def _baseline_sample_counts(
    all_runs: list[MotorRun],
    score_table: dict[tuple[str, str], list[int]],
) -> tuple[dict[tuple[str, str, int], int], dict[tuple[str, str], int]]:
    """``compute_lane_baseline`` / ``compute_class_grade_avg`` と同じ母集団の件数。

    μ/σ 自体は index_features が返すものを使い、ここでは「そのセルが何本から
    出来ているか」だけを数え直す (μ/σ の算出側は件数を返さないため)。
    採点対象外の走 (F/L/失/妨/欠/不) は両方の母集団から外れる点も同じ。
    """
    lane_counts: dict[tuple[str, str, int], int] = {}
    cg_counts: dict[tuple[str, str], int] = {}
    for run in all_runs:
        if score_motor_run(score_table, run) is None:
            continue
        bucket = run.grade_bucket if run.racer_class in ("A1", "A2") else "全"
        cg_counts[(run.racer_class, bucket)] = cg_counts.get(
            (run.racer_class, bucket), 0) + 1
        if run.lane == 0:      # lane==0 はコース不明のセンチネル (母集団から除外)
            continue
        key = (run.racer_class, bucket, run.lane)
        lane_counts[key] = lane_counts.get(key, 0) + 1
    return lane_counts, cg_counts


# ---------------------------------------------------------------------------
# フレーム組み立て
# ---------------------------------------------------------------------------
def build_frames(
    repo: Path, day: dt.date, *, ctx: FeatureContext | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """``(runs_df, motors_df, baseline_df)`` を返す。

    ``ctx`` を渡すと ``build_index.py`` と履歴・ベースラインのキャッシュを共有できる
    (同一プロセスで両方走らせる場合に 90 日ぶんの race_cards 再読込を避ける)。
    渡さない場合は当日 1 日ぶんの Context を内部で作る。

    race_cards が無い日は 3 つとも列だけの空フレームを返す。
    """
    if ctx is None:
        ctx = FeatureContext(repo, window_start=day, window_end=day)

    targets = target_motors(ctx, day)
    if not targets:
        return (
            pd.DataFrame(columns=RUNS_COLUMNS),
            pd.DataFrame(columns=MOTORS_COLUMNS),
            pd.DataFrame(columns=BASELINE_COLUMNS),
        )

    history = ctx.motor_history(day)
    score_table = ctx.motor_score_table()
    lane_baseline, class_grade_avg = ctx.lane_baselines(day)
    ymd = day.isoformat()

    run_rows: list[dict] = []
    motor_rows: list[dict] = []
    for stadium, motor_num in targets:
        bd = motor_ability_breakdown(
            history, score_table, stadium, motor_num,
            lane_baseline=lane_baseline, class_grade_avg=class_grade_avg,
            target_day=day,
        )
        sessions = set()
        for c in bd.contributions:
            run = c.run
            sessions.add(c.session_index)
            run_rows.append({
                "記録日": ymd,
                "場コード": stadium,
                "モーター番号": motor_num,
                "節": c.session_index,       # 0 = 直近節
                "節最終日": run.session_end.isoformat(),
                "走行日": run.race_date.isoformat(),
                "日次": run.day_index,
                "走": run.run_index,
                "級別": run.racer_class,
                "グレード分類": run.grade_bucket,
                "進入": run.lane,
                "着順": run.finish,
                "生得点": int(c.raw_score),
                "セルμ": round(c.cell_mu, _ROUND_CELL),
                "セルσ": round(c.cell_sigma, _ROUND_CELL),
                "残差z": round(c.residual, _ROUND_WEIGHT),
                "減衰重み": round(c.weight, _ROUND_WEIGHT),
            })
        # 採点対象の走が 1 本も無いモーターも 1 行出す (素点は空欄)。
        # 「ファイルに無い」と「履歴が無い」を下流が区別できるようにするため。
        has_runs = bool(bd.contributions)
        motor_rows.append({
            "記録日": ymd,
            "場コード": stadium,
            "モーター番号": motor_num,
            "節数": len(sessions),
            "走数": len(bd.contributions),
            "Σw": round(bd.sum_w, _ROUND_WEIGHT) if has_runs else "",
            "Σw2": round(bd.sum_w2, _ROUND_WEIGHT) if has_runs else "",
            "n_eff": round(bd.n_eff, _ROUND_WEIGHT) if has_runs else "",
            "加重平均残差": round(bd.mean_residual, _ROUND_WEIGHT) if has_runs else "",
            "素点": round(bd.raw_pt, _ROUND_WEIGHT) if has_runs else "",
        })

    # ベースラインは絞り込み前の全場コーパス由来 (素点計算に実際に使われた値)。
    all_runs = [r for sess_list in history.values() for sess in sess_list for r in sess]
    lane_counts, cg_counts = _baseline_sample_counts(all_runs, score_table)
    baseline_rows: list[dict] = []
    for (cls, bucket, lane), (mu, sigma) in sorted(lane_baseline.items()):
        baseline_rows.append({
            "記録日": ymd, "級別": cls, "グレード分類": bucket, "進入": lane,
            "μ": round(mu, _ROUND_CELL), "σ": round(sigma, _ROUND_CELL),
            "サンプル数": lane_counts.get((cls, bucket, lane), 0),
        })
    for (cls, bucket), (mu, sigma) in sorted(class_grade_avg.items()):
        baseline_rows.append({
            "記録日": ymd, "級別": cls, "グレード分類": bucket, "進入": 0,
            "μ": round(mu, _ROUND_CELL), "σ": round(sigma, _ROUND_CELL),
            "サンプル数": cg_counts.get((cls, bucket), 0),
        })

    return (
        pd.DataFrame(run_rows, columns=RUNS_COLUMNS),
        pd.DataFrame(motor_rows, columns=MOTORS_COLUMNS),
        pd.DataFrame(baseline_rows, columns=BASELINE_COLUMNS),
    )


# ---------------------------------------------------------------------------
# 再計算による検算 (テスト / --verify 用)
# ---------------------------------------------------------------------------
def recompute_raw_pt(
    runs_df: pd.DataFrame, shrinkage_prior_k: float = SHRINKAGE_PRIOR_K,
) -> dict:
    """runs CSV だけから ``(場コード, モーター番号) → 素点`` を再計算する。

    下流 (fun-site) が runs 行から素点を組み立てる手順そのもの。CSV に丸めた値しか
    無いので Python 内部の float とは末尾数桁がずれる。**厳密一致の検証ではなく、
    列の意味と式が合っているかの回帰テスト用**。
    """
    out: dict[tuple[str, int], float] = {}
    if runs_df.empty:
        return out
    for (stadium, motor_num), grp in runs_df.groupby(["場コード", "モーター番号"]):
        w = grp["減衰重み"].astype(float)
        z = grp["残差z"].astype(float)
        sum_w = float(w.sum())
        if sum_w == 0.0:
            continue
        sum_w2 = float((w * w).sum())
        n_eff = (sum_w * sum_w) / sum_w2
        mean_resid = float((w * z).sum()) / sum_w
        out[(str(stadium), int(motor_num))] = (
            n_eff / (n_eff + shrinkage_prior_k) * mean_resid
        )
    return out


def resolve_cell(
    baseline_df: pd.DataFrame, racer_class: str, bucket: str, lane: int,
) -> tuple[float, float]:
    """baseline CSV から ``cell_stats()`` と同じフォールバック順で (μ, σ) を引く。

    1. ``(級別, グレード分類, 進入)`` 行
    2. ``(級別, グレード分類, 進入=0)`` 行
    3. ``(0.0, 1.0)`` (補正なし)

    baseline CSV にはサンプル数が ``LANE_BASELINE_MIN_SAMPLES`` 未満のセルが
    そもそも入らないため、行の有無をそのまま判定に使える。
    """
    if lane >= 1:
        hit = baseline_df[
            (baseline_df["級別"] == racer_class)
            & (baseline_df["グレード分類"] == bucket)
            & (baseline_df["進入"].astype(int) == lane)
        ]
        if not hit.empty:
            return float(hit.iloc[0]["μ"]), float(hit.iloc[0]["σ"])
    hit = baseline_df[
        (baseline_df["級別"] == racer_class)
        & (baseline_df["グレード分類"] == bucket)
        & (baseline_df["進入"].astype(int) == 0)
    ]
    if not hit.empty:
        return float(hit.iloc[0]["μ"]), float(hit.iloc[0]["σ"])
    return (0.0, 1.0)


__all__ = [
    "OUTPUT_ROOT",
    "RUNS_COLUMNS", "MOTORS_COLUMNS", "BASELINE_COLUMNS",
    "runs_path", "motors_path", "baseline_path",
    "target_motors", "build_frames", "recompute_raw_pt", "resolve_cell",
]

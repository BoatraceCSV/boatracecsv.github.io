"""Unit tests for モーターpt 素点の内訳 (``boatrace.motor_pt_breakdown``).

カバー範囲:

- ``motor_ability_breakdown()`` が ``motor_ability_pt()`` と同じ素点を返すこと
  (内訳 CSV と index CSV のモーターpt が食い違わないことの担保)
- 1 走ぶんの明細 (生得点 / セル μ,σ / 残差 z / 減衰重み) の値
- スキップトークン (F/L/失/妨/欠/不) と機材起因トークン (転/落/沈/エ) の扱い
- ``build_frames()`` が出す 3 フレームの形と、runs だけから素点を再構成できること
- ``resolve_cell()`` のフォールバック階層が ``cell_stats()`` と一致すること

設計: docs/data/motor_pt.md / docs/design/motor_ability_index_v2.md
"""

from __future__ import annotations

import datetime as dt
import math
from pathlib import Path

import pandas as pd
import pytest

from boatrace.index_features import (  # type: ignore[import-not-found]
    DECAY_LAMBDA,
    SHRINKAGE_PRIOR_K,
    STADIUM_NAMES,
    FeatureContext,
    MotorRun,
    cell_stats,
    compute_class_grade_avg,
    compute_lane_baseline,
    motor_ability_breakdown,
    motor_ability_pt,
)
from boatrace.motor_pt_breakdown import (  # type: ignore[import-not-found]
    BASELINE_COLUMNS,
    MOTORS_COLUMNS,
    RUNS_COLUMNS,
    baseline_path,
    build_frames,
    motors_path,
    recompute_raw_pt,
    resolve_cell,
    runs_path,
    target_motors,
)


DAY = dt.date(2026, 5, 10)

SCORE_TABLE = {
    ("B2", "全"): [125, 100, 75, 50, 25, 0],
    ("B1", "全"): [100, 80, 60, 40, 20, 0],
    ("A2", "SG_G1"): [125, 100, 75, 50, 25, 0],
    ("A2", "G2_G3_一般"): [75, 60, 45, 30, 15, 0],
    ("A1", "SG_G1"): [100, 80, 60, 40, 20, 0],
    ("A1", "G2_G3_一般"): [50, 40, 30, 20, 10, 0],
}

SCORE_CSV_TEXT = """級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,100,75,50,25,0
B1,全,100,80,60,40,20,0
A2,SG_G1,125,100,75,50,25,0
A2,G2_G3_一般,75,60,45,30,15,0
A1,SG_G1,100,80,60,40,20,0
A1,G2_G3_一般,50,40,30,20,10,0
"""


def _run(
    finish: str,
    *,
    racer_class: str = "A1",
    grade_bucket: str = "G2_G3_一般",
    lane: int = 1,
    race_date: dt.date = DAY,
    session_end: dt.date = DAY,
    motor_num: int = 1,
    stadium: str = "01",
    day_index: int = 1,
    run_index: int = 1,
) -> MotorRun:
    return MotorRun(
        session_end=session_end, stadium=stadium, motor_num=motor_num,
        grade_bucket=grade_bucket, racer_class=racer_class, finish=finish,
        race_date=race_date, lane=lane,
        day_index=day_index, run_index=run_index,
    )


# ─────────────────────────────────────────────────────────────────────
# 1. motor_ability_pt との一致 (内訳が本体とズレないこと)
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("finishes", [
    ["1", "2", "3"],
    ["6", "6", "5", "4"],
    ["1", "転", "F", "3"],
    ["F", "L", "欠"],          # 全部スキップ → 素点 NaN
])
def test_breakdown_raw_pt_matches_motor_ability_pt(finishes):
    runs = [
        _run(f, race_date=DAY - dt.timedelta(days=i * 3), lane=(i % 6) + 1)
        for i, f in enumerate(finishes)
    ]
    history = {("01", 1): [runs]}
    lb = compute_lane_baseline(runs, SCORE_TABLE, min_samples=1)
    cga = compute_class_grade_avg(runs, SCORE_TABLE, min_samples=1)

    kwargs = dict(lane_baseline=lb, class_grade_avg=cga, target_day=DAY)
    expected = motor_ability_pt(history, SCORE_TABLE, "01", 1, **kwargs)
    got = motor_ability_breakdown(history, SCORE_TABLE, "01", 1, **kwargs).raw_pt

    if math.isnan(expected):
        assert math.isnan(got)
    else:
        assert got == expected      # 累算順序が同一なので厳密一致するはず


def test_breakdown_of_unknown_motor_is_empty_and_nan():
    bd = motor_ability_breakdown({}, SCORE_TABLE, "01", 99)
    assert bd.contributions == []
    assert bd.sum_w == 0.0
    assert math.isnan(bd.raw_pt)


# ─────────────────────────────────────────────────────────────────────
# 2. 1 走ぶんの明細
# ─────────────────────────────────────────────────────────────────────
def test_contribution_fields_follow_the_formula():
    """生得点 / セル μ,σ / 残差 z / 減衰重み がそれぞれ定義どおりであること。"""
    runs = [
        _run("1", lane=1, race_date=DAY),                              # 50pt
        _run("4", lane=1, race_date=DAY - dt.timedelta(days=30)),      # 20pt
    ]
    history = {("01", 1): [runs]}
    lb = compute_lane_baseline(runs, SCORE_TABLE, min_samples=1)
    cga = compute_class_grade_avg(runs, SCORE_TABLE, min_samples=1)
    bd = motor_ability_breakdown(
        history, SCORE_TABLE, "01", 1,
        lane_baseline=lb, class_grade_avg=cga, target_day=DAY,
    )

    assert [c.raw_score for c in bd.contributions] == [50.0, 20.0]
    assert all(c.session_index == 0 for c in bd.contributions)

    mu, sigma = cell_stats(lb, cga, "A1", "G2_G3_一般", 1)
    for c in bd.contributions:
        assert c.cell_mu == mu and c.cell_sigma == sigma
        assert c.residual == pytest.approx((c.raw_score - mu) / sigma)

    assert bd.contributions[0].weight == pytest.approx(1.0)
    assert bd.contributions[1].weight == pytest.approx(math.exp(-DECAY_LAMBDA * 30))

    # 収縮: n_eff / (n_eff + k) × 加重平均残差
    assert bd.raw_pt == pytest.approx(
        bd.n_eff / (bd.n_eff + SHRINKAGE_PRIOR_K) * bd.mean_residual
    )


def test_skip_tokens_are_absent_and_negative_tokens_score_minus_100():
    runs = [_run("1"), _run("F"), _run("欠"), _run("転")]
    history = {("01", 1): [runs]}
    bd = motor_ability_breakdown(history, SCORE_TABLE, "01", 1, target_day=DAY)
    # F / 欠 は分子分母とも計上しない → 明細は 2 本
    assert [c.raw_score for c in bd.contributions] == [50.0, -100.0]
    assert [c.run.finish for c in bd.contributions] == ["1", "転"]


def test_session_index_counts_from_the_most_recent_session():
    newest = [_run("1", session_end=DAY, race_date=DAY)]
    older = [_run("2", session_end=DAY - dt.timedelta(days=20),
                  race_date=DAY - dt.timedelta(days=20))]
    history = {("01", 1): [newest, older]}   # 新→旧
    bd = motor_ability_breakdown(history, SCORE_TABLE, "01", 1, target_day=DAY)
    assert [c.session_index for c in bd.contributions] == [0, 1]


# ─────────────────────────────────────────────────────────────────────
# 3. リポジトリ fixture (build_frames のエンドツーエンド)
# ─────────────────────────────────────────────────────────────────────
def _write_static_tables(repo: Path) -> None:
    p = repo / "data" / "estimate" / "motor_ability_score.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(SCORE_CSV_TEXT, encoding="utf-8")
    (repo / "data" / "estimate" / "motor_ability_score_v4.csv").write_text(
        SCORE_CSV_TEXT, encoding="utf-8")


def _race_card_row(
    day: dt.date, stadium: str, race_round: int, *,
    motors: list[int], finishes: list[str],
) -> dict:
    """6 艇ぶんの race_cards 行。``finishes`` を D1走1..D1走N に流し込む。"""
    code = f"{day:%Y%m%d}{stadium}{race_round:02d}"
    row = {"レースコード": code, "レース回": f"{race_round:02d}R"}
    for n in range(1, 7):
        row[f"艇{n}_モーター番号"] = str(motors[n - 1])
        row[f"艇{n}_級別"] = "A1"
        row[f"艇{n}_枠"] = str(n)
        for d in range(1, 8):
            for s in (1, 2):
                slot = (d - 1) * 2 + (s - 1)
                row[f"艇{n}_節D{d}走{s}_着順"] = (
                    finishes[slot] if slot < len(finishes) else ""
                )
                row[f"艇{n}_節D{d}走{s}_進入"] = str(n) if slot < len(finishes) else ""
                row[f"艇{n}_節D{d}走{s}_枠"] = str(n) if slot < len(finishes) else ""
    return row


def _write_race_cards(
    repo: Path, day: dt.date, stadiums: list[str], *,
    motors: list[int], finishes: list[str],
) -> None:
    p = (repo / "data" / "programs" / "race_cards"
         / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        _race_card_row(day, s, 1, motors=motors, finishes=finishes)
        for s in stadiums
    ]
    pd.DataFrame(rows).to_csv(p, index=False)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """当日 + 過去 2 節ぶんの race_cards を持つ最小リポジトリ。

    節は「連続開催日」で束ねられるので、当日から離れた 2 つの塊を作る。
    当日を含む節は履歴から除外される (``detect_sessions`` が window_end を除く)。
    """
    _write_static_tables(tmp_path)
    stadiums = ["01"]
    motors = [1, 2, 3, 4, 5, 6]
    # 当日 (履歴には入らない)
    _write_race_cards(tmp_path, DAY, stadiums, motors=motors, finishes=["1"])
    # 直近節: DAY-5 .. DAY-3
    for back in (5, 4, 3):
        _write_race_cards(
            tmp_path, DAY - dt.timedelta(days=back), stadiums,
            motors=motors, finishes=["1", "3", "6"],
        )
    # ひとつ前の節: DAY-20 .. DAY-19
    for back in (20, 19):
        _write_race_cards(
            tmp_path, DAY - dt.timedelta(days=back), stadiums,
            motors=motors, finishes=["2", "F", "転"],
        )
    return tmp_path


def test_target_motors_lists_the_days_motors(repo: Path):
    ctx = FeatureContext(repo, window_start=DAY, window_end=DAY)
    assert target_motors(ctx, DAY) == [("01", n) for n in range(1, 7)]


def test_build_frames_shapes_and_columns(repo: Path):
    runs_df, motors_df, baseline_df = build_frames(repo, DAY)

    assert list(runs_df.columns) == RUNS_COLUMNS
    assert list(motors_df.columns) == MOTORS_COLUMNS
    assert list(baseline_df.columns) == BASELINE_COLUMNS

    # 当日出走する 6 基ぶん、必ず 1 行ずつ出る
    assert len(motors_df) == 6
    assert set(motors_df["モーター番号"]) == set(range(1, 7))
    assert (motors_df["記録日"] == DAY.isoformat()).all()

    # F はスキップ、転 は -100 で計上される
    assert "F" not in set(runs_df["着順"])
    assert set(runs_df.loc[runs_df["着順"] == "転", "生得点"]) == {-100}

    # 直近節 (節=0) と前節 (節=1) の 2 節が入る
    assert set(runs_df["節"]) == {0, 1}


def test_build_frames_runs_reproduce_the_raw_pt(repo: Path):
    """runs 行だけから素点を再構成しても motors 行と一致すること。

    下流 (fun-site) が内訳から素点を組み立てる手順そのもの。CSV に丸めた値しか
    無いので厳密一致ではなく丸め誤差内で比較する。
    """
    runs_df, motors_df, _ = build_frames(repo, DAY)
    recomputed = recompute_raw_pt(runs_df)
    assert recomputed          # 空だとテストが素通りしてしまう

    for _, row in motors_df.iterrows():
        key = (row["場コード"], int(row["モーター番号"]))
        assert recomputed[key] == pytest.approx(float(row["素点"]), abs=1e-5)


def test_build_frames_matches_motor_ability_pt(repo: Path):
    """motors 行の素点が ``motor_ability_pt()`` と一致すること。"""
    ctx = FeatureContext(repo, window_start=DAY, window_end=DAY)
    history = ctx.motor_history(DAY)
    table = ctx.motor_score_table()
    lb, cga = ctx.lane_baselines(DAY)

    _, motors_df, _ = build_frames(repo, DAY, ctx=ctx)
    for _, row in motors_df.iterrows():
        expected = motor_ability_pt(
            history, table, row["場コード"], int(row["モーター番号"]),
            lane_baseline=lb, class_grade_avg=cga, target_day=DAY,
        )
        assert float(row["素点"]) == pytest.approx(expected, abs=5e-7)


def test_motor_without_history_gets_a_row_with_blank_raw_pt(tmp_path: Path):
    """履歴ゼロのモーターも motors に 1 行出る (「未収録」との区別のため)。"""
    _write_static_tables(tmp_path)
    _write_race_cards(
        tmp_path, DAY, ["01"], motors=[1, 2, 3, 4, 5, 6], finishes=["1"],
    )  # 当日しか race_cards が無い = 履歴なし

    runs_df, motors_df, baseline_df = build_frames(tmp_path, DAY)
    assert runs_df.empty
    assert baseline_df.empty
    assert len(motors_df) == 6
    assert (motors_df["走数"] == 0).all()
    assert (motors_df["節数"] == 0).all()
    assert (motors_df["素点"] == "").all()


def test_build_frames_without_race_cards_returns_empty_frames(tmp_path: Path):
    _write_static_tables(tmp_path)
    runs_df, motors_df, baseline_df = build_frames(tmp_path, DAY)
    assert runs_df.empty and motors_df.empty and baseline_df.empty
    assert list(runs_df.columns) == RUNS_COLUMNS


# ─────────────────────────────────────────────────────────────────────
# 4. baseline CSV からのセル解決
# ─────────────────────────────────────────────────────────────────────
def test_resolve_cell_matches_cell_stats(repo: Path):
    """baseline CSV 経由で引いた (μ,σ) が ``cell_stats()`` と一致すること。"""
    ctx = FeatureContext(repo, window_start=DAY, window_end=DAY)
    lb, cga = ctx.lane_baselines(DAY)
    _, _, baseline_df = build_frames(repo, DAY, ctx=ctx)

    for cls, bucket, lane in {(c, b, lane)
                              for (c, b, lane) in lb.keys()} | {
                                  (c, b, 0) for (c, b) in cga.keys()}:
        want = cell_stats(lb, cga, cls, bucket, lane)
        got = resolve_cell(baseline_df, cls, bucket, lane)
        assert got == pytest.approx(want, abs=1e-4)


def test_resolve_cell_falls_back_to_class_grade_then_identity():
    baseline_df = pd.DataFrame(
        [
            {"記録日": DAY.isoformat(), "級別": "A1", "グレード分類": "G2_G3_一般",
             "進入": 1, "μ": 40.0, "σ": 12.0, "サンプル数": 30},
            {"記録日": DAY.isoformat(), "級別": "A1", "グレード分類": "G2_G3_一般",
             "進入": 0, "μ": 25.0, "σ": 18.0, "サンプル数": 120},
        ],
        columns=BASELINE_COLUMNS,
    )
    # 1. lane セルあり
    assert resolve_cell(baseline_df, "A1", "G2_G3_一般", 1) == (40.0, 12.0)
    # 2. lane セルなし → 級別 × グレードへ
    assert resolve_cell(baseline_df, "A1", "G2_G3_一般", 4) == (25.0, 18.0)
    # 3. どちらも無い → 補正なし
    assert resolve_cell(baseline_df, "B2", "全", 3) == (0.0, 1.0)


# ─────────────────────────────────────────────────────────────────────
# 5. 出力パス
# ─────────────────────────────────────────────────────────────────────
def test_output_paths(tmp_path: Path):
    assert runs_path(tmp_path, DAY) == (
        tmp_path / "data/estimate/motor_pt/runs/2026/05/10.csv")
    assert motors_path(tmp_path, DAY) == (
        tmp_path / "data/estimate/motor_pt/motors/2026/05/10.csv")
    assert baseline_path(tmp_path, DAY) == (
        tmp_path / "data/estimate/motor_pt/baseline/2026/05/10.csv")


def test_stadium_names_cover_the_fixture_stadium():
    """fixture が使う場コード 01 が registry にあること (前提の明示)。"""
    assert 1 in STADIUM_NAMES

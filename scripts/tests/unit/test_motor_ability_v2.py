"""Unit tests for v2 motor_ability_pt (時間減衰 + コース補正 + ベイズ収縮).

v1 算術等価性、v2 各フィーチャーフラグの個別動作、コース baseline 算出、
時間減衰重み、Kish n_eff、ベイズ収縮を網羅する。

設計書: docs/design/motor_ability_index_v2.md
"""

from __future__ import annotations

import datetime as dt
import math

import pytest

from boatrace import index_features as ifeat  # type: ignore[import-not-found]
from boatrace.index_features import (  # type: ignore[import-not-found]
    DECAY_HALF_LIFE_DAYS,
    DECAY_LAMBDA,
    LANE_BASELINE_SD_FLOOR,
    SHRINKAGE_PRIOR_K,
    MotorRun,
    cell_stats,
    compute_class_grade_avg,
    compute_lane_baseline,
    motor_ability_pt,
    parse_lane,
)


# ─────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────
SCORE_TABLE = {
    ("B2", "全"): [125, 100, 75, 50, 25, 0],
    ("B1", "全"): [100, 80, 60, 40, 20, 0],
    ("A2", "SG_G1"): [125, 100, 75, 50, 25, 0],
    ("A2", "G2_G3_一般"): [75, 60, 45, 30, 15, 0],
    ("A1", "SG_G1"): [100, 80, 60, 40, 20, 0],
    ("A1", "G2_G3_一般"): [50, 40, 30, 20, 10, 0],
}


def _run(
    racer_class: str, grade_bucket: str, finish: str,
    *,
    lane: int = 1,
    race_date: dt.date = dt.date(2026, 5, 10),
    session_end: dt.date = dt.date(2026, 5, 10),
    motor_num: int = 1,
    stadium: str = "01",
) -> MotorRun:
    return MotorRun(
        session_end=session_end, stadium=stadium, motor_num=motor_num,
        grade_bucket=grade_bucket, racer_class=racer_class, finish=finish,
        race_date=race_date, lane=lane,
    )


# ─────────────────────────────────────────────────────────────────────
# parse_lane
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("shinnyu,waku,expected", [
    ("3", "1", 3),       # 進入優先
    ("", "2", 2),        # 進入欠損 → 枠
    (None, "5", 5),
    ("nan", "4", 4),
    ("0", "6", 6),       # 不正進入 → 枠
    ("7", "5", 5),
    ("3.0", "1", 3),     # float 文字列
    ("１", "2", 1),      # 全角
    (None, None, None),
    ("", "", None),
    ("0", "0", None),
])
def test_parse_lane(shinnyu, waku, expected):
    assert parse_lane(shinnyu, waku) == expected


# ─────────────────────────────────────────────────────────────────────
# compute_lane_baseline / compute_class_grade_avg
# ─────────────────────────────────────────────────────────────────────
def test_compute_lane_baseline_basic():
    """同一 (cls, grade, lane) セルに 5 走与えると平均と母集団 SD が返る。"""
    runs = [_run("B2", "全", str(f), lane=1) for f in [1, 2, 3, 4, 5]]
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5,
                                      sd_floor=0.0)
    assert ("B2", "全", 1) in baseline
    mu, sigma = baseline[("B2", "全", 1)]
    # スコア: 125, 100, 75, 50, 25 → 平均 = 75
    assert mu == pytest.approx(75.0)
    # 母集団分散 = ((50)^2 + (25)^2 + 0 + (25)^2 + (50)^2) / 5 = 6250/5 = 1250
    # SD ≈ 35.355
    assert sigma == pytest.approx(math.sqrt(1250.0), abs=1e-4)


def test_compute_lane_baseline_drops_undersampled_cells():
    """min_samples 未満のセルは結果から除外される。"""
    runs = [_run("B2", "全", "1", lane=1)] * 4  # 4 < 5
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5)
    assert baseline == {}


def test_compute_lane_baseline_skip_tokens_not_counted():
    """F/L/失/妨/欠/不 は分母に乗らない。"""
    runs = [
        _run("B2", "全", "1", lane=1),
        _run("B2", "全", "1", lane=1),
        _run("B2", "全", "1", lane=1),
        _run("B2", "全", "1", lane=1),
        _run("B2", "全", "1", lane=1),
        _run("B2", "全", "F", lane=1),  # スキップ
        _run("B2", "全", "欠", lane=1),  # スキップ
    ]
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5)
    mu, _ = baseline[("B2", "全", 1)]
    # 5 走分の 1 着のみ = 125
    assert mu == pytest.approx(125.0)


def test_compute_lane_baseline_negative_tokens_count():
    """転/落/沈/エ は -100 として加算される。"""
    runs = [
        _run("B2", "全", "1", lane=1), _run("B2", "全", "1", lane=1),
        _run("B2", "全", "1", lane=1), _run("B2", "全", "1", lane=1),
        _run("B2", "全", "転", lane=1),
    ]
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5,
                                      sd_floor=0.0)
    mu, _ = baseline[("B2", "全", 1)]
    # 125*4 + (-100) = 400 → 平均 80
    assert mu == pytest.approx(80.0)


def test_compute_lane_baseline_sd_floor_applied():
    """SD < sd_floor のセルは sd_floor に丸められる。"""
    runs = [_run("B2", "全", "1", lane=1)] * 6  # 全て同じスコア → SD=0
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5,
                                      sd_floor=10.0)
    _, sigma = baseline[("B2", "全", 1)]
    assert sigma == 10.0


def test_compute_lane_baseline_excludes_lane_zero():
    """lane=0(センチネル)は集計対象外。"""
    runs = [_run("B2", "全", "1", lane=0)] * 10
    baseline = compute_lane_baseline(runs, SCORE_TABLE, min_samples=5)
    assert baseline == {}


def test_compute_class_grade_avg_pools_across_lanes():
    """class_grade_avg は lane を畳んで集計する。"""
    runs = (
        [_run("B2", "全", "1", lane=1)] * 3 +
        [_run("B2", "全", "6", lane=6)] * 3
    )
    cg_avg = compute_class_grade_avg(runs, SCORE_TABLE, min_samples=5,
                                      sd_floor=0.0)
    mu, _ = cg_avg[("B2", "全")]
    # (125*3 + 0*3) / 6 = 62.5
    assert mu == pytest.approx(62.5)


# ─────────────────────────────────────────────────────────────────────
# cell_stats フォールバック階層
# ─────────────────────────────────────────────────────────────────────
def test_cell_stats_lane_priority():
    lane_bl = {("A1", "SG_G1", 3): (40.0, 25.0)}
    cg_avg = {("A1", "SG_G1"): (50.0, 30.0)}
    assert cell_stats(lane_bl, cg_avg, "A1", "SG_G1", 3) == (40.0, 25.0)


def test_cell_stats_falls_back_to_class_grade():
    lane_bl = {}
    cg_avg = {("A1", "SG_G1"): (50.0, 30.0)}
    assert cell_stats(lane_bl, cg_avg, "A1", "SG_G1", 3) == (50.0, 30.0)


def test_cell_stats_falls_back_to_identity():
    """両方無ければ (0.0, 1.0)(コース補正なし、スケール変更なし)。"""
    assert cell_stats({}, {}, "A1", "SG_G1", 3) == (0.0, 1.0)


def test_cell_stats_lane_zero_skips_lane_lookup():
    lane_bl = {("A1", "SG_G1", 0): (40.0, 25.0)}  # 偶発的なキー
    cg_avg = {("A1", "SG_G1"): (50.0, 30.0)}
    # lane=0 は lane lookup スキップして class_grade_avg にフォールバック
    assert cell_stats(lane_bl, cg_avg, "A1", "SG_G1", 0) == (50.0, 30.0)


# ─────────────────────────────────────────────────────────────────────
# 時間減衰の重み
# ─────────────────────────────────────────────────────────────────────
def test_decay_weight_at_half_life():
    assert math.exp(-DECAY_LAMBDA * 60) == pytest.approx(0.5)


def test_decay_weight_at_zero_days():
    assert math.exp(-DECAY_LAMBDA * 0) == 1.0


def test_decay_weight_at_two_half_lives():
    assert math.exp(-DECAY_LAMBDA * 120) == pytest.approx(0.25)


def test_decay_half_life_constant():
    assert DECAY_HALF_LIFE_DAYS == 60.0


# ─────────────────────────────────────────────────────────────────────
# v1 算術等価性(段階リリース基盤)
# ─────────────────────────────────────────────────────────────────────
def test_v1_arithmetic_equivalence(monkeypatch):
    """全フラグ OFF + N=5 で motor_ability_pt が v1 と同じ単純平均を返す。"""
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)
    history = {("01", 1): [[
        _run("B2", "全", "1"), _run("B2", "全", "2"),
        _run("B2", "全", "3"), _run("B2", "全", "転"),
    ]]}
    val = motor_ability_pt(history, SCORE_TABLE, "01", 1)
    # v1: (125 + 100 + 75 - 100) / 4 = 50
    assert val == pytest.approx(50.0)


# ─────────────────────────────────────────────────────────────────────
# フラグ個別 ON 検証
# ─────────────────────────────────────────────────────────────────────
def test_only_shrinkage_on(monkeypatch):
    """SHRINKAGE のみ ON → 単純平均生得点を n/(n+k) 倍。

    4 走分の単純平均 = 50。n_eff=4, k=10 → 50 × 4/14 ≈ 14.29
    """
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", True)
    history = {("01", 1): [[
        _run("B2", "全", "1"), _run("B2", "全", "2"),
        _run("B2", "全", "3"), _run("B2", "全", "転"),
    ]]}
    val = motor_ability_pt(history, SCORE_TABLE, "01", 1)
    assert val == pytest.approx(50.0 * 4 / (4 + SHRINKAGE_PRIOR_K))


def test_only_decay_on(monkeypatch):
    """DECAY のみ ON → 加重平均生得点。"""
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", True)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)
    # 2 走: 直近(target_day)で 100 点 + 60 日前(半減期)で 0 点
    target = dt.date(2026, 5, 1)
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "1", race_date=target),                # raw=50, w=1
        _run("A1", "G2_G3_一般", "6",
             race_date=target - dt.timedelta(days=60)),                  # raw=0, w=0.5
    ]]}
    val = motor_ability_pt(history, SCORE_TABLE, "01", 1, target_day=target)
    # 加重平均 = (1×50 + 0.5×0) / (1 + 0.5) = 50/1.5 ≈ 33.33
    assert val == pytest.approx(50.0 / 1.5)


def test_only_lane_correction_on(monkeypatch):
    """LANE のみ ON → 単純平均 z 残差(等加重、収縮なし)。"""
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", True)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "1", lane=1),
        _run("A1", "G2_G3_一般", "1", lane=1),
    ]]}
    # 手組みの baseline: (μ=30, σ=20) → residual = (50 - 30) / 20 = 1.0
    lane_bl = {("A1", "G2_G3_一般", 1): (30.0, 20.0)}
    cg_avg = {("A1", "G2_G3_一般"): (30.0, 20.0)}
    val = motor_ability_pt(
        history, SCORE_TABLE, "01", 1,
        lane_baseline=lane_bl, class_grade_avg=cg_avg,
    )
    assert val == pytest.approx(1.0)


# ─────────────────────────────────────────────────────────────────────
# 統合: 全フラグ ON
# ─────────────────────────────────────────────────────────────────────
def test_full_v2_end_to_end():
    """全フラグ ON。手計算と一致することを検算。

    2 走:
      - 直近 (target_day): A1一般 1コース 1着 = raw 50, lane_bl μ=30 σ=20
        → residual = (50-30)/20 = 1.0, w = 1.0
      - 60 日前: A1一般 1コース 6着 = raw 0
        → residual = (0-30)/20 = -1.5, w = 0.5
    Σw=1.5, Σwr=1×1 + 0.5×(-1.5)=0.25, Σw²=1.25
    mean_resid = 0.25/1.5 ≈ 0.167
    n_eff = 2.25/1.25 = 1.8
    収縮: 1.8/(1.8+10) × 0.167 ≈ 0.0254
    """
    target = dt.date(2026, 5, 1)
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "1", lane=1, race_date=target),
        _run("A1", "G2_G3_一般", "6", lane=1,
             race_date=target - dt.timedelta(days=60)),
    ]]}
    lane_bl = {("A1", "G2_G3_一般", 1): (30.0, 20.0)}
    cg_avg = {("A1", "G2_G3_一般"): (30.0, 20.0)}
    val = motor_ability_pt(
        history, SCORE_TABLE, "01", 1,
        lane_baseline=lane_bl, class_grade_avg=cg_avg, target_day=target,
    )
    # raw: A1一般 1着=50, 6着=0
    sum_w = 1.0 + 0.5
    sum_wr = 1.0 * ((50 - 30) / 20) + 0.5 * ((0 - 30) / 20)
    sum_w2 = 1.0 + 0.25
    mean_resid = sum_wr / sum_w
    n_eff = (sum_w ** 2) / sum_w2
    expected = n_eff / (n_eff + SHRINKAGE_PRIOR_K) * mean_resid
    assert val == pytest.approx(expected)


def test_motor_ability_pt_no_history_returns_nan():
    """履歴ゼロは flag に関わらず NaN."""
    assert math.isnan(motor_ability_pt({}, SCORE_TABLE, "01", 1))


def test_motor_ability_pt_only_skip_tokens_returns_nan():
    """全走 F/L 等で集計対象ゼロ → NaN."""
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "F"),
        _run("A1", "G2_G3_一般", "L"),
    ]]}
    assert math.isnan(motor_ability_pt(history, SCORE_TABLE, "01", 1))


# ─────────────────────────────────────────────────────────────────────
# Kish n_eff 検算(関数内ロジックを通じた間接テスト)
# ─────────────────────────────────────────────────────────────────────
def test_n_eff_via_shrinkage(monkeypatch):
    """等加重 10 走 + SHRINKAGE ON で n_eff=10 が効くことを検算。

    全走 1 着、raw=50、residual=raw(LANE OFF)、w=1。
    n_eff = (10)²/10 = 10. mean=50.
    出力 = 10/(10+10) × 50 = 25.
    """
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", True)
    history = {("01", 1): [[_run("A1", "G2_G3_一般", "1") for _ in range(10)]]}
    val = motor_ability_pt(history, SCORE_TABLE, "01", 1)
    assert val == pytest.approx(25.0)


# ─────────────────────────────────────────────────────────────────────
# 時計巻き戻し: target_day < race_date でも days_ago=0 で動く
# ─────────────────────────────────────────────────────────────────────
def test_target_day_before_race_date_uses_zero_decay(monkeypatch):
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", True)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "1",
             race_date=dt.date(2026, 6, 1)),
    ]]}
    val = motor_ability_pt(history, SCORE_TABLE, "01", 1,
                             target_day=dt.date(2026, 5, 1))
    # days_ago は max(0, …) で 0 → w=1, mean=50
    assert val == pytest.approx(50.0)


# ─────────────────────────────────────────────────────────────────────
# 定数値の sanity check(回帰防止)
# ─────────────────────────────────────────────────────────────────────
def test_constants_match_design_doc():
    assert ifeat.DECAY_HALF_LIFE_DAYS == 60.0
    assert ifeat.SHRINKAGE_PRIOR_K == 10.0
    assert ifeat.SHRINKAGE_PRIOR_MEAN == 0.0
    assert ifeat.LANE_BASELINE_MIN_SAMPLES == 5
    assert ifeat.LANE_BASELINE_SD_FLOOR == 10.0
    assert ifeat.MOTOR_HISTORY_SESSIONS == 6
    assert ifeat.MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS == 10
    assert ifeat.ENABLE_DECAY is True
    assert ifeat.ENABLE_LANE_CORRECTION is True
    assert ifeat.ENABLE_SHRINKAGE is True

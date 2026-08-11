"""Unit tests for 選手別 推定ST (racer_st)。

構成: 推定ST = shrunk_EWMA(実測ST履歴, 半減期30日, k=10) + コース補正 + F本数補正
経緯: notebooks/st_estimation/phase2_report.md
設計: docs/design/st_estimation.md
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from boatrace.racer_st import (  # type: ignore[import-not-found]
    CLASS_PRIOR,
    COURSE_OFFSET,
    DECAY_PER_DAY,
    F_OFFSET,
    GLOBAL_PRIOR,
    HALF_LIFE_DAYS,
    K_PRIOR,
    K_SIGMA,
    Q75_K,
    SD_PRIOR,
    SIGMA_BASE,
    RacerStState,
    advance_state,
    build_day_estimates,
    estimate_band_for_racer,
    estimate_for_racer,
    load_state,
    save_state,
)


# ---------------------------------------------------------------------------
# RacerStState (EWMA 状態)
# ---------------------------------------------------------------------------
def test_decay_half_life():
    """半減期日数ぶん進めると重みがちょうど半分になる。"""
    state = RacerStState(base_day=dt.date(2026, 1, 1), racers={100: (0.2, 0.04, 1.0)})
    state.decay_to(dt.date(2026, 1, 1) + dt.timedelta(days=int(HALF_LIFE_DAYS)))
    ws, ws2, wt = state.racers[100]
    assert ws == pytest.approx(0.1)
    assert ws2 == pytest.approx(0.02)
    assert wt == pytest.approx(0.5)


def test_estimate_base_shrinks_to_prior():
    """履歴なし → prior。履歴が増えるほど実測平均へ寄る。"""
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    assert state.estimate_base(999, prior=0.18) == pytest.approx(0.18)

    # 実測 0.10 を 10 走 (減衰なし) → (1.0 + 10*0.18) / (10 + 10) = 0.14
    for _ in range(10):
        state.add_run(999, 0.10)
    assert state.estimate_base(999, prior=0.18) == pytest.approx(0.14)


def test_decay_to_past_raises():
    state = RacerStState(base_day=dt.date(2026, 1, 10))
    with pytest.raises(ValueError):
        state.decay_to(dt.date(2026, 1, 9))


# ---------------------------------------------------------------------------
# estimate_for_racer (M3 の合成)
# ---------------------------------------------------------------------------
def test_estimate_uses_pub_avg_as_prior():
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    est = estimate_for_racer(state, 1, waku=2, avg_st_pub=0.16, class_grade="A1", flying_count=0)
    assert est == pytest.approx(0.16 + COURSE_OFFSET[2] + F_OFFSET[0])


def test_estimate_falls_back_to_class_prior_when_pub_zero():
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    est = estimate_for_racer(state, 1, waku=6, avg_st_pub=0.0, class_grade="B2", flying_count=0)
    assert est == pytest.approx(CLASS_PRIOR["B2"] + COURSE_OFFSET[6] + F_OFFSET[0])
    est = estimate_for_racer(state, 1, waku=6, avg_st_pub=0.0, class_grade="??", flying_count=0)
    assert est == pytest.approx(GLOBAL_PRIOR + COURSE_OFFSET[6] + F_OFFSET[0])


def test_estimate_f_count_clipped_at_2():
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    est2 = estimate_for_racer(state, 1, waku=1, avg_st_pub=0.15, class_grade="A1", flying_count=2)
    est5 = estimate_for_racer(state, 1, waku=1, avg_st_pub=0.15, class_grade="A1", flying_count=5)
    assert est2 == pytest.approx(est5)
    assert est2 - estimate_for_racer(
        state, 1, waku=1, avg_st_pub=0.15, class_grade="A1", flying_count=0
    ) == pytest.approx(F_OFFSET[2] - F_OFFSET[0])


# ---------------------------------------------------------------------------
# ファイル入出力 + advance_state (合成リポジトリ)
# ---------------------------------------------------------------------------
RACE_CODE_D1 = "202601100101"
RACE_CODE_D2 = "202601110101"


def _write_cards(repo: Path, day: dt.date, race_code: str, regnos: list[int]) -> None:
    row = {
        "レースコード": race_code,
        "レース日": day.isoformat(),
        "レース場コード": "01",
        "レース回": "01R",
    }
    for b, regno in enumerate(regnos, start=1):
        row[f"艇{b}_登録番号"] = regno
        row[f"艇{b}_級別"] = "A1"
        row[f"艇{b}_全国平均ST"] = "0.15"
        row[f"艇{b}_F本数"] = "0"
    path = repo / "data/programs/race_cards" / f"{day:%Y/%m/%d}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_csv(path, index=False)


def _write_results(
    repo: Path, day: dt.date, race_code: str, sts: list[str], f_marks: list[str]
) -> None:
    row = {"レースコード": race_code, "レース日": day.isoformat()}
    for c in range(1, 7):
        row[f"{c}コース_艇番"] = c  # 枠なり
        row[f"{c}コース_スタートタイミング"] = sts[c - 1]
        row[f"{c}コース_F"] = f_marks[c - 1]
    path = repo / "data/results/realtime" / f"{day:%Y/%m/%d}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_csv(path, index=False)


@pytest.fixture
def synth_repo(tmp_path: Path) -> Path:
    """2 日分の結果 + 3 日目の出走表を持つ合成リポジトリ。"""
    d1, d2, d3 = dt.date(2026, 1, 10), dt.date(2026, 1, 11), dt.date(2026, 1, 12)
    regnos = [100, 200, 300, 400, 500, 600]
    _write_cards(tmp_path, d1, RACE_CODE_D1, regnos)
    _write_cards(tmp_path, d2, RACE_CODE_D2, regnos)
    _write_cards(tmp_path, d3, "202601120101", regnos)
    # d1: 選手100 は 0.10。選手600 は F (負値 + マーク) → 履歴に入らない
    _write_results(tmp_path, d1, RACE_CODE_D1,
                   ["0.10", "0.20", "0.20", "0.20", "0.20", "-0.02"],
                   ["", "", "", "", "", "F"])
    # d2: 選手100 は 0.12
    _write_results(tmp_path, d2, RACE_CODE_D2,
                   ["0.12", "0.20", "0.20", "0.20", "0.20", "0.20"],
                   ["", "", "", "", "", ""])
    return tmp_path


def test_advance_and_estimate(synth_repo: Path):
    state = RacerStState()
    processed, skipped = advance_state(
        synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10)
    )
    assert [d.isoformat() for d in processed] == ["2026-01-10", "2026-01-11"]
    assert skipped == []
    assert state.base_day == dt.date(2026, 1, 11)

    # 選手100: d1=0.10 (1日減衰), d2=0.12 → EWMA = (0.10*a + 0.12) / (a + 1), a=decay^1
    a = DECAY_PER_DAY
    ewma_num = 0.10 * a + 0.12
    ewma_den = a + 1.0
    expected_base = (ewma_num + K_PRIOR * 0.15) / (ewma_den + K_PRIOR)
    est = estimate_for_racer(state, 100, waku=1, avg_st_pub=0.15, class_grade="A1", flying_count=0)
    assert est == pytest.approx(expected_base + COURSE_OFFSET[1] + F_OFFSET[0])

    # 選手600 の d1 は F → 除外され、d2 の 0.20 のみが履歴に入る
    ws, ws2, wt = state.racers[600]
    assert ws == pytest.approx(0.20)
    assert ws2 == pytest.approx(0.04)
    assert wt == pytest.approx(1.0)


def test_f_and_negative_excluded(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 11), start_day=dt.date(2026, 1, 10))
    assert 100 in state.racers
    assert 600 not in state.racers  # F 除外


def test_advance_is_idempotent(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    snapshot = dict(state.racers)
    processed, skipped = advance_state(synth_repo, state, dt.date(2026, 1, 12))
    assert processed == [] and skipped == []
    assert state.racers == snapshot


def test_advance_rejects_past_target(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    with pytest.raises(ValueError):
        advance_state(synth_repo, state, dt.date(2026, 1, 11))


def test_state_roundtrip(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    save_state(synth_repo, state)
    loaded = load_state(synth_repo)
    assert loaded.base_day == state.base_day
    assert set(loaded.racers) == set(state.racers)
    for regno, (ws, ws2, wt) in state.racers.items():
        lws, lws2, lwt = loaded.racers[regno]
        assert lws == pytest.approx(ws)
        assert lws2 == pytest.approx(ws2)
        assert lwt == pytest.approx(wt)


def test_build_day_estimates_columns(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    df = build_day_estimates(synth_repo, state, dt.date(2026, 1, 12))
    assert list(df.columns[:4]) == ["レースコード", "レース日", "レース場コード", "レース回"]
    for b in range(1, 7):
        assert f"{b}枠_登録番号" in df.columns
        assert f"{b}枠_推定ST" in df.columns
    assert len(df) == 1
    # 全艇に数値の推定 ST が入る
    for b in range(1, 7):
        assert float(df.iloc[0][f"{b}枠_推定ST"]) > 0


# ---------------------------------------------------------------------------
# ST 予測区間 (スリット図の帯)
# 経緯: notebooks/slit_sim/report.md §4 / docs/design/slit_sim_plan.md §9
# ---------------------------------------------------------------------------
def test_sigma_multiplier_no_history_is_neutral():
    """履歴 1 走以下は倍率 1.0 (= 全体平均の帯幅)。"""
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    assert state.sigma_multiplier(999) == pytest.approx(1.0)
    state.add_run(999, 0.15)
    assert state.sigma_multiplier(999) == pytest.approx(1.0)


def test_sigma_multiplier_orders_stable_and_unstable():
    """ばらつきの大きい選手ほど倍率が大きく、収縮で 1.0 側へ寄る。"""
    stable = RacerStState(base_day=dt.date(2026, 1, 1))
    wild = RacerStState(base_day=dt.date(2026, 1, 1))
    for i in range(20):
        stable.add_run(1, 0.15 + (0.005 if i % 2 else -0.005))
        wild.add_run(1, 0.15 + (0.08 if i % 2 else -0.08))
    m_stable, m_wild = stable.sigma_multiplier(1), wild.sigma_multiplier(1)
    assert m_stable < 1.0 < m_wild
    # 収縮: 生の sd 比 (0.005/SD_PRIOR, 0.08/SD_PRIOR) よりは 1.0 に近い
    assert m_stable > (0.005 / SD_PRIOR)
    assert m_wild < (0.08 / SD_PRIOR)


def test_sigma_multiplier_shrinkage_strength():
    """実効走数 = K_SIGMA なら 生の sd と SD_PRIOR のちょうど中間になる。"""
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    sd = 0.02
    for i in range(int(K_SIGMA)):
        state.add_run(1, 0.15 + (sd if i % 2 else -sd))
    assert state.sigma_multiplier(1) == pytest.approx((sd + SD_PRIOR) / (2 * SD_PRIOR), rel=1e-6)


def test_estimate_band_scales_with_sigma():
    state = RacerStState(base_day=dt.date(2026, 1, 1))
    assert estimate_band_for_racer(state, 999) == pytest.approx(Q75_K * SIGMA_BASE)
    for i in range(20):
        state.add_run(1, 0.15 + (0.08 if i % 2 else -0.08))
    assert estimate_band_for_racer(state, 1) > Q75_K * SIGMA_BASE


def test_band_columns_bracket_the_estimate(synth_repo: Path):
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    df = build_day_estimates(synth_repo, state, dt.date(2026, 1, 12))
    row = df.iloc[0]
    for b in range(1, 7):
        est = float(row[f"{b}枠_推定ST"])
        p25, p75 = float(row[f"{b}枠_推定ST_p25"]), float(row[f"{b}枠_推定ST_p75"])
        assert p25 < est < p75
        # 推定 ST を中心に対称 (許容は CSV の丸め 4 桁ぶん)
        assert (est - p25) == pytest.approx(p75 - est, abs=2e-4)


def test_load_state_without_ws2_column_falls_back(synth_repo: Path, tmp_path: Path):
    """旧 state.csv (重み付き二乗和 なし) は帯幅一定へ退避し、例外を投げない。"""
    state = RacerStState()
    advance_state(synth_repo, state, dt.date(2026, 1, 12), start_day=dt.date(2026, 1, 10))
    save_state(synth_repo, state)
    path = synth_repo / "data/estimate/racer_st/state.csv"
    legacy = pd.read_csv(path).drop(columns=["重み付き二乗和"])
    legacy.to_csv(path, index=False)

    loaded = load_state(synth_repo)
    assert set(loaded.racers) == set(state.racers)
    for regno in loaded.racers:
        assert loaded.sigma_multiplier(regno) == pytest.approx(1.0)

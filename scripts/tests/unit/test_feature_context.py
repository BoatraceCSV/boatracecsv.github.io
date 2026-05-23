"""Unit tests for ``FeatureContext`` (バッチ呼出し用共有キャッシュ).

Strategy:
- Build a minimal in-tmp_path repository with just enough data files for
  ``compute_features_for_day`` to run end-to-end.
- For each behavior, compare ``ctx``-driven results against the existing
  module-level functions (``load_motor_history``, ``detect_session_end_days``)
  so any divergence shows up immediately.

Reference design: ``docs/design/feature_context_refactor.md`` §3.2 / §6.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from boatrace.index_features import (  # type: ignore[import-not-found]
    FeatureContext,
    MOTOR_HISTORY_LOOKBACK_DAYS,
    MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS,
    PARAM_FEATURES,
    STADIUM_NAMES,
    compute_features_for_day,
    detect_session_end_days,
    load_motor_history,
)


# ─────────────────────────────────────────────────────────────────────
# Repository fixture helpers
# ─────────────────────────────────────────────────────────────────────
SCORE_CSV_TEXT = """級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,100,75,50,25,0
B1,全,100,80,60,40,20,0
A2,SG_G1,125,100,75,50,25,0
A2,G2_G3_一般,75,60,45,30,15,0
A1,SG_G1,100,80,60,40,20,0
A1,G2_G3_一般,50,40,30,20,10,0
"""


def _seasons() -> list[str]:
    return ["春", "夏", "秋", "冬"]


def _write_win_rate(repo: Path) -> None:
    """All 24 stadiums × 4 seasons with uniform 0.5 win rates."""
    p = repo / "data" / "estimate" / "stadium" / "win_rate.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for code in STADIUM_NAMES.keys():
        for season in _seasons():
            rows.append({
                "場コード": f"{code:02d}",
                "季節": season,
                "1コース勝率": "0.5", "2コース勝率": "0.2",
                "3コース勝率": "0.15", "4コース勝率": "0.10",
                "5コース勝率": "0.04", "6コース勝率": "0.01",
            })
    pd.DataFrame(rows).to_csv(p, index=False)


def _write_sui_params(repo: Path) -> None:
    """All 24 stadiums with zero coefs (= weather advantage は常に 0)."""
    p = repo / "data" / "estimate" / "stadium" / "sui_params.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    cols = ["stadium"]
    for c in range(1, 7):
        cols.append(f"base_c{c}")
    for feat in PARAM_FEATURES:
        for c in range(1, 7):
            cols.append(f"{feat}_c{c}")
    rows = []
    for name in STADIUM_NAMES.values():
        row = {"stadium": name}
        for c in range(1, 7):
            row[f"base_c{c}"] = 0.0
        for feat in PARAM_FEATURES:
            for c in range(1, 7):
                row[f"{feat}_c{c}"] = 0.0
        rows.append(row)
    pd.DataFrame(rows, columns=cols).to_csv(p, index=False)


def _write_motor_score(repo: Path) -> None:
    p = repo / "data" / "estimate" / "motor_ability_score.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(SCORE_CSV_TEXT, encoding="utf-8")


def _race_card_row(day: dt.date, stadium: str, race_round: int) -> dict:
    """Minimal race_cards row with 6 boats filled (motor + class only)."""
    code = f"{day:%Y%m%d}{stadium}{race_round:02d}"
    row = {"レースコード": code, "レース回": f"{race_round:02d}R"}
    for n in range(1, 7):
        row[f"艇{n}_モーター番号"] = str(n * 10)
        row[f"艇{n}_級別"] = "A1"
        # 14 slot 着順 (空でも extract_runs_for_session が動くだけにする)
        for d in range(1, 8):
            for s in (1, 2):
                row[f"艇{n}_節D{d}走{s}_着順"] = "1" if (d == 1 and s == 1) else ""
    return row


def _write_race_cards(
    repo: Path, day: dt.date, stadiums: list[str], races_per_stadium: int = 1,
) -> None:
    p = (repo / "data" / "programs" / "race_cards"
         / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for s in stadiums:
        for r in range(1, races_per_stadium + 1):
            rows.append(_race_card_row(day, s, r))
    pd.DataFrame(rows).to_csv(p, index=False)


def _write_results(
    repo: Path, day: dt.date, stadiums: list[str], races_per_stadium: int = 1,
) -> None:
    """data/results/realtime/YYYY/MM/DD.csv with one race per stadium."""
    p = (repo / "data" / "results" / "realtime"
         / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for s in stadiums:
        for r in range(1, races_per_stadium + 1):
            code = f"{day:%Y%m%d}{s}{r:02d}"
            row = {"レースコード": code}
            for rank in range(1, 7):
                row[f"{rank}着_艇番"] = str(rank)
            rows.append(row)
    pd.DataFrame(rows).to_csv(p, index=False)


def _build_repo(
    tmp_path: Path,
    *,
    open_days: dict[dt.date, list[str]],
) -> Path:
    """Build a minimal repository with the listed (day → stadiums) coverage.

    Each open day gets a race_cards entry (1 race per stadium) and a
    matching results/realtime entry. Static tables (win_rate / sui_params /
    motor_ability_score) are written once.

    Returns the repo root (= tmp_path).
    """
    _write_win_rate(tmp_path)
    _write_sui_params(tmp_path)
    _write_motor_score(tmp_path)
    for day, stadiums in open_days.items():
        _write_race_cards(tmp_path, day, stadiums)
        _write_results(tmp_path, day, stadiums)
    return tmp_path


# ─────────────────────────────────────────────────────────────────────
# 1. Constructor validation
# ─────────────────────────────────────────────────────────────────────
def test_constructor_rejects_inverted_window(tmp_path: Path):
    with pytest.raises(ValueError, match="window_end"):
        FeatureContext(
            tmp_path,
            window_start=dt.date(2026, 5, 10),
            window_end=dt.date(2026, 5, 1),
        )


def test_constructor_accepts_single_day_window(tmp_path: Path):
    day = dt.date(2026, 5, 10)
    ctx = FeatureContext(tmp_path, window_start=day, window_end=day)
    assert ctx.window_start == day
    assert ctx.window_end == day


# ─────────────────────────────────────────────────────────────────────
# 2. window-外日付の fail-fast
# ─────────────────────────────────────────────────────────────────────
def test_compute_with_ctx_rejects_day_outside_window(tmp_path: Path):
    repo = _build_repo(tmp_path, open_days={
        dt.date(2026, 5, 10): ["01"],
    })
    ctx = FeatureContext(repo, window_start=dt.date(2026, 5, 5),
                         window_end=dt.date(2026, 5, 15))
    with pytest.raises(ValueError, match="outside ctx window"):
        compute_features_for_day(repo, dt.date(2026, 5, 20), ctx=ctx)
    with pytest.raises(ValueError, match="outside ctx window"):
        compute_features_for_day(repo, dt.date(2026, 5, 1), ctx=ctx)


def test_compute_with_ctx_accepts_boundary_days(tmp_path: Path):
    """window_start ちょうど・window_end ちょうどは valid。"""
    start = dt.date(2026, 5, 5)
    end = dt.date(2026, 5, 15)
    repo = _build_repo(tmp_path, open_days={
        start: ["01"], end: ["01"],
    })
    ctx = FeatureContext(repo, window_start=start, window_end=end)
    # どちらも例外を投げず DataFrame を返す
    df_start = compute_features_for_day(repo, start, ctx=ctx)
    df_end = compute_features_for_day(repo, end, ctx=ctx)
    assert not df_start.empty
    assert not df_end.empty


# ─────────────────────────────────────────────────────────────────────
# 3. session_end_days_for parity with detect_session_end_days
# ─────────────────────────────────────────────────────────────────────
def test_session_end_days_for_matches_detect_session_end_days(tmp_path: Path):
    """連続日・ギャップ・上限切詰めを含む配置で両 API が一致。"""
    # 場01: 5/1-5/3, 5/6-5/8, 5/10 の 3 節
    open_days = {}
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 2), dt.date(2026, 5, 3),
              dt.date(2026, 5, 6), dt.date(2026, 5, 7), dt.date(2026, 5, 8),
              dt.date(2026, 5, 10)]:
        open_days[d] = ["01"]
    repo = _build_repo(tmp_path, open_days=open_days)

    target = dt.date(2026, 5, 15)
    ctx = FeatureContext(repo, window_start=target, window_end=target)

    legacy = detect_session_end_days(repo, "01", target)
    ctx_result = ctx.session_end_days_for(target, "01")
    assert ctx_result == legacy


def test_session_end_days_for_returns_empty_when_no_open_days(tmp_path: Path):
    repo = _build_repo(tmp_path, open_days={})
    target = dt.date(2026, 5, 15)
    ctx = FeatureContext(repo, window_start=target, window_end=target)
    assert ctx.session_end_days_for(target, "01") == []


def test_session_end_days_for_excludes_target_day(tmp_path: Path):
    """target_day 当日の開催は集計対象から外れる(detect と同じ仕様)。"""
    target = dt.date(2026, 5, 10)
    open_days = {
        dt.date(2026, 5, 1): ["01"],
        dt.date(2026, 5, 5): ["01"],
        target: ["01"],
    }
    repo = _build_repo(tmp_path, open_days=open_days)
    ctx = FeatureContext(repo, window_start=target, window_end=target)

    legacy = detect_session_end_days(repo, "01", target)
    ctx_result = ctx.session_end_days_for(target, "01")
    assert ctx_result == legacy
    assert target not in ctx_result


def test_session_end_days_for_caps_at_max_sessions(tmp_path: Path):
    """MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS を超えたら切り詰め(新→旧の順)。"""
    target = dt.date(2026, 5, 30)
    # 上限 + 2 節を作る(7 日おきの 1 日節)。全て 90 日 window 内。
    n_sessions = MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS + 2
    open_days = {
        target - dt.timedelta(days=7 * (i + 1)): ["01"]
        for i in range(n_sessions)
    }
    repo = _build_repo(tmp_path, open_days=open_days)
    ctx = FeatureContext(repo, window_start=target, window_end=target)

    legacy = detect_session_end_days(repo, "01", target)
    ctx_result = ctx.session_end_days_for(target, "01")
    assert ctx_result == legacy
    assert len(ctx_result) == MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS


def test_session_end_days_for_works_across_window(tmp_path: Path):
    """window 内の複数 target_day で各々 legacy と一致する。"""
    # 5/1〜5/30 まで毎日開催(=1 節と扱われる)
    open_days = {
        dt.date(2026, 5, d): ["01"] for d in range(1, 31)
    }
    repo = _build_repo(tmp_path, open_days=open_days)

    start = dt.date(2026, 5, 20)
    end = dt.date(2026, 5, 30)
    ctx = FeatureContext(repo, window_start=start, window_end=end)

    d = start
    while d <= end:
        legacy = detect_session_end_days(repo, "01", d)
        ctx_result = ctx.session_end_days_for(d, "01")
        assert ctx_result == legacy, f"mismatch on {d}: ctx={ctx_result} legacy={legacy}"
        d += dt.timedelta(days=1)


# ─────────────────────────────────────────────────────────────────────
# 4. motor_history parity with load_motor_history
# ─────────────────────────────────────────────────────────────────────
def test_motor_history_matches_load_motor_history(tmp_path: Path):
    """同一 day で ctx.motor_history と load_motor_history が一致する。"""
    # 場01 で 5/1-5/3, 5/8-5/10 の 2 節
    open_days = {}
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 2), dt.date(2026, 5, 3),
              dt.date(2026, 5, 8), dt.date(2026, 5, 9), dt.date(2026, 5, 10)]:
        open_days[d] = ["01"]
    repo = _build_repo(tmp_path, open_days=open_days)

    target = dt.date(2026, 5, 15)
    ctx = FeatureContext(repo, window_start=target, window_end=target)

    legacy = load_motor_history(repo, target)
    ctx_result = ctx.motor_history(target)

    assert set(ctx_result.keys()) == set(legacy.keys())
    for key in legacy:
        # MotorRun is a frozen dataclass; equality compares fields
        assert ctx_result[key] == legacy[key]


# ─────────────────────────────────────────────────────────────────────
# 5. compute_features_for_day parity (ctx あり vs なし)
# ─────────────────────────────────────────────────────────────────────
def _multi_day_repo(tmp_path: Path) -> tuple[Path, dt.date, dt.date]:
    """7 日間連続で 場01/場02 が開催されている repo を作る。"""
    start = dt.date(2026, 5, 10)
    end = dt.date(2026, 5, 16)
    open_days = {}
    d = start
    while d <= end:
        open_days[d] = ["01", "02"]
        d += dt.timedelta(days=1)
    repo = _build_repo(tmp_path, open_days=open_days)
    return repo, start, end


def test_compute_with_ctx_matches_without_ctx_single_day(tmp_path: Path):
    repo, start, _ = _multi_day_repo(tmp_path)
    day = start
    ctx = FeatureContext(repo, window_start=day, window_end=day)

    no_ctx = compute_features_for_day(repo, day)
    with_ctx = compute_features_for_day(repo, day, ctx=ctx)

    # Frame レベルで完全一致を確認
    pd.testing.assert_frame_equal(no_ctx, with_ctx)


def test_compute_with_ctx_matches_without_ctx_across_window(tmp_path: Path):
    """window 内の全日について、ctx あり/なしの結果が一致する。"""
    repo, start, end = _multi_day_repo(tmp_path)
    ctx = FeatureContext(repo, window_start=start, window_end=end)

    d = start
    while d <= end:
        no_ctx = compute_features_for_day(repo, d)
        with_ctx = compute_features_for_day(repo, d, ctx=ctx)
        pd.testing.assert_frame_equal(
            no_ctx, with_ctx,
            obj=f"compute_features_for_day mismatch on {d}",
        )
        d += dt.timedelta(days=1)


# ─────────────────────────────────────────────────────────────────────
# 6. 耐性: race_cards 不在日が window 内にあっても session_index 構築失敗しない
# ─────────────────────────────────────────────────────────────────────
def test_session_index_tolerates_missing_race_cards(tmp_path: Path):
    """window 内の一部の日に race_cards が無くても落ちず、ある日だけ拾う。"""
    open_days = {
        dt.date(2026, 5, 1): ["01"],
        # 5/2, 5/3 は race_cards 不在
        dt.date(2026, 5, 4): ["01"],
    }
    repo = _build_repo(tmp_path, open_days=open_days)

    target = dt.date(2026, 5, 10)
    ctx = FeatureContext(repo, window_start=target, window_end=target)
    # 例外なく取れて、5/1 と 5/4 が拾えていること
    result = ctx.session_end_days_for(target, "01")
    # 5/1 と 5/4 はギャップ(3日)なので別節。新→旧で [5/4, 5/1]
    assert result == [dt.date(2026, 5, 4), dt.date(2026, 5, 1)]


def test_race_cards_for_returns_none_when_missing(tmp_path: Path):
    repo = _build_repo(tmp_path, open_days={})
    ctx = FeatureContext(
        repo,
        window_start=dt.date(2026, 5, 1),
        window_end=dt.date(2026, 5, 1),
    )
    assert ctx.race_cards_for(dt.date(2026, 5, 1)) is None


def test_compute_returns_empty_when_race_cards_missing(tmp_path: Path):
    """race_cards 不在日に compute_features_for_day を呼ぶと空 DataFrame を返す。"""
    repo = _build_repo(tmp_path, open_days={
        dt.date(2026, 5, 10): ["01"],
    })
    target = dt.date(2026, 5, 11)  # 不在日
    ctx = FeatureContext(repo, window_start=target, window_end=target)
    df = compute_features_for_day(repo, target, ctx=ctx)
    assert df.empty


# ─────────────────────────────────────────────────────────────────────
# 7. 静的テーブルの lazy load & cache 共有
# ─────────────────────────────────────────────────────────────────────
def test_static_tables_loaded_once(tmp_path: Path):
    """waku_table / motor_score_table / sui_params は 2 回目以降キャッシュ参照。"""
    repo = _build_repo(tmp_path, open_days={dt.date(2026, 5, 1): ["01"]})
    ctx = FeatureContext(repo, window_start=dt.date(2026, 5, 1),
                         window_end=dt.date(2026, 5, 1))
    first = ctx.waku_table()
    second = ctx.waku_table()
    assert first is second  # same object

    first_ms = ctx.motor_score_table()
    second_ms = ctx.motor_score_table()
    assert first_ms is second_ms

    first_sp = ctx.sui_params()
    second_sp = ctx.sui_params()
    assert first_sp is second_sp


def test_race_cards_for_caches_dataframe_object(tmp_path: Path):
    """同じ day を 2 回呼ぶと同一の DataFrame オブジェクトが返る。"""
    day = dt.date(2026, 5, 10)
    repo = _build_repo(tmp_path, open_days={day: ["01"]})
    ctx = FeatureContext(repo, window_start=day, window_end=day)
    first = ctx.race_cards_for(day)
    second = ctx.race_cards_for(day)
    assert first is second

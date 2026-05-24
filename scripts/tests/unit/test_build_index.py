"""Unit tests for ``scripts/build_index.py`` upsert behaviour.

Focus: ``update_index_for_races`` must add a 状態=realtime row alongside any
existing 状態=daily row for the same race, so the morning batch's daily AI
evaluation survives until the end of the day. See the bug where 当日買い目
disappeared once 直前情報 came in for context.

These tests stub ``compute_features_for_day`` and ``find_weights_file`` so
we don't need the full preview / programs / weights file chain on disk.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

import build_index  # type: ignore[import-not-found]
from build_index import (
    STATE_DAILY,
    STATE_REALTIME,
    _build_one_race_row,
    atomic_write_csv,
    index_columns,
    index_csv_path,
    update_index_for_races,
)
from boatrace.predictors import predictor_by_id  # type: ignore[import-not-found]


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

DAY = dt.date(2026, 5, 10)

# Phase 1 のテストは v1_basic (= 現行 5 成分) に対して動作確認する。
# 新規予想者を追加した際は、各シナリオを registry の active 予想者で
# 反復するように `pytest.fixture(params=...)` 化を検討。
V1 = predictor_by_id("v1_basic")


def _make_long_df(race_codes: list[str]) -> pd.DataFrame:
    """Build the long-format DataFrame ``compute_features_for_day`` returns.

    One row per (race × 枠 1..6) with the 5 raw feature columns. We use
    deterministic non-NaN values so ``_build_one_race_row`` produces a
    valid row even though we feed empty weights (the params=None branch
    fills NaN, which is fine for our row-level assertions).
    """
    rows = []
    for code in race_codes:
        stadium_code2 = code[8:10]
        race_round = str(int(code[10:12]))
        for waku in range(1, 7):
            rows.append({
                "レースコード": code,
                "レース日": DAY.strftime("%Y-%m-%d"),
                "レース場コード": stadium_code2,
                "レース回": race_round,
                "枠番": waku,
                "waku": 0.0,
                "racer": 0.0,
                "motor": 0.0,
                "exhibit": 0.0,
                "weather": 0.0,
            })
    return pd.DataFrame(rows)


def _build_initial_daily_csv(repo: Path, race_codes: list[str]) -> Path:
    """Write a ``状態=daily`` CSV that the morning batch would have produced.

    Schema follows ``index_columns(V1)``. Numeric pt columns are left empty
    (equivalent to NaN) — we only care about meta columns + 状態 here.
    """
    cols = index_columns(V1)
    rows = []
    for code in race_codes:
        row = {c: "" for c in cols}
        row["レースコード"] = code
        row["レース日"] = DAY.strftime("%Y-%m-%d")
        row["レース場コード"] = code[8:10]
        row["レース回"] = str(int(code[10:12]))
        row["状態"] = STATE_DAILY
        rows.append(row)
    df = pd.DataFrame(rows, columns=cols)
    csv_path = index_csv_path(repo, DAY, V1)
    atomic_write_csv(df, csv_path)
    return csv_path


@pytest.fixture
def patched_features(monkeypatch):
    """Stub feature/weights loading so tests don't touch real data files.

    Returns a closure that the test calls with the list of race codes
    that should be present in the long DataFrame.
    """

    def install(race_codes: list[str]) -> None:
        monkeypatch.setattr(
            build_index,
            "compute_features_for_day",
            lambda repo, day: _make_long_df(race_codes),
        )
        monkeypatch.setattr(
            build_index,
            "find_weights_file",
            lambda repo, predictor, day: None,
        )

    return install


def _read_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=object)
    df["レースコード"] = df["レースコード"].astype(str)
    return df


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────


def test_realtime_row_is_added_alongside_daily(tmp_path, patched_features):
    """Daily 行が 1 件ある CSV に realtime 行が **追加** される (合計 2 行)。"""
    code = "202605101508"  # 丸亀 (15) 8R
    csv_path = _build_initial_daily_csv(tmp_path, [code])
    patched_features([code])

    n = update_index_for_races(tmp_path, DAY, [code], V1)
    assert n == 1
    df = _read_csv(csv_path)
    assert len(df) == 2
    states = df.loc[df["レースコード"] == code, "状態"].tolist()
    assert sorted(states) == [STATE_DAILY, STATE_REALTIME]


def test_repeated_realtime_call_upserts_in_place(tmp_path, patched_features):
    """同一レースに対して 2 回呼んでも行数は 2 のまま (realtime のみ upsert)。"""
    code = "202605101508"
    csv_path = _build_initial_daily_csv(tmp_path, [code])
    patched_features([code])

    update_index_for_races(tmp_path, DAY, [code], V1)
    update_index_for_races(tmp_path, DAY, [code], V1)
    update_index_for_races(tmp_path, DAY, [code], V1)

    df = _read_csv(csv_path)
    assert len(df) == 2
    counts = df["状態"].value_counts().to_dict()
    assert counts.get(STATE_DAILY) == 1
    assert counts.get(STATE_REALTIME) == 1


def test_other_races_are_byte_equivalent(tmp_path, patched_features):
    """対象外レースの daily 行は変化しない。"""
    target = "202605101508"
    other = "202605101201"  # 平和島 1R
    csv_path = _build_initial_daily_csv(tmp_path, [target, other])
    before = _read_csv(csv_path)
    other_before = before[before["レースコード"] == other].reset_index(drop=True)

    patched_features([target])  # only target has fresh features
    update_index_for_races(tmp_path, DAY, [target], V1)

    after = _read_csv(csv_path)
    other_after = after[after["レースコード"] == other].reset_index(drop=True)
    pd.testing.assert_frame_equal(other_before, other_after, check_dtype=False)


def test_legacy_csv_without_state_column_is_treated_as_daily(
    tmp_path, patched_features,
):
    """``状態`` 列が存在しない旧 CSV を読み込んでも壊れず daily 扱いで残る。"""
    code = "202605101508"
    # 旧 CSV: 状態列を **意図的に** 落として書き出す
    cols = [c for c in index_columns(V1) if c != "状態"]
    row = {c: "" for c in cols}
    row["レースコード"] = code
    row["レース日"] = DAY.strftime("%Y-%m-%d")
    row["レース場コード"] = code[8:10]
    row["レース回"] = str(int(code[10:12]))
    legacy = pd.DataFrame([row], columns=cols)
    csv_path = index_csv_path(tmp_path, DAY, V1)
    atomic_write_csv(legacy, csv_path)

    patched_features([code])
    n = update_index_for_races(tmp_path, DAY, [code], V1)

    assert n == 1
    df = _read_csv(csv_path)
    assert len(df) == 2
    states = sorted(df.loc[df["レースコード"] == code, "状態"].tolist())
    assert states == [STATE_DAILY, STATE_REALTIME]


def test_missing_csv_returns_zero(tmp_path, patched_features):
    """CSV が存在しない場合は no-op で 0 を返す。"""
    code = "202605101508"
    patched_features([code])
    n = update_index_for_races(tmp_path, DAY, [code], V1)
    assert n == 0
    assert not index_csv_path(tmp_path, DAY, V1).exists()


def test_empty_race_codes_returns_zero(tmp_path, patched_features):
    """レースコードリストが空なら no-op で 0 を返し、CSV にも触らない。"""
    code = "202605101508"
    csv_path = _build_initial_daily_csv(tmp_path, [code])
    before_bytes = csv_path.read_bytes()

    patched_features([code])
    n = update_index_for_races(tmp_path, DAY, [], V1)

    assert n == 0
    assert csv_path.read_bytes() == before_bytes


def test_missing_racer_pt_falls_back_to_30_not_50():
    """選手pt が欠損(新人 / 長期離脱明け)の場合は 30 で補完する。

    通常の欠損補完値は 50(平均扱い)だが、選手ptを 50 扱いすると
    出走履歴が無い新人を平均的な選手として過大評価してしまう。
    回帰防止のためのテスト。
    """
    code = "202605231201"
    boats_rows = []
    for w in range(1, 7):
        boats_rows.append({
            "レースコード": code,
            "レース日": "2026-05-23",
            "レース場コード": "12",
            "レース回": "1",
            "枠番": w,
            "waku":    50.0,
            # 1枠だけ 選手pt 欠損(=新人想定)、他は通常値
            "racer":   float("nan") if w == 1 else 60.0,
            "motor":   50.0,
            # 2枠だけ 展示pt 欠損 → これは従来通り 50 補完
            "exhibit": float("nan") if w == 2 else 50.0,
            "weather": 50.0,
        })
    boats = pd.DataFrame(boats_rows)
    weights = {
        "住之江": {
            "mu":    {"waku": 50, "racer": 50, "motor": 50, "exhibit": 50, "weather": 50},
            "sigma": {"waku": 10, "racer": 10, "motor": 10, "exhibit": 10, "weather": 10},
            "w":     {"waku": 0.2, "racer": 0.2, "motor": 0.2, "exhibit": 0.2, "weather": 0.2},
        },
    }

    row = _build_one_race_row(
        code=code, meta_row=boats.iloc[0], boats=boats,
        weights=weights, state=STATE_REALTIME, predictor=V1,
        skip_preview=False,
    )

    # 1枠は選手pt欠損 → 30で補完される(50ではない)
    assert row["1枠_選手pt"] == 30.0
    # 寄与は w(=0.2) × 30 = 6.0
    assert row["1枠_寄与_選手pt"] == 6.0
    # 2枠の展示pt欠損は従来通り 50 補完(racer以外は変更なし)
    assert row["2枠_展示pt"] == 50.0


def test_output_is_sorted_by_race_code_then_state(tmp_path, patched_features):
    """出力 CSV の行順は race_code 昇順 → 同一 race_code 内では daily, realtime の順。"""
    a = "202605101201"  # 平和島 1R
    b = "202605101508"  # 丸亀 8R
    csv_path = _build_initial_daily_csv(tmp_path, [b, a])  # わざと逆順で投入

    patched_features([a, b])
    update_index_for_races(tmp_path, DAY, [a, b], V1)

    df = _read_csv(csv_path)
    pairs = list(zip(df["レースコード"].tolist(), df["状態"].tolist()))
    assert pairs == [
        (a, STATE_DAILY),
        (a, STATE_REALTIME),
        (b, STATE_DAILY),
        (b, STATE_REALTIME),
    ]

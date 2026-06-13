"""Unit tests for モーター能力指数(`motor_ability_pt` & friends).

Covers:
- スコアテーブル CSV ロード
- グレード分類正規化(SG/PG/G1 → SG_G1、他 → G2_G3_一般)
- 級別×グレード分類の解決(B1/B2 は常に 全)
- トークン正規化(全角 → 半角、未知 → None)
- score_motor_run: 1〜6着 / 転落沈エ / FL失妨欠不 / 未知トークン
- 節境界検出(連続日 → 1 節、ギャップで分割)
- 期境界フィルタ(モーター期起算日より前の節を剪定)
- motor_ability_pt の平均計算と転覆の -100 寄与

これらは I/O を伴わない純粋関数テスト中心。
file I/O が絡む `load_motor_period_starts` / `detect_session_end_days` /
`extract_runs_for_session` / `load_motor_history` は tmp_path で最小限の
CSV を組み立てて統合確認する。
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

from boatrace import index_features as ifeat  # type: ignore[import-not-found]
from boatrace.index_features import (  # type: ignore[import-not-found]
    MOTOR_NEGATIVE_SCORE,
    MOTOR_NEGATIVE_TOKENS,
    MOTOR_SKIP_TOKENS,
    MotorRun,
    detect_session_end_days,
    extract_runs_for_session,
    grade_bucket_for_grade,
    load_motor_history,
    load_motor_period_starts,
    load_motor_score_table,
    motor_ability_pt,
    normalize_finish_token,
    resolve_grade_bucket,
    score_motor_run,
)


# ─────────────────────────────────────────────────────────────────────
# v1 mode fixture — このファイルの既存テストは v1 算術等価モード(全フラグ OFF)
# での挙動を確認する。v2 固有の挙動は test_motor_ability_v2.py 側で検証する。
# ─────────────────────────────────────────────────────────────────────
@pytest.fixture
def v1_mode(monkeypatch):
    """ENABLE_DECAY / ENABLE_LANE_CORRECTION / ENABLE_SHRINKAGE を全 OFF にして
    v1 算術等価モードに切り替える。MOTOR_HISTORY_SESSIONS も v1 値(5)に戻す。
    """
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)
    monkeypatch.setattr(ifeat, "MOTOR_HISTORY_SESSIONS", 5)
    yield


# ─────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────
SCORE_CSV_TEXT = """級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,100,75,50,25,0
B1,全,100,80,60,40,20,0
A2,SG_G1,125,100,75,50,25,0
A2,G2_G3_一般,75,60,45,30,15,0
A1,SG_G1,100,80,60,40,20,0
A1,G2_G3_一般,50,40,30,20,10,0
"""


@pytest.fixture
def repo_with_score(tmp_path: Path) -> Path:
    score_dir = tmp_path / "data" / "estimate"
    score_dir.mkdir(parents=True)
    (score_dir / "motor_ability_score.csv").write_text(SCORE_CSV_TEXT, encoding="utf-8")
    return tmp_path


@pytest.fixture
def score_table(repo_with_score: Path) -> dict:
    return load_motor_score_table(repo_with_score)


# ─────────────────────────────────────────────────────────────────────
# load_motor_score_table
# ─────────────────────────────────────────────────────────────────────
def test_load_score_table_returns_6_keys(score_table):
    assert len(score_table) == 6
    expected_keys = {
        ("B2", "全"), ("B1", "全"),
        ("A2", "SG_G1"), ("A2", "G2_G3_一般"),
        ("A1", "SG_G1"), ("A1", "G2_G3_一般"),
    }
    assert set(score_table) == expected_keys
    for vals in score_table.values():
        assert len(vals) == 6
        assert all(isinstance(v, int) for v in vals)


def test_load_score_table_missing_file_raises(tmp_path: Path):
    with pytest.raises(RuntimeError, match="motor_ability_score.csv not found"):
        load_motor_score_table(tmp_path)


# ─────────────────────────────────────────────────────────────────────
# grade_bucket_for_grade
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,expected", [
    ("SG", "SG_G1"),
    ("ＳＧ", "SG_G1"),
    ("PG1", "SG_G1"),
    ("ＰＧ１", "SG_G1"),
    ("G1", "SG_G1"),
    ("Ｇ１", "SG_G1"),
    ("ＧⅠ", "SG_G1"),
    ("G2", "G2_G3_一般"),
    ("ＧⅡ", "G2_G3_一般"),
    ("G3", "G2_G3_一般"),
    ("ＧⅢ", "G2_G3_一般"),
    ("IP", "G2_G3_一般"),
    ("", "G2_G3_一般"),
    (None, "G2_G3_一般"),
])
def test_grade_bucket_for_grade(raw, expected):
    assert grade_bucket_for_grade(raw) == expected


# ─────────────────────────────────────────────────────────────────────
# resolve_grade_bucket
# ─────────────────────────────────────────────────────────────────────
def test_resolve_grade_bucket_b_class_is_always_zen():
    assert resolve_grade_bucket("B1", "SG_G1") == "全"
    assert resolve_grade_bucket("B2", "G2_G3_一般") == "全"


def test_resolve_grade_bucket_a_class_passes_through():
    assert resolve_grade_bucket("A1", "SG_G1") == "SG_G1"
    assert resolve_grade_bucket("A2", "G2_G3_一般") == "G2_G3_一般"


# ─────────────────────────────────────────────────────────────────────
# normalize_finish_token
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,expected", [
    ("1", "1"), ("6", "6"),
    ("１", "1"), ("６", "6"),
    ("4.0", "4"),
    ("F", "F"), ("Ｆ", "F"),
    ("L", "L"), ("Ｌ", "L"),
    ("転", "転"), ("落", "落"), ("沈", "沈"), ("エ", "エ"),
    ("欠", "欠"), ("不", "不"), ("失", "失"), ("妨", "妨"),
    ("", None), ("  ", None),
    ("nan", None), (None, None),
    ("?", None), ("9", None), ("0", None),
])
def test_normalize_finish_token(raw, expected):
    assert normalize_finish_token(raw) == expected


# ─────────────────────────────────────────────────────────────────────
# score_motor_run
# ─────────────────────────────────────────────────────────────────────
def _run(racer_class: str, grade_bucket: str, finish: str) -> MotorRun:
    return MotorRun(
        session_end=dt.date(2026, 5, 10),
        stadium="01", motor_num=1,
        grade_bucket=grade_bucket, racer_class=racer_class,
        finish=finish,
    )


def test_score_b2_first_place_125(score_table):
    assert score_motor_run(score_table, _run("B2", "全", "1")) == (125, 1)


def test_score_a1_general_fourth_20(score_table):
    assert score_motor_run(score_table, _run("A1", "G2_G3_一般", "4")) == (20, 1)


def test_score_a2_sg_first_125(score_table):
    assert score_motor_run(score_table, _run("A2", "SG_G1", "1")) == (125, 1)


def test_score_a1_pg1_second_80(score_table):
    """PG1 は grade_bucket_for_grade で SG_G1 にマップされる。
    本テストではすでに SG_G1 として渡される前提を確認する。"""
    assert score_motor_run(score_table, _run("A1", "SG_G1", "2")) == (80, 1)


@pytest.mark.parametrize("token", sorted(MOTOR_NEGATIVE_TOKENS))
def test_score_negative_tokens_yield_minus_100(score_table, token):
    assert score_motor_run(score_table, _run("A1", "G2_G3_一般", token)) == (
        MOTOR_NEGATIVE_SCORE, 1,
    )
    assert MOTOR_NEGATIVE_SCORE == -100


@pytest.mark.parametrize("token", sorted(MOTOR_SKIP_TOKENS))
def test_score_skip_tokens_yield_none(score_table, token):
    assert score_motor_run(score_table, _run("A1", "G2_G3_一般", token)) is None


def test_score_unknown_token_returns_none(score_table):
    assert score_motor_run(score_table, _run("A1", "G2_G3_一般", "?")) is None


def test_score_unknown_class_returns_none(score_table):
    assert score_motor_run(score_table, _run("X1", "全", "1")) is None


# ─────────────────────────────────────────────────────────────────────
# motor_ability_pt
# ─────────────────────────────────────────────────────────────────────
def test_motor_ability_pt_no_history_returns_nan(score_table, v1_mode):
    import math
    assert math.isnan(motor_ability_pt({}, score_table, "01", 1))


def test_motor_ability_pt_average_with_negative_finish(score_table, v1_mode):
    """v1 mode: B2 級で 1,2,3 着 + 転 1 回 = (125 + 100 + 75 + -100) / 4 = 50."""
    history = {("01", 1): [[
        _run("B2", "全", "1"),
        _run("B2", "全", "2"),
        _run("B2", "全", "3"),
        _run("B2", "全", "転"),
    ]]}
    val = motor_ability_pt(history, score_table, "01", 1)
    assert val == pytest.approx((125 + 100 + 75 - 100) / 4)
    assert val == pytest.approx(50.0)


def test_motor_ability_pt_skip_tokens_not_in_denominator(score_table, v1_mode):
    """v1 mode: F は分母にも分子にも加わらない。
    A1 一般で 1 着 + F → 50 / 1 = 50.0"""
    history = {("01", 1): [[
        _run("A1", "G2_G3_一般", "1"),
        _run("A1", "G2_G3_一般", "F"),
    ]]}
    val = motor_ability_pt(history, score_table, "01", 1)
    assert val == pytest.approx(50.0)


def test_motor_ability_pt_only_skip_tokens_returns_nan(score_table, v1_mode):
    import math
    history = {("01", 1): [[_run("A1", "G2_G3_一般", "F")]]}
    assert math.isnan(motor_ability_pt(history, score_table, "01", 1))


def test_motor_ability_pt_multiple_sessions(score_table, v1_mode):
    """v1 mode: 2 節分の走を均等に平均する(走数で重み付け)。
    節1: A1一般 1着 (50) + 3着 (30) = 80/2
    節2: A1一般 2着 (40) = 40/1
    全体平均 = (50+30+40) / 3 = 40.0"""
    history = {("01", 1): [
        [_run("A1", "G2_G3_一般", "1"), _run("A1", "G2_G3_一般", "3")],
        [_run("A1", "G2_G3_一般", "2")],
    ]}
    val = motor_ability_pt(history, score_table, "01", 1)
    assert val == pytest.approx((50 + 30 + 40) / 3)


# ─────────────────────────────────────────────────────────────────────
# detect_session_end_days
# ─────────────────────────────────────────────────────────────────────
def _make_race_cards(repo: Path, day: dt.date, stadiums: list[str]) -> None:
    """Write a minimal race_cards CSV with one race per stadium."""
    p = repo / "data" / "programs" / "race_cards" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"レースコード": f"{day:%Y%m%d}{s}01"} for s in stadiums]
    pd.DataFrame(rows).to_csv(p, index=False)


def test_detect_session_end_days_groups_consecutive(tmp_path: Path):
    """場01: 5/1-5/3, 5/6-5/8, 5/10 の 3 節。target=5/15 → 5/10, 5/8, 5/3 の順。"""
    target = dt.date(2026, 5, 15)
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 2), dt.date(2026, 5, 3),
              dt.date(2026, 5, 6), dt.date(2026, 5, 7), dt.date(2026, 5, 8),
              dt.date(2026, 5, 10)]:
        _make_race_cards(tmp_path, d, ["01"])

    result = detect_session_end_days(tmp_path, "01", target, max_sessions=10)
    assert result == [dt.date(2026, 5, 10), dt.date(2026, 5, 8), dt.date(2026, 5, 3)]


def test_detect_session_end_days_excludes_target(tmp_path: Path):
    """target_day を含む節は集計対象から外す(=target_day の直近開催も除外)。"""
    target = dt.date(2026, 5, 10)
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 5)]:
        _make_race_cards(tmp_path, d, ["01"])
    _make_race_cards(tmp_path, target, ["01"])  # この日は除外される

    result = detect_session_end_days(tmp_path, "01", target)
    assert result == [dt.date(2026, 5, 5), dt.date(2026, 5, 1)]


def test_detect_session_end_days_caps_at_max_sessions(tmp_path: Path):
    """max_sessions を超える節があった場合、新しい順に切り詰める。
    window_days のデフォルト 90 日に収まる範囲で 6 節を作成 → 3 節までに絞られる。"""
    target = dt.date(2026, 5, 30)
    # 6 個の 1 日節を 7 日おきに作成(全て target から 90 日以内)
    for d in [target - dt.timedelta(days=7 * (i + 1)) for i in range(6)]:
        _make_race_cards(tmp_path, d, ["01"])

    result = detect_session_end_days(tmp_path, "01", target, max_sessions=3)
    assert len(result) == 3
    # 新→旧に並んでいること
    assert result == sorted(result, reverse=True)


# ─────────────────────────────────────────────────────────────────────
# load_motor_period_starts
# ─────────────────────────────────────────────────────────────────────
def _make_motor_stats(repo: Path, day: dt.date, rows: list[dict]) -> None:
    p = repo / "data" / "programs" / "motor_stats" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)


def test_load_motor_period_starts_basic(tmp_path: Path):
    target = dt.date(2026, 5, 10)
    _make_motor_stats(tmp_path, target, [
        {"記録日": "2026-05-10", "モーター期起算日": "2026-04-01",
         "場コード": "01", "モーター番号": "10"},
        {"記録日": "2026-05-10", "モーター期起算日": "2026-04-01",
         "場コード": "01", "モーター番号": "11"},
    ])
    result = load_motor_period_starts(tmp_path, target)
    assert result == {
        ("01", 10): dt.date(2026, 4, 1),
        ("01", 11): dt.date(2026, 4, 1),
    }


def test_load_motor_period_starts_fallback_to_earlier_day(tmp_path: Path):
    """target 当日に motor_stats が無い場合、過去日まで遡る。"""
    target = dt.date(2026, 5, 10)
    earlier = dt.date(2026, 5, 7)
    _make_motor_stats(tmp_path, earlier, [
        {"記録日": "2026-05-07", "モーター期起算日": "2026-04-01",
         "場コード": "01", "モーター番号": "10"},
    ])
    result = load_motor_period_starts(tmp_path, target)
    assert result == {("01", 10): dt.date(2026, 4, 1)}


def test_load_motor_period_starts_uses_latest_snapshot_per_stadium(tmp_path: Path):
    """同じ場の motor_stats が複数日に存在する場合、最新のものだけを採用。"""
    target = dt.date(2026, 5, 10)
    _make_motor_stats(tmp_path, dt.date(2026, 5, 10), [
        {"記録日": "2026-05-10", "モーター期起算日": "2026-04-15",
         "場コード": "01", "モーター番号": "10"},
    ])
    _make_motor_stats(tmp_path, dt.date(2026, 5, 8), [
        {"記録日": "2026-05-08", "モーター期起算日": "2026-03-01",
         "場コード": "01", "モーター番号": "10"},
    ])
    result = load_motor_period_starts(tmp_path, target)
    # 最新の 5/10 スナップショット = 2026-04-15 を採用
    assert result == {("01", 10): dt.date(2026, 4, 15)}


# ─────────────────────────────────────────────────────────────────────
# load_motor_history (期境界フィルタの組み合わせ確認)
# ─────────────────────────────────────────────────────────────────────
def _make_race_cards_with_motor(
    repo: Path, day: dt.date, stadium: str,
    motor_num: int, racer_class: str,
    slot_finishes: dict[str, str],  # {"D1走1": "1", "D2走1": "2", ...}
) -> None:
    """1艇1モーターだけ載せた最小 race_cards。他の艇は空欄。"""
    p = repo / "data" / "programs" / "race_cards" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    row = {"レースコード": f"{day:%Y%m%d}{stadium}01"}
    for n in range(1, 7):
        row[f"艇{n}_モーター番号"] = str(motor_num) if n == 1 else ""
        row[f"艇{n}_級別"] = racer_class if n == 1 else ""
        for d in range(1, 8):
            for s in (1, 2):
                key = f"艇{n}_節D{d}走{s}_着順"
                if n == 1:
                    row[key] = slot_finishes.get(f"D{d}走{s}", "")
                else:
                    row[key] = ""
    pd.DataFrame([row]).to_csv(p, index=False)


def test_load_motor_history_filters_by_period_start(tmp_path: Path):
    """期境界(モーター期起算日)より前の節は剪定される。"""
    # スコアCSVを置く
    score_dir = tmp_path / "data" / "estimate"
    score_dir.mkdir(parents=True)
    (score_dir / "motor_ability_score.csv").write_text(SCORE_CSV_TEXT, encoding="utf-8")

    target = dt.date(2026, 5, 30)
    # 節1(古): 5/1, 節2(新): 5/20
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 20)]:
        _make_race_cards_with_motor(
            tmp_path, d, "01", motor_num=10, racer_class="A1",
            slot_finishes={"D1走1": "1"})

    # 期境界を 5/15 に設定 → 5/1 の節は剪定される
    _make_motor_stats(tmp_path, target, [
        {"記録日": "2026-05-30", "モーター期起算日": "2026-05-15",
         "場コード": "01", "モーター番号": "10"},
    ])

    history = load_motor_history(tmp_path, target)
    assert ("01", 10) in history
    sessions = history[("01", 10)]
    assert len(sessions) == 1
    # 残った節の最終日は 2026-05-20
    assert sessions[0][0].session_end == dt.date(2026, 5, 20)


def test_load_motor_history_no_period_start_keeps_all_sessions(tmp_path: Path):
    """motor_stats に該当モーターが無い場合、フィルタを適用しない(妥協)。"""
    score_dir = tmp_path / "data" / "estimate"
    score_dir.mkdir(parents=True)
    (score_dir / "motor_ability_score.csv").write_text(SCORE_CSV_TEXT, encoding="utf-8")

    target = dt.date(2026, 5, 30)
    for d in [dt.date(2026, 5, 1), dt.date(2026, 5, 20)]:
        _make_race_cards_with_motor(
            tmp_path, d, "01", motor_num=10, racer_class="A1",
            slot_finishes={"D1走1": "1"})

    # motor_stats は無し
    history = load_motor_history(tmp_path, target)
    assert ("01", 10) in history
    assert len(history[("01", 10)]) == 2


# ─────────────────────────────────────────────────────────────────────
# extract_runs_for_session 統合確認
# ─────────────────────────────────────────────────────────────────────
def test_extract_runs_for_session_picks_up_all_slots(tmp_path: Path):
    day = dt.date(2026, 5, 20)
    _make_race_cards_with_motor(
        tmp_path, day, "01", motor_num=10, racer_class="A1",
        slot_finishes={"D1走1": "1", "D2走1": "3", "D3走1": "転"})

    runs = extract_runs_for_session(tmp_path, "01", day)
    finishes = sorted(r.finish for r in runs)
    assert finishes == ["1", "3", "転"]
    for r in runs:
        assert r.motor_num == 10
        assert r.racer_class == "A1"
        assert r.stadium == "01"
        assert r.session_end == day

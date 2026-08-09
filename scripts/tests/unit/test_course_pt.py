"""Unit tests for the v6_course コースpt component.

Covers:
- ``scripts/build_course_rate.py``: レース回パース・勝ちコース突合・
  ベイズ収縮の計算・CLI 出力(手計算できる小型フィクスチャで検証)
- ``boatrace.index_features``: ``load_course_table`` / ``course_pt`` の
  読み込み・フォールバック連鎖
- ``compute_features_for_day``: long-format 出力に ``course`` 列が乗り、
  daily(枠番フォールバック)と realtime(実進入)で参照が切り替わること
- ``boatrace.predictors.registry``: ``v6_course`` スペックの整合

Reference design: ``docs/design/course_strength_v6.md``.
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pandas as pd
import pytest

import build_course_rate  # type: ignore[import-not-found]
from boatrace.index_features import (  # type: ignore[import-not-found]
    FeatureContext,
    compute_features_for_day,
    course_pt,
    load_course_table,
)
from boatrace.predictors import predictor_by_id  # type: ignore[import-not-found]
from boatrace.predictors.registry import (  # type: ignore[import-not-found]
    COMPONENT_LABELS_REGISTRY,
)

from tests.unit.test_feature_context import (  # type: ignore[import-not-found]
    _build_repo,
)


# ─────────────────────────────────────────────────────────────────────
# build_course_rate.py — parsing helpers
# ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,expected", [
    ("01R", 1), ("1R", 1), ("12R", 12), ("7", 7), ("07", 7),
    ("", None), (None, None), ("R", None), ("13R", None), ("0R", None),
    ("xx", None),
])
def test_parse_race_round(raw, expected):
    assert build_course_rate.parse_race_round(raw) == expected


def test_winner_course_matches_entry_order():
    row = {"1着_艇番": "3"}
    for c, boat in zip(range(1, 7), ["1", "2", "4", "3", "5", "6"]):
        row[f"{c}コース_艇番"] = boat
    assert build_course_rate.winner_course(row) == 4


def test_winner_course_missing_or_inconsistent():
    assert build_course_rate.winner_course({"1着_艇番": ""}) is None
    # 1着艇番がどのコースにも見つからない(不整合行)
    row = {"1着_艇番": "9"}
    for c in range(1, 7):
        row[f"{c}コース_艇番"] = str(c)
    assert build_course_rate.winner_course(row) is None


# ─────────────────────────────────────────────────────────────────────
# build_course_rate.py — shrinkage + CLI end-to-end
# ─────────────────────────────────────────────────────────────────────
def _write_results_file(repo: Path, day: dt.date, rows: list[dict]) -> None:
    p = (repo / "data" / "results" / "realtime"
         / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    fields = ["レースコード", "レース場", "レース回", "1着_艇番"] + [
        f"{c}コース_艇番" for c in range(1, 7)
    ]
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def _result_row(day: dt.date, jo: str, rno: int, winner_course: int) -> dict:
    """枠なり進入で ``winner_course`` のコースが勝った結果行。"""
    row = {
        "レースコード": f"{day:%Y%m%d}{jo}{rno:02d}",
        "レース場": jo,
        "レース回": f"{rno:02d}R",
        "1着_艇番": str(winner_course),
    }
    for c in range(1, 7):
        row[f"{c}コース_艇番"] = str(c)
    return row


def test_build_course_rate_shrinkage_hand_computed(tmp_path: Path):
    """場01・1R に 10 レース(1コース6勝・2コース4勝)、2R に 10 レース
    (全て1コース勝ち)を置き、k=10 の収縮値を手計算と突合する。"""
    day = dt.date(2026, 5, 1)
    rows = []
    for i in range(6):
        rows.append(_result_row(day, "01", 1, 1))
    for i in range(4):
        rows.append(_result_row(day, "01", 1, 2))
    for i in range(10):
        rows.append(_result_row(day, "01", 2, 1))
    _write_results_file(tmp_path, day, rows)

    rc = build_course_rate.main(["--repo", str(tmp_path), "--k", "10"])
    assert rc == 0
    out = tmp_path / "data" / "estimate" / "stadium" / "course_win_rate.csv"
    df = pd.read_csv(out, dtype=str)
    # 24 場 × 12 レース回の全格子
    assert len(df) == 24 * 12

    # base(01, c1) = 16/20 = 0.8, base(01, c2) = 4/20 = 0.2
    r1 = df[(df["場コード"] == "1") | (df["場コード"] == "01")]
    r1 = r1[r1["レース回"] == "1"].iloc[0]
    # rate(01,1R,c1) = (6 + 10*0.8) / (10 + 10) = 0.70
    assert float(r1["1コース勝率"]) == pytest.approx(70.0, abs=0.01)
    # rate(01,1R,c2) = (4 + 10*0.2) / 20 = 0.30
    assert float(r1["2コース勝率"]) == pytest.approx(30.0, abs=0.01)
    assert int(r1["n"]) == 10

    r2 = df[(df["場コード"] == "1") | (df["場コード"] == "01")]
    r2 = r2[r2["レース回"] == "2"].iloc[0]
    # rate(01,2R,c1) = (10 + 10*0.8) / 20 = 0.90
    assert float(r2["1コース勝率"]) == pytest.approx(90.0, abs=0.01)

    # データの無いセル(3R)は base へフォールバック(n=0 → rate = base)
    r3 = df[(df["場コード"] == "1") | (df["場コード"] == "01")]
    r3 = r3[r3["レース回"] == "3"].iloc[0]
    assert int(r3["n"]) == 0
    assert float(r3["1コース勝率"]) == pytest.approx(80.0, abs=0.01)

    # データの無い場は全国率へフォールバック
    r_other = df[df["場コード"].isin(["2", "02"])]
    r_other = r_other[r_other["レース回"] == "1"].iloc[0]
    assert float(r_other["1コース勝率"]) == pytest.approx(80.0, abs=0.01)


def test_build_course_rate_missing_results_dir(tmp_path: Path):
    assert build_course_rate.main(["--repo", str(tmp_path)]) == 1


# ─────────────────────────────────────────────────────────────────────
# index_features — load_course_table / course_pt
# ─────────────────────────────────────────────────────────────────────
def _write_course_table(repo: Path, rows: list[dict]) -> None:
    p = repo / "data" / "estimate" / "stadium" / "course_win_rate.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(p, index=False)


def _course_row(jo: str, rno: int, n: int, rates: list[float]) -> dict:
    row = {"場コード": jo, "レース回": rno, "n": n}
    for c in range(1, 7):
        row[f"{c}コース勝率"] = rates[c - 1]
    return row


def test_load_course_table_and_fallback(tmp_path: Path):
    _write_course_table(tmp_path, [
        _course_row("01", 1, 100, [60, 20, 10, 5, 3, 2]),
        _course_row("01", 2, 300, [40, 25, 15, 10, 6, 4]),
    ])
    table, fallback = load_course_table(tmp_path)
    assert table[("01", 1)][0] == pytest.approx(60.0)
    assert table[("01", 2)][0] == pytest.approx(40.0)
    # fallback = n 加重平均: (100*60 + 300*40) / 400 = 45.0
    assert fallback["01"][0] == pytest.approx(45.0)


def test_load_course_table_missing_file(tmp_path: Path):
    table, fallback = load_course_table(tmp_path)
    assert table == {} and fallback == {}


def test_course_pt_lookup_chain(tmp_path: Path):
    _write_course_table(tmp_path, [
        _course_row("01", 1, 100, [60, 20, 10, 5, 3, 2]),
        _course_row("01", 2, 300, [40, 25, 15, 10, 6, 4]),
    ])
    table, fallback = load_course_table(tmp_path)
    # セルヒット
    assert course_pt(table, fallback, "01", 1, 1) == pytest.approx(60.0)
    assert course_pt(table, fallback, "01", 2, 2) == pytest.approx(25.0)
    # セル欠損(レース回 12 は未登録)→ 場×コース全体率
    assert course_pt(table, fallback, "01", 12, 1) == pytest.approx(45.0)
    # レース番号不明 → 場×コース全体率
    assert course_pt(table, fallback, "01", None, 1) == pytest.approx(45.0)
    # 場ごと欠損 → NaN
    assert pd.isna(course_pt(table, fallback, "99", 1, 1))
    # 不正コース → NaN
    assert pd.isna(course_pt(table, fallback, "01", 1, 0))
    assert pd.isna(course_pt(table, fallback, "01", 1, 7))


# ─────────────────────────────────────────────────────────────────────
# compute_features_for_day — course 列の統合
# ─────────────────────────────────────────────────────────────────────
def _full_course_rows() -> list[dict]:
    """全 24 場 × 12R。1R だけ独自値にして参照検証に使う。"""
    rows = []
    from boatrace.index_features import STADIUM_NAMES
    for code in STADIUM_NAMES.keys():
        jo = f"{code:02d}"
        for rno in range(1, 13):
            if rno == 1:
                rates = [70, 12, 8, 5, 3, 2]
            else:
                rates = [50, 20, 12, 9, 6, 3]
            rows.append(_course_row(jo, rno, 100, rates))
    return rows


def test_compute_features_day_has_course_column(tmp_path: Path):
    day = dt.date(2026, 5, 5)
    repo = _build_repo(tmp_path, open_days={day: ["02"]})
    _write_course_table(repo, _full_course_rows())

    df = compute_features_for_day(repo, day)
    assert "course" in df.columns
    sub = df[df["レース回"] == "1R"].sort_values("枠番")
    # preview 無し → 枠番フォールバック: 枠 N はコース N の 1R 率
    assert list(sub["course"]) == pytest.approx([70, 12, 8, 5, 3, 2])


def test_compute_features_day_course_uses_stt_entry(tmp_path: Path):
    """stt preview で 1・2 号艇の進入が入れ替わったら course も入れ替わる。"""
    day = dt.date(2026, 5, 5)
    repo = _build_repo(tmp_path, open_days={day: ["02"]})
    _write_course_table(repo, _full_course_rows())

    code = f"{day:%Y%m%d}0201"
    stt = {"レースコード": code}
    entry = {1: 2, 2: 1, 3: 3, 4: 4, 5: 5, 6: 6}
    for boat, c in entry.items():
        stt[f"艇{boat}_コース"] = c
    p = (repo / "data" / "previews" / "stt"
         / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([stt]).to_csv(p, index=False)

    df = compute_features_for_day(repo, day)
    sub = df[df["レース回"] == "1R"].sort_values("枠番")
    # 枠1 はコース2(12%)、枠2 はコース1(70%)を引く
    assert list(sub["course"]) == pytest.approx([12, 70, 8, 5, 3, 2])


def test_feature_context_course_table_cached(tmp_path: Path):
    _write_course_table(tmp_path, [_course_row("01", 1, 10, [60, 20, 10, 5, 3, 2])])
    ctx = FeatureContext(
        tmp_path,
        window_start=dt.date(2026, 5, 1), window_end=dt.date(2026, 5, 2),
    )
    t1 = ctx.course_table()
    t2 = ctx.course_table()
    assert t1 is t2  # lazy-load one-shot cache


# ─────────────────────────────────────────────────────────────────────
# registry — v6_course spec
# ─────────────────────────────────────────────────────────────────────
def test_registry_v6_course_spec():
    spec = predictor_by_id("v6_course")
    # 2026-08-09 退役 (control 比 -6.91pt, p=0.0047)。成分定義と course の計算
    # ロジックは残すので、以下の構成アサーションはそのまま維持する。
    assert not spec.is_active()
    assert spec.status == "retired"
    assert spec.component_keys == (
        "course", "racer", "motor", "exhibit", "weather",
    )
    assert COMPONENT_LABELS_REGISTRY["course"] == "コースpt"
    # control (v1_basic) との差分が waku → course の 1 成分のみであること
    control = predictor_by_id("v1_basic")
    assert set(control.component_keys) - set(spec.component_keys) == {"waku"}
    assert set(spec.component_keys) - set(control.component_keys) == {"course"}

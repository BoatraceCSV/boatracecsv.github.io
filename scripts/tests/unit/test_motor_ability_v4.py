"""Unit tests for v4_motor 用モーターpt チューニングパラメータ。

v4_motor の motor4 成分は v2 と同じ計算式で、以下のみが異なる:
  - スコア表: data/estimate/motor_ability_score_v4.csv (γ=1.5 凸カーブ)
  - ペナルティ: MOTOR4_NEGATIVE_SCORE = -50 (v2 は -100)
  - 採用節数: MOTOR4_HISTORY_SESSIONS = 5 (v2 は 6)

経緯: notebooks/motor_score_tuning/report.md
設計: docs/design/motor_score_tuning_v4.md
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from boatrace import index_features as ifeat  # type: ignore[import-not-found]
from boatrace.index_features import (  # type: ignore[import-not-found]
    MOTOR4_HISTORY_SESSIONS,
    MOTOR4_NEGATIVE_SCORE,
    MOTOR4_SCORE_FILENAME,
    MOTOR_NEGATIVE_SCORE,
    MotorRun,
    load_motor_score_table,
    motor_ability_pt,
    score_motor_run,
)
from boatrace.predictors import (  # type: ignore[import-not-found]
    component_label, component_missing_fallback, predictor_by_id,
)

REPO_ROOT = Path(__file__).resolve().parents[3]

V4_TABLE = {
    ("B2", "全"): [125, 89, 58, 32, 11, 0],
    ("B1", "全"): [100, 72, 46, 25, 9, 0],
    ("A2", "SG_G1"): [125, 89, 58, 32, 11, 0],
    ("A2", "G2_G3_一般"): [75, 54, 35, 19, 7, 0],
    ("A1", "SG_G1"): [100, 72, 46, 25, 9, 0],
    ("A1", "G2_G3_一般"): [50, 36, 23, 13, 4, 0],
}


def _run(finish: str, cls: str = "B1", bucket: str = "全",
         session_end: dt.date = dt.date(2026, 7, 1)) -> MotorRun:
    return MotorRun(
        session_end=session_end, stadium="01", motor_num=10,
        grade_bucket=bucket, racer_class=cls, finish=finish,
        race_date=session_end, lane=0,
    )


# ─────────────────────────────────────────────────────────────────────
# 定数・スコア表 CSV
# ─────────────────────────────────────────────────────────────────────
class TestV4Constants:
    def test_constants(self):
        assert MOTOR4_SCORE_FILENAME == "motor_ability_score_v4.csv"
        assert MOTOR4_NEGATIVE_SCORE == -50
        assert MOTOR4_HISTORY_SESSIONS == 5

    def test_v2_constants_unchanged(self):
        """v1_basic 側の従来定数が据え置きであること(意味を変えない)。"""
        assert MOTOR_NEGATIVE_SCORE == -100
        assert ifeat.MOTOR_HISTORY_SESSIONS == 6

    def test_v4_score_csv_loads_and_matches(self):
        """リポジトリの v4 スコア表 CSV が期待値(γ=1.5 凸カーブ)と一致する。"""
        table = load_motor_score_table(REPO_ROOT, filename=MOTOR4_SCORE_FILENAME)
        assert table == V4_TABLE

    def test_v4_rows_convex(self):
        """全行が単調減少かつ凸(1着プレミアム: 上位の間隔 > 下位の間隔)。"""
        for key, pts in V4_TABLE.items():
            diffs = [pts[i] - pts[i + 1] for i in range(5)]
            assert all(d > 0 for d in diffs), key
            assert diffs == sorted(diffs, reverse=True), key

    def test_default_filename_reads_v2_table(self):
        """filename 省略時は従来テーブルを読む(後方互換)。"""
        table = load_motor_score_table(REPO_ROOT)
        assert table[("B2", "全")] == [125, 100, 75, 50, 25, 0]

    def test_missing_file_raises(self, tmp_path):
        (tmp_path / "data" / "estimate").mkdir(parents=True)
        with pytest.raises(RuntimeError):
            load_motor_score_table(tmp_path, filename=MOTOR4_SCORE_FILENAME)


# ─────────────────────────────────────────────────────────────────────
# score_motor_run: negative_score 引数
# ─────────────────────────────────────────────────────────────────────
class TestScoreMotorRunNegativeScore:
    def test_default_negative_is_v2(self):
        assert score_motor_run(V4_TABLE, _run("転")) == (-100, 1)

    def test_v4_negative_override(self):
        for tok in ("転", "落", "沈", "エ"):
            assert score_motor_run(
                V4_TABLE, _run(tok), MOTOR4_NEGATIVE_SCORE
            ) == (-50, 1)

    def test_skip_tokens_unaffected(self):
        for tok in ("F", "L", "失", "妨", "欠", "不"):
            assert score_motor_run(
                V4_TABLE, _run(tok), MOTOR4_NEGATIVE_SCORE
            ) is None

    def test_finish_scores_from_v4_table(self):
        assert score_motor_run(V4_TABLE, _run("1"), -50) == (100, 1)
        assert score_motor_run(V4_TABLE, _run("2"), -50) == (72, 1)
        assert score_motor_run(
            V4_TABLE, _run("4", cls="A1", bucket="G2_G3_一般"), -50
        ) == (13, 1)


# ─────────────────────────────────────────────────────────────────────
# motor_ability_pt: max_sessions / negative_score
# ─────────────────────────────────────────────────────────────────────
@pytest.fixture
def flags_off(monkeypatch):
    """全フィーチャーフラグ OFF → 単純平均に縮退させ検算しやすくする。"""
    monkeypatch.setattr(ifeat, "ENABLE_DECAY", False)
    monkeypatch.setattr(ifeat, "ENABLE_LANE_CORRECTION", False)
    monkeypatch.setattr(ifeat, "ENABLE_SHRINKAGE", False)


class TestMotorAbilityPtV4Args:
    def _history(self):
        """6 節分: 新しい順に 1着, 2着, 3着, 4着, 5着, 6着 (B1, 全)。"""
        sessions = []
        for i, fin in enumerate(["1", "2", "3", "4", "5", "6"]):
            d = dt.date(2026, 7, 1) - dt.timedelta(days=7 * i)
            sessions.append([_run(fin, session_end=d)])
        return {("01", 10): sessions}

    def test_max_sessions_slices_newest(self, flags_off):
        hist = self._history()
        # 全 6 節 (v4 表): (100+72+46+25+9+0)/6 = 42.0
        assert motor_ability_pt(hist, V4_TABLE, "01", 10) == pytest.approx(42.0)
        # 直近 5 節のみ: (100+72+46+25+9)/5 = 50.4
        assert motor_ability_pt(
            hist, V4_TABLE, "01", 10, max_sessions=5
        ) == pytest.approx(50.4)
        # 直近 1 節のみ: 100.0
        assert motor_ability_pt(
            hist, V4_TABLE, "01", 10, max_sessions=1
        ) == pytest.approx(100.0)

    def test_negative_score_passthrough(self, flags_off):
        hist = {("01", 10): [[_run("1"), _run("転")]]}
        # v2 default: (100 - 100) / 2 = 0.0
        assert motor_ability_pt(hist, V4_TABLE, "01", 10) == pytest.approx(0.0)
        # v4: (100 - 50) / 2 = 25.0
        assert motor_ability_pt(
            hist, V4_TABLE, "01", 10, negative_score=MOTOR4_NEGATIVE_SCORE
        ) == pytest.approx(25.0)

    def test_empty_after_slice_is_nan_safe(self, flags_off):
        """max_sessions=0 は全走除外 → NaN (下流で 50 補完)。"""
        hist = self._history()
        v = motor_ability_pt(hist, V4_TABLE, "01", 10, max_sessions=0)
        assert v != v  # NaN


# ─────────────────────────────────────────────────────────────────────
# レジストリ
# ─────────────────────────────────────────────────────────────────────
class TestV4Registry:
    def test_v4_motor_spec(self):
        spec = predictor_by_id("v4_motor")
        # 2026-08-10 退役 (control 比 +0.30pt, p=0.884 で有意差なし)。成分定義と
        # motor4 の計算ロジックは残すので、以下の構成アサーションは維持する。
        assert not spec.is_active()
        assert spec.status == "retired"
        assert spec.component_keys == (
            "waku", "racer", "motor4", "exhibit", "weather")
        assert spec.started_at == dt.date(2026, 7, 20)

    def test_motor4_label_and_fallback(self):
        # v1_basic と同じ列名(CSV 互換)。欠損補完はデフォルト 50。
        assert component_label("motor4") == "モーターpt"
        assert component_missing_fallback("motor4") == 50.0

    def test_v1_basic_untouched(self):
        spec = predictor_by_id("v1_basic")
        assert spec.component_keys == (
            "waku", "racer", "motor", "exhibit", "weather")

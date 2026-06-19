"""Unit tests for 展開優位pt (v2_tenkai) feature計算とレジストリ統合。

Phase 2 で追加した特徴量。スタート展示の進入コース変更 (枠番デフォルト
コース → 実際の進入コース) を勝率差として捉える。
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from boatrace.index_features import (  # noqa: E402
    COMPONENT_KEYS,
    parse_motor_2rate,
    tenkai_yui_pt,
)
import math  # noqa: E402
from boatrace.predictors import (  # noqa: E402
    COMPONENT_LABELS_REGISTRY,
    active_predictors,
    predictor_by_id,
)


# 場 "01" 春 の合成勝率テーブル (1→6 で単調減少: イン強い、アウト弱い)
WIN_RATE_TABLE = {
    ("01", "春"): [0.55, 0.18, 0.13, 0.08, 0.04, 0.02],
}


class TestTenkaiYuiPt:
    """tenkai_yui_pt() の戻り値の振る舞い。"""

    def test_no_course_change_returns_zero(self):
        """進入変更が無い (waku == actual) なら 0.0 を返す。"""
        for c in range(1, 7):
            assert tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", c, c) == 0.0

    def test_better_course_returns_positive(self):
        """より良い (= 番号が小さい) コースに入った場合は正。"""
        # 枠4 → 進入1: rates[0] - rates[3] = 0.55 - 0.08 = +0.47
        assert tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", 4, 1) == pytest.approx(0.47)
        # 枠6 → 進入5: rates[4] - rates[5] = 0.04 - 0.02 = +0.02
        assert tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", 6, 5) == pytest.approx(0.02)

    def test_worse_course_returns_negative(self):
        """より悪いコースに入った場合は負。"""
        # 枠1 → 進入4: rates[3] - rates[0] = 0.08 - 0.55 = -0.47
        assert tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", 1, 4) == pytest.approx(-0.47)

    def test_invalid_waku_returns_nan(self):
        """枠番が 1〜6 範囲外なら NaN を返す。"""
        import math
        for bad in (0, 7, -1):
            r = tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", bad, 1)
            assert math.isnan(r)

    def test_invalid_course_returns_nan(self):
        """進入コースが 1〜6 範囲外なら NaN を返す。"""
        import math
        for bad in (0, 7, -1):
            r = tenkai_yui_pt(WIN_RATE_TABLE, "01", "春", 1, bad)
            assert math.isnan(r)

    def test_unknown_stadium_returns_nan(self):
        """場/季節が table に無い場合は NaN を返す。"""
        import math
        assert math.isnan(tenkai_yui_pt(WIN_RATE_TABLE, "99", "春", 1, 1))
        assert math.isnan(tenkai_yui_pt(WIN_RATE_TABLE, "01", "夏", 1, 1))


class TestParseMotor2Rate:
    """motor2rate 成分の素点パーサ。"""

    def test_normal_value(self):
        assert parse_motor_2rate("48.3") == 48.3
        assert parse_motor_2rate("33.33") == 33.33

    def test_percent_sign_and_spaces(self):
        assert parse_motor_2rate(" 40.9% ") == 40.9

    def test_numeric_input(self):
        assert parse_motor_2rate(35) == 35.0

    def test_missing_returns_nan(self):
        for raw in (None, "", "nan", "  ", "abc"):
            assert math.isnan(parse_motor_2rate(raw))


class TestRegistryV2Tenkai:
    """v2_tenkai 予想者のレジストリ登録状況。"""

    def test_v2_tenkai_is_active(self):
        v2 = predictor_by_id("v2_tenkai")
        assert v2.is_active()
        assert v2.display_name == "B君予想"
        assert v2.slot == 2

    def test_v2_tenkai_motor_replaced_by_motor2rate(self):
        # 展開優位pt 撤去後、次の実験として着順ベース motor を motor2rate に
        # 置き換えた 5 成分構成(成分数は control と同じで motor 指標だけ差し替え)。
        v2 = predictor_by_id("v2_tenkai")
        assert v2.component_keys == (
            "waku", "racer", "motor2rate", "exhibit", "weather",
        )
        assert "tenkai" not in v2.component_keys
        # 着順ベースの motor は使わない(motor2rate に置換済み)。
        assert "motor" not in v2.component_keys
        assert "motor2rate" in v2.component_keys
        # control (v1_basic) と同じ 5 成分で、motor の位置だけ motor2rate に
        # 差し替わっていること。
        v1 = predictor_by_id("v1_basic")
        assert len(v2.component_keys) == len(v1.component_keys)
        assert v2.component_keys == tuple(
            "motor2rate" if k == "motor" else k for k in v1.component_keys
        )

    def test_v2_tenkai_started_at_reset(self):
        # recipe 変更に伴い started_at を 2026-06-13 にリセット。
        assert predictor_by_id("v2_tenkai").started_at == dt.date(2026, 6, 13)

    def test_motor2rate_label_registered(self):
        assert COMPONENT_LABELS_REGISTRY["motor2rate"] == "モーター2連率pt"

    def test_tenkai_label_still_registered_for_future_reuse(self):
        # 計算ロジックとラベルは将来の再利用に備えて残してある。
        assert COMPONENT_LABELS_REGISTRY["tenkai"] == "展開優位pt"

    def test_v1_basic_unchanged_by_v2_addition(self):
        """v2 を追加しても v1_basic は 5 成分のままであること (回帰防止)。"""
        v1 = predictor_by_id("v1_basic")
        assert v1.component_keys == (
            "waku", "racer", "motor", "exhibit", "weather",
        )
        # COMPONENT_KEYS (legacy export) も v1_basic と一致。
        assert COMPONENT_KEYS == list(v1.component_keys)

    def test_all_predictors_active(self):
        """active_predictors() が v1_basic, v2_tenkai, v3_tenkai を slot 順で返す。"""
        actives = active_predictors()
        ids = [p.predictor_id for p in actives]
        assert ids == ["v1_basic", "v2_tenkai", "v3_tenkai"]


class TestRegistryV3Tenkai:
    """v3_tenkai(展開予想)= control + 展開優位pt の 6 成分。"""

    def test_v3_tenkai_is_active(self):
        v3 = predictor_by_id("v3_tenkai")
        assert v3.is_active()
        assert v3.display_name == "展開予想"
        assert v3.slot == 3

    def test_v3_tenkai_is_control_plus_tenkai(self):
        v3 = predictor_by_id("v3_tenkai")
        assert v3.component_keys == (
            "waku", "racer", "motor", "exhibit", "weather", "tenkai",
        )
        assert "tenkai" in v3.component_keys
        # control (v1_basic) の 5 成分に tenkai を 1 つ足しただけ。
        v1 = predictor_by_id("v1_basic")
        assert len(v3.component_keys) == len(v1.component_keys) + 1
        assert v3.component_keys[: len(v1.component_keys)] == v1.component_keys

    def test_v3_tenkai_started_at(self):
        assert predictor_by_id("v3_tenkai").started_at == dt.date(2026, 6, 20)

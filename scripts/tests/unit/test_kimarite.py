"""Unit tests for the 荒れ度メーター (決まり手セルモデル).

Covers:
- ``boatrace.kimarite``: クラス凍結・特徴量の並び・展示進入での並べ替え
- ``build_kimarite_probs``: 係数 CSV からの softmax・daily/realtime の共存・
  スキーマ不一致の検出

学習 (``build_kimarite.py``) は sklearn 依存かつ実データ規模でないと意味が無いので、
ここでは推論側と特徴量側だけを固定する。学習の妥当性は
``notebooks/ana_prediction/report.md`` と ``--report`` オプションで確認する。

Reference design: ``docs/design/ana_prediction.md`` (§12 / §14)。
"""

from __future__ import annotations

import csv
import datetime as dt
import math
from pathlib import Path

import pytest

import build_kimarite_probs  # type: ignore[import-not-found]
from boatrace import kimarite  # type: ignore[import-not-found]

DAY = dt.date(2026, 8, 12)
CARD_HEADER = ["レースコード", "レース日", "レース場コード", "レース回"] + [
    f"艇{b}_{label}" for b in range(1, 7) for label, _ in kimarite.CARD_FEATURES
]
STT_HEADER = ["レースコード"] + [
    f"艇{b}_{c}" for b in range(1, 7) for c in ("コース", "スタート展示")
]


def _write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


class TestCells:
    def test_frozen_class_list_is_unique_and_covers_every_course(self) -> None:
        assert len(kimarite.CELLS) == len(set(kimarite.CELLS))
        for c in range(1, 7):
            assert f"その他_{c}" in kimarite.CELL_INDEX
        assert kimarite.NIGE_CELL in kimarite.CELL_INDEX

    def test_unknown_cell_falls_back_to_sonota(self) -> None:
        # 恵まれ_1 は凍結リストに無い → その他_1 に畳まれる
        assert kimarite.cell_of("恵まれ", 1) == "その他_1"
        assert kimarite.cell_of("まくり", 3) == "まくり_3"

    def test_nige_from_course_two_is_a_real_class(self) -> None:
        """展示進入基準なので 逃げ_2 は起きる (§12.2)。捨てずにクラスとして持つ。"""
        assert kimarite.cell_of("逃げ", 2) == "逃げ_2"


class TestFeatures:
    def test_feature_name_count_matches_vector_length(self) -> None:
        card = {f"艇{b}_全国勝率": "5.0" for b in range(1, 7)}
        for state in ("daily", "realtime"):
            x = kimarite.build_features(state, card, None, None, None, "01", 1.0)
            assert len(x) == len(kimarite.feature_names(state))

    def test_realtime_orders_boats_by_entry_course(self) -> None:
        """前付けがあると、コース順に並べ替わる。"""
        card = {f"艇{b}_全国勝率": f"{b}.0" for b in range(1, 7)}
        # 艇5 が 2 コース、艇2 が 5 コース
        stt = {f"艇{b}_コース": c for b, c in zip(range(1, 7), [1, 5, 3, 4, 2, 6])}
        names = kimarite.feature_names("realtime")
        x = kimarite.build_features("realtime", card, stt, None, None, "01", 1.0)
        idx = names.index("c2_全国勝率")
        assert x[idx] == 5.0  # 2 コースには 艇5 が入っている

    def test_daily_keeps_waku_order_even_with_stt(self) -> None:
        card = {f"艇{b}_全国勝率": f"{b}.0" for b in range(1, 7)}
        stt = {f"艇{b}_コース": c for b, c in zip(range(1, 7), [1, 5, 3, 4, 2, 6])}
        names = kimarite.feature_names("daily")
        x = kimarite.build_features("daily", card, stt, None, None, "01", 1.0)
        assert x[names.index("w2_全国勝率")] == 2.0

    def test_duplicate_entry_courses_fall_back_to_wakunari(self) -> None:
        stt = {f"艇{b}_コース": "1" for b in range(1, 7)}
        assert kimarite.entry_courses(stt) == [1, 2, 3, 4, 5, 6]

    def test_missing_values_become_nan_not_zero(self) -> None:
        """欠損を 0 にすると「勝率 0」と区別できない。NaN にして補完側に任せる。"""
        names = kimarite.feature_names("daily")
        x = kimarite.build_features("daily", {}, None, None, None, "01", 1.0)
        assert math.isnan(x[names.index("w1_全国勝率")])
        # F 本数だけは「記載なし = F なし」なので 0
        assert x[names.index("w1_F本数")] == 0.0

    def test_stadium_one_hot(self) -> None:
        names = kimarite.feature_names("daily")
        x = kimarite.build_features("daily", {}, None, None, None, "24", 1.0)
        assert x[names.index("場_24")] == 1.0
        assert x[names.index("場_01")] == 0.0


def _fake_model_csv(path: Path, state: str, nige_weight: float = 0.0) -> None:
    """全クラス等確率の係数 CSV。``nige_weight`` で 逃げ_1 だけ切片をずらせる。"""
    names = kimarite.feature_names(state)
    n = len(names)
    header = ["行種別", "クラス", "切片"] + names
    rows = [
        ["median", "", ""] + ["0"] * n,
        ["center", "", ""] + ["0"] * n,
        ["scale", "", ""] + ["1"] * n,
    ]
    for cell in kimarite.CELLS:
        b = nige_weight if cell == kimarite.NIGE_CELL else 0.0
        rows.append(["coef", cell, str(b)] + ["0"] * n)
    _write(path, header, rows)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    _write(
        tmp_path / "data" / "programs" / "race_cards" / "2026" / "08" / "12.csv",
        CARD_HEADER,
        [["202608120101", "2026-08-12", "01", "1R"] + ["5.0"] * 48],
    )
    _write(
        tmp_path / "data" / "previews" / "stt" / "2026" / "08" / "12.csv",
        STT_HEADER,
        [["202608120101"] + [v for b in range(1, 7) for v in (str(b), "0.15")]],
    )
    for state in ("daily", "realtime"):
        _fake_model_csv(
            tmp_path / build_kimarite_probs.TABLES_DIR / f"cell_coef_{state}.csv",
            state,
        )
    return tmp_path


class TestInference:
    def test_probabilities_sum_to_one(self, repo: Path) -> None:
        rows = build_kimarite_probs.build_day(repo, DAY, "realtime")
        assert len(rows) == 1
        probs = [float(v) for v in rows[0][6:]]
        assert sum(probs) == pytest.approx(1.0, abs=1e-3)

    def test_arehido_is_one_minus_nige(self, repo: Path) -> None:
        rows = build_kimarite_probs.build_day(repo, DAY, "realtime")
        header_idx = build_kimarite_probs.HEADER.index(f"P_{kimarite.NIGE_CELL}")
        nige = float(rows[0][header_idx])
        assert float(rows[0][5]) == pytest.approx(1.0 - nige, abs=1e-3)

    def test_intercept_shifts_nige_probability(self, repo: Path) -> None:
        """切片を上げると P(逃げ) が上がり、荒れ度が下がる。"""
        before = float(build_kimarite_probs.build_day(repo, DAY, "realtime")[0][5])
        _fake_model_csv(
            repo / build_kimarite_probs.TABLES_DIR / "cell_coef_realtime.csv",
            "realtime",
            nige_weight=5.0,
        )
        after = float(build_kimarite_probs.build_day(repo, DAY, "realtime")[0][5])
        assert after < before

    def test_daily_and_realtime_coexist(self, repo: Path) -> None:
        build_kimarite_probs.write_day(repo, DAY, "daily", None)
        build_kimarite_probs.write_day(repo, DAY, "realtime", None)
        path = build_kimarite_probs.probs_csv_path(repo, DAY)
        with open(path, encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        assert [r["状態"] for r in rows] == ["daily", "realtime"]

    def test_upsert_is_idempotent(self, repo: Path) -> None:
        build_kimarite_probs.write_day(repo, DAY, "realtime", None)
        path = build_kimarite_probs.probs_csv_path(repo, DAY)
        first = path.read_text(encoding="utf-8")
        build_kimarite_probs.write_day(repo, DAY, "realtime", None)
        assert path.read_text(encoding="utf-8") == first

    def test_missing_model_raises_with_actionable_message(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="build_kimarite.py"):
            build_kimarite_probs.build_day(tmp_path, DAY, "realtime")

    def test_class_mismatch_is_detected(self, repo: Path) -> None:
        """係数 CSV が古い (クラス構成が違う) 場合は黙って動かず落ちる。"""
        path = repo / build_kimarite_probs.TABLES_DIR / "cell_coef_realtime.csv"
        lines = path.read_text(encoding="utf-8").splitlines()
        path.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")
        with pytest.raises(ValueError, match="クラス構成"):
            build_kimarite_probs.build_day(repo, DAY, "realtime")

    def test_no_race_cards_returns_empty(self, repo: Path) -> None:
        assert build_kimarite_probs.build_day(repo, dt.date(2026, 8, 13), "realtime") == []

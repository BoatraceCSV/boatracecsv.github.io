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
from boatrace import kimarite_blend as blend  # type: ignore[import-not-found]

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


class TestCalibration:
    """校正集計 (build_kimarite_calibration)。"""

    @staticmethod
    def _setup(repo: Path, pred_upset: str, winner: str, kimarite_name: str) -> None:
        import build_kimarite_probs as bkp

        probs_header = bkp.HEADER
        # 正解セルの確率だけ 0.5、残りを等分して合計 1 にする
        rest = 0.5 / (len(kimarite.CELLS) - 1)
        probs = []
        for cell in kimarite.CELLS:
            probs.append("0.500000" if cell == kimarite_name else f"{rest:.6f}")
        _write(
            repo / "data" / "estimate" / "kimarite" / "2026" / "08" / "12.csv",
            probs_header,
            [["202608120101", "2026-08-12", "01", "1R", "realtime", pred_upset] + probs],
        )
        _write(
            repo / "data" / "results" / "realtime" / "2026" / "08" / "12.csv",
            ["レースコード", "レース日", "決まり手", "1着_艇番"],
            [["202608120101", "2026-08-12", "逃　げ" if kimarite_name == "逃げ_1" else "まくり", winner]],
        )

    def test_nige_counts_as_not_upset(self, tmp_path: Path) -> None:
        import build_kimarite_calibration as cal

        self._setup(tmp_path, "0.3000", "1", "逃げ_1")
        rows = cal.build_rows(cal.collect(tmp_path, None))
        total = next(r for r in rows if r[0] == "合計")
        assert total[1] == 1
        assert float(total[3]) == 0.0  # 実測荒れ度

    def test_non_nige_counts_as_upset(self, tmp_path: Path) -> None:
        import build_kimarite_calibration as cal

        self._setup(tmp_path, "0.6000", "3", "まくり_3")
        rows = cal.build_rows(cal.collect(tmp_path, None))
        total = next(r for r in rows if r[0] == "合計")
        assert float(total[3]) == 1.0

    def test_daily_rows_are_excluded(self, tmp_path: Path) -> None:
        """朝の暫定予測は校正統計に混ぜない。"""
        import build_kimarite_calibration as cal
        import build_kimarite_probs as bkp

        self._setup(tmp_path, "0.3000", "1", "逃げ_1")
        path = tmp_path / "data" / "estimate" / "kimarite" / "2026" / "08" / "12.csv"
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader)
            row = next(reader)
        row[bkp.HEADER.index("状態")] = "daily"
        _write(path, header, [row])
        assert cal.build_rows(cal.collect(tmp_path, None)) == []

    def test_band_label_covers_the_full_range(self) -> None:
        import build_kimarite_calibration as cal

        for v in (0.0, 0.15, 0.5, 0.99, 1.0):
            assert cal.band_label(v)


class TestPairTable:
    """Stage2: 決まり手セル条件付きの 2-3 着テーブル。"""

    @staticmethod
    def _repo(tmp_path: Path) -> Path:
        _write(
            tmp_path / "data" / "results" / "realtime" / "2026" / "08" / "12.csv",
            ["レースコード", "レース日", "決まり手", "1着_艇番", "2着_艇番", "3着_艇番"],
            [
                # まくり差し 1着3c → 2着1c (内が残る形)
                ["202608120101", "2026-08-12", "まくり差し", "3", "1", "4"],
                ["202608120102", "2026-08-12", "まくり差し", "3", "1", "5"],
                # 2着が 2c のケースも入れておく (周辺分布に 2c を乗せるため。
                # 収縮の事前分布は m2 ⊗ m3 なので、m2 に無いコースは
                # どれだけ収縮しても 0 のまま持ち上がらない)
                ["202608120104", "2026-08-12", "まくり差し", "3", "2", "1"],
                # まくり 1着3c → 2着4c (外が続く形)
                ["202608120103", "2026-08-12", "まくり", "3", "4", "5"],
            ],
        )
        return tmp_path

    def test_same_first_course_splits_by_kimarite(self, tmp_path: Path) -> None:
        """同じ 1着3コースでも、まくりとまくり差しで別セルに入る。"""
        import build_kimarite_pairs as bkp

        counts, used = bkp.collect(self._repo(tmp_path), None, None)
        assert used == 4
        assert counts["まくり差し_3"][(1, 4)] == 1
        assert counts["まくり差し_3"][(1, 5)] == 1
        assert counts["まくり_3"][(4, 5)] == 1
        assert (4, 5) not in counts["まくり差し_3"]

    def test_probabilities_normalize_per_cell(self, tmp_path: Path) -> None:
        import build_kimarite_pairs as bkp

        counts, _ = bkp.collect(self._repo(tmp_path), None, None)
        rows = bkp.build_rows(counts, k=150.0)
        assert len(rows) == len(kimarite.CELLS) * 20
        by_cell: dict[str, float] = {}
        for cell, _a, _b, _n, prob in rows:
            by_cell[cell] = by_cell.get(cell, 0.0) + float(prob)
        for cell, total in by_cell.items():
            assert total == pytest.approx(1.0, abs=1e-5), cell

    def test_shrinkage_pulls_unobserved_cells_off_zero(self, tmp_path: Path) -> None:
        import build_kimarite_pairs as bkp

        counts, _ = bkp.collect(self._repo(tmp_path), None, None)
        raw = {(c, a, b): float(p) for c, a, b, _n, p in bkp.build_rows(counts, 0.0)}
        shrunk = {(c, a, b): float(p) for c, a, b, _n, p in bkp.build_rows(counts, 150.0)}
        # (2,4) は未観測だが、2着=2c と 3着=4c はどちらも周辺分布に乗っているので
        # 収縮で 0 から持ち上がる
        assert raw[("まくり差し_3", 2, 4)] == 0.0
        assert shrunk[("まくり差し_3", 2, 4)] > 0.0
        # (2,6) は 3着=6c が周辺分布に無いので、収縮しても 0 のまま
        assert shrunk[("まくり差し_3", 2, 6)] == 0.0

    def test_pair_keys_exclude_first_course(self) -> None:
        import build_kimarite_pairs as bkp

        for first in range(1, 7):
            keys = bkp.pair_keys(first)
            assert len(keys) == 20
            assert all(first not in pair and pair[0] != pair[1] for pair in keys)


class TestBlend:
    """Stage1 × Stage2 の合成と Plackett-Luce とのブレンド (§4.3)。"""

    @staticmethod
    def _flat_table() -> dict[str, dict[tuple[int, int], float]]:
        """全セルが 20 ペア一様のペア表。決まり手の型を消した対照。"""
        import build_kimarite_pairs as bkp

        return {
            cell: {key: 1.0 / 20 for key in bkp.pair_keys(blend.first_course_of(cell))}
            for cell in kimarite.CELLS
        }

    def test_triples_cover_all_120(self) -> None:
        assert len(blend.TRIPLES) == 120
        assert len(set(blend.TRIPLES)) == 120

    def test_joint_is_a_distribution_and_respects_first_course(self) -> None:
        """P1 を 1 セルに集中させると、そのセルの 1着コースだけに確率が乗る。"""
        p1 = [0.0] * len(kimarite.CELLS)
        p1[kimarite.CELL_INDEX["まくり_4"]] = 1.0
        probs = blend.kimarite_joint(p1, self._flat_table(), [0.0] * 6)

        assert sum(probs.values()) == pytest.approx(1.0)
        assert all(p == 0.0 for t, p in probs.items() if t[0] != 4)
        # 1着4コース の 20 通りに一様に散る
        assert probs[(4, 1, 2)] == pytest.approx(1.0 / 20)

    def test_gamma_moves_strong_boats_up(self) -> None:
        """γ > 0 なら、同じセルでも強い艇の入るコースが 2着に来やすくなる。"""
        p1 = [0.0] * len(kimarite.CELLS)
        p1[kimarite.CELL_INDEX["まくり_4"]] = 1.0
        tab = self._flat_table()
        z = [0.0] * 6
        z[0] = 2.0  # 1 コースだけ強い

        flat = blend.kimarite_joint(p1, tab, [0.0] * 6, gamma=0.5)
        tilted = blend.kimarite_joint(p1, tab, z, gamma=0.5)
        assert tilted[(4, 1, 2)] > flat[(4, 1, 2)]
        assert tilted[(4, 2, 3)] < flat[(4, 2, 3)]
        # γ=0 なら 強さpt は効かない
        assert blend.kimarite_joint(p1, tab, z, gamma=0.0) == pytest.approx(flat)

    def test_plackett_luce_is_a_distribution_and_orders_by_strength(self) -> None:
        pl = blend.plackett_luce([1.0, 0.0, 0.0, 0.0, 0.0, -1.0])
        assert sum(pl.values()) == pytest.approx(1.0)
        assert pl[(1, 2, 3)] > pl[(2, 1, 3)] > pl[(6, 1, 2)]

    def test_blend_is_convex_combination(self) -> None:
        p1 = [1.0 / len(kimarite.CELLS)] * len(kimarite.CELLS)
        tab = self._flat_table()
        z = [0.5, 0.2, 0.0, -0.1, -0.3, -0.4]
        kim = blend.kimarite_joint(p1, tab, z)
        pl = blend.plackett_luce(z)
        mixed = blend.blend(p1, tab, z)
        assert sum(mixed.values()) == pytest.approx(1.0)
        for t in blend.TRIPLES:
            expected = blend.BLEND_W * kim[t] + (1 - blend.BLEND_W) * pl[t]
            assert mixed[t] == pytest.approx(expected)

    def test_top_picks_skip_first_course_one(self) -> None:
        """穴予想なので 1 コース頭は買わない (§5.2)。"""
        probs = {t: 0.0 for t in blend.TRIPLES}
        probs[(1, 2, 3)] = 0.5   # 一番濃いが 1 コース頭なので買わない
        probs[(3, 1, 2)] = 0.2
        probs[(4, 1, 2)] = 0.1
        picks = blend.top_picks(probs, top_k=2)
        assert picks == [(3, 1, 2), (4, 1, 2)]
        # 除外しなければ 1 コース頭も入る
        assert blend.top_picks(probs, top_k=1, exclude_first_course=None) == [(1, 2, 3)]

    def test_top_picks_break_ties_deterministically(self) -> None:
        """同確率は出目の昇順。再実行で買い目が入れ替わらないようにする。"""
        probs = {t: 0.0 for t in blend.TRIPLES}
        for t in ((5, 6, 4), (2, 1, 3), (2, 3, 1)):
            probs[t] = 0.1
        assert blend.top_picks(probs, top_k=3) == [(2, 1, 3), (2, 3, 1), (5, 6, 4)]

    def test_cell_with_empty_pair_row_is_dropped_not_crashed(self) -> None:
        """学習窓に 1 度も出なかったセルは、確率を落として残りで正規化する。"""
        tab = self._flat_table()
        tab["逃げ_2"] = {key: 0.0 for key in tab["逃げ_2"]}
        p1 = [0.0] * len(kimarite.CELLS)
        p1[kimarite.CELL_INDEX["逃げ_2"]] = 0.5
        p1[kimarite.CELL_INDEX["まくり_4"]] = 0.5
        probs = blend.kimarite_joint(p1, tab, [0.0] * 6)
        assert sum(probs.values()) == pytest.approx(1.0)
        assert all(p == 0.0 for t, p in probs.items() if t[0] == 2)

    def test_z_scores_follow_entry_courses(self) -> None:
        # コース1←艇1, コース2←艇5, コース5←艇2 (前付け)
        boat_at = [0, 1, 5, 3, 4, 2, 6]
        z = blend.z_scores([60.0, 40.0, 50.0, 50.0, 70.0, 50.0], boat_at)
        assert z[0] == pytest.approx(1.0)   # コース1 = 艇1 (60)
        assert z[1] == pytest.approx(2.0)   # コース2 = 艇5 (70)
        assert z[4] == pytest.approx(-1.0)  # コース5 = 艇2 (40)

    def test_pair_table_missing_cell_is_reported(self, tmp_path: Path) -> None:
        path = tmp_path / "pair_table.csv"
        _write(path, ["セル", "2着コース", "3着コース", "n", "確率"],
               [["まくり_4", "1", "2", "1", "1.0"]])
        with pytest.raises(ValueError, match="セルが足りません"):
            blend.load_pair_table(path)

    def test_pair_table_missing_file_says_how_to_build(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="build_kimarite_pairs.py"):
            blend.load_pair_table(tmp_path / "nope.csv")


class TestKimaritePicks:
    """`build_kimarite_picks.py`: 合成 → 出目 → CSV の一連。"""

    INDEX_HEADER = (
        ["レースコード", "レース日", "レース場コード", "レース回", "状態"]
        + [f"{b}枠_強さpt" for b in range(1, 7)]
    )

    @classmethod
    def _repo(cls, tmp_path: Path) -> Path:
        import build_kimarite_pairs as bkp

        _write(
            tmp_path / "data" / "estimate" / "v10_kimarite" / "2026" / "08" / "12.csv",
            cls.INDEX_HEADER,
            [
                ["202608120301", "2026-08-12", "03", "1R", "daily",
                 "60", "55", "70", "50", "45", "40"],
                ["202608120301", "2026-08-12", "03", "1R", "realtime",
                 "60", "55", "70", "50", "45", "40"],
            ],
        )
        # Stage1: すべての確率を まくり_3 に集中させる (合成の効きを見やすくする)
        probs = [0.0] * len(kimarite.CELLS)
        probs[kimarite.CELL_INDEX["まくり_3"]] = 1.0
        _write(
            tmp_path / "data" / "estimate" / "kimarite" / "2026" / "08" / "12.csv",
            build_kimarite_probs.HEADER,
            [
                ["202608120301", "2026-08-12", "03", "1R", state, "0.9"]
                + [f"{v:.6f}" for v in probs]
                for state in ("daily", "realtime")
            ],
        )
        # Stage2: まくり_3 は (4,5) が濃い。他のセルは一様
        rows = []
        for cell in kimarite.CELLS:
            keys = bkp.pair_keys(blend.first_course_of(cell))
            for a, b in keys:
                p = 0.81 if (cell, a, b) == ("まくり_3", 4, 5) else 0.01
                rows.append([cell, a, b, 1, f"{p:.6f}"])
        _write(tmp_path / blend.PAIR_TABLE_RELPATH,
               ["セル", "2着コース", "3着コース", "n", "確率"], rows)
        # 決まり手注釈 (両案共通の静的テーブル)
        _write(
            tmp_path / "data" / "estimate" / "suji" / "tables" / "kimarite_table.csv",
            ["場コード", "1着コース", "2着コース", "3着コース", "n", "最頻決まり手"],
            [["00", "3", "4", "5", "10", "まくり"]],
        )
        _write(
            tmp_path / "data" / "previews" / "stt" / "2026" / "08" / "12.csv",
            ["レースコード"] + [f"艇{b}_コース" for b in range(1, 7)],
            [["202608120301", "1", "2", "3", "4", "5", "6"]],
        )
        return tmp_path

    def test_top_pick_follows_the_pair_table(self, tmp_path: Path) -> None:
        import build_kimarite_picks as bkpk

        repo = self._repo(tmp_path)
        assert bkpk.write_day(repo, DAY, "realtime", None) == 1
        rows = list(csv.DictReader(
            open(bkpk.picks_csv_path(repo, DAY), encoding="utf-8")))
        assert len(rows) == 1
        # 枠なりなので コース = 艇番。まくり_3 × (4,5) が最有力
        assert rows[0]["買い目1"] == "3-4-5"
        assert rows[0]["決まり手1"] == "まくり"
        assert all(rows[0][f"買い目{i}"] for i in range(1, 6))

    def test_picks_are_mapped_through_entry_courses(self, tmp_path: Path) -> None:
        """前付けがあると、同じコース並びでも艇番が変わる。"""
        import build_kimarite_picks as bkpk

        repo = self._repo(tmp_path)
        _write(
            repo / "data" / "previews" / "stt" / "2026" / "08" / "12.csv",
            ["レースコード"] + [f"艇{b}_コース" for b in range(1, 7)],
            [["202608120301", "1", "5", "3", "4", "2", "6"]],
        )
        bkpk.write_day(repo, DAY, "realtime", None)
        rows = list(csv.DictReader(
            open(bkpk.picks_csv_path(repo, DAY), encoding="utf-8")))
        # コース (3,4,5) → 艇 (3,4,2)
        assert rows[0]["買い目1"] == "3-4-2"

    def test_daily_and_realtime_rows_coexist(self, tmp_path: Path) -> None:
        import build_kimarite_picks as bkpk

        repo = self._repo(tmp_path)
        bkpk.write_day(repo, DAY, "daily", None)
        bkpk.write_day(repo, DAY, "realtime", None)
        rows = list(csv.DictReader(
            open(bkpk.picks_csv_path(repo, DAY), encoding="utf-8")))
        assert [r["状態"] for r in rows] == ["daily", "realtime"]

    def test_race_without_stage1_probs_is_skipped(self, tmp_path: Path) -> None:
        """荒れ度メーターが未生成のレースは買い目を出さない (無言で 0 行)。"""
        import build_kimarite_picks as bkpk

        repo = self._repo(tmp_path)
        _write(repo / "data" / "estimate" / "kimarite" / "2026" / "08" / "12.csv",
               build_kimarite_probs.HEADER, [])
        assert bkpk.write_day(repo, DAY, "realtime", None) == 0

    def test_missing_index_says_how_to_build(self, tmp_path: Path) -> None:
        import build_kimarite_picks as bkpk

        repo = self._repo(tmp_path)
        (repo / "data" / "estimate" / "v10_kimarite" / "2026" / "08" / "12.csv").unlink()
        with pytest.raises(FileNotFoundError, match="--predictor v10_kimarite"):
            bkpk.write_day(repo, DAY, "realtime", None)


class TestLogLossAB:
    """`build_kimarite_logloss.py`: 主判定 (PL 対 ブレンド) の月次集計。"""

    @staticmethod
    def _repo(tmp_path: Path, *, favour_blend: bool) -> Path:
        """1 レースだけの最小リポジトリ。

        結果は 3-4-5 (枠なりなのでコースも 3-4-5)。ペア表は まくり_3 の (4,5) に
        寄せてある。``favour_blend`` は Stage1 が当てたか外したかを切り替える:
        まくり_3 なら 1着3コースを指しているので当たり、まくり_5 なら
        1着5コースを指すので 決まり手モデルの寄与が 0 になり、ブレンドは
        PL を 0.3 倍に薄めただけの分だけ負ける。
        """
        import build_kimarite_pairs as bkp

        _write(
            tmp_path / "data" / "results" / "realtime" / "2026" / "08" / "13.csv",
            ["レースコード", "レース日", "決まり手", "1着_艇番", "2着_艇番", "3着_艇番"],
            [["202608130301", "2026-08-13", "まくり", "3", "4", "5"]],
        )
        _write(
            tmp_path / "data" / "estimate" / "v10_kimarite" / "2026" / "08" / "13.csv",
            TestKimaritePicks.INDEX_HEADER,
            [["202608130301", "2026-08-13", "03", "1R", "realtime",
              "60", "55", "50", "50", "45", "40"]],
        )
        probs = [0.0] * len(kimarite.CELLS)
        probs[kimarite.CELL_INDEX["まくり_3" if favour_blend else "まくり_5"]] = 1.0
        _write(
            tmp_path / "data" / "estimate" / "kimarite" / "2026" / "08" / "13.csv",
            build_kimarite_probs.HEADER,
            [["202608130301", "2026-08-13", "03", "1R", "realtime", "0.9"]
             + [f"{v:.6f}" for v in probs]],
        )
        rows = []
        for cell in kimarite.CELLS:
            for a, b in bkp.pair_keys(blend.first_course_of(cell)):
                p = 0.81 if (cell == "まくり_3" and (a, b) == (4, 5)) else 0.01
                rows.append([cell, a, b, 1, f"{p:.6f}"])
        _write(tmp_path / blend.PAIR_TABLE_RELPATH,
               ["セル", "2着コース", "3着コース", "n", "確率"], rows)
        return tmp_path

    def test_blend_wins_when_the_model_points_at_the_result(self, tmp_path: Path) -> None:
        import build_kimarite_logloss as ll

        rows = ll.build_rows(ll.collect(self._repo(tmp_path, favour_blend=True),
                                        None, None))
        assert [r[0] for r in rows] == ["2026-08", ll.TOTAL_LABEL]
        assert rows[0][1] == 1
        assert float(rows[0][4]) > 0  # 改善nat > 0 → ブレンドの勝ち

    def test_blend_loses_when_the_model_points_elsewhere(self, tmp_path: Path) -> None:
        import build_kimarite_logloss as ll

        rows = ll.build_rows(ll.collect(self._repo(tmp_path, favour_blend=False),
                                        None, None))
        assert float(rows[0][4]) < 0

    def test_daily_rows_are_excluded(self, tmp_path: Path) -> None:
        """朝バッチは進入も強さpt も暫定値なので集計しない (回収率と同じ規約)。"""
        import build_kimarite_logloss as ll

        repo = self._repo(tmp_path, favour_blend=True)
        for rel in ("estimate/v10_kimarite", "estimate/kimarite"):
            path = repo / "data" / rel / "2026" / "08" / "13.csv"
            text = path.read_text(encoding="utf-8").replace("realtime", "daily")
            path.write_text(text, encoding="utf-8")
        assert ll.build_rows(ll.collect(repo, None, None)) == []

    def test_from_date_filters(self, tmp_path: Path) -> None:
        import build_kimarite_logloss as ll

        repo = self._repo(tmp_path, favour_blend=True)
        assert ll.collect(repo, "2026-08-14", None) == {}
        assert ll.collect(repo, "2026-08-13", None)

    def test_summarize_handles_empty(self) -> None:
        import build_kimarite_logloss as ll

        assert ll.summarize("2026-08", []) == ["2026-08", 0, "", "", "", "", ""]

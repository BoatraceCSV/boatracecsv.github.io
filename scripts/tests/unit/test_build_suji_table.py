"""Unit tests for ``scripts/build_suji_table.py`` (穴予想 v9_suji のスジ表).

Covers:
- ``suji_keys``: 1着コースを除く 20 通りの (2着, 3着) 列挙
- ``_entry_courses``: 展示進入の読み取りと枠なりフォールバック / 重複の棄却
- ``_finish_courses``: 艇番 → コースの写像と欠損・重複の棄却
- ``collect``: 学習窓 (from 以上 / to 未満) と stt 突合
- ``build_suji_rows``: 確率の正規化と収縮 (k=0 は生の経験分布)
- ``build_kimarite_rows``: 全 120 出目・最頻決まり手・分布
- ``main``: CLI 経由の出力

Reference design: ``docs/design/ana_prediction.md`` (§13 A案 / §14 決まり手の表示方式)。
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

import build_suji_table  # type: ignore[import-not-found]

RESULT_HEADER = [
    "レースコード", "レース日", "決まり手",
    "1着_艇番", "2着_艇番", "3着_艇番",
]
STT_HEADER = ["レースコード"] + [f"艇{b}_コース" for b in range(1, 7)]


def _write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """4 レースぶんの最小リポジトリ。

    - r1 (2026-01-01): 枠なり(stt あり)。着順 = 艇 1,2,3 → コース 1-2-3 / 逃げ
    - r2 (2026-01-01): stt 無し → 枠なりフォールバック。艇 1,2,3 → コース 1-2-3 / 逃げ
    - r3 (2026-01-01): stt 無し → 枠なり。艇 1,3,4 → コース 1-3-4 / 逃げ
    - r4 (2026-02-01): **前付け**(艇4 が 2 コース、艇2 が 4 コース)。
      艇 3,4,1 → コース 3-2-1 / まくり差し

    1着コース 1 の分布を **積形にならない** (2,3)×2 / (3,4)×1 にしてあるので、
    収縮が周辺分布の積へ引き戻すことを検証できる。
    """
    _write_csv(
        tmp_path / "data" / "results" / "realtime" / "2026" / "01" / "01.csv",
        RESULT_HEADER,
        [
            ["202601010101", "2026-01-01", "逃　げ", "1", "2", "3"],
            ["202601010102", "2026-01-01", "逃　げ", "1", "2", "3"],
            ["202601010103", "2026-01-01", "逃　げ", "1", "3", "4"],
        ],
    )
    _write_csv(
        tmp_path / "data" / "results" / "realtime" / "2026" / "02" / "01.csv",
        RESULT_HEADER,
        [["202602010101", "2026-02-01", "まくり差し", "3", "4", "1"]],
    )
    # r1 のみ stt あり (枠なり)。r2 / r3 は stt 無し → 枠なりフォールバック。
    # r4 は前付け。
    _write_csv(
        tmp_path / "data" / "previews" / "stt" / "2026" / "01" / "01.csv",
        STT_HEADER,
        [["202601010101", "1", "2", "3", "4", "5", "6"]],
    )
    _write_csv(
        tmp_path / "data" / "previews" / "stt" / "2026" / "02" / "01.csv",
        STT_HEADER,
        [["202602010101", "1", "4", "3", "2", "5", "6"]],
    )
    return tmp_path


class TestSujiKeys:
    def test_excludes_first_and_enumerates_20(self) -> None:
        for first in range(1, 7):
            keys = build_suji_table.suji_keys(first)
            assert len(keys) == 20
            assert len(set(keys)) == 20
            assert all(first not in pair for pair in keys)
            assert all(a != b for a, b in keys)


class TestEntryCourses:
    def test_reads_maegake(self) -> None:
        row = {f"艇{b}_コース": v for b, v in zip(range(1, 7), "143256")}
        assert build_suji_table._entry_courses(row) == [1, 4, 3, 2, 5, 6]

    def test_missing_falls_back_to_wakunari(self) -> None:
        # 艇3 だけ欠損 → 艇3 は 3 コース扱いで枠なりが成立する
        row = {f"艇{b}_コース": v for b, v in zip(range(1, 7), ["1", "2", "", "4", "5", "6"])}
        assert build_suji_table._entry_courses(row) == [1, 2, 3, 4, 5, 6]

    def test_duplicate_course_rejected(self) -> None:
        row = {f"艇{b}_コース": v for b, v in zip(range(1, 7), "113456")}
        assert build_suji_table._entry_courses(row) is None

    def test_empty_row_is_wakunari(self) -> None:
        assert build_suji_table._entry_courses({}) == [1, 2, 3, 4, 5, 6]


class TestFinishCourses:
    def test_maps_boats_to_courses(self) -> None:
        row = {"1着_艇番": "3", "2着_艇番": "4", "3着_艇番": "1"}
        # 艇4 が 2 コース、艇2 が 4 コース
        assert build_suji_table._finish_courses(row, [1, 4, 3, 2, 5, 6]) == (3, 2, 1)

    def test_missing_returns_none(self) -> None:
        row = {"1着_艇番": "1", "2着_艇番": "", "3着_艇番": "3"}
        assert build_suji_table._finish_courses(row, [1, 2, 3, 4, 5, 6]) is None

    def test_out_of_range_returns_none(self) -> None:
        row = {"1着_艇番": "7", "2着_艇番": "2", "3着_艇番": "3"}
        assert build_suji_table._finish_courses(row, [1, 2, 3, 4, 5, 6]) is None


class TestCollect:
    def test_counts_all_races(self, repo: Path) -> None:
        suji, kim, used = build_suji_table.collect(repo, None, None)
        pooled = build_suji_table.POOLED_STADIUM
        assert used == 4
        assert suji[(pooled, 1)][(2, 3)] == 2
        assert suji[(pooled, 1)][(3, 4)] == 1
        # 前付けレースはコース空間で 3-2-1
        assert suji[(pooled, 3)][(2, 1)] == 1
        assert kim[(3, 2, 1)]["まくり差し"] == 1

    def test_counts_are_also_kept_per_stadium(self, repo: Path) -> None:
        """レースコード = YYYYMMDD + 場コード(2桁) + レース回(2桁)。"""
        suji, _, _ = build_suji_table.collect(repo, None, None)
        # フィクスチャは全レースが 場コード "01"
        assert suji[("01", 1)][(2, 3)] == 2
        assert suji[("01", 3)][(2, 1)] == 1

    def test_from_date_is_inclusive_and_to_date_exclusive(self, repo: Path) -> None:
        _, _, used = build_suji_table.collect(repo, "2026-01-01", "2026-02-01")
        assert used == 3  # 2026-02-01 のレースは除外される
        _, _, used = build_suji_table.collect(repo, "2026-02-01", None)
        assert used == 1

    def test_stt_missing_falls_back_to_wakunari(self, repo: Path) -> None:
        # r3 は stt が無いが、枠なり扱いでコース 1-3-4 として数えられている
        suji, _, _ = build_suji_table.collect(repo, None, None)
        assert suji[(build_suji_table.POOLED_STADIUM, 1)][(3, 4)] == 1


class TestBuildSujiRows:
    def test_probabilities_normalize_per_first_course(self, repo: Path) -> None:
        suji, _, _ = build_suji_table.collect(repo, None, None)
        rows = build_suji_table.build_suji_rows(suji, k=0.0, by_stadium=False)
        assert len(rows) == 120
        for first in range(1, 7):
            total = sum(float(r[5]) for r in rows if r[1] == first)
            assert total == pytest.approx(1.0, abs=1e-6)

    def test_k_zero_is_raw_empirical(self, repo: Path) -> None:
        suji, _, _ = build_suji_table.collect(repo, None, None)
        rows = {(r[1], r[2], r[3]): r for r in build_suji_table.build_suji_rows(suji, 0.0, by_stadium=False)}
        # 1着コース 1 の観測は (2,3)×2 と (3,4)×1
        assert float(rows[(1, 2, 3)][5]) == pytest.approx(2 / 3)
        assert float(rows[(1, 3, 4)][5]) == pytest.approx(1 / 3)
        assert float(rows[(1, 2, 4)][5]) == pytest.approx(0.0)

    def test_shrinkage_pulls_toward_marginal_product(self, repo: Path) -> None:
        suji, _, _ = build_suji_table.collect(repo, None, None)
        raw = {(r[1], r[2], r[3]): float(r[5])
               for r in build_suji_table.build_suji_rows(suji, 0.0, by_stadium=False)}
        shrunk = {(r[1], r[2], r[3]): float(r[5])
                  for r in build_suji_table.build_suji_rows(suji, 100.0, by_stadium=False)}
        # 観測されたセルは事前分布へ引き戻され、未観測セルは 0 から持ち上がる
        assert shrunk[(1, 2, 3)] < raw[(1, 2, 3)]
        assert shrunk[(1, 2, 4)] > raw[(1, 2, 4)]
        for first in range(1, 7):
            total = sum(v for (f, _, _), v in shrunk.items() if f == first)
            assert total == pytest.approx(1.0, abs=1e-6)

    def test_by_stadium_emits_pooled_plus_per_stadium(self, repo: Path) -> None:
        """場別モードは 全場プール 120 行 + 場ごとに 120 行を出す。

        本番は全場プールのみ (場別は予測を悪化させる。
        notebooks/ana_prediction/report.md)。ここでは形だけ担保する。
        """
        suji, _, _ = build_suji_table.collect(repo, None, None)
        rows = build_suji_table.build_suji_rows(suji, 50.0, by_stadium=True)
        assert len(rows) == 120 * 2  # プール + 場コード "01" の 1 場ぶん
        for stadium in {r[0] for r in rows}:
            for first in range(1, 7):
                total = sum(float(r[5]) for r in rows
                            if r[0] == stadium and r[1] == first)
                assert total == pytest.approx(1.0, abs=1e-6)

    def test_unseen_first_course_is_uniform(self, repo: Path) -> None:
        suji, _, _ = build_suji_table.collect(repo, None, None)
        rows = {(r[1], r[2], r[3]): float(r[5])
                for r in build_suji_table.build_suji_rows(suji, 0.0, by_stadium=False)}
        # 1着コース 5 は 1 度も観測されていない → 一様 (1/20)
        assert rows[(5, 1, 2)] == pytest.approx(0.05)


class TestBuildKimariteRows:
    def test_all_120_combinations_present(self, repo: Path) -> None:
        _, kim, _ = build_suji_table.collect(repo, None, None)
        rows = build_suji_table.build_kimarite_rows(kim)
        assert len(rows) == 120
        assert len({(r[0], r[1], r[2]) for r in rows}) == 120

    def test_mode_and_shares(self, repo: Path) -> None:
        _, kim, _ = build_suji_table.collect(repo, None, None)
        rows = {(r[0], r[1], r[2]): r for r in build_suji_table.build_kimarite_rows(kim)}
        observed = rows[(3, 2, 1)]
        assert observed[3] == 1  # n
        assert observed[4] == "まくり差し"
        makuri_sashi_idx = 5 + build_suji_table.KIMARITE_ORDER.index("まくり差し")
        assert float(observed[makuri_sashi_idx]) == pytest.approx(1.0)

    def test_unobserved_combination_has_blank_mode(self, repo: Path) -> None:
        _, kim, _ = build_suji_table.collect(repo, None, None)
        rows = {(r[0], r[1], r[2]): r for r in build_suji_table.build_kimarite_rows(kim)}
        unobserved = rows[(5, 1, 2)]
        assert unobserved[3] == 0
        assert unobserved[4] == ""


class TestCli:
    def test_writes_both_tables(self, repo: Path, tmp_path: Path, monkeypatch) -> None:
        suji_out = tmp_path / "out" / "suji.csv"
        kim_out = tmp_path / "out" / "kim.csv"
        monkeypatch.setattr(
            "sys.argv",
            [
                "build_suji_table.py",
                "--repo-root", str(repo),
                "--suji-out", str(suji_out),
                "--kimarite-out", str(kim_out),
            ],
        )
        assert build_suji_table.main() == 0
        with open(suji_out, encoding="utf-8") as fh:
            suji_rows = list(csv.DictReader(fh))
        with open(kim_out, encoding="utf-8") as fh:
            kim_rows = list(csv.DictReader(fh))
        assert len(suji_rows) == 120
        assert len(kim_rows) == 120
        assert suji_rows[0].keys() >= {"場コード", "1着コース", "2着コース", "3着コース", "n", "確率"}

    def test_returns_error_when_no_races(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.setattr(
            "sys.argv",
            ["build_suji_table.py", "--repo-root", str(tmp_path)],
        )
        assert build_suji_table.main() == 1

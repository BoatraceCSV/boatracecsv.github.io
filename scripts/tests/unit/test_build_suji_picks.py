"""Unit tests for ``scripts/build_suji_picks.py`` (穴予想 v9_suji の買い目 CSV).

Covers:
- ``build_row``: 1着の選び方(1 コース以外で 強さpt 最大)・前付け時のコース→艇番写像
- ``build_day`` / ``write_day``: daily と realtime の共存(upsert しても消えない)
- 出力列と 5 点固定
- 集計母数の担保: daily 行と realtime 行が 状態 で区別できること

Reference design: ``docs/design/ana_prediction.md`` (§13 A案)。
"""

from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pytest

import build_suji_picks  # type: ignore[import-not-found]
import build_suji_table  # type: ignore[import-not-found]

DAY = dt.date(2026, 8, 10)

INDEX_HEADER = (
    ["レースコード", "レース日", "レース場コード", "レース回", "状態"]
    + [f"{b}枠_強さpt" for b in range(1, 7)]
)
STT_HEADER = ["レースコード"] + [f"艇{b}_コース" for b in range(1, 7)]


def _write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """v9_suji の index と スジ表 / 決まり手表 を持つ最小リポジトリ。

    index は 2 レース。どちらも 3 号艇が 1 コース以外で最強 (強さpt 70)。
    - r1: 枠なり
    - r2: 前付け (艇5 が 2 コース、艇2 が 5 コース)
    """
    idx = tmp_path / "data" / "estimate" / "v9_suji" / "2026" / "08" / "10.csv"
    _write(
        idx,
        INDEX_HEADER,
        [
            ["202608100301", "2026-08-10", "03", "1R", "daily",
             "60", "55", "70", "50", "45", "40"],
            ["202608100301", "2026-08-10", "03", "1R", "realtime",
             "60", "55", "70", "50", "45", "40"],
            ["202608100302", "2026-08-10", "03", "2R", "realtime",
             "60", "55", "70", "50", "45", "40"],
        ],
    )
    _write(
        tmp_path / "data" / "previews" / "stt" / "2026" / "08" / "10.csv",
        STT_HEADER,
        [
            ["202608100301", "1", "2", "3", "4", "5", "6"],
            ["202608100302", "1", "5", "3", "4", "2", "6"],
        ],
    )
    # スジ表: 1着コース 3 のとき (1,4) > (1,2) > (2,4) > (1,5) > (1,6) の順
    suji_rows = []
    for c1 in range(1, 7):
        for a, b in build_suji_table.suji_keys(c1):
            if c1 == 3:
                p = {(1, 4): 0.30, (1, 2): 0.25, (2, 4): 0.20,
                     (1, 5): 0.15, (1, 6): 0.10}.get((a, b), 0.0)
            else:
                p = 1.0 / 20
            suji_rows.append([build_suji_table.POOLED_STADIUM, c1, a, b, 1,
                              f"{p:.6f}"])
    _write(tmp_path / build_suji_table.SUJI_OUT_RELPATH,
           build_suji_table.SUJI_HEADER, suji_rows)

    kim_rows = []
    for c1 in range(1, 7):
        for a, b in build_suji_table.suji_keys(c1):
            mark = "まくり差し" if (c1 == 3 and a == 1) else "まくり"
            kim_rows.append([c1, a, b, 1, mark] + ["0.0"] * 6)
    _write(tmp_path / build_suji_table.KIMARITE_OUT_RELPATH,
           build_suji_table.KIMARITE_HEADER, kim_rows)
    return tmp_path


def _read_out(repo: Path) -> list[dict[str, str]]:
    with open(build_suji_picks.picks_csv_path(repo, DAY), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


class TestBuildRow:
    def test_first_place_excludes_course_one(self, repo: Path) -> None:
        rows = build_suji_picks.build_day(repo, DAY, "realtime")
        r1 = next(r for r in rows if r[0] == "202608100301")
        # 1枠 (強さpt 60) は 1 コースなので除外され、3枠 (70) が選ばれる
        assert r1[5] == 3  # 1着コース
        assert r1[6] == 3  # 1着艇番

    def test_picks_are_top_k_in_probability_order(self, repo: Path) -> None:
        rows = build_suji_picks.build_day(repo, DAY, "realtime")
        r1 = next(r for r in rows if r[0] == "202608100301")
        assert r1[7:12] == ["3-1-4", "3-1-2", "3-2-4", "3-1-5", "3-1-6"]
        assert r1[12:17] == ["まくり差し", "まくり差し", "まくり", "まくり差し", "まくり差し"]

    def test_maegake_maps_courses_to_actual_boats(self, repo: Path) -> None:
        rows = build_suji_picks.build_day(repo, DAY, "realtime")
        r2 = next(r for r in rows if r[0] == "202608100302")
        # 艇5 が 2 コース、艇2 が 5 コース。1着は 3 コース = 艇3 のまま
        assert r2[5] == 3
        assert r2[6] == 3
        # (1着3c, 2着1c, 3着4c) → 艇 3-1-4 / (3c,2c,4c) → 艇 3-5-4
        assert r2[7] == "3-1-4"
        assert "3-5-4" in r2[7:12]

    def test_missing_strength_is_skipped(self, repo: Path) -> None:
        idx = repo / "data" / "estimate" / "v9_suji" / "2026" / "08" / "10.csv"
        _write(idx, INDEX_HEADER, [
            ["202608100301", "2026-08-10", "03", "1R", "realtime",
             "60", "", "70", "50", "45", "40"],
        ])
        assert build_suji_picks.build_day(repo, DAY, "realtime") == []


class TestDailyAndRealtimeCoexist:
    def test_daily_rows_survive_realtime_upsert(self, repo: Path) -> None:
        build_suji_picks.write_day(repo, DAY, "daily", None)
        assert {r["状態"] for r in _read_out(repo)} == {"daily"}
        build_suji_picks.write_day(repo, DAY, "realtime", None)
        rows = _read_out(repo)
        states = [r["状態"] for r in rows]
        assert states.count("daily") == 1
        assert states.count("realtime") == 2

    def test_realtime_upsert_is_idempotent(self, repo: Path) -> None:
        build_suji_picks.write_day(repo, DAY, "realtime", None)
        first = _read_out(repo)
        build_suji_picks.write_day(repo, DAY, "realtime", None)
        assert _read_out(repo) == first

    def test_partial_upsert_touches_only_listed_races(self, repo: Path) -> None:
        build_suji_picks.write_day(repo, DAY, "realtime", None)
        before = _read_out(repo)
        n = build_suji_picks.write_day(repo, DAY, "realtime", {"202608100302"})
        assert n == 1
        assert _read_out(repo) == before  # 内容は同じ (冪等)

    def test_daily_uses_wakunari_even_with_maegake_stt(self, repo: Path) -> None:
        """daily は展示前なので、stt があっても枠なりで組む。"""
        build_suji_picks.write_day(repo, DAY, "daily", None)
        rows = [r for r in _read_out(repo) if r["レースコード"] == "202608100301"]
        assert rows[0]["買い目1"] == "3-1-4"


class TestOutputShape:
    def test_header_and_five_points(self, repo: Path) -> None:
        build_suji_picks.write_day(repo, DAY, "realtime", None)
        rows = _read_out(repo)
        assert list(rows[0].keys()) == build_suji_picks.HEADER
        for r in rows:
            assert all(r[f"買い目{i}"] for i in range(1, 6))
            assert all(r[f"決まり手{i}"] for i in range(1, 6))

    def test_missing_index_raises_with_actionable_message(self, repo: Path) -> None:
        (repo / "data" / "estimate" / "v9_suji" / "2026" / "08" / "10.csv").unlink()
        with pytest.raises(FileNotFoundError, match="build_index.py"):
            build_suji_picks.build_day(repo, DAY, "realtime")

    def test_missing_suji_table_raises_with_actionable_message(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(FileNotFoundError, match="build_suji_table.py"):
            build_suji_picks.build_day(tmp_path, DAY, "realtime")

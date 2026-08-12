#!/usr/bin/env python3
"""決まり手セル条件付きの 2-3 着テーブル (Stage2) を生成する。

穴予想 B案 (`v10_kimarite`) が使う。Stage1 が出す「決まり手 × 1着コース」の
確率に、このテーブルを掛けて 3連単 120 通りの分布にする
(設計は docs/design/ana_prediction.md §4.2)。

    P(2着, 3着 | セル) ∝ tab[セル][c2][c3]
    tab[セル] = (count + k · m2 ⊗ m3) / 正規化

スジ表 (`build_suji_table.py`) との違いは条件の細かさ。スジ表は 1着コースだけで
条件付けるが、こちらは **決まり手も込みで**条件付ける。同じ「1着3コース」でも
まくりとまくり差しで 2着の分布が全く違う (まくり: 2着1c が 22.6% /
まくり差し: 63.4%) ため、こちらの方が鋭くなる。その代わりセルが 32 個に
分かれて 1 セルあたりのサンプルが減るので、**収縮が効く**。

同一ホールドアウトでの条件付き log-loss は **2.8024**。スジ表 (1着コースのみで
条件付け) の 2.8274 より **0.025 nat 鋭い**。設計書 §3.2 の想定どおり。

出力: `data/estimate/kimarite/tables/pair_table.csv` (32 セル × 20 ペア = 640 行)

進入コースは展示進入 (§12.2)。意図的に stdlib のみ。

Usage:
    python scripts/build_kimarite_pairs.py            # 既定の収縮強度
    python scripts/build_kimarite_pairs.py --k 300
    python scripts/build_kimarite_pairs.py --to-date 2026-06-25   # 検証用
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent))

from boatrace.kimarite import (  # noqa: E402
    CELLS,
    KIMARITE_MAP,
    cell_of,
    entry_courses,
    read_by_race,
)

# 収縮強度。ホールドアウト (2026-07-18〜、3,792 レース) の条件付き log-loss で選定:
#   k=0 → 2.8148 / k=50 → 2.8027 / **k=150 → 2.8024** / k=300 → 2.8025 /
#   k=600 → 2.8032 / k=1500 → 2.8051   (一様 = 2.9957)
# 50〜300 はほぼ横ばいで、k=0 だけ明確に悪い。スジ表 (1着コースのみで条件付け、
# 収縮が無意味だった) と違い、セルが 32 個に分かれてサンプルが減るぶん収縮が効く。
DEFAULT_K = 150.0
OUT_RELPATH = Path("data") / "estimate" / "kimarite" / "tables" / "pair_table.csv"
HEADER = ["セル", "2着コース", "3着コース", "n", "確率"]


def pair_keys(first: int) -> list[tuple[int, int]]:
    rest = [c for c in range(1, 7) if c != first]
    return [(a, b) for a in rest for b in rest if a != b]


def first_course_of(cell: str) -> int:
    return int(cell.rsplit("_", 1)[1])


def collect(repo: Path, from_date: str | None, to_date: str | None):
    counts: dict[str, dict[tuple[int, int], int]] = defaultdict(lambda: defaultdict(int))
    used = 0
    for res_path in sorted((repo / "data" / "results" / "realtime").glob("*/*/*.csv")):
        rel = res_path.relative_to(repo / "data" / "results" / "realtime")
        stt = read_by_race(repo / "data" / "previews" / "stt" / rel)
        with open(res_path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                day = (row.get("レース日") or "").strip()
                if from_date and day < from_date:
                    continue
                if to_date and day >= to_date:
                    continue
                kim = KIMARITE_MAP.get((row.get("決まり手") or "").strip())
                if kim is None:
                    continue
                try:
                    boats = [int(float(row[f"{p}着_艇番"])) for p in (1, 2, 3)]
                except (TypeError, ValueError, KeyError):
                    continue
                if any(not 1 <= b <= 6 for b in boats) or len(set(boats)) != 3:
                    continue
                courses = entry_courses(stt.get((row.get("レースコード") or "").strip()))
                c1, c2, c3 = (courses[b - 1] for b in boats)
                counts[cell_of(kim, c1)][(c2, c3)] += 1
                used += 1
    return counts, used


def build_rows(counts: dict, k: float) -> list[list]:
    rows: list[list] = []
    for cell in CELLS:
        cells = counts.get(cell, {})
        keys = pair_keys(first_course_of(cell))
        total = sum(cells.values())
        m2: dict[int, float] = defaultdict(float)
        m3: dict[int, float] = defaultdict(float)
        for (a, b), v in cells.items():
            m2[a] += v
            m3[b] += v
        if total > 0:
            for d in (m2, m3):
                for key in list(d):
                    d[key] /= total
        prior = {(a, b): m2.get(a, 0.0) * m3.get(b, 0.0) for a, b in keys}
        s = sum(prior.values())
        prior = ({key: v / s for key, v in prior.items()} if s > 0
                 else {key: 1.0 / len(keys) for key in keys})
        denom = total + k
        for a, b in keys:
            n = cells.get((a, b), 0)
            p = (n + k * prior[(a, b)]) / denom if denom > 0 else prior[(a, b)]
            rows.append([cell, a, b, n, f"{p:.6f}"])
    return rows


def atomic_write(path: Path, rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(HEADER)
            w.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--k", type=float, default=DEFAULT_K)
    p.add_argument("--from-date", default=None)
    p.add_argument("--to-date", default=None)
    p.add_argument("--out", default=None)
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    counts, used = collect(repo, args.from_date, args.to_date)
    if used == 0:
        print("no races collected", file=sys.stderr)
        return 1
    out = Path(args.out) if args.out else repo / OUT_RELPATH
    atomic_write(out, build_rows(counts, args.k))
    print(f"races={used} k={args.k} cells={len(CELLS)}")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

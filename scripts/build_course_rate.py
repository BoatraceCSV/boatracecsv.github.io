#!/usr/bin/env python3
"""場×レース番号×コースの収縮済み1着率テーブルを生成する。

`data/results/realtime/` の全履歴からレース番号別のコース1着率を集計し、
`data/estimate/stadium/course_win_rate.csv` に出力する。予想者 `v6_course` の
コースpt (`course`) の生値ソース(設計: docs/design/course_strength_v6.md)。

セル (場 j, レース番号 r, コース c) の公表値はベイズ収縮:

    rate(j, r, c) = (wins(j,r,c) + k * base(j,c)) / (n(j,r) + k)
    base(j,c)     = 場 j のコース c 全レース番号 1着率
    k = 50 (デフォルト。ホールドアウト検証で確定した値)

勝ちコースは `1着_艇番` と `Nコース_艇番` (実進入順) の突合で特定する。
結果の取れなかったレース・艇番不整合行はスキップ。

意図的に pandas 非依存 (stdlib のみ) で実装している。結果 CSV さえあれば
venv 無しの環境 (ローカル VM 等) でも再生成できるようにするため。

Usage:
    python scripts/build_course_rate.py            # 全履歴から再生成
    python scripts/build_course_rate.py --k 50     # 収縮強度の明示指定
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

STADIUM_CODES = [f"{i:02d}" for i in range(1, 25)]
RACE_ROUNDS = list(range(1, 13))
DEFAULT_K = 50.0

OUT_RELPATH = Path("data") / "estimate" / "stadium" / "course_win_rate.csv"

HEADER = (
    ["場コード", "レース回", "n"]
    + [f"{c}コース勝率" for c in range(1, 7)]
)


def parse_race_round(raw: str) -> int | None:
    """``"01R"`` / ``"1R"`` / ``"1"`` 形式からレース番号 int を取り出す。"""
    if raw is None:
        return None
    s = str(raw).strip().rstrip("R").lstrip("0")
    if not s:
        return None
    try:
        v = int(s)
    except ValueError:
        return None
    return v if 1 <= v <= 12 else None


def winner_course(row: dict) -> int | None:
    """1着艇番を実進入順 (``Nコース_艇番``) と突合して勝ちコースを返す。"""
    w = (row.get("1着_艇番") or "").strip()
    if not w:
        return None
    for c in range(1, 7):
        if (row.get(f"{c}コース_艇番") or "").strip() == w:
            return c
    return None


def aggregate(results_dir: Path):
    """(セル別 n / セル別勝数, 場別 n / 場別勝数, 全国 n / 全国勝数) を返す。"""
    cell_n: dict[tuple[str, int], int] = defaultdict(int)
    cell_w: dict[tuple[str, int, int], int] = defaultdict(int)
    st_n: dict[str, int] = defaultdict(int)
    st_w: dict[tuple[str, int], int] = defaultdict(int)
    nat_n = 0
    nat_w: dict[int, int] = defaultdict(int)

    n_files = 0
    for path in sorted(results_dir.glob("*/*/*.csv")):
        n_files += 1
        with path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                jo = (row.get("レース場") or "").strip()
                rno = parse_race_round(row.get("レース回", ""))
                course = winner_course(row)
                if jo not in STADIUM_CODES or rno is None or course is None:
                    continue
                cell_n[(jo, rno)] += 1
                cell_w[(jo, rno, course)] += 1
                st_n[jo] += 1
                st_w[(jo, course)] += 1
                nat_n += 1
                nat_w[course] += 1

    if nat_n == 0:
        raise SystemExit(f"no usable results under {results_dir}")
    return cell_n, cell_w, st_n, st_w, nat_n, nat_w, n_files


def build_rows(cell_n, cell_w, st_n, st_w, nat_n, nat_w, k: float) -> list[list]:
    rows = []
    for jo in STADIUM_CODES:
        for rno in RACE_ROUNDS:
            n = cell_n.get((jo, rno), 0)
            out = [jo, rno, n]
            for c in range(1, 7):
                # base(j,c): 場×コース全体率。場のデータが無ければ全国率。
                if st_n.get(jo, 0) > 0:
                    base = st_w.get((jo, c), 0) / st_n[jo]
                else:
                    base = nat_w.get(c, 0) / nat_n
                shrunk = (cell_w.get((jo, rno, c), 0) + k * base) / (n + k)
                out.append(f"{100.0 * shrunk:.2f}")
            rows.append(out)
    return rows


def atomic_write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--repo", type=Path, default=REPO_ROOT,
                   help="リポジトリルート (default: スクリプト位置から自動解決)")
    p.add_argument("--k", type=float, default=DEFAULT_K,
                   help=f"ベイズ収縮の強度 (default: {DEFAULT_K:g})")
    args = p.parse_args(argv)

    results_dir = args.repo / "data" / "results" / "realtime"
    if not results_dir.is_dir():
        print(f"results dir not found: {results_dir}", file=sys.stderr)
        return 1

    cell_n, cell_w, st_n, st_w, nat_n, nat_w, n_files = aggregate(results_dir)
    rows = build_rows(cell_n, cell_w, st_n, st_w, nat_n, nat_w, args.k)

    out_path = args.repo / OUT_RELPATH
    atomic_write(out_path, HEADER, rows)
    print(f"wrote {out_path} ({len(rows)} rows, "
          f"from {nat_n} races / {n_files} daily files, k={args.k:g})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

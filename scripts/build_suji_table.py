#!/usr/bin/env python3
"""スジ表と決まり手注釈テーブルを生成する。

穴予想 `v9_suji`(スジ予想)が使う静的テーブル 2 枚を
`data/results/realtime/` + `data/previews/stt/` の履歴から作る。
設計は docs/design/ana_prediction.md(§13 A案 / §14 決まり手の表示方式)。

出力 1: data/estimate/suji/tables/suji_table.csv
    1着コースを与えたときの 2-3 着コースの条件付き確率 P(R2, R3 | R1)。
    6 通りの 1着コース × 残り 5 コースから 2 つ選ぶ順列 20 = 120 行。
    ベイズ収縮:
        p(c2,c3 | c1) = (count + k * m2(c2|c1) * m3(c3|c1)) / (n + k)
    k=0(既定)なら生の経験分布。周辺分布の積 m2⊗m3 を事前分布に使う。

出力 2: data/estimate/suji/tables/kimarite_table.csv
    出目(1-2-3 着のコース並び)ごとの決まり手の分布と最頻値。同じく 120 行。
    買い目 1 点ごとの注釈(「3コースの まくり差し」)に使う。設計書 §14.1 の
    とおり、この注釈はモデルではなくこの静的テーブルだけで足りる。

**進入コースは展示進入(`previews/stt` の 艇N_コース)で統一する**(賭け時点で
使えるのはこれだけ。設計書 §12.2)。stt が取れないレースは枠なり(枠番 =
コース)にフォールバックする — 直前バッチの既存規約と同じ。

意図的に pandas 非依存(stdlib のみ)で実装している。結果 CSV さえあれば
venv 無しの環境でも再生成できるようにするため(build_course_rate.py と同じ方針)。

Usage:
    python scripts/build_suji_table.py                      # 全履歴・収縮なし
    python scripts/build_suji_table.py --k 50               # 収縮あり
    python scripts/build_suji_table.py --from-date 2026-05-01 --to-date 2026-06-25
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# 静的テーブルは日次 CSV (data/estimate/suji/YYYY/MM/DD.csv) と別ディレクトリに置く。
# infra/run.sh の sparse-checkout が日次ぶんを月単位でスコープしているため、
# 同じ階層に置くと静的テーブルを取るために日次の全履歴まで checkout されてしまう。
TABLES_DIR = Path("data") / "estimate" / "suji" / "tables"
SUJI_OUT_RELPATH = TABLES_DIR / "suji_table.csv"
KIMARITE_OUT_RELPATH = TABLES_DIR / "kimarite_table.csv"

# results/realtime の 決まり手 は全角スペース入り。表示用の短縮名に正規化する。
KIMARITE_MAP = {
    "逃　げ": "逃げ",
    "差　し": "差し",
    "まくり": "まくり",
    "まくり差し": "まくり差し",
    "抜　き": "抜き",
    "恵まれ": "恵まれ",
}
KIMARITE_ORDER = ["逃げ", "差し", "まくり", "まくり差し", "抜き", "恵まれ"]

SUJI_HEADER = ["場コード", "1着コース", "2着コース", "3着コース", "n", "確率"]
# 全場をプールした行の 場コード。場別セルはこの分布へ収縮する。
POOLED_STADIUM = "00"
KIMARITE_HEADER = (
    ["1着コース", "2着コース", "3着コース", "n", "最頻決まり手"] + KIMARITE_ORDER
)


def suji_keys(first: int) -> list[tuple[int, int]]:
    """1着コース ``first`` に対する (2着, 3着) コースの組を昇順で列挙する。"""
    rest = [c for c in range(1, 7) if c != first]
    return [(a, b) for a in rest for b in rest if a != b]


def _entry_courses(row: dict[str, str]) -> list[int] | None:
    """stt 由来の 艇番→コース を返す(1-indexed, 長さ 6)。不正なら None。

    列が無い / 値が欠けている艇は枠なり(艇番 = コース)にフォールバックする。
    フォールバック後もコースが重複する場合だけ None を返す。
    """
    course_of_boat = list(range(1, 7))
    for b in range(1, 7):
        raw = row.get(f"艇{b}_コース")
        if raw is None or str(raw).strip() == "":
            continue
        try:
            c = int(float(raw))
        except (TypeError, ValueError):
            continue
        if 1 <= c <= 6:
            course_of_boat[b - 1] = c
    if len(set(course_of_boat)) != 6:
        return None
    return course_of_boat


def _finish_courses(
    row: dict[str, str], course_of_boat: list[int]
) -> tuple[int, int, int] | None:
    """1〜3 着の **コース** を返す。欠損・重複があれば None。"""
    out: list[int] = []
    for pos in (1, 2, 3):
        raw = row.get(f"{pos}着_艇番")
        try:
            b = int(float(raw))
        except (TypeError, ValueError):
            return None
        if not 1 <= b <= 6:
            return None
        out.append(course_of_boat[b - 1])
    if len(set(out)) != 3:
        return None
    return out[0], out[1], out[2]


def _load_stt_courses(repo: Path) -> dict[str, list[int]]:
    """レースコード → 艇番→コース の対応表を previews/stt から作る。"""
    table: dict[str, list[int]] = {}
    pattern = str(repo / "data" / "previews" / "stt" / "*" / "*" / "*.csv")
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                code = (row.get("レースコード") or "").strip()
                if not code:
                    continue
                courses = _entry_courses(row)
                if courses is not None:
                    table[code] = courses
    return table


def collect(
    repo: Path, from_date: str | None, to_date: str | None
) -> tuple[dict, dict, int]:
    """結果 CSV を走査して (スジ カウント, 決まり手 カウント, 採用レース数) を返す。

    ``from_date`` は以上、``to_date`` は **未満**(学習窓から valid/test を
    落とすときに端が重ならないようにするため)。
    """
    stt_courses = _load_stt_courses(repo)
    # (場コード, 1着コース) → {(2着, 3着): 件数}。場コードは "00" が全場プール。
    suji_counts: dict[tuple[str, int], dict[tuple[int, int], int]] = defaultdict(
        lambda: defaultdict(int)
    )
    kimarite_counts: dict[tuple[int, int, int], dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    used = 0

    pattern = str(repo / "data" / "results" / "realtime" / "*" / "*" / "*.csv")
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                day = (row.get("レース日") or "").strip()
                if from_date and day < from_date:
                    continue
                if to_date and day >= to_date:
                    continue
                code = (row.get("レースコード") or "").strip()
                # stt が無いレースは枠なり (艇番 = コース) にフォールバック
                course_of_boat = stt_courses.get(code, list(range(1, 7)))
                fin = _finish_courses(row, course_of_boat)
                if fin is None:
                    continue
                kim = KIMARITE_MAP.get((row.get("決まり手") or "").strip())
                if kim is None:
                    continue
                c1, c2, c3 = fin
                # レースコード = YYYYMMDD + 場コード(2桁) + レース回(2桁)
                stadium = code[8:10] if len(code) >= 10 else ""
                suji_counts[(POOLED_STADIUM, c1)][(c2, c3)] += 1
                if stadium:
                    suji_counts[(stadium, c1)][(c2, c3)] += 1
                kimarite_counts[(c1, c2, c3)][kim] += 1
                used += 1
    return suji_counts, kimarite_counts, used


def _marginal_product_prior(
    cells: dict[tuple[int, int], int], keys: list[tuple[int, int]]
) -> dict[tuple[int, int], float]:
    """2着 / 3着 の周辺分布の積を正規化した事前分布。観測ゼロなら一様。"""
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
    if s > 0:
        return {key: v / s for key, v in prior.items()}
    return {key: 1.0 / len(keys) for key in keys}


def build_suji_rows(suji_counts: dict, k: float, by_stadium: bool) -> list[list]:
    """スジ表の行を作る。

    ``by_stadium=False``: 全場プールの 120 行のみ。``k`` は周辺分布の積への収縮強度。
    ``by_stadium=True``: 全場プール 120 行 + 場別 24 × 120 行。場別セルは
    **全場プール分布へ** ``k`` で収縮する(course_win_rate.csv と同じ考え方)。
    場ごとの観測は 1 場あたり 1,700 レース程度しかないので、収縮が効く。
    """
    rows: list[list] = []
    pooled: dict[int, dict[tuple[int, int], float]] = {}

    # --- 全場プール (場コード "00")。事前分布は周辺分布の積 ---
    for c1 in range(1, 7):
        cells = suji_counts.get((POOLED_STADIUM, c1), {})
        keys = suji_keys(c1)
        total = sum(cells.values())
        prior = _marginal_product_prior(cells, keys)
        denom = total + k
        dist = {}
        for a, b in keys:
            n = cells.get((a, b), 0)
            # denom == 0 は「観測が 1 件も無く、収縮も無効」の場合。全セル 0 だと
            # 合計 1 にならないので事前分布(= 一様)をそのまま使う。
            dist[(a, b)] = (n + k * prior[(a, b)]) / denom if denom > 0 else prior[(a, b)]
        pooled[c1] = dist
        for a, b in keys:
            rows.append([POOLED_STADIUM, c1, a, b, cells.get((a, b), 0),
                         f"{dist[(a, b)]:.6f}"])

    if not by_stadium:
        return rows

    # --- 場別。事前分布は全場プールの分布 ---
    stadiums = sorted({s for s, _ in suji_counts if s != POOLED_STADIUM})
    for stadium in stadiums:
        for c1 in range(1, 7):
            cells = suji_counts.get((stadium, c1), {})
            keys = suji_keys(c1)
            total = sum(cells.values())
            denom = total + k
            for a, b in keys:
                n = cells.get((a, b), 0)
                base = pooled[c1][(a, b)]
                p = (n + k * base) / denom if denom > 0 else base
                rows.append([stadium, c1, a, b, n, f"{p:.6f}"])
    return rows


def build_kimarite_rows(kimarite_counts: dict) -> list[list]:
    """決まり手注釈テーブルの行を作る(全 120 出目)。"""
    rows: list[list] = []
    for c1 in range(1, 7):
        for a, b in suji_keys(c1):
            counts = kimarite_counts.get((c1, a, b), {})
            n = sum(counts.values())
            if n > 0:
                # 同数なら KIMARITE_ORDER の順で先勝ち (再現性のため)
                top = max(KIMARITE_ORDER, key=lambda kk: (counts.get(kk, 0),
                                                          -KIMARITE_ORDER.index(kk)))
            else:
                top = ""
            shares = [
                f"{counts.get(kk, 0) / n:.4f}" if n > 0 else "" for kk in KIMARITE_ORDER
            ]
            rows.append([c1, a, b, n, top] + shares)
    return rows


def atomic_write(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
        os.replace(tmp, path)
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--k", type=float, default=0.0,
                   help="収縮強度 (場別なら全場プールへ、プールなら周辺積へ)")
    p.add_argument("--by-stadium", action="store_true",
                   help="場別のスジ表も出力する (全場プール行は常に出力)")
    p.add_argument("--from-date", default=None, help="学習窓の開始日 (YYYY-MM-DD, 以上)")
    p.add_argument("--to-date", default=None, help="学習窓の終了日 (YYYY-MM-DD, 未満)")
    p.add_argument("--suji-out", default=None, help="スジ表の出力先 (既定はリポジトリ内)")
    p.add_argument("--kimarite-out", default=None,
                   help="決まり手注釈テーブルの出力先 (既定はリポジトリ内)")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    suji_counts, kimarite_counts, used = collect(repo, args.from_date, args.to_date)
    if used == 0:
        print("no races collected; nothing to write", file=sys.stderr)
        return 1

    suji_out = Path(args.suji_out) if args.suji_out else repo / SUJI_OUT_RELPATH
    kimarite_out = (
        Path(args.kimarite_out) if args.kimarite_out else repo / KIMARITE_OUT_RELPATH
    )
    atomic_write(suji_out, SUJI_HEADER,
                 build_suji_rows(suji_counts, args.k, args.by_stadium))
    atomic_write(kimarite_out, KIMARITE_HEADER, build_kimarite_rows(kimarite_counts))

    window = f"{args.from_date or '(先頭)'} 〜 {args.to_date or '(末尾)'}"
    print(f"races={used} window={window} k={args.k}")
    print(f"wrote {suji_out}")
    print(f"wrote {kimarite_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

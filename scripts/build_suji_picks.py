#!/usr/bin/env python3
"""穴予想 `v9_suji`(スジ予想)の買い目 CSV を生成する。

`data/estimate/suji/YYYY/MM/DD.csv` に、レース 1 行 × 買い目 5 点を出力する。
fun-site はこの CSV を読むだけで、買い目の計算はしない
(設計は docs/design/ana_prediction.md §13 / §8.1)。

買い目の作り方(A案):

    1着 = **1 コース以外**で 強さpt が最大の艇
    2-3着 = スジ表 P(R2, R3 | R1) の上位 5 ペア
    各出目に「決まり手注釈」(例: 3コースの まくり差し)を添える

進入コースの扱いは既存の index と同じ規約:

* ``状態=daily``   … 枠なり(枠番 = コース)。強さpt も暫定値
* ``状態=realtime`` … `previews/stt` の展示進入コース。取れない艇は枠なり

daily 行と realtime 行は build_index.py と同様に **両方保持**する
(fun-site が「当日買い目」「直前買い目」として別々に表示・集計するため)。
回収率の集計母数は直前買い目だけなので、daily 行は表示専用。

意図的に pandas 非依存(stdlib のみ)。

Usage:
    # 朝バッチ: 当日の全レースを 状態=daily で出力
    python scripts/build_suji_picks.py --date 2026-08-11 --mode daily

    # 直前バッチ: 指定レースの 状態=realtime 行を upsert
    python scripts/build_suji_picks.py --date 2026-08-11 --mode realtime \
        --update-races 202608112301,202608112302
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent))

from build_suji_table import (  # noqa: E402
    KIMARITE_OUT_RELPATH,
    POOLED_STADIUM,
    SUJI_OUT_RELPATH,
    _entry_courses,
)

PREDICTOR_ID = "v9_suji"
STATE_DAILY = "daily"
STATE_REALTIME = "realtime"
TOP_K = 5

HEADER = (
    ["レースコード", "レース日", "レース場コード", "レース回", "状態",
     "1着コース", "1着艇番"]
    + [f"買い目{i}" for i in range(1, TOP_K + 1)]
    + [f"決まり手{i}" for i in range(1, TOP_K + 1)]
)


# ---------------------------------------------------------------------------
# テーブル読み込み
# ---------------------------------------------------------------------------
def _require(path: Path, how_to_build: str) -> None:
    """入力ファイルが無いときは、作り方まで示して落とす。

    月次ジョブ (build_suji_table.py) や朝バッチ (build_index.py) が回っていない
    ときに、素の FileNotFoundError だけ出て原因が分からない状態を避ける。
    """
    if not path.exists():
        raise FileNotFoundError(f"{path} が見つかりません。\n  {how_to_build}")


def load_suji_table(path: Path) -> dict[tuple[str, int], list[tuple[int, int]]]:
    """(場コード, 1着コース) → 確率降順の (2着コース, 3着コース) リスト。

    本番のスジ表は全場プール(場コード = ``POOLED_STADIUM``)のみだが、
    ``build_suji_table.py --by-stadium`` で場別行を足した表も読めるようにしてある
    (場別の検証結果は notebooks/ana_prediction/report.md)。
    """
    _require(path, "python scripts/build_suji_table.py を先に実行してください")
    rows: dict[tuple[str, int], list[tuple[float, int, int]]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            key = (str(r.get("場コード", POOLED_STADIUM)), int(r["1着コース"]))
            rows.setdefault(key, []).append(
                (float(r["確率"]), int(r["2着コース"]), int(r["3着コース"]))
            )
    # 確率降順。同値は (2着, 3着) 昇順で決定的に並べる(再実行の冪等性のため)
    return {
        k: [(a, b) for _, a, b in sorted(v, key=lambda x: (-x[0], x[1], x[2]))]
        for k, v in rows.items()
    }


def load_kimarite_table(path: Path) -> dict[tuple[int, int, int], str]:
    """(1着, 2着, 3着) コース → 最頻決まり手。"""
    _require(path, "python scripts/build_suji_table.py を先に実行してください")
    out: dict[tuple[int, int, int], str] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            key = (int(r["1着コース"]), int(r["2着コース"]), int(r["3着コース"]))
            out[key] = (r.get("最頻決まり手") or "").strip()
    return out


def load_stt_courses(repo: Path, day: dt.date) -> dict[str, list[int]]:
    """対象日の 展示進入コース(レースコード → 艇番→コース)。"""
    path = (repo / "data" / "previews" / "stt"
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    if not path.exists():
        return {}
    out: dict[str, list[int]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            code = (row.get("レースコード") or "").strip()
            courses = _entry_courses(row)
            if code and courses is not None:
                out[code] = courses
    return out


def load_index_rows(
    repo: Path, day: dt.date, state: str, predictor_id: str = PREDICTOR_ID
) -> dict[str, dict[str, str]]:
    """予想者 index CSV から、指定 状態 の行を レースコード 引きで返す。

    ``predictor_id`` は B案 (`build_kimarite_picks.py`) からも使うので引数にしてある。
    """
    path = (repo / "data" / "estimate" / predictor_id
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")
    _require(
        path,
        f"python scripts/build_index.py --date {day:%Y-%m-%d} "
        f"--predictor {predictor_id} を先に実行してください",
    )
    out: dict[str, dict[str, str]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if (row.get("状態") or STATE_DAILY).strip() != state:
                continue
            code = (row.get("レースコード") or "").strip()
            if code:
                out[code] = row
    return out


# ---------------------------------------------------------------------------
# 買い目の生成
# ---------------------------------------------------------------------------
def strengths_by_boat(index_row: dict[str, str]) -> list[float] | None:
    """index 行から 艇番順の 強さpt を取り出す。1 つでも欠ければ None。"""
    out: list[float] = []
    for b in range(1, 7):
        raw = index_row.get(f"{b}枠_強さpt")
        try:
            v = float(raw)
        except (TypeError, ValueError):
            return None
        out.append(v)
    return out


def build_row(
    index_row: dict[str, str],
    state: str,
    course_of_boat: list[int],
    suji: dict[tuple[str, int], list[tuple[int, int]]],
    kimarite: dict[tuple[int, int, int], str],
) -> list | None:
    """1 レース分の出力行を作る。買い目が組めなければ None。"""
    strength = strengths_by_boat(index_row)
    if strength is None:
        return None
    if len(set(course_of_boat)) != 6:
        return None
    # コース → 艇番
    boat_at = [0] * 7
    for boat, course in enumerate(course_of_boat, start=1):
        boat_at[course] = boat

    # 1着 = 1 コース以外で 強さpt 最大。同値は内側コース優先(決定的にするため)
    first_course = max(range(2, 7), key=lambda c: (strength[boat_at[c] - 1], -c))
    # 場別行があればそれを、無ければ全場プールを使う
    stadium = str(index_row.get("レース場コード", "")).strip()
    pairs = (
        suji.get((stadium, first_course))
        or suji.get((POOLED_STADIUM, first_course), [])
    )[:TOP_K]
    if not pairs:
        return None

    combos, marks = [], []
    for c2, c3 in pairs:
        combos.append(
            f"{boat_at[first_course]}-{boat_at[c2]}-{boat_at[c3]}"
        )
        marks.append(kimarite.get((first_course, c2, c3), ""))
    # TOP_K 未満しか取れなかった場合は空欄で埋める(列数を固定するため)
    combos += [""] * (TOP_K - len(combos))
    marks += [""] * (TOP_K - len(marks))

    return (
        [
            index_row.get("レースコード", ""),
            index_row.get("レース日", ""),
            index_row.get("レース場コード", ""),
            index_row.get("レース回", ""),
            state,
            first_course,
            boat_at[first_course],
        ]
        + combos
        + marks
    )


# ---------------------------------------------------------------------------
# 入出力
# ---------------------------------------------------------------------------
def picks_csv_path(repo: Path, day: dt.date) -> Path:
    return (repo / "data" / "estimate" / "suji"
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")


def read_existing(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows = list(reader)
    return rows[1:] if rows else []


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


def sort_rows(rows: list[list]) -> list[list]:
    """(レースコード 昇順, 状態 = daily → realtime) で並べる。"""
    order = {STATE_DAILY: 0, STATE_REALTIME: 1}
    return sorted(rows, key=lambda r: (str(r[0]), order.get(str(r[4]), 2)))


def build_day(
    repo: Path, day: dt.date, state: str, race_codes: set[str] | None = None
) -> list[list]:
    suji = load_suji_table(repo / SUJI_OUT_RELPATH)
    kimarite = load_kimarite_table(repo / KIMARITE_OUT_RELPATH)
    index_rows = load_index_rows(repo, day, state)
    stt = load_stt_courses(repo, day) if state == STATE_REALTIME else {}

    out: list[list] = []
    for code, index_row in sorted(index_rows.items()):
        if race_codes is not None and code not in race_codes:
            continue
        # daily は枠なり固定。realtime は展示進入(取れなければ枠なり)
        course_of_boat = (
            stt.get(code, list(range(1, 7)))
            if state == STATE_REALTIME
            else list(range(1, 7))
        )
        row = build_row(index_row, state, course_of_boat, suji, kimarite)
        if row is not None:
            out.append(row)
    return out


def write_day(repo: Path, day: dt.date, state: str, race_codes: set[str] | None) -> int:
    """指定 状態 の行を upsert する。他の 状態 の行は保持する。"""
    new_rows = build_day(repo, day, state, race_codes)
    path = picks_csv_path(repo, day)
    kept = [
        r for r in read_existing(path)
        if len(r) > 4 and not (
            r[4] == state and (race_codes is None or r[0] in race_codes)
        )
    ]
    atomic_write(path, sort_rows(kept + new_rows))
    return len(new_rows)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--date", required=True, help="YYYY-MM-DD")
    p.add_argument("--mode", choices=[STATE_DAILY, STATE_REALTIME],
                   default=STATE_REALTIME)
    p.add_argument("--update-races", default=None,
                   help="カンマ区切りのレースコード。指定した行だけ upsert する")
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    day = dt.date.fromisoformat(args.date)
    codes = (
        {c.strip() for c in args.update_races.split(",") if c.strip()}
        if args.update_races
        else None
    )
    n = write_day(repo, day, args.mode, codes)
    print(f"{PREDICTOR_ID}: wrote {n} 状態={args.mode} rows "
          f"→ {picks_csv_path(repo, day)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""穴予想 `v10_kimarite`(決まり手モデル)の買い目 CSV を生成する。

`data/estimate/kimarite/picks/YYYY/MM/DD.csv` に、レース 1 行 × 買い目 5 点を出力する。
fun-site はこの CSV を読むだけで、買い目の計算はしない(A案 `v9_suji` と同じ規約)。

買い目の作り方(B案。設計は docs/design/ana_prediction.md §4.3 / §5.2):

    Stage1 (32 セルの確率)  ×  Stage2 (セル条件付きの 2-3 着表)  → 120 通り
    そこに Plackett-Luce(強さpt)を w:1−w で混ぜ、
    **1コース頭を除いた**上位 5 点を買う

合成そのものは `boatrace/kimarite_blend.py` にある(log-loss 集計と共有)。

A案との違い:

* A案は 1着を「1コース以外で強さpt 最大」と**決め打つ**。B案は 120 通りの
  確率の中で決まるので、1 レースの買い目で 1着艇が複数になりうる
* 決まり手の注釈は **両案共通の静的テーブル**を使う(§14.1。モデルの
  `P(セル|出目)` より静的テーブルの方が一致率が高かった)

進入コースの扱いは既存の index と同じ規約:

* ``状態=daily``   … 枠なり。強さpt も暫定値、Stage1 も daily 係数
* ``状態=realtime`` … `previews/stt` の展示進入コース、Stage1 は realtime 係数

意図的に stdlib のみ(sklearn は Stage1 の**学習**にしか要らない)。

Usage:
    python scripts/build_kimarite_picks.py --date 2026-08-12 --mode daily
    python scripts/build_kimarite_picks.py --date 2026-08-12 --mode realtime \
        --update-races 202608122301,202608122302
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

from boatrace.kimarite_blend import (  # noqa: E402
    PAIR_TABLE_RELPATH,
    TOP_K,
    blend,
    load_pair_table,
    read_cell_probs,
    top_picks,
    z_scores,
)
from build_kimarite_probs import probs_csv_path  # noqa: E402
from build_suji_picks import (  # noqa: E402
    STATE_DAILY,
    STATE_REALTIME,
    load_index_rows,
    load_kimarite_table,
    load_stt_courses,
    strengths_by_boat,
)
from build_suji_table import KIMARITE_OUT_RELPATH  # noqa: E402

PREDICTOR_ID = "v10_kimarite"
PICKS_DIR = Path("data") / "estimate" / "kimarite" / "picks"

HEADER = (
    ["レースコード", "レース日", "レース場コード", "レース回", "状態"]
    + [f"買い目{i}" for i in range(1, TOP_K + 1)]
    + [f"決まり手{i}" for i in range(1, TOP_K + 1)]
)


def picks_csv_path(repo: Path, day: dt.date) -> Path:
    return repo / PICKS_DIR / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"


def build_row(
    index_row: dict[str, str],
    state: str,
    course_of_boat: list[int],
    p1: list[float],
    tab,
    kimarite: dict[tuple[int, int, int], str],
) -> list | None:
    """1 レース分の出力行を作る。買い目が組めなければ None。"""
    strength = strengths_by_boat(index_row)
    if strength is None or len(set(course_of_boat)) != 6:
        return None
    # コース → 艇番
    boat_at = [0] * 7
    for boat, course in enumerate(course_of_boat, start=1):
        boat_at[course] = boat

    picks = top_picks(blend(p1, tab, z_scores(strength, boat_at)))
    if not picks:
        return None

    combos = [f"{boat_at[a]}-{boat_at[b]}-{boat_at[c]}" for a, b, c in picks]
    marks = [kimarite.get(t, "") for t in picks]
    # TOP_K 未満しか取れなかった場合は空欄で埋める(列数を固定するため)
    combos += [""] * (TOP_K - len(combos))
    marks += [""] * (TOP_K - len(marks))

    return [
        index_row.get("レースコード", ""),
        index_row.get("レース日", ""),
        index_row.get("レース場コード", ""),
        index_row.get("レース回", ""),
        state,
    ] + combos + marks


def build_day(
    repo: Path, day: dt.date, state: str, race_codes: set[str] | None = None
) -> list[list]:
    tab = load_pair_table(repo / PAIR_TABLE_RELPATH)
    kimarite = load_kimarite_table(repo / KIMARITE_OUT_RELPATH)
    index_rows = load_index_rows(repo, day, state, PREDICTOR_ID)
    cell_probs = read_cell_probs(probs_csv_path(repo, day), state)
    stt = load_stt_courses(repo, day) if state == STATE_REALTIME else {}

    out: list[list] = []
    for code, index_row in sorted(index_rows.items()):
        if race_codes is not None and code not in race_codes:
            continue
        p1 = cell_probs.get(code)
        if p1 is None:
            # Stage1 の確率が無いレースは買い目を出さない
            # (build_kimarite_probs.py が先に回っている必要がある)
            continue
        course_of_boat = (
            stt.get(code, list(range(1, 7)))
            if state == STATE_REALTIME
            else list(range(1, 7))
        )
        row = build_row(index_row, state, course_of_boat, p1, tab, kimarite)
        if row is not None:
            out.append(row)
    return out


def read_existing(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.reader(fh))
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
    order = {STATE_DAILY: 0, STATE_REALTIME: 1}
    rows = sorted(kept + new_rows, key=lambda r: (str(r[0]), order.get(str(r[4]), 2)))
    atomic_write(path, rows)
    return len(new_rows)


def main() -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
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

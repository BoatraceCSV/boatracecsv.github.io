#!/usr/bin/env python3
"""荒れ度メーターの校正と log-loss を集計する。

`data/estimate/kimarite/**` の予測 (状態=realtime) を `data/results/realtime/`
の実績と突き合わせ、**予測帯ごとに「予測した荒れ度」と「実際に荒れた率」**を並べる。
設計書 docs/design/ana_prediction.md §7 の KPI「各予測帯で実測との差が ±5pt 以内」を
継続的に監視するためのもの。

出力: `data/estimate/kimarite/tables/calibration.csv`

| 列 | 説明 |
| --- | --- |
| `予測帯` | `0.0-0.2` など。`合計` 行が 1 本入る |
| `n` | そのレース数 |
| `予測荒れ度` | 予測の平均 |
| `実測荒れ度` | 実際に 1コース以外が 1着だった率 |
| `差pt` | 実測 − 予測 (パーセントポイント) |
| `logloss` | 32 クラスの log-loss。Phase 2 (A案 vs B案) の主判定に使う指標 |

**なぜ log-loss も出すか**: 回収率で穴予想どうしを比べると差の検出に 8 ヶ月かかる
(§13.3)。log-loss なら数週間で決着するので、判定の主指標はこちらになる。
その値を日々の運用データから取れるようにしておく。

意図的に stdlib のみ。

Usage:
    python scripts/build_kimarite_calibration.py
    python scripts/build_kimarite_calibration.py --from-date 2026-08-01
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent))

from boatrace.kimarite import (  # noqa: E402
    KIMARITE_MAP,
    NIGE_CELL,
    cell_of,
    entry_courses,
    read_by_race,
)

OUT_RELPATH = Path("data") / "estimate" / "kimarite" / "tables" / "calibration.csv"
BANDS = [0.0, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.01]
HEADER = ["予測帯", "n", "予測荒れ度", "実測荒れ度", "差pt", "logloss"]


def band_label(v: float) -> str:
    for lo, hi in zip(BANDS[:-1], BANDS[1:]):
        if lo <= v < hi:
            return f"{lo:.1f}-{min(hi, 1.0):.1f}"
    return f"{BANDS[-2]:.1f}-1.0"


def collect(repo: Path, from_date: str | None):
    """(予測帯 → [(予測荒れ度, 実際に荒れたか, 正解セルの確率)]) を返す。"""
    buckets: dict[str, list[tuple[float, int, float]]] = defaultdict(list)
    for probs_path in sorted(
        (repo / "data" / "estimate" / "kimarite").glob("[0-9]*/*/*.csv")
    ):
        rel = probs_path.relative_to(repo / "data" / "estimate" / "kimarite")
        results = read_by_race(repo / "data" / "results" / "realtime" / rel)
        if not results:
            continue
        stt = read_by_race(repo / "data" / "previews" / "stt" / rel)

        with open(probs_path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                # 集計対象は直前予測のみ (朝の暫定値は混ぜない)
                if (row.get("状態") or "").strip() != "realtime":
                    continue
                day = (row.get("レース日") or "").strip()
                if from_date and day < from_date:
                    continue
                code = (row.get("レースコード") or "").strip()
                res = results.get(code)
                if not res:
                    continue
                kim = KIMARITE_MAP.get((res.get("決まり手") or "").strip())
                if kim is None:
                    continue
                try:
                    winner = int(float(res.get("1着_艇番")))
                    pred = float(row.get("荒れ度"))
                except (TypeError, ValueError):
                    continue
                if not 1 <= winner <= 6:
                    continue
                first_course = entry_courses(stt.get(code))[winner - 1]
                truth = cell_of(kim, first_course)
                # 実際に荒れた = 1コース以外が 1着
                upset = 0 if truth == NIGE_CELL else 1
                p_true = float(row.get(f"P_{truth}", 0.0) or 0.0)
                buckets[band_label(pred)].append((pred, upset, p_true))
    return buckets


def build_rows(buckets: dict[str, list[tuple[float, int, float]]]) -> list[list]:
    rows: list[list] = []
    all_items: list[tuple[float, int, float]] = []
    for label in sorted(buckets):
        items = buckets[label]
        all_items.extend(items)
        rows.append(summarize(label, items))
    if all_items:
        rows.append(summarize("合計", all_items))
    return rows


def summarize(label: str, items: list[tuple[float, int, float]]) -> list:
    n = len(items)
    pred = sum(i[0] for i in items) / n
    act = sum(i[1] for i in items) / n
    ll = -sum(math.log(max(i[2], 1e-12)) for i in items) / n
    return [label, n, f"{pred:.4f}", f"{act:.4f}", f"{(act - pred) * 100:+.1f}", f"{ll:.4f}"]


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
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--repo-root", default=str(REPO_ROOT))
    p.add_argument("--from-date", default=None)
    p.add_argument("--out", default=None)
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    buckets = collect(repo, args.from_date)
    rows = build_rows(buckets)
    if not rows:
        print("no settled races with predictions yet; nothing to write")
        return 0
    out = Path(args.out) if args.out else repo / OUT_RELPATH
    atomic_write(out, rows)
    for r in rows:
        print(f"  {r[0]:>9s} n={r[1]:>6} 予測={r[2]} 実測={r[3]} 差={r[4]}pt logloss={r[5]}")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

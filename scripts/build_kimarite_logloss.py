#!/usr/bin/env python3
"""穴予想 B案 `v10_kimarite` の主判定 — 3連単 log-loss の A/B を月次で集計する。

**回収率では A案と B案の差は決着しない。** 観測された差 (+4.2pt) を有意にするには
約 34,700 レース = 8.2 ヶ月かかる。一方 log-loss は同じ 3,500 レースで既に効果の
5.8 倍の精度がある(設計書 §13.3)。そこで判定はこちらで行う:

    対照 (baseline) … Plackett-Luce(強さpt, β=1.4)
                      control `v1_basic` と A案 `v9_suji` が持つ情報と等価
    実験 (B案)      … Stage1 × Stage2 を Plackett-Luce とブレンドした 120 通り

**B案が有意に上回らなければ B案を退役する。** 回収率はガードレール
(control 比 -7pt 級の劣化の検知)としてのみ使う。

出力: `data/estimate/kimarite/tables/logloss.csv`

    集計月 / n / PL_logloss / ブレンド_logloss / 改善nat / 95%CI下限 / 95%CI上限

CI はレース単位の差 `d_i` の標準誤差による正規近似(`d̄ ± 1.96·SE`)。
`d_i` は独立なので、この n では bootstrap と実質同じになる。

対象は **状態=realtime かつ確定したレース**のみ。朝バッチ (daily) は
進入コースも強さpt も暫定値なので、回収率の集計母数と同じ規約で除く。

意図的に stdlib のみ。

Usage:
    python scripts/build_kimarite_logloss.py
    python scripts/build_kimarite_logloss.py --from-date 2026-08-13
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

from boatrace.kimarite import entry_courses, read_by_race  # noqa: E402
from boatrace.kimarite_blend import (  # noqa: E402
    PAIR_TABLE_RELPATH,
    PL_BASELINE_BETA,
    blend,
    load_pair_table,
    plackett_luce,
    read_cell_probs,
    z_scores,
)

PREDICTOR_ID = "v10_kimarite"
STATE = "realtime"
OUT_RELPATH = Path("data") / "estimate" / "kimarite" / "tables" / "logloss.csv"
HEADER = ["集計月", "n", "PL_logloss", "ブレンド_logloss",
          "改善nat", "95%CI下限", "95%CI上限"]
TOTAL_LABEL = "累計"


def _day_rel(path: Path, root: Path) -> Path:
    return path.relative_to(root)


def load_strengths(path: Path, state: str) -> dict[str, list[float]]:
    """予想者 index CSV → レースコード → 艇番順の 強さpt。無い日は空。"""
    if not path.exists():
        return {}
    out: dict[str, list[float]] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if (row.get("状態") or "").strip() != state:
                continue
            code = (row.get("レースコード") or "").strip()
            try:
                values = [float(row[f"{b}枠_強さpt"]) for b in range(1, 7)]
            except (KeyError, TypeError, ValueError):
                continue
            if code:
                out[code] = values
    return out


def collect(repo: Path, from_date: str | None, to_date: str | None):
    """月 → レース単位の (PL の log-loss, ブレンドの log-loss) のリスト。"""
    tab = load_pair_table(repo / PAIR_TABLE_RELPATH)
    results_root = repo / "data" / "results" / "realtime"
    per_month: dict[str, list[tuple[float, float]]] = defaultdict(list)

    for res_path in sorted(results_root.glob("*/*/*.csv")):
        rel = _day_rel(res_path, results_root)
        cell_probs = read_cell_probs(repo / "data" / "estimate" / "kimarite" / rel, STATE)
        if not cell_probs:
            continue
        strengths = load_strengths(
            repo / "data" / "estimate" / PREDICTOR_ID / rel, STATE)
        if not strengths:
            continue
        stt = read_by_race(repo / "data" / "previews" / "stt" / rel)

        with open(res_path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                day = (row.get("レース日") or "").strip()
                if (from_date and day < from_date) or (to_date and day >= to_date):
                    continue
                code = (row.get("レースコード") or "").strip()
                p1, strength = cell_probs.get(code), strengths.get(code)
                if p1 is None or strength is None:
                    continue
                try:
                    boats = [int(float(row[f"{p}着_艇番"])) for p in (1, 2, 3)]
                except (TypeError, ValueError, KeyError):
                    continue
                if any(not 1 <= b <= 6 for b in boats) or len(set(boats)) != 3:
                    continue

                courses = entry_courses(stt.get(code))
                boat_at = [0] * 7
                for boat, course in enumerate(courses, start=1):
                    boat_at[course] = boat
                truth = tuple(courses[b - 1] for b in boats)

                z = z_scores(strength, boat_at)
                pl = plackett_luce(z, PL_BASELINE_BETA)
                mixed = blend(p1, tab, z)
                per_month[day[:7]].append((
                    -math.log(max(pl[truth], 1e-12)),
                    -math.log(max(mixed[truth], 1e-12)),
                ))
    return per_month


def summarize(label: str, pairs: list[tuple[float, float]]) -> list:
    n = len(pairs)
    if n == 0:
        return [label, 0, "", "", "", "", ""]
    pl = sum(a for a, _ in pairs) / n
    mixed = sum(b for _, b in pairs) / n
    diffs = [a - b for a, b in pairs]  # 正なら ブレンドの勝ち
    mean = sum(diffs) / n
    if n > 1:
        var = sum((d - mean) ** 2 for d in diffs) / (n - 1)
        half = 1.96 * math.sqrt(var / n)
    else:
        half = float("nan")
    return [label, n, f"{pl:.4f}", f"{mixed:.4f}",
            f"{mean:+.4f}", f"{mean - half:+.4f}", f"{mean + half:+.4f}"]


def build_rows(per_month: dict[str, list[tuple[float, float]]]) -> list[list]:
    rows = [summarize(month, per_month[month]) for month in sorted(per_month)]
    everything = [p for month in per_month for p in per_month[month]]
    if everything:
        rows.append(summarize(TOTAL_LABEL, everything))
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
    p.add_argument("--from-date", default=None,
                   help="集計の開始日 (YYYY-MM-DD, 以上)。既定は v10_kimarite の started_at")
    p.add_argument("--to-date", default=None, help="集計の終了日 (YYYY-MM-DD, 未満)")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    repo = Path(args.repo_root).resolve()
    from_date = args.from_date
    if from_date is None:
        from boatrace.predictors.registry import predictor_by_id

        from_date = f"{predictor_by_id(PREDICTOR_ID).started_at:%Y-%m-%d}"

    per_month = collect(repo, from_date, args.to_date)
    rows = build_rows(per_month)
    out = Path(args.out) if args.out else repo / OUT_RELPATH
    atomic_write(out, rows)

    print(f"from={from_date} months={len(per_month)}")
    for row in rows:
        print("  " + "  ".join(str(v) for v in row))
    print(f"wrote {out}")
    if not rows:
        print("(まだ 状態=realtime の確定レースがありません)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

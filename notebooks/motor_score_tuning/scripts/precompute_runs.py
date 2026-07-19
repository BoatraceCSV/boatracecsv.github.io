"""Ground-truth 4場ぶんの MotorRun 履歴を前計算してフラットCSVに落とす。

各 (stadium, target_day) について load_motor_history() を実行し、
全24場の run を flat table 化する(lane baseline は全場プールで計算するため)。

出力: runs_{stadium}_{target_day}.csv
  列: hist_stadium, motor_num, racer_class, grade_bucket, lane, race_date, finish,
      session_idx (0=最新節)
"""
import sys
import datetime as dt
from pathlib import Path

import pandas as pd

REPO = Path(__file__).parent / "repo"
sys.path.insert(0, str(REPO / "scripts"))
from boatrace import index_features as ifs  # noqa: E402

TARGETS = [
    ("04", dt.date(2026, 7, 15)),   # 平和島 (おかぺん 6/18-7/14)
    ("23", dt.date(2026, 7, 16)),   # 唐津 (〜7/15)
    ("24", dt.date(2026, 7, 18)),   # 大村 (5/24-7/17)
    ("14", dt.date(2026, 6, 14)),   # 鳴門 (4/11-6/13)
]

OUT = Path(__file__).parent / "cache"
OUT.mkdir(exist_ok=True)

for stadium, day in TARGETS:
    print(f"=== {stadium} @ {day} ===", flush=True)
    history = ifs.load_motor_history(REPO, day)
    rows = []
    for (hist_stadium, motor_num), sessions in history.items():
        for si, sess in enumerate(sessions):
            for r in sess:
                rows.append({
                    "hist_stadium": hist_stadium,
                    "motor_num": motor_num,
                    "racer_class": r.racer_class,
                    "grade_bucket": r.grade_bucket,
                    "lane": r.lane,
                    "race_date": r.race_date.isoformat(),
                    "finish": r.finish,
                    "session_idx": si,
                })
    df = pd.DataFrame(rows)
    fp = OUT / f"runs_{stadium}_{day.isoformat()}.csv"
    df.to_csv(fp, index=False)
    tgt = df[df["hist_stadium"] == stadium]
    print(f"  total runs={len(df)}, target-stadium runs={len(tgt)}, "
          f"motors={tgt['motor_num'].nunique()}", flush=True)

    # motor2rate 参照値も取得(target_day の race_cards から)
    rc_path = REPO / "data" / "programs" / "race_cards" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    if rc_path.exists():
        rc = pd.read_csv(rc_path, dtype=str)
        rc = rc[rc["レースコード"].astype(str).str[8:10] == stadium]
        m2 = {}
        for _, row in rc.iterrows():
            for n in range(1, 7):
                try:
                    num = int(float(row.get(f"艇{n}_モーター番号")))
                except (ValueError, TypeError):
                    continue
                v = ifs.parse_motor_2rate(row.get(f"艇{n}_モーター2連対率"))
                if num not in m2 and v == v:
                    m2[num] = v
        pd.DataFrame(sorted(m2.items()), columns=["motor_num", "motor2rate"]).to_csv(
            OUT / f"motor2rate_{stadium}_{day.isoformat()}.csv", index=False)
        print(f"  motor2rate rows={len(m2)} (race_cards {day} 開催あり)", flush=True)
    else:
        print(f"  race_cards {day} なし → motor2rate は近傍日で別途取得", flush=True)

print("done")

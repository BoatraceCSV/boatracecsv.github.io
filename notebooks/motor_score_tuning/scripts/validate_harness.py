"""ハーネスが現行実装 (index_features.motor_ability_pt) と一致するか検証。"""
import sys
import datetime as dt
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
REPO = HERE / "repo"
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(HERE))
from boatrace import index_features as ifs  # noqa: E402
import harness  # noqa: E402

stadium, day = "04", dt.date(2026, 7, 15)
df = pd.read_csv(harness.CACHE / f"runs_{stadium}_{day.isoformat()}.csv",
                 dtype={"hist_stadium": str, "finish": str})

# MotorRun 再構築
history = {}
for (st, mn), g in df.groupby(["hist_stadium", "motor_num"]):
    sessions = []
    for si, sg in sorted(g.groupby("session_idx"), key=lambda x: x[0]):
        runs = [ifs.MotorRun(
            session_end=day, stadium=st, motor_num=int(mn),
            grade_bucket=r.grade_bucket, racer_class=r.racer_class,
            finish=r.finish, race_date=dt.date.fromisoformat(r.race_date),
            lane=int(r.lane)) for r in sg.itertuples()]
        sessions.append(runs)
    history[(st, int(mn))] = sessions

table = ifs.load_motor_score_table(REPO)
all_runs = [r for sess_list in history.values() for sess in sess_list for r in sess]
lane_b = ifs.compute_lane_baseline(all_runs, table)
cg_avg = ifs.compute_class_grade_avg(all_runs, table)

ref = {}
for (st, mn) in history:
    if st != stadium:
        continue
    ref[mn] = ifs.motor_ability_pt(history, table, stadium, mn,
                                   lane_baseline=lane_b, class_grade_avg=cg_avg,
                                   target_day=day)

targets = harness.load_targets()
t04 = [t for t in targets if t.stadium == "04"][0]
pts, rho = harness.evaluate(t04, **harness.CURRENT)

diffs = []
for mn, v in ref.items():
    hv = pts.get(mn, float("nan"))
    if v == v and hv == hv:
        diffs.append(abs(v - hv))
    elif (v == v) != (hv == hv):
        print(f"NaN mismatch motor {mn}: ref={v} harness={hv}")
diffs = np.array(diffs)
print(f"compared {len(diffs)} motors: max|diff|={diffs.max():.3e} mean={diffs.mean():.3e}")
print(f"harness Spearman(平和島, 現行構成) = {rho:.3f}")
assert diffs.max() < 1e-9, "MISMATCH"
print("VALIDATION OK")

"""全パラメータ一括の Optuna 探索 + leave-one-stadium-out 検証。"""
import json
import sys
from pathlib import Path

import numpy as np
import optuna

sys.path.insert(0, str(Path(__file__).parent))
import harness

optuna.logging.set_verbosity(optuna.logging.WARNING)

TARGETS = harness.load_targets()
NAMES = {"04": "平和島", "23": "唐津", "24": "大村", "14": "鳴門"}
ROW_TAGS = ["B2_全", "B1_全", "A2_SG", "A2_一般", "A1_SG", "A1_一般"]


def params_from_trial(trial: optuna.Trial) -> dict:
    gamma = np.array([trial.suggest_float(f"gamma_{t}", 0.25, 4.0, log=True)
                      for t in ROW_TAGS])
    amp = np.array([100.0 if t == "B1_全" else
                    trial.suggest_float(f"amp_{t}", 20.0, 300.0, log=True)
                    for t in ROW_TAGS])
    return dict(
        lane_on=trial.suggest_categorical("lane_on", [True, False]),
        half_life=trial.suggest_float("half_life", 15.0, 240.0, log=True),
        shrink_k=trial.suggest_float("shrink_k", 0.0, 50.0),
        penalty=trial.suggest_float("penalty", -300.0, 0.0),
        n_sessions=trial.suggest_int("n_sessions", 4, 6),
        gamma=gamma, amp=amp,
    )


def make_objective(targets):
    def _obj(trial):
        p = params_from_trial(trial)
        val, detail = harness.objective(targets, **p)
        for s, r in detail.items():
            trial.set_user_attr(f"rho_{s}", r)
        return val if val == val else -1.0
    return _obj


CURRENT_AS_TRIAL = {
    "lane_on": True, "half_life": 60.0, "shrink_k": 10.0, "penalty": -100.0,
    "n_sessions": 6,
    **{f"gamma_{t}": 1.0 for t in ROW_TAGS},
    **{f"amp_{t}": a for t, a in zip(ROW_TAGS, [125, 100, 125, 75, 100, 50])
       if t != "B1_全"},
}


def run_study(targets, n_trials, seed=42):
    study = optuna.create_study(
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=seed, multivariate=True))
    study.enqueue_trial(CURRENT_AS_TRIAL)
    study.optimize(make_objective(targets), n_trials=n_trials, n_jobs=1,
                   show_progress_bar=False)
    return study


def rebuild_params(bp: dict) -> dict:
    gamma = np.array([bp[f"gamma_{t}"] for t in ROW_TAGS])
    amp = np.array([100.0 if t == "B1_全" else bp[f"amp_{t}"] for t in ROW_TAGS])
    return dict(lane_on=bp["lane_on"], half_life=bp["half_life"],
                shrink_k=bp["shrink_k"], penalty=bp["penalty"],
                n_sessions=bp["n_sessions"], gamma=gamma, amp=amp)


if __name__ == "__main__":
    n_trials = int(sys.argv[1]) if len(sys.argv) > 1 else 3000

    print(f"=== 全4場での探索 ({n_trials} trials) ===", flush=True)
    study = run_study(TARGETS, n_trials)
    best = study.best_trial
    print(f"best objective = {best.value:+.4f}  (現行 {study.trials[0].value:+.4f})")
    for s in NAMES:
        print(f"  {NAMES[s]}: {best.user_attrs.get(f'rho_{s}'):+.3f} "
              f"(現行 {study.trials[0].user_attrs.get(f'rho_{s}'):+.3f})")
    print("best params:")
    for k, v in sorted(best.params.items()):
        print(f"  {k} = {v:.4g}" if isinstance(v, float) else f"  {k} = {v}")

    out = {"best_value": best.value, "current_value": study.trials[0].value,
           "best_params": best.params,
           "best_detail": {s: best.user_attrs.get(f"rho_{s}") for s in NAMES},
           "current_detail": {s: study.trials[0].user_attrs.get(f"rho_{s}")
                              for s in NAMES}}

    print("\n=== leave-one-stadium-out 検証 (各1500 trials) ===", flush=True)
    loso = {}
    for held in NAMES:
        train = [t for t in TARGETS if t.stadium != held]
        heldt = [t for t in TARGETS if t.stadium == held]
        st = run_study(train, 1500, seed=7)
        p = rebuild_params(st.best_trial.params)
        _, rho_held = harness.evaluate(heldt[0], **p)
        _, rho_held_cur = harness.evaluate(heldt[0], **harness.CURRENT)
        loso[held] = {"train_obj": st.best_trial.value,
                      "held_rho_tuned": rho_held, "held_rho_current": rho_held_cur}
        print(f"  held-out {NAMES[held]}: tuned {rho_held:+.3f} vs 現行 {rho_held_cur:+.3f} "
              f"(train obj {st.best_trial.value:+.3f})", flush=True)
    out["loso"] = loso

    with open("tune_result.json", "w") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    # trial 履歴も保存(感度分析用)
    rows = []
    for tr in study.trials:
        if tr.value is None:
            continue
        rows.append({**tr.params, "value": tr.value,
                     **{f"rho_{s}": tr.user_attrs.get(f"rho_{s}") for s in NAMES}})
    import pandas as pd
    pd.DataFrame(rows).to_csv("trials.csv", index=False)
    print("\nsaved: tune_result.json / trials.csv")

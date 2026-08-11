"""選手別 推定ST (racer_st) の計算ロジック.

fun-site のスリット予想 / 1マーク予想が使う「予測 ST」を、公表 全国平均ST に
代えて実測 ST 履歴から推定する。構成は Phase 2 で確定した M3
(docs/design/st_estimation.md / notebooks/st_estimation/phase2_report.md):

    推定ST = shrunk_EWMA(選手の実測ST履歴) + コース補正(枠番) + F本数補正

- EWMA: 半減期 30 日の時間減衰平均。**対象日より前の日** の実測 ST のみ使用
  (daily バッチが朝時点の情報しか持たない制約に合わせる)。F (負値 ST) は除外
- 収縮: 事前分布 (公表 全国平均ST > 0 ならその値、無ければ級別平均) へ実効 10 走ぶん
- 定数は Phase 2 の学習窓 (2026-03-01〜06-20) で確定した値を凍結。再学習する場合は
  notebooks/st_estimation/phase2_models.py を再実行して本モジュールの定数を更新する

状態ファイル (インクリメンタル更新)
------------------------------------
実測 ST 履歴の EWMA は選手ごとに (重み付き和, 重み計) の 2 変数で表せるため、
全履歴を毎回走査せず、状態 CSV に永続化して日次で前日結果だけ取り込む。

    data/estimate/racer_st/state.csv
        登録番号, 重み付き和, 重み計, 基準日

「基準日」は全行共通で、重みはその日まで減衰済みであることを意味する。
日次更新は (旧基準日, 対象日) の結果 CSV を順に取り込み基準日を進める。
同一対象日での再実行は取り込む新規日が無いため冪等。

結果 CSV には登録番号が無いため、艇番→登録番号の紐付けに **同日の race_cards**
を使う。従って取り込み対象日の race_cards CSV も checkout に必要
(infra/run-daily-sync.sh は race_cards の当月 + 前月を sparse 対象にしている)。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Phase 2 で確定した定数 (notebooks/st_estimation/phase2_report.json の config)
# ---------------------------------------------------------------------------
HALF_LIFE_DAYS = 30.0
K_PRIOR = 10.0
#: 学習窓の H1 残差から推定したコース (枠番) 別オフセット
COURSE_OFFSET: Dict[int, float] = {
    1: -0.0090,
    2: -0.0015,
    3: -0.0075,
    4: -0.0045,
    5: -0.0016,
    6: 0.0119,
}
#: 学習窓の H1+H2 残差から推定した F 本数 (0/1/2+) 別オフセット
F_OFFSET: Dict[int, float] = {0: -0.0019, 1: 0.0102, 2: 0.0383}
#: 公表 全国平均ST が 0.00 (実績なし) の選手の事前分布 (級別平均, 2025-11〜2026-02)
CLASS_PRIOR: Dict[str, float] = {"A1": 0.1489, "A2": 0.1643, "B1": 0.1754, "B2": 0.1918}
GLOBAL_PRIOR = 0.1672

DECAY_PER_DAY = 0.5 ** (1.0 / HALF_LIFE_DAYS)

# ---------------------------------------------------------------------------
# ST 予測区間 (スリット図の帯) の定数
# notebooks/slit_sim/report.md §4 で確定。テスト窓 47,000 艇走で被覆率は
# 名目 50/80/95% に対し 49.6/80.7/95.8% (誤差 ±1pt 以内)。
# ---------------------------------------------------------------------------
#: 予測残差 (実測ST - 推定ST) の標準偏差
SIGMA_BASE = 0.0684
#: 選手別 σ を SD_PRIOR へ収縮するときの実効走数
K_SIGMA = 10.0
#: 選手別 σ の事前分布 (全選手の実測 ST 標準偏差の平均)。相対倍率の分母
SD_PRIOR = 0.0664
#: 25/75 パーセンタイル係数 = t.ppf(0.75, df=9.14) * scale 0.879。
#: 残差は超過尖度 2.4 と裾が重く、正規 (0.6745) では被覆が 3.5pt 過大になる
Q75_K = 0.6173

STATE_RELATIVE_PATH = "data/estimate/racer_st/state.csv"
STATE_COLUMNS = ["登録番号", "重み付き和", "重み付き二乗和", "重み計", "基準日"]


@dataclass
class RacerStState:
    """選手ごとの EWMA 状態。重みは ``base_day`` 時点まで減衰済み。

    ``重み付き二乗和`` は ST の分散 (= スリット図の帯幅) 用。平均と同じ減衰・
    加算で更新できるので、全履歴を走査せずに σ を持てる。
    """

    base_day: Optional[dt.date] = None
    #: 登録番号 -> (重み付き和, 重み付き二乗和, 重み計)
    racers: Dict[int, tuple[float, float, float]] = field(default_factory=dict)

    def decay_to(self, new_base: dt.date) -> None:
        """基準日を ``new_base`` へ進め、全選手の重みを経過日数ぶん減衰する。"""
        if self.base_day is not None:
            elapsed = (new_base - self.base_day).days
            if elapsed < 0:
                raise ValueError(
                    f"state base_day {self.base_day} is after {new_base}; "
                    "rebuild the state (--rebuild)"
                )
            if elapsed > 0:
                f = DECAY_PER_DAY**elapsed
                self.racers = {
                    r: (ws * f, ws2 * f, wt * f) for r, (ws, ws2, wt) in self.racers.items()
                }
        self.base_day = new_base

    def add_run(self, regno: int, st: float) -> None:
        """基準日当日の 1 走を状態へ加算する (重み 1.0)。"""
        ws, ws2, wt = self.racers.get(regno, (0.0, 0.0, 0.0))
        self.racers[regno] = (ws + st, ws2 + st * st, wt + 1.0)

    def estimate_base(self, regno: int, prior: float) -> float:
        """収縮つき EWMA 推定 (コース・F 補正前) を返す。履歴が無ければ prior。"""
        ws, _ws2, wt = self.racers.get(regno, (0.0, 0.0, 0.0))
        return (ws + K_PRIOR * prior) / (wt + K_PRIOR)

    def sigma_multiplier(self, regno: int) -> float:
        """帯幅の選手別倍率 (1.0 = 平均的なばらつき) を返す。

        EWMA の分散 ``ws2/wt - (ws/wt)^2`` を ``K_SIGMA`` ぶん ``SD_PRIOR`` へ
        収縮し、``SD_PRIOR`` で割って相対倍率にする。履歴 1 走以下は 1.0。

        ST のばらつきは選手の持ち味として実在する (前半/後半の split-half 相関
        0.555、最安定五分位 0.058 / 最不安定五分位 0.076)。この倍率を入れると
        帯の被覆率が σ 分位間で 0.124 → 0.031 に揃う (report.md §4.2)。
        """
        ws, ws2, wt = self.racers.get(regno, (0.0, 0.0, 0.0))
        if wt <= 1.0:
            return 1.0
        var = ws2 / wt - (ws / wt) ** 2
        sd = var**0.5 if var > 0.0 else SD_PRIOR
        shrunk = (wt * sd + K_SIGMA * SD_PRIOR) / (wt + K_SIGMA)
        return shrunk / SD_PRIOR


# ---------------------------------------------------------------------------
# 状態 CSV の読み書き
# ---------------------------------------------------------------------------
def load_state(repo: Path) -> RacerStState:
    path = repo / STATE_RELATIVE_PATH
    if not path.exists():
        return RacerStState()
    df = pd.read_csv(path, dtype={"登録番号": int, "基準日": str})
    if df.empty:
        return RacerStState()
    base = dt.date.fromisoformat(str(df["基準日"].iloc[0]))
    # 重み付き二乗和 は帯幅 (σ) 導入時に追加した列。旧 state.csv には無いので
    # 欠損時は 0 として読む → sigma_multiplier() が 1.0 (帯幅一定) に退避する。
    # 正しい σ を得るには --rebuild で作り直すこと (docs/operations.md)。
    has_ws2 = "重み付き二乗和" in df.columns
    racers = {
        int(row["登録番号"]): (
            float(row["重み付き和"]),
            float(row["重み付き二乗和"]) if has_ws2 else 0.0,
            float(row["重み計"]),
        )
        for _, row in df.iterrows()
    }
    return RacerStState(base_day=base, racers=racers)


def save_state(repo: Path, state: RacerStState) -> Path:
    if state.base_day is None:
        raise ValueError("cannot save state without base_day")
    path = repo / STATE_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "登録番号": regno,
            "重み付き和": round(ws, 10),
            "重み付き二乗和": round(ws2, 10),
            "重み計": round(wt, 10),
            "基準日": state.base_day.isoformat(),
        }
        for regno, (ws, ws2, wt) in sorted(state.racers.items())
    ]
    pd.DataFrame(rows, columns=STATE_COLUMNS).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# 日次データの読み込み
# ---------------------------------------------------------------------------
def _ymd_path(repo: Path, prefix: str, day: dt.date) -> Path:
    return repo / prefix / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"


def results_path(repo: Path, day: dt.date) -> Path:
    return _ymd_path(repo, "data/results/realtime", day)


def race_cards_path(repo: Path, day: dt.date) -> Path:
    return _ymd_path(repo, "data/programs/race_cards", day)


def load_day_runs(repo: Path, day: dt.date) -> Optional[pd.DataFrame]:
    """``day`` の実測 ST を (登録番号, 実測ST) の long 形式で返す。

    - 結果 CSV が無い日は None (開催なし or 未収集)
    - race_cards が無く艇番→登録番号を紐付けられない日も None (警告は呼び出し側)
    - F (ST < 0)・非数値は除外
    """
    rpath = results_path(repo, day)
    cpath = race_cards_path(repo, day)
    if not rpath.exists() or not cpath.exists():
        return None
    res = pd.read_csv(rpath, dtype=str)
    cards = pd.read_csv(cpath, dtype=str)

    regno_by_race_boat: Dict[tuple[str, int], int] = {}
    for _, row in cards.iterrows():
        for b in range(1, 7):
            regno = pd.to_numeric(row.get(f"艇{b}_登録番号"), errors="coerce")
            if pd.notna(regno):
                regno_by_race_boat[(str(row["レースコード"]), b)] = int(regno)

    rows = []
    for _, row in res.iterrows():
        race_code = str(row["レースコード"])
        for c in range(1, 7):
            boat = pd.to_numeric(row.get(f"{c}コース_艇番"), errors="coerce")
            st = pd.to_numeric(row.get(f"{c}コース_スタートタイミング"), errors="coerce")
            f_mark = str(row.get(f"{c}コース_F") or "")
            if pd.isna(boat) or pd.isna(st):
                continue
            if "F" in f_mark or st < 0:
                continue  # フライングは履歴に含めない (docs/design/st_estimation.md §2.1)
            regno = regno_by_race_boat.get((race_code, int(boat)))
            if regno is None:
                continue
            rows.append({"登録番号": regno, "実測ST": float(st)})
    if not rows:
        return pd.DataFrame(columns=["登録番号", "実測ST"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 状態の更新
# ---------------------------------------------------------------------------
def advance_state(
    repo: Path,
    state: RacerStState,
    target_day: dt.date,
    start_day: Optional[dt.date] = None,
) -> tuple[list[dt.date], list[dt.date]]:
    """状態を ``target_day - 1`` まで進める。(取り込んだ日, スキップした日) を返す。

    ``state.base_day`` の翌日から ``target_day - 1`` までの結果を日順に取り込む。
    結果 CSV または race_cards が無い日はスキップ (次回以降も取り込まれないが、
    基準日は進むため冪等性は保たれる)。初期状態 (base_day=None) の場合は
    ``start_day`` (必須) から走査する。

    ``state.base_day`` が ``target_day - 1`` より未来の場合 (過去日を新しい状態で
    再生成しようとした場合) は未来情報のリークになるため ValueError を送出する。
    """
    new_base = target_day - dt.timedelta(days=1)
    if state.base_day is None:
        if start_day is None:
            raise ValueError("initial state requires start_day (or use --rebuild)")
        first = start_day
    else:
        if state.base_day > new_base:
            raise ValueError(
                f"state base_day {state.base_day} is newer than target {target_day}; "
                "regenerating a past date with a newer state would leak future data. "
                "Use --rebuild instead."
            )
        if state.base_day == new_base:
            return [], []  # 冪等: 取り込み済み
        first = state.base_day + dt.timedelta(days=1)

    processed: list[dt.date] = []
    skipped: list[dt.date] = []
    day = first
    while day <= new_base:
        runs = load_day_runs(repo, day)
        state.decay_to(day)
        if runs is not None:
            for _, row in runs.iterrows():
                state.add_run(int(row["登録番号"]), float(row["実測ST"]))
            processed.append(day)
        else:
            skipped.append(day)
        day += dt.timedelta(days=1)
    state.decay_to(new_base)
    return processed, skipped


# ---------------------------------------------------------------------------
# 対象日の推定 CSV 生成
# ---------------------------------------------------------------------------
def class_prior_for(class_grade: str) -> float:
    return CLASS_PRIOR.get(str(class_grade).strip(), GLOBAL_PRIOR)


def estimate_for_racer(
    state: RacerStState,
    regno: int,
    waku: int,
    avg_st_pub: float,
    class_grade: str,
    flying_count: int,
) -> float:
    """1 選手 × 1 枠の推定 ST (M3 構成)。"""
    prior = avg_st_pub if avg_st_pub > 0 else class_prior_for(class_grade)
    base = state.estimate_base(regno, prior)
    course_adj = COURSE_OFFSET.get(waku, 0.0)
    f_adj = F_OFFSET.get(min(max(flying_count, 0), 2), 0.0)
    return base + course_adj + f_adj


def estimate_band_for_racer(state: RacerStState, regno: int) -> float:
    """推定 ST の 25-75 パーセンタイル半幅 (秒) を返す。

    スリット図の帯はこれを推定 ST の前後に取る。全体の残差 sd に選手別の
    ばらつき倍率を掛けるだけなので、コース依存は入れない (σ のコース差は
    1C 0.048〜6C 0.053 と小さく、帯の見た目に効かない)。
    """
    return Q75_K * SIGMA_BASE * state.sigma_multiplier(regno)


def build_day_estimates(repo: Path, state: RacerStState, day: dt.date) -> pd.DataFrame:
    """``day`` の全レースについて 1 レース 1 行の推定 ST テーブルを作る。"""
    cpath = race_cards_path(repo, day)
    if not cpath.exists():
        raise FileNotFoundError(f"race_cards not found for {day}: {cpath}")
    cards = pd.read_csv(cpath, dtype=str)

    out_rows = []
    for _, row in cards.iterrows():
        out: Dict[str, object] = {
            "レースコード": str(row["レースコード"]),
            "レース日": str(row["レース日"]),
            "レース場コード": str(row["レース場コード"]),
            "レース回": str(row["レース回"]),
        }
        for b in range(1, 7):
            regno = pd.to_numeric(row.get(f"艇{b}_登録番号"), errors="coerce")
            avg_st = pd.to_numeric(row.get(f"艇{b}_全国平均ST"), errors="coerce")
            fcount = pd.to_numeric(row.get(f"艇{b}_F本数"), errors="coerce")
            grade = str(row.get(f"艇{b}_級別") or "")
            if pd.isna(regno):
                out[f"{b}枠_登録番号"] = ""
                out[f"{b}枠_推定ST"] = ""
                out[f"{b}枠_推定ST_p25"] = ""
                out[f"{b}枠_推定ST_p75"] = ""
                continue
            est = estimate_for_racer(
                state,
                int(regno),
                b,
                float(avg_st) if pd.notna(avg_st) else 0.0,
                grade,
                int(fcount) if pd.notna(fcount) else 0,
            )
            half = estimate_band_for_racer(state, int(regno))
            out[f"{b}枠_登録番号"] = int(regno)
            out[f"{b}枠_推定ST"] = round(est, 4)
            out[f"{b}枠_推定ST_p25"] = round(est - half, 4)
            out[f"{b}枠_推定ST_p75"] = round(est + half, 4)
        out_rows.append(out)

    columns = ["レースコード", "レース日", "レース場コード", "レース回"]
    for b in range(1, 7):
        columns += [
            f"{b}枠_登録番号",
            f"{b}枠_推定ST",
            f"{b}枠_推定ST_p25",
            f"{b}枠_推定ST_p75",
        ]
    return pd.DataFrame(out_rows, columns=columns)


def output_path(repo: Path, day: dt.date) -> Path:
    return _ymd_path(repo, "data/estimate/racer_st", day)

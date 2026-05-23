"""
Reusable feature builders for the strength index pipeline.

Both scripts/build_index.py (daily output) and scripts/build_weights.py
(monthly weight fitting) consume these helpers so feature definitions
stay in lockstep.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────
# Stadium master & helpers
# ─────────────────────────────────────────────────────────────────────
STADIUM_NAMES = {
    1: "桐生", 2: "戸田", 3: "江戸川", 4: "平和島", 5: "多摩川", 6: "浜名湖",
    7: "蒲郡", 8: "常滑", 9: "津", 10: "三国", 11: "びわこ", 12: "住之江",
    13: "尼崎", 14: "鳴門", 15: "丸亀", 16: "児島", 17: "宮島", 18: "徳山",
    19: "下関", 20: "若松", 21: "芦屋", 22: "福岡", 23: "唐津", 24: "大村",
}
STADIUM_FACING = {
    "桐生": 90, "戸田": 0, "江戸川": 200, "平和島": 270, "多摩川": 180, "浜名湖": 90,
    "蒲郡": 90, "常滑": 270, "津": 90, "三国": 270, "びわこ": 0, "住之江": 0,
    "尼崎": 0, "鳴門": 0, "丸亀": 0, "児島": 0, "宮島": 90, "徳山": 0,
    "下関": 0, "若松": 0, "芦屋": 0, "福岡": 0, "唐津": 0, "大村": 0,
}
WIND_CODE_TO_DEG = {1: 0, 2: 45, 3: 90, 4: 135, 5: 180, 6: 225, 7: 270, 8: 315}
WEATHER_CODE_TO_LABEL = {1: "晴", 2: "曇", 3: "雨", 4: "雨", 5: "雨", 6: "晴", 9: "晴"}

PARAM_FEATURES = [
    "wave_cm", "temp_diff",
    "wind_tail_ms", "wind_head_ms",
    "is_cloudy", "is_rainy",
]

SEASON_BY_MONTH = {
    1: "冬", 2: "冬", 3: "春", 4: "春", 5: "春",
    6: "夏", 7: "夏", 8: "夏",
    9: "秋", 10: "秋", 11: "秋",
    12: "冬",
}

# Order of the 5 strength components — used as canonical column ordering.
COMPONENT_KEYS = ["waku", "racer", "motor", "exhibit", "weather"]
COMPONENT_LABELS = {
    "waku":    "枠番pt",
    "racer":   "選手pt",
    "motor":   "モーターpt",
    "exhibit": "展示pt",
    "weather": "気象pt",
}


# ─────────────────────────────────────────────────────────────────────
# 1. 枠番ポイント
# ─────────────────────────────────────────────────────────────────────
def load_waku_table(repo: Path) -> dict:
    df = pd.read_csv(repo / "data" / "estimate" / "stadium" / "win_rate.csv", dtype=str)
    table: dict[tuple[str, str], list[float]] = {}
    for _, r in df.iterrows():
        key = (str(r["場コード"]).zfill(2), str(r["季節"]))
        table[key] = [
            float(r["1コース勝率"]), float(r["2コース勝率"]), float(r["3コース勝率"]),
            float(r["4コース勝率"]), float(r["5コース勝率"]), float(r["6コース勝率"]),
        ]
    return table


def waku_pt(table: dict, stadium_code2: str, season: str, course: int) -> float:
    rates = table.get((stadium_code2, season))
    if rates is None or not (1 <= course <= 6):
        return float("nan")
    return rates[course - 1]


# ─────────────────────────────────────────────────────────────────────
# 2. 選手ポイント (能力指数)
# ─────────────────────────────────────────────────────────────────────
SCORE_TABLE = {
    ("SG_GI", "yusho"):  [100, 98, 94, 91, 88, 85],
    ("SG_GI", "other"):  [85, 82, 77, 73, 69, 65],
    ("GII",   "yusho"):  [80, 78, 74, 71, 68, 65],
    ("GII",   "other"):  [70, 67, 62, 58, 54, 50],
    ("GIII",  "yusho"):  [65, 63, 59, 55, 52, 50],
    ("GIII",  "other"):  [60, 58, 55, 50, 46, 45],
}


def grade_of(grade_str: str) -> str:
    if not grade_str:
        return "GIII"
    s = grade_str.strip()
    if "ＳＧ" in s or "SG" in s or "ＰＧ" in s or "PG" in s:
        return "SG_GI"
    if "ＧⅠ" in s or "GⅠ" in s or "G1" in s or "Ｇ１" in s:
        return "SG_GI"
    if "ＧⅡ" in s or "GⅡ" in s or "G2" in s or "Ｇ２" in s:
        return "GII"
    if "ＧⅢ" in s or "GⅢ" in s or "G3" in s or "Ｇ３" in s:
        return "GIII"
    return "GIII"


ZEN_TO_HAN_DIGIT = {"１": 1, "２": 2, "３": 3, "４": 4, "５": 5, "６": 6}
RACER_RESPONSIBLE_TOKENS = {"F", "L", "失", "妨", "Ｆ", "Ｌ"}
NOT_RACER_RESPONSIBLE_TOKENS = {"欠", "転", "落", "沈", "エ", "不"}


def score_for_finish(grade: str, finish: int, is_yusho: bool) -> int:
    if finish < 1 or finish > 6:
        return 0
    return SCORE_TABLE[(grade, "yusho" if is_yusho else "other")][finish - 1]


def parse_finishes(seq: str) -> list[tuple[str, bool]]:
    out: list[tuple[str, bool]] = []
    if not seq:
        return out
    i, n = 0, len(seq)
    while i < n:
        ch = seq[i]
        if ch in ("[", "［"):
            j = seq.find("]", i + 1)
            if j == -1:
                j = seq.find("］", i + 1)
            if j == -1:
                i += 1
                continue
            for c in seq[i + 1:j].strip():
                if c in ZEN_TO_HAN_DIGIT:
                    out.append((str(ZEN_TO_HAN_DIGIT[c]), True))
                elif c in "123456":
                    out.append((c, True))
            i = j + 1
            continue
        if ch in (" ", "　", "\t"):
            i += 1
            continue
        if ch in ZEN_TO_HAN_DIGIT:
            out.append((str(ZEN_TO_HAN_DIGIT[ch]), False))
        elif ch in "123456":
            out.append((ch, False))
        else:
            out.append((ch, False))
        i += 1
    return out


def racer_pt_for_boat(boat_records: list[tuple[str, str]]) -> float:
    total_score = 0
    total_runs = 0
    for grade_str, seq in boat_records:
        grade = grade_of(grade_str)
        for token, is_yusho in parse_finishes(seq):
            if token in ("1", "2", "3", "4", "5", "6"):
                total_score += score_for_finish(grade, int(token), is_yusho)
                total_runs += 1
            elif token in RACER_RESPONSIBLE_TOKENS:
                total_runs += 1
            elif token in NOT_RACER_RESPONSIBLE_TOKENS:
                continue
    if total_runs == 0:
        return float("nan")
    return round(total_score / total_runs)


# ─────────────────────────────────────────────────────────────────────
# 3. モーターポイント(モーター能力指数)
# ─────────────────────────────────────────────────────────────────────
# 直近 N 節の出走実績を「級別 × グレード分類」のスコアテーブルで得点化し、
# 平均値を返す。詳細設計は docs/design/motor_ability_index.md 参照。
#
# 着順トークン分類(モーター固有ルール):
#   - "1"〜"6"            → スコアテーブルの値(+1 打点)
#   - 転 / 落 / 沈 / エ   → 機材起因 → -100 点(+1 打点)
#   - F / L / 失 / 妨     → 選手起因 → 集計除外(分子分母とも 0)
#   - 欠 / 不             → 無効走  → 集計除外
MOTOR_NEGATIVE_TOKENS: set[str] = {"転", "落", "沈", "エ"}
MOTOR_NEGATIVE_SCORE: int = -100
MOTOR_SKIP_TOKENS: set[str] = {"F", "L", "失", "妨", "欠", "不"}

MOTOR_HISTORY_SESSIONS: int = 5            # 直近 5 節
MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS: int = 10  # 期境界で剪定する前に取得する節数の上限
MOTOR_HISTORY_LOOKBACK_DAYS: int = 90      # 各場の節検出のために遡る日数
MOTOR_PERIOD_FALLBACK_DAYS: int = 14       # motor_stats が当日無いときの fallback 日数

_VALID_FINISH_TOKENS: set[str] = (
    {"1", "2", "3", "4", "5", "6"}
    | MOTOR_NEGATIVE_TOKENS
    | MOTOR_SKIP_TOKENS
    | {"沈", "失"}  # 重複だが明示
)
_ZEN_TO_HAN_FINISH = ZEN_TO_HAN_DIGIT


@dataclass(frozen=True)
class MotorRun:
    """モーター 1 走分のレコード(履歴ビルダーの内部表現)。"""
    session_end: dt.date   # 当該走を含む節の最終開催日
    stadium: str           # "01"〜"24"
    motor_num: int         # 物理モーター番号
    grade_bucket: str      # "SG_G1" / "G2_G3_一般" / "全"
    racer_class: str       # "A1" / "A2" / "B1" / "B2"
    finish: str            # 正規化済 着順トークン


# --- スコアテーブル -------------------------------------------------------
def load_motor_score_table(repo: Path) -> dict[tuple[str, str], list[int]]:
    """Returns {(級別, グレード分類): [1着pt..6着pt]}.

    `data/estimate/motor_ability_score.csv` を読み込む。ファイル不在は
    `RuntimeError` で fail-fast(モーターpt の意味が変わるため検知重視)。
    """
    p = repo / "data" / "estimate" / "motor_ability_score.csv"
    if not p.exists():
        raise RuntimeError(
            f"motor_ability_score.csv not found at {p}. "
            "This file is required for モーターpt 計算. See docs/data/motor_ability_score.md."
        )
    df = pd.read_csv(p)
    table: dict[tuple[str, str], list[int]] = {}
    for _, row in df.iterrows():
        key = (str(row["級別"]).strip(), str(row["グレード分類"]).strip())
        table[key] = [int(row[f"{k}着pt"]) for k in range(1, 7)]
    return table


def grade_bucket_for_grade(grade_raw: str) -> str:
    """`title.csv` の `グレード` 列 → "SG_G1" / "G2_G3_一般"."""
    s = (grade_raw or "").strip()
    for tag in ("SG", "ＳＧ", "PG", "ＰＧ", "G1", "Ｇ１", "ＧⅠ"):
        if tag in s:
            return "SG_G1"
    return "G2_G3_一般"


def resolve_grade_bucket(racer_class: str, race_grade_bucket: str) -> str:
    """級別が B1/B2 ならグレードに関わらず '全' を返す。"""
    if racer_class in ("B1", "B2"):
        return "全"
    return race_grade_bucket


def normalize_finish_token(raw) -> str | None:
    """race_cards 14スロットの `着順` を正規化。未知/未充填は None."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() == "nan":
        return None
    if s in _ZEN_TO_HAN_FINISH:
        return str(_ZEN_TO_HAN_FINISH[s])
    # 数値形式("4" / "4.0" 等)
    try:
        i = int(float(s))
        if 1 <= i <= 6:
            return str(i)
    except (ValueError, TypeError):
        pass
    s = s.replace("Ｆ", "F").replace("Ｌ", "L")
    return s if s in _VALID_FINISH_TOKENS else None


# --- 1 走スコアリング -----------------------------------------------------
def score_motor_run(
    table: dict[tuple[str, str], list[int]], run: MotorRun
) -> tuple[int, int] | None:
    """Returns (得点, 分母増分=1) or None (=分母にも乗らない)."""
    bucket = run.grade_bucket if run.racer_class in ("A1", "A2") else "全"
    pts = table.get((run.racer_class, bucket))
    if pts is None:
        return None
    f = run.finish
    if f in ("1", "2", "3", "4", "5", "6"):
        return pts[int(f) - 1], 1
    if f in MOTOR_NEGATIVE_TOKENS:
        return MOTOR_NEGATIVE_SCORE, 1
    return None  # MOTOR_SKIP_TOKENS / 未知トークン


# --- モーター期起算日テーブル -------------------------------------------
def load_motor_period_starts(
    repo: Path, target_day: dt.date,
    fallback_days: int = MOTOR_PERIOD_FALLBACK_DAYS,
) -> dict[tuple[str, int], dt.date]:
    """Returns {(場コード2桁, モーター番号): モーター期起算日}.

    `data/programs/motor_stats/YYYY/MM/DD.csv` を target_day から遡って読む。
    motor_stats は当日開催のある場のみ収録するため、場ごとに「最初に見つかった
    スナップショット = 最新スナップショット」だけを採用する。
    """
    out: dict[tuple[str, int], dt.date] = {}
    seen_stadiums: set[str] = set()
    base = repo / "data" / "programs" / "motor_stats"
    for back in range(0, fallback_days + 1):
        d = target_day - dt.timedelta(days=back)
        p = base / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.csv"
        if not p.exists():
            continue
        df = pd.read_csv(p, dtype=str)
        if df.empty:
            continue
        new_stadiums: set[str] = set()
        for _, row in df.iterrows():
            try:
                stadium = str(row["場コード"]).zfill(2)
            except (KeyError, ValueError):
                continue
            if stadium in seen_stadiums:
                continue
            try:
                num = int(float(row["モーター番号"]))
                start = dt.date.fromisoformat(str(row["モーター期起算日"]).strip())
            except (ValueError, TypeError, KeyError):
                continue
            key = (stadium, num)
            if key not in out:
                out[key] = start
            new_stadiums.add(stadium)
        seen_stadiums |= new_stadiums
    return out


# --- 節境界検出 ----------------------------------------------------------
def _race_cards_path(repo: Path, day: dt.date) -> Path:
    return (repo / "data" / "programs" / "race_cards"
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")


def _title_path(repo: Path, day: dt.date) -> Path:
    return (repo / "data" / "programs" / "title"
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv")


def _has_races_at(repo: Path, day: dt.date, stadium: str) -> bool:
    p = _race_cards_path(repo, day)
    if not p.exists():
        return False
    try:
        df = pd.read_csv(p, dtype=str, usecols=["レースコード"])
    except (ValueError, KeyError):
        return False
    if df.empty:
        return False
    codes = df["レースコード"].dropna().astype(str)
    return bool((codes.str[8:10] == stadium).any())


def detect_session_end_days(
    repo: Path, stadium: str, window_end: dt.date,
    max_sessions: int = MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS,
    window_days: int = MOTOR_HISTORY_LOOKBACK_DAYS,
) -> list[dt.date]:
    """場 stadium の直近 max_sessions 節分の「節最終日」を新→旧で返す。

    window_end は除外(当日を含む節は計算対象から外す)。
    連続開催日(日差 1 日)を 1 節として束ねる。
    """
    open_days: list[dt.date] = []
    for back in range(1, window_days + 1):
        d = window_end - dt.timedelta(days=back)
        if _has_races_at(repo, d, stadium):
            open_days.append(d)
    if not open_days:
        return []
    open_days.sort()  # 古→新

    sessions: list[list[dt.date]] = []
    cur: list[dt.date] = []
    for d in open_days:
        if not cur or (d - cur[-1]).days <= 1:
            cur.append(d)
        else:
            sessions.append(cur)
            cur = [d]
    if cur:
        sessions.append(cur)

    last_days = [s[-1] for s in sessions]
    return last_days[-max_sessions:][::-1]  # 新→旧


# --- 1 節分の MotorRun 抽出 ---------------------------------------------
def extract_runs_for_session(
    repo: Path, stadium: str, session_end: dt.date,
) -> list[MotorRun]:
    """節最終日の race_cards + title から MotorRun のリストを生成。"""
    rc_path = _race_cards_path(repo, session_end)
    if not rc_path.exists():
        return []
    rc = pd.read_csv(rc_path, dtype=str)

    # 当該節当該場のグレード(節内一定)
    tt_path = _title_path(repo, session_end)
    grade_bucket = "G2_G3_一般"
    if tt_path.exists():
        tt = pd.read_csv(tt_path, dtype=str)
        if not tt.empty:
            match = tt[tt["レースコード"].astype(str).str[8:10] == stadium]
            if not match.empty:
                grade_bucket = grade_bucket_for_grade(
                    str(match.iloc[0].get("グレード", "")))

    # 当該場の全レースを舐めて (motor_num → (級別, slot dict)) 辞書を作る(先勝ち)
    motor_rows: dict[int, tuple[str, dict[str, object]]] = {}
    for _, row in rc.iterrows():
        code = str(row.get("レースコード", ""))
        if len(code) < 10 or code[8:10] != stadium:
            continue
        for n in range(1, 7):
            motor_raw = row.get(f"艇{n}_モーター番号")
            racer_class = str(row.get(f"艇{n}_級別") or "").strip()
            try:
                motor_num = int(float(motor_raw))
            except (ValueError, TypeError):
                continue
            if not racer_class or motor_num in motor_rows:
                continue
            slots: dict[str, object] = {}
            for d in range(1, 8):
                for s in (1, 2):
                    slots[f"D{d}走{s}"] = row.get(f"艇{n}_節D{d}走{s}_着順")
            motor_rows[motor_num] = (racer_class, slots)

    runs: list[MotorRun] = []
    for motor_num, (racer_class, slots) in motor_rows.items():
        eff_bucket = resolve_grade_bucket(racer_class, grade_bucket)
        for d in range(1, 8):
            for s in (1, 2):
                token = normalize_finish_token(slots[f"D{d}走{s}"])
                if token is None:
                    continue
                runs.append(MotorRun(
                    session_end=session_end, stadium=stadium,
                    motor_num=motor_num, grade_bucket=eff_bucket,
                    racer_class=racer_class, finish=token,
                ))
    return runs


# --- 全場横断履歴ローダ -------------------------------------------------
def load_motor_history(
    repo: Path, target_day: dt.date,
    period_starts: dict[tuple[str, int], dt.date] | None = None,
) -> dict[tuple[str, int], list[list[MotorRun]]]:
    """Returns {(場, モーター番号): [節1 MotorRun[], 節2, ...]} を新→旧で。

    各リストが 1 節分。最大 MOTOR_HISTORY_SESSIONS (=5) 件。
    period_starts が与えられた場合、節最終日 < モーター期起算日 の節は剪定する。
    """
    if period_starts is None:
        period_starts = load_motor_period_starts(repo, target_day)

    out: dict[tuple[str, int], list[list[MotorRun]]] = defaultdict(list)
    for stadium in (f"{i:02d}" for i in sorted(STADIUM_NAMES.keys())):
        session_ends = detect_session_end_days(repo, stadium, target_day)
        per_motor: dict[int, list[list[MotorRun]]] = defaultdict(list)
        for sess_end in session_ends:  # 新→旧
            sess_runs = extract_runs_for_session(repo, stadium, sess_end)
            grouped: dict[int, list[MotorRun]] = defaultdict(list)
            for r in sess_runs:
                grouped[r.motor_num].append(r)
            for motor_num, runs in grouped.items():
                per_motor[motor_num].append(runs)
        for motor_num, sessions in per_motor.items():
            period_start = period_starts.get((stadium, motor_num))
            if period_start is not None:
                sessions = [s for s in sessions
                            if s and s[0].session_end >= period_start]
            if sessions:
                out[(stadium, motor_num)] = sessions[:MOTOR_HISTORY_SESSIONS]
    return out


def motor_ability_pt(
    history: dict[tuple[str, int], list[list[MotorRun]]],
    score_table: dict[tuple[str, str], list[int]],
    stadium_code2: str, motor_num: int,
) -> float:
    """直近 5 節のスコア平均を返す。データなしは NaN(z 化で 50 補完)。"""
    sessions = history.get((stadium_code2, motor_num))
    if not sessions:
        return float("nan")
    total_pt, total_runs = 0, 0
    for sess in sessions:
        for run in sess:
            r = score_motor_run(score_table, run)
            if r is None:
                continue
            total_pt += r[0]
            total_runs += r[1]
    if total_runs == 0:
        return float("nan")
    return total_pt / total_runs


# ─────────────────────────────────────────────────────────────────────
# 4. 展示タイム偏差値
# ─────────────────────────────────────────────────────────────────────
def hensachi(values: list[float]) -> list[float]:
    arr = np.array(values, dtype=float)
    valid = ~np.isnan(arr)
    if valid.sum() < 2:
        return [float("nan")] * len(values)
    mean = arr[valid].mean()
    std = arr[valid].std(ddof=0)
    out = []
    for v in arr:
        if np.isnan(v):
            out.append(float("nan"))
        elif std == 0:
            out.append(50.0)
        else:
            out.append(50.0 + 10.0 * (mean - v) / std)
    return out


# ─────────────────────────────────────────────────────────────────────
# 5. 気象ポイント
# ─────────────────────────────────────────────────────────────────────
def load_sui_params(repo: Path) -> dict:
    df = pd.read_csv(repo / "data" / "estimate" / "stadium" / "sui_params.csv")
    out = {}
    for _, row in df.iterrows():
        stadium = row["stadium"]
        intercepts = pd.Series({c: row[f"base_c{c}"] for c in range(1, 7)})
        coefs = pd.DataFrame(
            {c: [row[f"{feat}_c{c}"] for feat in PARAM_FEATURES] for c in range(1, 7)},
            index=PARAM_FEATURES,
        )
        out[stadium] = {"intercepts": intercepts, "coefs": coefs}
    return out


def weather_features(weather: dict, facing_deg: float) -> np.ndarray:
    rel = (weather["wind_deg"] - facing_deg) % 360
    is_tail = int(rel < 45 or rel >= 315)
    is_head = int(135 <= rel < 225)
    return np.array([
        weather["wave_cm"],
        weather["air_temp"] - weather["water_temp"],
        is_tail * weather["wind_ms"],
        is_head * weather["wind_ms"],
        int(weather["weather"] == "曇"),
        int(weather["weather"] == "雨"),
    ])


def weather_advantage(params: dict, stadium_name: str, weather: dict) -> dict:
    """各コースについて「気象条件による有利pt変動」のみを返す。

    sui_params の `base_c{course}` 切片はコース固定有利(イン強さなど)を
    表すが、この情報は枠番pt(場×季節×コース別勝率)とほぼ完全に重複する。
    重み学習時の多重共線性(相関0.98+)を避けるため、ここでは切片を除いた
    「波・風・天候・気温水温差による相対変動」だけを返す。

    値は正負を取りうる(波が立つとイン不利=負、アウト有利=正など)。
    """
    facing = STADIUM_FACING.get(stadium_name, 0)
    feats = weather_features(weather, facing)
    p = params.get(stadium_name)
    if p is None:
        return {c: float("nan") for c in range(1, 7)}
    return {c: float(np.dot(feats, p["coefs"][c].values))
            for c in range(1, 7)}


# ─────────────────────────────────────────────────────────────────────
# 6. FeatureContext: バッチ呼出し向け共有キャッシュ
# ─────────────────────────────────────────────────────────────────────
class FeatureContext:
    """Shared cache for ``compute_features_for_day`` across a date window.

    Single-day callers (``build_index.py``) can ignore this entirely; the
    convenience entry point ``compute_features_for_day`` will construct a
    per-call context implicitly. Multi-day callers (``build_weights.py``)
    construct one context up-front covering ``[window_start, window_end]``,
    so static tables and file reads are amortized across days.

    Cache scope:
      * static tables (``waku_table`` / ``motor_score_table`` / ``sui_params``)
        — loaded once on first access, never refreshed for the lifetime of
        the Context.
      * ``race_cards`` / ``title`` per-day DataFrame caches — unbounded.
        Memory cost is ~7 MB for an 8-month window (~30 KB/file × ~240 files).
      * ``session_end_days`` — derived from a window-wide pre-computed
        ``session_index`` (built lazily on first ``motor_history`` call).
      * ``extract_runs_for_session`` — memoized per ``(stadium, session_end)``.
      * ``load_motor_period_starts`` — memoized per ``day``.

    NOTE: Not thread-safe. mutable dict キャッシュにロックを持たない。
    将来 multiprocessing を入れる場合は worker ごとに別 Context を持つこと。

    See ``docs/design/feature_context_refactor.md`` for the design rationale.
    """

    def __init__(self, repo: Path, *, window_start: dt.date, window_end: dt.date):
        if window_end < window_start:
            raise ValueError(
                f"window_end={window_end} must be >= window_start={window_start}"
            )
        self.repo = repo
        self.window_start = window_start
        self.window_end = window_end
        self._all_stadiums: list[str] = [
            f"{i:02d}" for i in sorted(STADIUM_NAMES.keys())
        ]
        # 静的テーブル(遅延ロード、1 回のみ)
        self._waku_table: dict | None = None
        self._motor_score_table: dict[tuple[str, str], list[int]] | None = None
        self._sui_params: dict | None = None
        # キャッシュ付きファイルアクセサ(無制限キャッシュ)
        self._race_cards_cache: dict[dt.date, pd.DataFrame | None] = {}
        self._title_cache: dict[dt.date, pd.DataFrame | None] = {}
        # session_index は遅延構築(初回 motor_history 呼出し時)
        self._session_index: dict[str, list[dt.date]] | None = None
        # extract_runs_for_session の memoize
        self._runs_cache: dict[tuple[str, dt.date], list[MotorRun]] = {}
        # load_motor_period_starts の per-day memoize
        self._period_starts_cache: dict[dt.date, dict[tuple[str, int], dt.date]] = {}

    # ─── 静的テーブル ──────────────────────────────────────────
    def waku_table(self) -> dict:
        if self._waku_table is None:
            self._waku_table = load_waku_table(self.repo)
        return self._waku_table

    def motor_score_table(self) -> dict[tuple[str, str], list[int]]:
        if self._motor_score_table is None:
            self._motor_score_table = load_motor_score_table(self.repo)
        return self._motor_score_table

    def sui_params(self) -> dict:
        if self._sui_params is None:
            self._sui_params = load_sui_params(self.repo)
        return self._sui_params

    # ─── キャッシュ付きファイルアクセサ ────────────────────────
    def race_cards_for(self, day: dt.date) -> pd.DataFrame | None:
        if day not in self._race_cards_cache:
            p = _race_cards_path(self.repo, day)
            self._race_cards_cache[day] = (
                pd.read_csv(p, dtype=str) if p.exists() else None
            )
        return self._race_cards_cache[day]

    def title_for(self, day: dt.date) -> pd.DataFrame | None:
        if day not in self._title_cache:
            p = _title_path(self.repo, day)
            self._title_cache[day] = (
                pd.read_csv(p, dtype=str) if p.exists() else None
            )
        return self._title_cache[day]

    # ─── モーター履歴 ─────────────────────────────────────────
    def _build_session_index(self) -> dict[str, list[dt.date]]:
        """全 24 場について window 内で参照しうる全 open-day を列挙。

        リード範囲:
          earliest = window_start - MOTOR_HISTORY_LOOKBACK_DAYS (90)
          latest   = window_end  - 1 day  (target_day 当日は除外設計)
        """
        earliest = self.window_start - dt.timedelta(days=MOTOR_HISTORY_LOOKBACK_DAYS)
        latest = self.window_end - dt.timedelta(days=1)
        out: dict[str, list[dt.date]] = {s: [] for s in self._all_stadiums}
        d = earliest
        while d <= latest:
            rc = self.race_cards_for(d)
            if rc is not None and not rc.empty and "レースコード" in rc.columns:
                codes = rc["レースコード"].dropna().astype(str)
                present = set(codes.str[8:10].unique())
                for s in present:
                    if s in out:
                        out[s].append(d)
            d += dt.timedelta(days=1)
        return out

    def session_end_days_for(
        self, target_day: dt.date, stadium: str,
    ) -> list[dt.date]:
        """``detect_session_end_days(repo, stadium, target_day)`` と byte-equivalent。

        事前構築した ``session_index`` から派生させてファイル再走査を回避する。
        """
        if self._session_index is None:
            self._session_index = self._build_session_index()
        cutoff_min = target_day - dt.timedelta(days=MOTOR_HISTORY_LOOKBACK_DAYS)
        in_window = [
            d for d in self._session_index.get(stadium, [])
            if cutoff_min <= d < target_day
        ]
        if not in_window:
            return []
        # 連続日を 1 節として束ねる (detect_session_end_days と同一ロジック)
        sessions: list[list[dt.date]] = []
        cur = [in_window[0]]
        for d in in_window[1:]:
            if (d - cur[-1]).days <= 1:
                cur.append(d)
            else:
                sessions.append(cur)
                cur = [d]
        sessions.append(cur)
        last_days = [s[-1] for s in sessions]
        return last_days[-MOTOR_HISTORY_LOOKBACK_MAX_SESSIONS:][::-1]

    def _extract_runs_for_session_cached(
        self, stadium: str, session_end: dt.date,
    ) -> list[MotorRun]:
        """``(stadium, session_end)`` 単位で ``extract_runs_for_session`` を memoize。

        既存関数をそのまま呼ぶので race_cards/title の二重読みは発生するが、
        呼出し回数は window × 24 場 × 5 節 ≒ 1,350 件で抑えられる。
        """
        key = (stadium, session_end)
        if key not in self._runs_cache:
            self._runs_cache[key] = extract_runs_for_session(
                self.repo, stadium, session_end,
            )
        return self._runs_cache[key]

    def _period_starts(self, day: dt.date) -> dict[tuple[str, int], dt.date]:
        if day not in self._period_starts_cache:
            self._period_starts_cache[day] = load_motor_period_starts(
                self.repo, day,
            )
        return self._period_starts_cache[day]

    def motor_history(
        self, day: dt.date,
    ) -> dict[tuple[str, int], list[list[MotorRun]]]:
        """``load_motor_history(repo, day)`` と byte-equivalent。Context キャッシュ経由。"""
        period_starts = self._period_starts(day)
        out: dict[tuple[str, int], list[list[MotorRun]]] = defaultdict(list)
        for stadium in self._all_stadiums:
            session_ends = self.session_end_days_for(day, stadium)
            per_motor: dict[int, list[list[MotorRun]]] = defaultdict(list)
            for sess_end in session_ends:
                grouped: dict[int, list[MotorRun]] = defaultdict(list)
                for r in self._extract_runs_for_session_cached(stadium, sess_end):
                    grouped[r.motor_num].append(r)
                for m, runs in grouped.items():
                    per_motor[m].append(runs)
            for m, sessions in per_motor.items():
                ps = period_starts.get((stadium, m))
                if ps is not None:
                    sessions = [s for s in sessions if s and s[0].session_end >= ps]
                if sessions:
                    out[(stadium, m)] = sessions[:MOTOR_HISTORY_SESSIONS]
        return out


# ─────────────────────────────────────────────────────────────────────
# 7. Per-day feature computation
# ─────────────────────────────────────────────────────────────────────
def parse_orig_exhibit_row(row) -> dict:
    out = {}
    for i in range(1, 7):
        vs = []
        for k in (1, 2, 3):
            try:
                v = float(row[f"艇{i}_値{k}"])
            except (ValueError, TypeError, KeyError):
                v = float("nan")
            vs.append(v)
        out[i] = vs
    return out


def build_recent_records(recent_row, boat_no: int) -> list[tuple[str, str]]:
    recs = []
    for k in range(1, 6):
        g = recent_row.get(f"艇{boat_no}_前{k}節_グレード", "") or ""
        seq = recent_row.get(f"艇{boat_no}_前{k}節_着順列", "") or ""
        if isinstance(g, float) and np.isnan(g):
            g = ""
        if isinstance(seq, float) and np.isnan(seq):
            seq = ""
        if g or seq:
            recs.append((str(g), str(seq)))
    return recs


def _load_realtime_preview_by_code(repo: Path, day: dt.date) -> dict:
    """Read per-source realtime preview CSVs and assemble a dict keyed by
    レースコード with the per-race weather / boats / oe_vals payload that
    ``compute_features_for_day`` consumes.

    Sources (under data/previews/):
        - sui/YYYY/MM/DD.csv  → weather (風速・風向・波・天候・気温・水温)
        - tkz/YYYY/MM/DD.csv  → 艇N_展示タイム
        - stt/YYYY/MM/DD.csv  → 艇N_コース
        - original_exhibition/YYYY/MM/DD.csv → 計測項目 + 艇N_値1〜3

    Returns ``{race_code: {"weather": dict|None, "boats": {1..6: {course,
    exhibit_time}}, "oe_vals": {1..6: [v1, v2, v3]}}}``. Races with no data
    in any source are absent.
    """
    base = repo / "data" / "previews"
    sui_p = base / "sui" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    tkz_p = base / "tkz" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    stt_p = base / "stt" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    oex_p = base / "original_exhibition" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"

    def _read(p):
        return pd.read_csv(p, dtype=str) if p.exists() else pd.DataFrame()

    sui = _read(sui_p)
    tkz = _read(tkz_p)
    stt = _read(stt_p)
    oex = _read(oex_p)

    sui_by = {r["レースコード"]: r for _, r in sui.iterrows()} if not sui.empty else {}
    tkz_by = {r["レースコード"]: r for _, r in tkz.iterrows()} if not tkz.empty else {}
    stt_by = {r["レースコード"]: r for _, r in stt.iterrows()} if not stt.empty else {}
    oex_by = {r["レースコード"]: r for _, r in oex.iterrows()} if not oex.empty else {}

    all_codes = set(sui_by) | set(tkz_by) | set(stt_by) | set(oex_by)
    if not all_codes:
        return {}

    def _f(row, key, default=float("nan")):
        if row is None:
            return default
        try:
            v = float(row[key])
            return default if np.isnan(v) else v
        except (ValueError, TypeError, KeyError):
            return default

    def _i(row, key, default=None):
        if row is None:
            return default
        try:
            v = float(row[key])
            return default if np.isnan(v) else int(v)
        except (ValueError, TypeError, KeyError):
            return default

    out = {}
    for code in all_codes:
        sui_row = sui_by.get(code)
        weather = None
        if sui_row is not None:
            air = _f(sui_row, "気温(℃)")
            wat = _f(sui_row, "水温(℃)")
            if not (np.isnan(air) or np.isnan(wat)):
                wind_ms = _f(sui_row, "風速(m)", 0.0)
                wind_code = _i(sui_row, "風向", 1)
                wave = _f(sui_row, "波の高さ(cm)", 0.0)
                weather_code = _i(sui_row, "天候", 1)
                weather = {
                    "wind_ms":    0.0 if np.isnan(wind_ms) else wind_ms,
                    "wind_deg":   WIND_CODE_TO_DEG.get(wind_code, 0),
                    "wave_cm":    0.0 if np.isnan(wave) else wave,
                    "weather":    WEATHER_CODE_TO_LABEL.get(weather_code, "晴"),
                    "air_temp":   air,
                    "water_temp": wat,
                }

        tkz_row = tkz_by.get(code)
        stt_row = stt_by.get(code)
        boats = {}
        for i in range(1, 7):
            course = _i(stt_row, f"艇{i}_コース", i) or i
            extime = _f(tkz_row, f"艇{i}_展示タイム")
            boats[i] = {"course": course, "exhibit_time": extime}

        oex_row = oex_by.get(code)
        oe_vals = (parse_orig_exhibit_row(oex_row) if oex_row is not None
                   else {i: [float("nan")] * 3 for i in range(1, 7)})

        out[code] = {"weather": weather, "boats": boats, "oe_vals": oe_vals}
    return out


def compute_features_for_day(
    repo: Path, day: dt.date, *, ctx: "FeatureContext | None" = None,
) -> pd.DataFrame:
    """Return a long-format DataFrame: one row per (race × boat 1..6).

    Columns: レースコード, レース日, レース場コード(2桁), レース回, 枠番,
             waku, racer, motor, exhibit, weather (5 raw feature pts).

    Race universe is taken from ``data/programs/race_cards/YYYY/MM/DD.csv``
    (boatcast.jp `bc_j_str3` API snapshot, written by ``race-card.py``).
    This source reflects the actual current-day schedule from boatcast and
    therefore stays correct on series-transition days (初日/最終日).

    Preview source: ``data/previews/{sui,tkz,stt,original_exhibition}/``
    (realtime per-source CSVs).  These are the sole preview source —
    the legacy combined ``data/previews/daily/`` file family was removed
    after its historical coverage was reconstructed into the per-source
    families.

    Motor pts: ``モーターpt`` = average of (級別×グレード得点) over the motor's
    last 5 節 at the same stadium, with motor period boundary (`モーター期起算日`)
    enforced. See ``docs/design/motor_ability_index.md`` for details.

    Missing previews / motor history / recent form fall back to NaN in the
    relevant columns.

    Parameters
    ----------
    ctx : FeatureContext, optional
        Shared cache for batch invocation across a date window. When omitted
        (single-day callers), a per-call Context is constructed implicitly
        with ``window=[day, day]``. When supplied, ``day`` must lie within
        ``[ctx.window_start, ctx.window_end]`` or ``ValueError`` is raised
        (the session_index would not cover the day's lookback otherwise).
        See ``docs/design/feature_context_refactor.md``.
    """
    if ctx is None:
        ctx = FeatureContext(repo, window_start=day, window_end=day)
    elif not (ctx.window_start <= day <= ctx.window_end):
        raise ValueError(
            f"day={day} is outside ctx window "
            f"[{ctx.window_start}, {ctx.window_end}]. "
            f"Construct a Context covering the day, or omit ctx for single-day use."
        )

    season = SEASON_BY_MONTH[day.month]

    waku_tab = ctx.waku_table()
    motor_score_table = ctx.motor_score_table()
    motor_history = ctx.motor_history(day)
    sui = ctx.sui_params()

    prog = ctx.race_cards_for(day)
    if prog is None:
        return pd.DataFrame()

    rn_path   = repo / "data" / "programs" / "recent_national" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    rl_path   = repo / "data" / "programs" / "recent_local"   / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"

    rn   = pd.read_csv(rn_path, dtype=str)   if rn_path.exists()   else pd.DataFrame()
    rl   = pd.read_csv(rl_path, dtype=str)   if rl_path.exists()   else pd.DataFrame()

    rn_by   = {r["レースコード"]: r for _, r in rn.iterrows()}   if not rn.empty   else {}
    rl_by   = {r["レースコード"]: r for _, r in rl.iterrows()}   if not rl.empty   else {}

    # Sole preview source: the realtime per-source CSV families
    # (sui / tkz / stt / original_exhibition).  When a race has no row in
    # any of them, preview-derived features fall back to NaN per-race below.
    realtime_by = _load_realtime_preview_by_code(repo, day)

    rows = []
    for _, prog_row in prog.iterrows():
        code = prog_row["レースコード"]
        try:
            stadium_code = int(code[8:10])
        except (ValueError, TypeError):
            continue
        stadium_code2 = f"{stadium_code:02d}"
        stadium_name = STADIUM_NAMES.get(stadium_code, "")
        # race_cards 形式の "01R" を "1R" に正規化。
        race_round_raw = prog_row.get("レース回", "")
        race_round = race_round_raw.lstrip("0") if isinstance(race_round_raw, str) else ""

        # Pull realtime per-source preview payload; default to NaN-everywhere
        # for races with no row in any of sui/tkz/stt/original_exhibition.
        rt = realtime_by.get(code)
        if rt is not None:
            prev_info = {"weather": rt["weather"], "boats": rt["boats"]}
            oe_vals = rt["oe_vals"]
        else:
            prev_info = {
                "weather": None,
                "boats": {i: {"course": i, "exhibit_time": float("nan")} for i in range(1, 7)},
            }
            oe_vals = {i: [float("nan")] * 3 for i in range(1, 7)}

        ext_z = hensachi([prev_info["boats"][i]["exhibit_time"] for i in range(1, 7)])
        v1_z = hensachi([oe_vals[i][0] for i in range(1, 7)])
        v2_z = hensachi([oe_vals[i][1] for i in range(1, 7)])
        v3_z = hensachi([oe_vals[i][2] for i in range(1, 7)])

        adv = (weather_advantage(sui, stadium_name, prev_info["weather"])
               if prev_info["weather"] is not None
               else {c: float("nan") for c in range(1, 7)})

        rn_row = rn_by.get(code)
        rl_row = rl_by.get(code)

        for waku in range(1, 7):
            course = prev_info["boats"][waku]["course"]
            if not (1 <= course <= 6):
                course = waku
            wpt = waku_pt(waku_tab, stadium_code2, season, course)

            recs = []
            if rn_row is not None:
                recs.extend(build_recent_records(rn_row, waku))
            if rl_row is not None:
                recs.extend(build_recent_records(rl_row, waku))
            rpt = racer_pt_for_boat(recs)

            # race_cards は "艇N_モーター番号" 形式。
            motor_raw = prog_row.get(f"艇{waku}_モーター番号", "")
            try:
                m_num = int(float(motor_raw))
                mpt = motor_ability_pt(
                    motor_history, motor_score_table, stadium_code2, m_num)
            except (ValueError, TypeError):
                mpt = float("nan")

            zs = [ext_z[waku - 1], v1_z[waku - 1], v2_z[waku - 1], v3_z[waku - 1]]
            zs = [z for z in zs if not (isinstance(z, float) and np.isnan(z))]
            ept = round(sum(zs) / len(zs), 2) if zs else float("nan")

            v_kishou = adv.get(course, float("nan"))
            kpt = round(v_kishou, 4) if not (isinstance(v_kishou, float)
                                             and np.isnan(v_kishou)) else float("nan")

            rows.append({
                "レースコード": code,
                "レース日":    f"{day:%Y-%m-%d}",
                "レース場コード": stadium_code2,
                "レース回":    race_round,
                "枠番":       waku,
                "waku":       wpt,
                "racer":      rpt,
                "motor":      mpt,
                "exhibit":    ept,
                "weather":    kpt,
            })
    return pd.DataFrame(rows)

"""
Reusable feature builders for the strength index pipeline.

Both scripts/build_index.py (daily output) and scripts/build_weights.py
(monthly weight fitting) consume these helpers so feature definitions
stay in lockstep.
"""
from __future__ import annotations

import datetime as dt
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
# 3. モーターポイント
# ─────────────────────────────────────────────────────────────────────
def load_motor_table_for_day(repo: Path, day: dt.date) -> dict:
    """Returns {(場コード2桁, モーター番号int): 勝率float}; falls back to up to 7 prior days."""
    p = repo / "data" / "programs" / "motor_stats" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    if not p.exists():
        for back in range(1, 8):
            alt = day - dt.timedelta(days=back)
            p_alt = repo / "data" / "programs" / "motor_stats" / f"{alt:%Y}" / f"{alt:%m}" / f"{alt:%d}.csv"
            if p_alt.exists():
                p = p_alt
                break
    table: dict[tuple[str, int], float] = {}
    if not p.exists():
        return table
    df = pd.read_csv(p, dtype=str)
    for _, r in df.iterrows():
        try:
            code = str(r["場コード"]).zfill(2)
            num = int(float(r["モーター番号"]))
            rate = float(r["勝率"]) if r["勝率"] not in (None, "", "nan") else 0.0
        except (ValueError, TypeError):
            continue
        table[(code, num)] = rate
    return table


def motor_pt(table: dict, stadium_code2: str, motor_num: int) -> float:
    """モーター勝率を返す。データなし or 勝率0 (モーター交換直後で実績ゼロ) の
    場合は NaN を返す。重み学習時は dropna で除外され、index 出力時もそのまま
    NaN として伝播する(欠損は欠損として明示)。
    """
    rate = table.get((stadium_code2, motor_num))
    if rate is None or rate == 0:
        return float("nan")
    return rate


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
# 6. Per-day feature computation
# ─────────────────────────────────────────────────────────────────────
def parse_preview_row(row) -> dict:
    def _f(key, default=float("nan")):
        try:
            v = float(row[key])
            return v if not np.isnan(v) else default
        except (ValueError, TypeError, KeyError):
            return default

    def _i(key, default=None):
        try:
            v = float(row[key])
            return default if np.isnan(v) else int(v)
        except (ValueError, TypeError, KeyError):
            return default

    weather = None
    try:
        air = _f("気温(℃)", float("nan"))
        wat = _f("水温(℃)", float("nan"))
        if not (np.isnan(air) or np.isnan(wat)):
            weather = {
                "wind_ms":    _f("風速(m)", 0.0) if not np.isnan(_f("風速(m)", 0.0)) else 0.0,
                "wind_deg":   WIND_CODE_TO_DEG.get(_i("風向", 1), 0),
                "wave_cm":    _f("波の高さ(cm)", 0.0) if not np.isnan(_f("波の高さ(cm)", 0.0)) else 0.0,
                "weather":    WEATHER_CODE_TO_LABEL.get(_i("天候", 1), "晴"),
                "air_temp":   air,
                "water_temp": wat,
            }
    except Exception:
        weather = None

    boats = {}
    for i in range(1, 7):
        try:
            course = int(float(row[f"艇{i}_コース"]))
            extime = float(row[f"艇{i}_展示タイム"])
        except (ValueError, TypeError, KeyError):
            course, extime = i, float("nan")
        boats[i] = {"course": course, "exhibit_time": extime}
    return {"weather": weather, "boats": boats}


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
    レースコード that has the same shape as ``parse_preview_row`` output.

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


def compute_features_for_day(repo: Path, day: dt.date) -> pd.DataFrame:
    """Return a long-format DataFrame: one row per (race × boat 1..6).

    Columns: レースコード, レース日, レース場コード(2桁), レース回, 枠番,
             waku, racer, motor, exhibit, weather (5 raw feature pts).

    Includes only races present in `data/programs/`. Missing previews,
    motors, etc. fall back to NaN in the relevant columns.
    """
    season = SEASON_BY_MONTH[day.month]

    waku_tab = load_waku_table(repo)
    motor_tab = load_motor_table_for_day(repo, day)
    sui = load_sui_params(repo)

    prog_path = repo / "data" / "programs" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    if not prog_path.exists():
        return pd.DataFrame()

    prev_path = repo / "data" / "previews" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    rn_path   = repo / "data" / "programs" / "recent_national" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    rl_path   = repo / "data" / "programs" / "recent_local"   / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
    oe_path   = repo / "data" / "previews" / "original_exhibition" / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"

    prog = pd.read_csv(prog_path, dtype=str)
    prev = pd.read_csv(prev_path, dtype=str) if prev_path.exists() else pd.DataFrame()
    rn   = pd.read_csv(rn_path, dtype=str)   if rn_path.exists()   else pd.DataFrame()
    rl   = pd.read_csv(rl_path, dtype=str)   if rl_path.exists()   else pd.DataFrame()
    oe   = pd.read_csv(oe_path, dtype=str)   if oe_path.exists()   else pd.DataFrame()

    prev_by = {r["レースコード"]: r for _, r in prev.iterrows()} if not prev.empty else {}
    rn_by   = {r["レースコード"]: r for _, r in rn.iterrows()}   if not rn.empty   else {}
    rl_by   = {r["レースコード"]: r for _, r in rl.iterrows()}   if not rl.empty   else {}
    oe_by   = {r["レースコード"]: r for _, r in oe.iterrows()}   if not oe.empty   else {}

    # Realtime per-source CSVs (preview-realtime.py appendages) override the
    # combined daily file when present, so the index reflects the freshest
    # weather/exhibit data without waiting for the once-a-day rebuild.
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
        race_round = prog_row.get("レース回", "")

        # Prefer realtime per-source CSVs; fall back to the combined daily file.
        rt = realtime_by.get(code)
        if rt is not None:
            prev_info = {"weather": rt["weather"], "boats": rt["boats"]}
            oe_vals = rt["oe_vals"]
        else:
            prev_row = prev_by.get(code)
            prev_info = parse_preview_row(prev_row) if prev_row is not None else {
                "weather": None,
                "boats": {i: {"course": i, "exhibit_time": float("nan")} for i in range(1, 7)},
            }
            oe_row = oe_by.get(code)
            oe_vals = parse_orig_exhibit_row(oe_row) if oe_row is not None else {
                i: [float("nan")] * 3 for i in range(1, 7)
            }

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

            try:
                m_num = int(float(prog_row.get(f"{waku}枠_モーター番号", "")))
                mpt = motor_pt(motor_tab, stadium_code2, m_num)
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

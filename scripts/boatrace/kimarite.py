"""決まり手セル (決まり手 × 1着コース) の特徴量構築とクラス定義。

穴予想の Stage1 — 「このレースはどう決まるか」を 32 クラスの多項分布で表す
モデルの入力側。学習 (``scripts/build_kimarite.py``) と推論
(``scripts/build_kimarite_probs.py``) で **同じ関数**を使い、
特徴量の作り方がズレないようにする。

設計は ``docs/design/ana_prediction.md`` (§4 モデル設計 / §12 High-Medium 指摘の解決)。
確定した構成:

* 学習窓は **全履歴**。log-loss が窓長に単調改善し 4 ヶ月で飽和 (§12.5)
* クラスは **凍結した 32 個**。月次再学習でスキーマが変わらないように (§12.1)
* 進入コースは **展示進入 (previews/stt)**。取れない艇は枠なり (§12.2)
* 場・天候は **one-hot**、風向は **sin/cos** (§12.6)
* 欠損は **全体 median** で補完。凝った方式はいずれも悪化した (§12.4)
* **daily 版と realtime 版の 2 本**を持つ。daily でも realtime の情報利得の 79% (§12.7)

この主目的は「荒れ度 = 1 − P(逃げ)」を出すこと。決まり手そのものを
レース単位で当てることはできていない (§14.2) ので、argmax は表示に使わない。
"""
from __future__ import annotations

import csv
import glob
import math
from pathlib import Path
from typing import Iterable

# results/realtime の 決まり手 は全角スペース入り
KIMARITE_MAP = {
    "逃　げ": "逃げ",
    "差　し": "差し",
    "まくり": "まくり",
    "まくり差し": "まくり差し",
    "抜　き": "抜き",
    "恵まれ": "恵まれ",
}

# ---------------------------------------------------------------------------
# クラス (セル) の凍結
# ---------------------------------------------------------------------------
# 決まり手 × 1着コース のうち、**2026-08-12 時点の全履歴 (41,740 レース) で
# n >= 60 だった 26 セル** + 受け皿の「その他_{1着コース}」6 個 = 32 クラス。
#
# **この一覧はコードに凍結する**。月次再学習のたびに閾値を評価し直すと
# クラスが増減して cell_coef.csv のスキーマが変わってしまうため (§12.1)。
# データが増えて新しいセルが 60 を超えても、ここを手で更新するまでは
# 「その他_{1着コース}」に入る (推論側と学習側で必ず同じ形になる)。
#
# 1着コースは **展示進入** 基準 (§12.2)。公式の決まり手は実進入基準なので、
# 展示と本番で進入が変わったレースでは「逃げ_2」のような一見ありえない
# 組合せが出る (実データで n=188)。これは定義どおりの挙動で、捨てない。
CELLS: tuple[str, ...] = (
    "その他_1", "その他_2", "その他_3", "その他_4", "その他_5", "その他_6",
    "まくり_2", "まくり_3", "まくり_4", "まくり_5", "まくり_6",
    "まくり差し_3", "まくり差し_4", "まくり差し_5", "まくり差し_6",
    "差し_2", "差し_3", "差し_4", "差し_5", "差し_6",
    "抜き_1", "抜き_2", "抜き_3", "抜き_4", "抜き_5", "抜き_6",
    "恵まれ_2", "恵まれ_3", "恵まれ_4", "恵まれ_5",
    "逃げ_1", "逃げ_2",
)
CELL_INDEX = {c: i for i, c in enumerate(CELLS)}
NIGE_CELL = "逃げ_1"

GRADE_SCORE = {"A1": 4.0, "A2": 3.0, "B1": 2.0, "B2": 1.0}

# 場コードは "01".."24"。one-hot の順序を固定する。
STADIUM_CODES = tuple(f"{i:02d}" for i in range(1, 25))
# 天候コード (results/previews の 天候 列)。1=晴 2=曇 3=雨 4=雪 5=風 (公式準拠)
WEATHER_CODES = (1, 2, 3, 4, 5)

# 艇ごとの特徴量。realtime は進入コース順、daily は枠番順に並べる。
CARD_FEATURES = (
    ("級別", "grade"),
    ("F本数", "fcount"),
    ("全国勝率", "num"),
    ("当地勝率", "num"),
    ("全国2連対率", "num"),
    ("モーター2連対率", "num"),
    ("ボート2連対率", "num"),
    ("全国平均ST", "num"),
)
# preview 由来 (realtime のみ)
PREVIEW_FEATURES = (
    ("スタート展示", "num"),
    ("展示タイム", "num"),
    ("チルト", "num"),
)


def cell_of(kimarite: str, first_course: int) -> str:
    """(決まり手, 1着コース) → 凍結クラス名。一覧に無ければ その他_{コース}。"""
    raw = f"{kimarite}_{first_course}"
    return raw if raw in CELL_INDEX else f"その他_{first_course}"


def feature_names(state: str) -> list[str]:
    """``state`` (daily / realtime) の特徴量名を固定順で返す。

    係数 CSV の列順もこの順序に一致させる。
    """
    names: list[str] = []
    prefix = "c" if state == "realtime" else "w"
    for slot in range(1, 7):
        for label, _ in CARD_FEATURES:
            names.append(f"{prefix}{slot}_{label}")
    if state == "realtime":
        for slot in range(1, 7):
            for label, _ in PREVIEW_FEATURES:
                names.append(f"c{slot}_{label}")
        names += ["枠なり", "前付け数", "風速", "波高", "気温", "水温",
                  "風向sin", "風向cos"]
        names += [f"天候_{w}" for w in WEATHER_CODES]
    names.append("レース回")
    names += [f"場_{s}" for s in STADIUM_CODES]
    return names


def _num(raw) -> float:
    try:
        v = float(str(raw).strip())
    except (TypeError, ValueError):
        return math.nan
    return v


def _fcount(raw) -> float:
    """"F1" / "F 1" / "" → 数値。取れなければ 0 (F なしとみなす)。"""
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return float(s) if s else 0.0


def entry_courses(stt_row: dict | None) -> list[int]:
    """展示進入コース (艇番→コース)。取れない艇は枠なりにフォールバック。

    フォールバック後もコースが重複する場合は枠なりに倒す
    (build_suji_table._entry_courses と同じ規約だが、あちらは None を返して
    そのレースを捨てる。こちらは推論で必ず値が要るので枠なりにする)。
    """
    courses = list(range(1, 7))
    if stt_row:
        for b in range(1, 7):
            v = _num(stt_row.get(f"艇{b}_コース"))
            if math.isfinite(v) and 1 <= v <= 6:
                courses[b - 1] = int(v)
    if len(set(courses)) != 6:
        return list(range(1, 7))
    return courses


def build_features(
    state: str,
    card_row: dict,
    stt_row: dict | None,
    tkz_row: dict | None,
    sui_row: dict | None,
    stadium_code: str,
    race_round: float,
) -> list[float]:
    """1 レース分の特徴量ベクトルを ``feature_names(state)`` と同じ順で返す。

    ``state == "daily"`` は ``race_cards`` と 場 / レース回 だけを使い、
    艇は **枠番順**に並べる (朝は展示進入が無いため)。
    ``state == "realtime"`` は **展示進入コース順**に並べ替え、preview と気象を足す。
    """
    courses = entry_courses(stt_row) if state == "realtime" else list(range(1, 7))
    # コース → 艇番
    boat_at = [0] * 7
    for boat, course in enumerate(courses, start=1):
        boat_at[course] = boat

    out: list[float] = []
    slots = range(1, 7)
    for slot in slots:
        boat = boat_at[slot] if state == "realtime" else slot
        for label, kind in CARD_FEATURES:
            raw = card_row.get(f"艇{boat}_{label}")
            if kind == "grade":
                out.append(GRADE_SCORE.get(str(raw).strip(), math.nan))
            elif kind == "fcount":
                out.append(_fcount(raw))
            else:
                out.append(_num(raw))

    if state == "realtime":
        for slot in slots:
            boat = boat_at[slot]
            for label, _ in PREVIEW_FEATURES:
                src = stt_row if label == "スタート展示" else tkz_row
                out.append(_num((src or {}).get(f"艇{boat}_{label}")))
        wakunari = 1.0 if courses == list(range(1, 7)) else 0.0
        maegake = float(sum(1 for b, c in enumerate(courses, start=1) if b != c))
        out += [wakunari, maegake]
        s = sui_row or {}
        out.append(_num(s.get("風速(m)")))
        out.append(_num(s.get("波の高さ(cm)")))
        out.append(_num(s.get("気温(℃)")))
        out.append(_num(s.get("水温(℃)")))
        wd = _num(s.get("風向"))
        if math.isfinite(wd):
            out.append(math.sin(2 * math.pi * wd / 16))
            out.append(math.cos(2 * math.pi * wd / 16))
        else:
            out += [math.nan, math.nan]
        weather = _num(s.get("天候"))
        for w in WEATHER_CODES:
            out.append(1.0 if weather == w else 0.0)

    out.append(race_round)
    for code in STADIUM_CODES:
        out.append(1.0 if stadium_code == code else 0.0)
    return out


# ---------------------------------------------------------------------------
# CSV 読み出しヘルパー (学習・推論で共有)
# ---------------------------------------------------------------------------
def read_by_race(path: Path) -> dict[str, dict]:
    """CSV を レースコード 引きの辞書にする。存在しなければ空。"""
    if not path.exists():
        return {}
    out: dict[str, dict] = {}
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            code = (row.get("レースコード") or "").strip()
            if code:
                out[code] = row
    return out


def iter_day_paths(repo: Path, rel: str) -> Iterable[Path]:
    """``data/<rel>/YYYY/MM/DD.csv`` を日付順に返す。"""
    for p in sorted(glob.glob(str(repo / "data" / rel / "*" / "*" / "*.csv"))):
        yield Path(p)


def race_round_of(raw) -> float:
    """"01R" / "1R" / "1" → 1.0。取れなければ NaN。"""
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    return float(s) if s else math.nan


def stadium_of(race_code: str) -> str:
    """レースコード = YYYYMMDD + 場コード(2桁) + レース回(2桁)。"""
    return str(race_code)[8:10]

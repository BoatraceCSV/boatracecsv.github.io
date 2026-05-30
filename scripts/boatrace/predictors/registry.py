"""予想者(predictor)レジストリ。

各予想者は固有 ID (``v1_basic``, ``v2_tenkai`` ...) を持ち、表示名・特徴量
セット (``component_keys``)・出力パス・運用ステータスをここで一元管理する。

新規予想者の追加: 必要なら ``COMPONENT_LABELS_REGISTRY`` に新成分を足し、
``PREDICTORS`` タプルに ``PredictorSpec`` を追加するだけ。
退役: 該当エントリの ``status`` を ``"retired"`` に変更する (過去データは保持)。

ID の命名規則:
  - 退役後も同じ ID は再利用しない (累計回収率が混ざるのを防ぐため)。
  - ``<バージョン>_<特徴>`` 形式を推奨 (例: ``v1_basic``, ``v2_tenkai``)。

詳細仕様は ``docs/data/estimate.md`` を参照。
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


# ─────────────────────────────────────────────────────────────────────
# Component キー / ラベル / 欠損補完値
# ─────────────────────────────────────────────────────────────────────
# Component key → 日本語ラベル (CSV 列名に使う)。
# 新規 component を追加するときは、ここに 1 行追加してから
# 該当の特徴量計算ロジックを ``index_features.py`` に実装する。
COMPONENT_LABELS_REGISTRY: Mapping[str, str] = {
    "waku":    "枠番pt",
    "racer":   "選手pt",
    "motor":   "モーターpt",
    "exhibit": "展示pt",
    "weather": "気象pt",
    # v2_tenkai (B君予想) で採用。スタート展示の進入コースと枠番のコース勝率
    # 差分を場別標準化した「進入変更による有利度」。
    "tenkai":  "展開優位pt",
}

# Component key → 欠損補完値 (偏差値pt スケール)。
# 通常は平均 50。選手pt のように欠損サンプルが実力下位に偏る場合は 30 を使う
# (新人 / 長期離脱明けを 50 扱いすると過大評価になりやすい)。
COMPONENT_MISSING_FALLBACK: Mapping[str, float] = {
    "racer": 30.0,
}
COMPONENT_MISSING_FALLBACK_DEFAULT: float = 50.0


def component_label(key: str) -> str:
    """Component key の日本語ラベルを返す。未登録なら ``KeyError``。"""
    return COMPONENT_LABELS_REGISTRY[key]


def component_missing_fallback(key: str) -> float:
    """Component key の欠損補完値 (偏差値pt スケール) を返す。"""
    return COMPONENT_MISSING_FALLBACK.get(
        key, COMPONENT_MISSING_FALLBACK_DEFAULT,
    )


# ─────────────────────────────────────────────────────────────────────
# Predictor spec
# ─────────────────────────────────────────────────────────────────────
STATUS_ACTIVE = "active"
STATUS_RETIRED = "retired"


@dataclass(frozen=True)
class PredictorSpec:
    """1 予想者の宣言的定義。"""

    predictor_id: str
    """予想者の固有 ID。退役後も再利用しない (累計回収率の同一性のため)。"""

    display_name: str
    """fun-site 等での表示名 (例: "A君予想")。"""

    slot: int
    """active な予想者の中での表示順。低いほど先頭に出る。"""

    status: str
    """``"active"`` か ``"retired"``。"""

    started_at: dt.date
    """この予想者で予想を出し始めた日 (累計回収率の起点)。"""

    component_keys: tuple[str, ...]
    """この予想者が使う特徴量キー (``COMPONENT_LABELS_REGISTRY`` の部分集合)。"""

    def __post_init__(self) -> None:
        if self.status not in (STATUS_ACTIVE, STATUS_RETIRED):
            raise ValueError(
                f"Unknown status {self.status!r} for "
                f"predictor {self.predictor_id!r}"
            )
        if not self.component_keys:
            raise ValueError(
                f"predictor {self.predictor_id!r} has no component_keys"
            )
        seen: set[str] = set()
        for key in self.component_keys:
            if key not in COMPONENT_LABELS_REGISTRY:
                raise ValueError(
                    f"Unknown component key {key!r} in "
                    f"predictor {self.predictor_id!r}. "
                    f"Register it in COMPONENT_LABELS_REGISTRY first."
                )
            if key in seen:
                raise ValueError(
                    f"Duplicate component key {key!r} in "
                    f"predictor {self.predictor_id!r}"
                )
            seen.add(key)

    # ── パス ──────────────────────────────────────────────────────
    def index_dir(self, repo: Path) -> Path:
        """``data/estimate/{predictor_id}/`` の絶対パス。"""
        return repo / "data" / "estimate" / self.predictor_id

    def index_csv_path(self, repo: Path, day: dt.date) -> Path:
        """``data/estimate/{predictor_id}/YYYY/MM/DD.csv``。"""
        return (
            self.index_dir(repo)
            / f"{day:%Y}" / f"{day:%m}" / f"{day:%d}.csv"
        )

    def weights_dir(self, repo: Path) -> Path:
        """``data/estimate/stadium/weights/{predictor_id}/``。"""
        return (
            repo / "data" / "estimate" / "stadium" / "weights"
            / self.predictor_id
        )

    def weights_csv_path(
        self, repo: Path, target_month: dt.date,
    ) -> Path:
        """``data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv``。"""
        return self.weights_dir(repo) / f"{target_month:%Y-%m}.csv"

    # ── ラベル ────────────────────────────────────────────────────
    def component_labels(self) -> dict[str, str]:
        """``component_keys`` → 日本語ラベル のマップ (registry から解決)。"""
        return {k: component_label(k) for k in self.component_keys}

    def is_active(self) -> bool:
        return self.status == STATUS_ACTIVE


# ─────────────────────────────────────────────────────────────────────
# レジストリ本体
# ─────────────────────────────────────────────────────────────────────
# Phase 1 では v1_basic = 現行 "A君予想" のみ active。
# Phase 2 で v2_tenkai (B君予想、6 成分) を追加予定。
#
# started_at は ``data/estimate/index/`` の最古ファイル日付に揃えてある
# (累計回収率の起点として fun-site 側で参照)。
PREDICTORS: tuple[PredictorSpec, ...] = (
    PredictorSpec(
        predictor_id="v1_basic",
        display_name="A君予想",
        slot=1,
        status=STATUS_ACTIVE,
        started_at=dt.date(2026, 5, 1),
        component_keys=("waku", "racer", "motor", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v2_tenkai",
        display_name="B君予想",
        slot=2,
        status=STATUS_ACTIVE,
        # Phase 2 投入日。展開優位pt の calculation が動き始める日付。
        # この日以前は data/estimate/v2_tenkai/ が生成されていないため
        # fun-site /predictors の累計回収率は当日からカウント開始する。
        started_at=dt.date(2026, 6, 1),
        component_keys=(
            "waku", "racer", "motor", "exhibit", "weather", "tenkai",
        ),
    ),
)


# ─────────────────────────────────────────────────────────────────────
# Lookup helpers
# ─────────────────────────────────────────────────────────────────────
def all_predictors() -> tuple[PredictorSpec, ...]:
    """登録されている全予想者 (active + retired) を返す。"""
    return PREDICTORS


def active_predictors() -> tuple[PredictorSpec, ...]:
    """``status == "active"`` の予想者を slot 昇順で返す。"""
    actives = [p for p in PREDICTORS if p.is_active()]
    return tuple(sorted(actives, key=lambda p: p.slot))


def predictor_by_id(predictor_id: str) -> PredictorSpec:
    """ID で 1 件取得。見つからなければ ``KeyError``。"""
    for p in PREDICTORS:
        if p.predictor_id == predictor_id:
            return p
    raise KeyError(f"Unknown predictor_id: {predictor_id!r}")

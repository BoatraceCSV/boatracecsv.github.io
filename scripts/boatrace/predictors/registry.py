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
    # v2_tenkai (B君予想) で採用。公式モーター2連対率(race_cards 由来の生値%)を
    # 場別標準化したもの。着順ベースの motor を置き換える独立指標。おかぺん評価との
    # 順位相関が高かった(notebooks/motor_pt_okapen_validation.ipynb)。
    "motor2rate": "モーター2連率pt",
    # v4_motor (モーター予想) で採用。エキスパート評価 4 場(平和島/唐津/大村/鳴門)
    # との順位相関でチューニングしたモーター能力指数。motor と同じ v2 計算式だが、
    # スコア表 v4(1着プレミアムの凸カーブ)・ペナルティ -50・直近 5 節を使う。
    # ラベルは v1_basic と同じ「モーターpt」(CSV 列名互換のため。ファイルは
    # predictor_id ごとに分かれるので衝突しない)。
    # 経緯: notebooks/motor_score_tuning/report.md
    "motor4": "モーターpt",
    # v6_course (コース予想) で採用。場×レース番号×コース別の収縮済み1着率
    # (data/estimate/stadium/course_win_rate.csv、build_course_rate.py が月次生成)。
    # 現行 waku (場×季節×コース) をレース番号次元に置き換えた指標。参照コースは
    # waku と同じ規約 (realtime = スタート展示の実進入、daily = 枠番フォールバック)。
    # ラベルは意味が変わるため「枠番pt」を流用せず新設 (列名は N枠_コースpt)。
    # 設計: docs/design/course_strength_v6.md
    "course": "コースpt",
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
# v1_basic = "A君予想" (5 成分、control)。現行唯一の active 予想者。
# v2_tenkai = "B君予想"。着順ベースの motor を公式モーター2連率 (motor2rate) に
# 置き換えた 5 成分構成 (2026-06-13〜)。導入当初 (2026-05-30〜06-13) は展開優位pt
# (tenkai) を加えた 6 成分版だった。
# v3_tenkai = "展開予想"。control (v1_basic) の 5 成分に展開優位pt (tenkai) を
# 加えた 6 成分版 (2026-06-20〜)。
#
# v4_motor = "モーター予想"。control (v1_basic) の motor をエキスパート評価
# チューニング版 (motor4) に差し替えた 5 成分版 (2026-07-20〜08-10)。
#
# v5_slit = "スリット予想"。control と同一の 5 成分で、fun-site 側の 1 マーク
# 走行距離計算・スリット図が使う予測 ST だけを AI 推定 ST (racer_st) に差し替えた
# 版 (2026-07-21〜08-10)。
#
# v8_aionly = "AI予想"。v7_aggregate と同一レシピ (index / 強さpt は同値) で、
# fun-site 側の買い目候補の選定だけを走行距離基準から強さpt のみ (±5.0pt 窓)
# に差し替えた版 (2026-07-28〜)。
#
# 2026-07-19 退役: v2_tenkai / v3_tenkai はいずれも control (v1_basic) に対して
# 有意な回収率差が得られなかったため status を "retired" にした。次の仮説を検証する
# ためのクリーンな状態へ戻す。退役後も過去データ (data/estimate/{id}/…) と成分定義
# (tenkai / motor2rate)・計算ロジックは保持する。命名規則どおり退役した
# predictor_id は再利用しない (累計回収率の同一性のため)。retired は
# active_predictors() から除外されるので、preview-realtime / build_index /
# build_weights / gcs_publisher いずれの計算対象からも自動的に外れる。
#
# 2026-08-09 退役: v6_course / v7_aggregate / v8_aionly の 3 つは、control
# (v1_basic) と同一レースで突き合わせたペア比較で **有意に悪い** と判定されたため
# status を "retired" にした。直前 (realtime) 買い目・確定レースのみを対象に、
# 各予想者の started_at 以降で control と同一レースを突き合わせた結果:
#
#   予想者          n      回収率   control  差        95%CI          p (並替) Holm
#   v6_course     3002   79.06%   85.97%   -6.91pt  [-13.1, -0.5]  0.0047   0.016
#   v7_aggregate  2717   78.10%   85.86%   -7.76pt  [-13.9, -1.7]  0.0040   0.016
#   v8_aionly     1892   77.30%   87.92%  -10.62pt  [-18.5, -2.9]  0.0001   0.0005
#   (参考) v4_motor 3035 85.97%   85.66%   +0.30pt  [ -2.4, +3.6]  0.884    0.884
#   (参考) v5_slit  3035 82.95%   85.66%   -2.72pt  [ -7.0, +1.2]  0.377    0.755
#
# 頑健性: 差分上位 20 レースを除外しても差はほぼ不変 (外れ値依存ではない)。日次でも
# control を下回った日が v6 17/20 日 (符号検定 p=0.0026)・v8 13/13 日 (p=0.0002)。
# 3 者とも control より買い目点数が多い (v8 は 14.7 点 vs 11.7 点) が、点数分布を
# control に合わせて標準化しても回収率は 76〜78% にとどまり、点数ではなく選定自体の
# 問題と判断した。
#
# 3 者に共通するのは waku → course の差し替え (v7/v8 は course + motor4)。course を
# 持たない v4_motor / v5_slit が control と同水準なので、course 成分が回収率を毀損
# している可能性が高い。的中率だけは v6/v7/v8 が高く (46.8〜48.9% vs 46.1〜46.6%)、
# 堅い決着は当てるが安いオッズを厚く買って EV を落とす負け方に見える。ただし 3 者は
# course を共有するため独立検定ではなく、実質「course 仮説を 1 回否定した」重み。
# 期間も 13〜20 日と短い。
#
# v2/v3 と同じく、退役後も過去データ (data/estimate/{id}/…) と成分定義 (course)・
# 計算ロジック (index_features.py の course_pt / build_course_rate.py) は保持する。
# course を作り直して再挑戦する場合は、退役した ID は再利用せず新しい ID を立てる。
#
# 2026-08-10 退役: v4_motor / v5_slit の 2 つは、上の比較 (2026-08-09 実施) で
# control (v1_basic) と **有意差なし** だったため status を "retired" にした。
#
#   予想者        n      回収率   control  差        95%CI          p (並替) Holm
#   v4_motor    3035   85.97%   85.66%   +0.30pt  [ -2.4, +3.6]  0.884    0.884
#   v5_slit     3035   82.95%   85.66%   -2.72pt  [ -7.0, +1.2]  0.377    0.755
#
# 「有意に悪い」ではないので消極的な退役だが、2 者とも control とレシピが近く
# (v5_slit は 5 成分が control と完全に同一で、差は fun-site 側の予測 ST のみ。
# v4_motor は motor → motor4 の 1 成分差)、実際に表示される買い目が control と
# 大きく重なる。似た買い目のスロットを並べても情報が増えないため、次の仮説を
# 検証するクリーンな状態 (active = control のみ) に戻すことを優先した。
#
# v5_slit については、退役を後押しする独立の観測がある: 実測 ST から組んだ
# スリット隊形は 1 着コースを強く規定する (強さpt に足して log loss -0.143) が、
# 締切前に取れる ST ではその 12% しか回収できず、75% を取るには ST の
# MAE 0.016 秒 (現行 0.053 秒) が必要という逆算結果 (docs/design/slit_tenkai.md)。
# 予測 ST の精度改善という v5_slit の路線は投資対効果が低い。
#
# 他と同じく、退役後も過去データ (data/estimate/{id}/…) と成分定義 (motor4)・
# 計算ロジック (index_features.py の motor4 / build_racer_st.py の racer_st) は
# 保持する。再挑戦する場合は退役した ID は再利用せず新しい ID を立てる。
#
# started_at は累計回収率の起点として fun-site 側で参照される。
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
        # 2026-07-19 退役。control (v1_basic) に対し有意な回収率差が得られなかった。
        status=STATUS_RETIRED,
        # recipe 変更日(展開優位pt 撤去 → motor を motor2rate に置換)。成績が
        # 混ざらないよう started_at をこの日にリセットし、累計回収率を当日から再計測する。
        started_at=dt.date(2026, 6, 13),
        # control (v1_basic) の motor を公式モーター2連率 (motor2rate) に差し替えた
        # 5 成分。motor 指標の優劣だけを A/B で比較する。
        component_keys=("waku", "racer", "motor2rate", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v3_tenkai",
        display_name="展開予想",
        slot=3,
        # 2026-07-19 退役。control (v1_basic) に対し有意な回収率差が得られなかった。
        status=STATUS_RETIRED,
        # 投入日。control (v1_basic) の 5 成分に展開優位pt (tenkai) を加えた
        # 6 成分版。展開優位pt は 2026-05-30〜06-13 に v2_tenkai で試行したが、
        # 当時は単独スロットでの再評価には至らなかったため、独立スロット
        # (v3_tenkai) として改めて累計回収率を計測する。
        started_at=dt.date(2026, 6, 20),
        component_keys=(
            "waku", "racer", "motor", "exhibit", "weather", "tenkai",
        ),
    ),
    PredictorSpec(
        predictor_id="v4_motor",
        display_name="モーター予想",
        slot=4,
        # 2026-08-10 退役。control (v1_basic) との同一レース比較で +0.30pt
        # (95%CI [-2.4, +3.6], p=0.884, n=3035) と有意差なし。control との差が
        # motor → motor4 の 1 成分のみで買い目も大きく重なるため、control 単独に
        # 戻した。詳細は上のレジストリ冒頭コメント。
        status=STATUS_RETIRED,
        # 投入日 (累計回収率の起点)。デプロイ日に合わせること。
        started_at=dt.date(2026, 7, 20),
        # control (v1_basic) の motor をチューニング済み motor4 に差し替えた
        # 5 成分。motor 指標のパラメータ差だけを A/B で比較する。
        # motor4 = スコア表 v4 (γ=1.5 凸カーブ) + ペナルティ -50 + 直近 5 節。
        component_keys=("waku", "racer", "motor4", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v5_slit",
        display_name="スリット予想",
        slot=5,
        # 2026-08-10 退役。control (v1_basic) との同一レース比較で -2.72pt
        # (95%CI [-7.0, +1.2], p=0.377, Holm 0.755, n=3035) と有意差なし。成分が
        # control と完全に同一で買い目もほぼ重なること、および予測 ST の精度改善の
        # 上限が低いこと (docs/design/slit_tenkai.md) から退役。
        # 詳細は上のレジストリ冒頭コメント。
        status=STATUS_RETIRED,
        # 投入日 (累計回収率の起点)。デプロイ日の翌日に合わせる。
        started_at=dt.date(2026, 7, 21),
        # control (v1_basic) と同一の 5 成分 (index / 強さpt は同一になる)。
        # 差分は fun-site 側の 1 マーク走行距離計算・スリット図が使う予測 ST のみ:
        # 全国平均ST → AI 推定 ST (data/estimate/racer_st/, build_racer_st.py)。
        # ST 推定の改善 (docs/design/st_estimation.md M3) だけを回収率 A/B で
        # 比較する。weights は成分が同一のため v1_basic と同値になる
        # (初月は v1_basic の weights ファイルをコピーしてブートストラップ)。
        component_keys=("waku", "racer", "motor", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v6_course",
        display_name="コース予想",
        slot=6,
        # 2026-08-09 退役。control (v1_basic) との同一レース比較で -6.91pt
        # (95%CI [-13.1, -0.5], p=0.0047, n=3002)。詳細は上のレジストリ冒頭コメント。
        status=STATUS_RETIRED,
        # 投入日 (累計回収率の起点)。デプロイ日に合わせること。
        started_at=dt.date(2026, 7, 22),
        # control (v1_basic) の waku を course (場×レース番号×コースの収縮済み
        # 1着率) に差し替えた 5 成分。テーブル定義の優劣だけを A/B で比較する。
        # 設計・ホールドアウト検証: docs/design/course_strength_v6.md
        component_keys=("course", "racer", "motor", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v7_aggregate",
        display_name="統合予想",
        slot=7,
        # 2026-08-09 退役。control (v1_basic) との同一レース比較で -7.76pt
        # (95%CI [-13.9, -1.7], p=0.0040, n=2717)。詳細は上のレジストリ冒頭コメント。
        status=STATUS_RETIRED,
        # 投入日 (累計回収率の起点)。デプロイ日に合わせること。
        started_at=dt.date(2026, 7, 23),
        # 統合予想 = v4_motor / v5_slit / v6_course の 3 仮説を全て適用した版。
        #   - v6_course 由来: waku → course (場×レース番号×コースの収縮済み1着率)
        #   - v4_motor  由来: motor → motor4 (スコア表 v4 + ペナルティ -50 + 直近 5 節)
        #   - v5_slit   由来: 予測 ST を全国平均 ST → AI 推定 ST (racer_st) に差し替え
        # component_keys には v6 の course と v4 の motor4 を両取りする。v5 の予測 ST
        # 差し替えは index / 強さpt には影響せず (成分は同一)、fun-site 側の
        # PredictorSpec.useEstimatedST フラグでのみ表現される (predictors.ts と同期)。
        # 単一仮説の control 比較ではなく、有望だった 3 仮説を束ねた総合スロット。
        component_keys=("course", "racer", "motor4", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v8_aionly",
        display_name="AI予想",
        slot=8,
        # 2026-08-09 退役。control (v1_basic) との同一レース比較で -10.62pt
        # (95%CI [-18.5, -2.9], p=0.0001, n=1892)。日次でも 13/13 日 control 未満。
        # 詳細は上のレジストリ冒頭コメント。
        status=STATUS_RETIRED,
        # 投入日 (累計回収率の起点)。デプロイ日に合わせること。
        started_at=dt.date(2026, 7, 28),
        # AI予想 = v7_aggregate と同一の 5 成分 (index / 強さpt は同値)。
        # 差分は fun-site 側の買い目候補の選定方法のみ: 1 マーク走行距離
        # (予測 ST + 強さpt/50) 基準の ±0.10 窓を、強さpt のみの ±5.0pt 窓
        # (等価スケール) に差し替える (fun-site predictors.ts の
        # PredictorSpec.strengthOnlyBetting)。予測 ST が買い目に与える影響を
        # 外した回収率を v7_aggregate と A/B 比較する。boatracecsv 側の
        # index / weights 計算は v7_aggregate と同一。weights は成分が同一の
        # ため v7_aggregate と同値になる (初月は v7_aggregate の weights
        # ファイルをコピーしてブートストラップ。翌月以降は monthly-weights の
        # --all-active が自動生成)。
        component_keys=("course", "racer", "motor4", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v9_suji",
        display_name="スジ予想",
        slot=9,
        status=STATUS_ACTIVE,
        # 投入日 (累計回収率の起点)。デプロイ日の翌日に合わせる。
        started_at=dt.date(2026, 8, 12),
        # 穴予想 (A案)。control (v1_basic) と **同一の 5 成分** で index / 強さpt は
        # 同値になる。差分は買い目の作り方だけ:
        #   1着   = 1 コース以外で 強さpt が最大の艇
        #   2-3着 = スジ表 P(2着, 3着 | 1着) の上位 5 ペア
        # フォーメーションでは表現できない出目集合になるため、買い目は
        # boatracecsv 側で確定させて data/estimate/suji/YYYY/MM/DD.csv に出す
        # (fun-site は表示と集計のみ。scripts/build_suji_picks.py)。
        # スジ表と決まり手注釈テーブルは data/estimate/suji/tables/ に月次生成
        # (scripts/build_suji_table.py)。
        # weights は成分が同一のため v1_basic と同値になる (初月は v1_basic の
        # weights ファイルをコピーしてブートストラップ。翌月以降は
        # monthly-weights の --all-active が自動生成)。
        # 設計・検証: docs/design/ana_prediction.md (§13 A案)
        component_keys=("waku", "racer", "motor", "exhibit", "weather"),
    ),
    PredictorSpec(
        predictor_id="v10_kimarite",
        display_name="穴予想",
        slot=10,
        status=STATUS_ACTIVE,
        # 投入日 (累計回収率の起点)。デプロイ日の翌日に合わせる。
        started_at=dt.date(2026, 8, 13),
        # 穴予想 (B案)。index / 強さpt は control (v1_basic) と同値で、差分は
        # 買い目の作り方:
        #   Stage1  決まり手 × 1着コース の 32 クラス確率 (多項ロジスティック回帰)
        #   Stage2  セル条件付きの 2-3 着表 P(2着, 3着 | セル)
        #   合成    120 通り → Plackett-Luce(強さpt) と w:1−w でブレンド
        #   買い目  1 コース頭を除いた上位 5 点
        # 買い目は boatracecsv 側で確定させて
        # data/estimate/kimarite/picks/YYYY/MM/DD.csv に出す
        # (fun-site は表示と集計のみ。scripts/build_kimarite_picks.py)。
        #
        # **A案 v9_suji との A/B は回収率では決着しない** (差 +4.2pt を検出するのに
        # 約 8.2 ヶ月かかる)。主判定は 3連単 log-loss
        # (scripts/build_kimarite_logloss.py が月次集計)。回収率は
        # 「control 比 -7pt 級の劣化」を見るガードレールとしてのみ使う。
        # 設計・検証: docs/design/ana_prediction.md (§13.3 判定基準)
        #
        # weights は成分が同一のため v1_basic と同値になる (初月は v1_basic の
        # weights ファイルをコピーしてブートストラップ)。
        component_keys=("waku", "racer", "motor", "exhibit", "weather"),
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

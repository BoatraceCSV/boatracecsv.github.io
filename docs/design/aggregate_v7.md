# 統合予想(3 仮説統合)v7_aggregate 設計書

control (`v1_basic`) に対して単独で検証してきた 3 つの改善仮説を **全て同時に適用**した
予想者 `v7_aggregate`(統合予想)の設計。

- 対象: `scripts/boatrace/predictors/registry.py`(`PredictorSpec` 追加のみ)
- 影響範囲: `infra/run-*.sh`(`ACTIVE_PREDICTORS`)、fun-site `packages/shared/src/predictors.ts`(`useEstimatedST: true`)、fun-site race 詳細ページの spec 駆動描画
- 新規の特徴量計算コード・新規データファイルは **なし**(既存 3 仮説の成分・ロジックを再利用)

---

## 1. 動機

`v4_motor` / `v5_slit` / `v6_course` は、それぞれ control (`v1_basic`) から **1 つの要素だけ**を
差し替えて回収率を A/B 比較するための実験スロットとして投入した:

| 予想者 | 仮説 | control からの差分 |
| --- | --- | --- |
| `v4_motor` | モーター指標をエキスパート評価でチューニング | `motor` → `motor4` |
| `v6_course` | 枠番強度をレース番号次元まで持つコース強度に | `waku` → `course` |
| `v5_slit` | 予測 ST を実測 ST 履歴ベースの AI 推定 ST に | 予測 ST(成分は不変) |

各仮説は独立した軸(モーター指標 / コース強度テーブル / スタート ST)を改善しており、
効果が直交する見込みが立ったため、**3 つを束ねた総合スロット**を control と比較して
「全部入り」の回収率を測る。単一仮説では控えめでも、重ねると効いてくる可能性を見る。

## 2. 構成

### 特徴量成分(index / 強さpt)

```
component_keys = (course, racer, motor4, exhibit, weather)
```

control (`v1_basic` = `waku, racer, motor, exhibit, weather`)から:

- `waku` → `course`(v6_course と同一。場×レース番号×コースの収縮済み1着率。
  参照コース規約・収縮式・テーブルソースは [`course_strength_v6.md`](./course_strength_v6.md) と同じ)
- `motor` → `motor4`(v4_motor と同一。スコア表 v4 + ペナルティ -50 + 直近 5 節。
  計算式・チューニング根拠は [`motor_score_tuning_v4.md`](./motor_score_tuning_v4.md) と同じ)

`course` と `motor4` は既に `index_features.py` の `compute_features_for_day` が
全予想者共通で毎回算出している素点列なので、**特徴量計算コードの追加は不要**。
`registry.py` に `component_keys` を宣言するだけで build_index / build_weights /
gcs_publisher が自動でこの組み合わせを処理する。

### 予測 ST(スタート予想図・1 マーク走行距離)

3 仮説目の `v5_slit` 由来の差し替え(全国平均 ST → AI 推定 ST。[`st_estimation.md`](./st_estimation.md))は
**index CSV には現れない**(強さpt の成分は上記 5 つで、v6_course 系と同一)。
fun-site 側で `PredictorSpec.useEstimatedST = true` として扱い、`computeOneMarkDistances` /
スタート予想図の予測 ST のみを `estimate/racer_st`(AI 推定 ST)に切り替える。
boatracecsv 側では `racer_st` は予想者非依存の共通ファイルなので追加生成は不要。

## 3. パス・運用

| 項目 | 値 |
| --- | --- |
| `predictor_id` | `v7_aggregate` |
| 表示名 | 統合予想 |
| slot | 7 |
| status | active |
| started_at | 2026-07-23 |
| index CSV | `data/estimate/v7_aggregate/YYYY/MM/DD.csv` |
| weights | `data/estimate/stadium/weights/v7_aggregate/YYYY-MM.csv` |

### weights のブートストラップ

`v7_aggregate` の成分組み合わせ(`course` + `motor4`)は既存のどの予想者とも
一致しない(`v6_course` は `motor`、`v4_motor` は `waku`)ため、weights ファイルを
他予想者からコピーしてブートストラップできない。投入初月分は全履歴 checkout の
ローカルで以下を実行して生成する:

```sh
python scripts/build_weights.py --predictor v7_aggregate --month 2026-07
```

翌月以降は monthly-weights ジョブ(`build_weights.py --all-active`)が
`registry.active_predictors()` を参照して自動生成する。weights が無い月の index は
強さpt が NaN になる(`build_index.py` の `find_weights_file` 参照)。

## 4. control との比較観点

`v7_aggregate` は 3 仮説を同時適用するため、control (`v1_basic`) との回収率差が出ても
**どの仮説が寄与したかは分離できない**(それは各単独スロット v4/v5/v6 の役割)。
本スロットの目的は「有望仮説を全部入れた版が control をどれだけ上回るか」の総合評価。
成分の寄与内訳は index CSV の `N枠_寄与_*` 列で個別レース単位に分解できる。

## 5. 経緯

- 2026-07-23: `v4_motor`(motor4)・`v6_course`(course)・`v5_slit`(AI 推定 ST)の
  3 仮説を統合した `v7_aggregate`(統合予想、slot=7)を投入。新規の特徴量計算・
  データファイルはなく、既存 3 仮説の成分・ロジックの組み合わせで構成。
- 2026-08-09: `v7_aggregate` を退役。control (`v1_basic`) と同一レースで突き合わせた
  ペア比較(直前買い目・確定レースのみ、2026-07-23〜08-09、n=2,717)で
  **78.10% vs 85.86%(-7.76pt、95%CI [-13.9, -1.7]、p=0.0040、Holm 補正後 0.016)**。
  差分上位 20 レースを除いても差は不変。同一レシピで買い目選定だけを変えた
  `v8_aionly` も同日退役(-10.62pt、p=0.0001)。
  §4 のとおり本スロット単独では寄与を分離できないが、同時期の単独スロット比較で
  `v4_motor` は +0.30pt(p=0.884)・`v5_slit` は -2.72pt(p=0.377)と control 同水準
  だったのに対し `v6_course` が -6.91pt(p=0.0047)だったため、**`course` の
  持ち込みが主因**と判断した。詳細は [`docs/data/estimate.md`](../data/estimate.md#現行レジストリ)
  の退役ノートおよび [`docs/design/course_strength_v6.md`](./course_strength_v6.md) を参照。

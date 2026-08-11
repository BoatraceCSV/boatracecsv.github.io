# Estimate(派生指標)

予測モデル向けに事前計算した派生データです。

- [予想者(Predictor)レジストリ](#予想者predictorレジストリ) — 複数予想者の管理と CSV パス規約
- [Strength Index](#strength-index) — レース 1 行 × 6 枠の「強さポイント」(偏差値)
- [Racer ST](#racer-st) — レース 1 行 × 6 枠の「選手別 推定ST」(秒)
- [穴予想 v9_suji: スジ表と買い目](#穴予想-v9_suji-スジ表と買い目) — スジ表 / 決まり手注釈テーブル / 日次の買い目
- [Stadium Parameters](#stadium-parameters) — Index 計算で参照する場別パラメータ

---

## 予想者(Predictor)レジストリ

このリポジトリは **複数の予想者(predictor)** を並行運用できる構造になっています。各予想者は固有 ID (`v1_basic`, `v2_tenkai`, ...) を持ち、採用する **特徴量セット (`component_keys`)** が異なります。レジストリの単一情報源は [`scripts/boatrace/predictors/registry.py`](../../scripts/boatrace/predictors/registry.py)。

### ID の命名規則

- 退役後も同じ ID は **再利用しない**(累計回収率の同一性のため)
- `<バージョン>_<特徴>` 形式を推奨。例: `v1_basic`, `v2_tenkai`(導入当初の採用特徴に由来。recipe を変更しても ID は据え置く)

### 出力パス規約

| 種別 | パス |
| --- | --- |
| 予想者ごとの index CSV | `data/estimate/{predictor_id}/YYYY/MM/DD.csv` |
| 予想者ごとの月次重み | `data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv` |
| 全予想者共通の場別パラメータ | `data/estimate/stadium/win_rate.csv`, `sui_params.csv` |

### 現行レジストリ

| ID | 表示名 | 状態 | 開始日 | 成分 |
| --- | --- | --- | --- | --- |
| `v1_basic` | A君予想 | active | 2026-05-01 | waku, racer, motor, exhibit, weather (5 成分) |
| `v2_tenkai` | B君予想 | **retired** (2026-07-19) | 2026-06-13 | waku, racer, **motor2rate**, exhibit, weather (5 成分) |
| `v3_tenkai` | 展開予想 | **retired** (2026-07-19) | 2026-06-20 | waku, racer, motor, exhibit, weather, **tenkai** (6 成分) |
| `v4_motor` | モーター予想 | **retired** (2026-08-10) | 2026-07-20 | waku, racer, **motor4**, exhibit, weather (5 成分) |
| `v5_slit` | スリット予想 | **retired** (2026-08-10) | 2026-07-21 | waku, racer, motor, exhibit, weather (v1_basic と同一 5 成分。**予測 ST のみ AI 推定 ST に差し替え**) |
| `v6_course` | コース予想 | **retired** (2026-08-09) | 2026-07-22 | **course**, racer, motor, exhibit, weather (waku を場×レース番号別コース強度に差し替え) |
| `v7_aggregate` | 統合予想 | **retired** (2026-08-09) | 2026-07-23 | **course**, racer, **motor4**, exhibit, weather + **予測 ST を AI 推定 ST に差し替え** (v4/v5/v6 の 3 仮説統合) |
| `v8_aionly` | AI予想 | **retired** (2026-08-09) | 2026-07-28 | course, racer, motor4, exhibit, weather (v7_aggregate と同一 5 成分。**買い目候補の選定のみ強さpt 基準 ±5.0pt に差し替え** — fun-site 側フラグ) |
| `v9_suji` | スジ予想 | active | 2026-08-12 | waku, racer, motor, exhibit, weather (v1_basic と同一 5 成分。**買い目の作り方のみ差し替え** — 下記) |

> **2026-08-10 退役**: `v4_motor` / `v5_slit` は control (`v1_basic`) と有意差が無く
> (p=0.884 / 0.377)、レシピが近いため買い目も control とほぼ重複していた。
> `registry.py` 冒頭コメントに検定結果。

> **`v9_suji`(スジ予想、2026-08-12 投入)** は **穴予想**。control (`v1_basic`) と
> **同一の 5 成分**(index / 強さpt は同値)で、差分は **買い目の作り方だけ**:
>
> * **1着** = **1 コース以外**で 強さpt が最大の艇
> * **2-3着** = スジ表 `P(2着コース, 3着コース | 1着コース)` の上位 5 ペア
>
> フォーメーション(各着の候補窓の直積)では表現できない出目集合になるため、
> **買い目は boatracecsv 側で確定させて [`data/estimate/suji/`](#穴予想-v9_suji-スジ表と買い目) に出力する**
> (fun-site は表示と集計のみ)。他の予想者は fun-site が強さpt から買い目を計算する
> ので、ここが構造的に違う。
>
> ホールドアウト実測(test 3,532 レース): 回収率 **80.6%** / 的中率 10.25% /
> 平均配当 **3,931 円** / **5.0 点** / 万舟 27 本。control (86.2% / 11.5 点 /
> 2,166 円) と回収率は同水準で、**購入額 43%・平均配当 1.8 倍**。
> 設計・検証は [`docs/design/ana_prediction.md`](../design/ana_prediction.md)(§13 A案)、
> テーブル構成の選定記録は
> [`notebooks/ana_prediction/report.md`](../../notebooks/ana_prediction/report.md)。
> weights は成分が同一のため v1_basic と同値になる(初月は v1_basic の weights を
> コピーしてブートストラップ)。

> **2026-07-19 退役**: `v2_tenkai`(motor2rate 版)と `v3_tenkai`(展開優位pt 版)はいずれも control である `v1_basic`(A君予想)に対して有意な回収率差が得られなかったため、`status` を `retired` にして運用から外した。

> **2026-08-09 退役**: `v6_course` / `v7_aggregate` / `v8_aionly` の 3 つは、control (`v1_basic`) と **同一レースで突き合わせたペア比較**で **有意に回収率が低い**と判定されたため `status` を `retired` にした。
>
> 検定は直前(realtime)買い目が組めた確定レースのみを対象に、各予想者の `started_at` 以降で control と同一レースを突き合わせて実施(ペア bootstrap 20,000 反復の 95% CI と、1 レースあたり収支差のペア並べ替え検定)。
>
> | 予想者 | n | 回収率 | control | 差 | 95% CI | p | Holm 補正 |
> | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |
> | `v6_course` | 3002 | 79.06% | 85.97% | **-6.91pt** | [-13.1, -0.5] | 0.0047 | 0.016 |
> | `v7_aggregate` | 2717 | 78.10% | 85.86% | **-7.76pt** | [-13.9, -1.7] | 0.0040 | 0.016 |
> | `v8_aionly` | 1892 | 77.30% | 87.92% | **-10.62pt** | [-18.5, -2.9] | 0.0001 | 0.0005 |
> | (参考) `v4_motor` | 3035 | 85.97% | 85.66% | +0.30pt | [-2.4, +3.6] | 0.884 | 0.884 |
> | (参考) `v5_slit` | 3035 | 82.95% | 85.66% | -2.72pt | [-7.0, +1.2] | 0.377 | 0.755 |
>
> 頑健性: 差分の大きい上位 20 レースを除外しても差はほぼ不変(外れ値依存ではない)。日次で control を下回った日数は `v6_course` 17/20 日(符号検定 p=0.0026)・`v8_aionly` 13/13 日(p=0.0002)。3 者とも control より買い目点数が多い(`v8_aionly` は 14.7 点 vs 11.7 点)が、点数分布を control に揃えて標準化しても回収率は 76〜78% にとどまるため、点数ではなく **選定そのもの**の問題と判断した。
>
> 3 者に共通するのは `waku` → `course` の差し替え(v7/v8 は `course` + `motor4`)。`course` を持たない `v4_motor` / `v5_slit` は control と同水準なので、**`course` 成分が回収率を毀損している**可能性が高い。的中率だけは v6/v7/v8 が高く(46.8〜48.9% vs 46.1〜46.6%)、堅い決着は当てるが安いオッズを厚く買って EV を落とす負け方に見える。ただし 3 者は `course` を共有するため独立検定ではなく、実質「`course` 仮説を 1 回否定した」重みで扱うこと。期間も 13〜20 日と短い。
>
> `course` 成分の定義・計算ロジック(`index_features.py` の コースpt / `build_course_rate.py` / `course_win_rate.csv` の月次再生成)は将来の再挑戦に備えて残してある。作り直して再投入する場合は、命名規則どおり退役した `predictor_id` は再利用せず新しい ID を立てる。

> **`v8_aionly`(AI予想、2026-08-09 退役)** は `v7_aggregate` と **同一の 5 成分**(index / 強さpt は同値。boatracecsv 側の index / weights 計算も同一)で、fun-site 側の **買い目候補の選定方法だけ**を差し替えた実験スロット。従来の 1 マーク走行距離(予測 ST + 強さpt/50)基準の ±0.10 窓ではなく、**強さpt のみの ±5.0pt 窓**(距離式が強さpt/50 を項に持つため等価スケール。ST 項を外した形)で各着の候補を選定する(fun-site `predictors.ts` の `PredictorSpec.strengthOnlyBetting`)。予測 ST が買い目に与える影響を外した回収率を `v7_aggregate` と A/B 比較する。weights は成分が同一のため `v7_aggregate` と同値になり、初月分は v7_aggregate の weights ファイルをコピーしてブートストラップする(翌月以降は monthly-weights の `--all-active` が自動生成)。

> **`v7_aggregate`(統合予想、2026-08-09 退役)** は、control に対して単独で検証してきた 3 仮説 — `v4_motor`(motor→**motor4**)・`v6_course`(waku→**course**)・`v5_slit`(予測 ST を **AI 推定 ST** に差し替え) — を **全て同時に適用**した総合スロット。`component_keys` は v6 の `course` と v4 の `motor4` を両取りした `course, racer, motor4, exhibit, weather`。v5 の予測 ST 差し替えは index / 強さpt には影響せず(成分は v6 系と同一)、fun-site 側の `PredictorSpec.useEstimatedST`(1 マーク走行距離計算・スリット図)でのみ効く。単一仮説の A/B ではなく、有望だった改善を束ねた版の回収率を control と比較する。設計は [`docs/design/aggregate_v7.md`](../design/aggregate_v7.md)。weights は独自の成分組み合わせ(`course` + `motor4`)のため他予想者からコピーできず、初月分は `build_weights.py --predictor v7_aggregate --month 2026-07` で生成してブートストラップする(翌月以降は monthly-weights の `--all-active` が自動生成)。

> **`v5_slit`(スリット予想)** は control (`v1_basic`) と **同一の 5 成分**(index / 強さpt は同値)で、fun-site 側の 1 マーク走行距離計算とスリット図が使う **予測 ST だけ**を全国平均 ST から [Racer ST](#racer-st)(AI 推定 ST)に差し替えた実験スロット。ST 推定の改善([`docs/design/st_estimation.md`](../design/st_estimation.md) M3 構成)単独の回収率効果を A/B 比較する。成分が同一のため weights も v1_basic と同値になり、初月分は v1_basic の weights ファイルをコピーしてブートストラップした(翌月からは build_weights --all-active が自動生成)。退役した予想者は `active_predictors()` から除外されるため、preview-realtime / build_index / build_weights / GCS ミラーいずれの計算対象からも自動的に外れる。過去の index CSV(`data/estimate/{id}/…`)と成分定義(`motor2rate` / `tenkai`)・計算ロジックは将来の再利用に備えて残してある。命名規則どおり退役した `predictor_id` は再利用しない。

> `v2_tenkai` は実験スロットだった。当初(2026-05-30〜06-13)は展開優位pt (`tenkai`) を加えた 6 成分版だったが control を下回ったため撤去し、2026-06-13 に A君予想の 5 成分のうち着順ベースの **`motor` を公式モーター2連率 `motor2rate` に置き換えた** 5 成分構成へ差し替え(成分数は control と同じで motor 指標だけを差し替え。おかぺん評価との順位相関検証で有望だった指標。[`notebooks/motor_pt_okapen_validation.ipynb`](../../notebooks/motor_pt_okapen_validation.ipynb))、`started_at` を当日へリセットして再計測していた。

> `v3_tenkai`(展開予想)は control (`v1_basic`) の 5 成分に **展開優位pt (`tenkai`)** を加えた 6 成分版を独立スロットとして投入したもの(2026-06-20〜07-19)。`tenkai` の計算ロジックは [`scripts/boatrace/index_features.py` の `tenkai_yui_pt()`](../../scripts/boatrace/index_features.py) に常駐しており、退役後も残している。`tenkai` は展示前(朝バッチ)に進入コース未取得のため [`DAILY_NEUTRAL_COMPONENTS`](../../scripts/build_index.py) で 50 に固定され、preview 反映後に確定していた。

新規予想者を追加するときは `registry.py` の `PREDICTORS` タプルに `PredictorSpec` を追記し、`COMPONENT_LABELS_REGISTRY` に新成分のラベルを追加します。`infra/run-*.sh` の `ACTIVE_PREDICTORS` 配列も同期して更新する必要があります(sparse-checkout と commit パス展開で参照)。

### CLI

`build_index.py` / `build_weights.py` ともに以下の引数で予想者を選択できます。

```sh
# v1_basic のみ
python scripts/build_index.py   --date 2026-05-24 --predictor v1_basic
python scripts/build_weights.py --month 2026-05  --predictor v1_basic

# active な全予想者をループ
python scripts/build_index.py   --date 2026-05-24 --all-active
python scripts/build_weights.py --month 2026-05  --all-active
```

`--predictor` を省略すると `v1_basic` がデフォルトになります(後方互換)。

---

## Strength Index

**強さポイント**(各予想者の中核出力)

- **ファイルパス**: `data/estimate/{predictor_id}/YYYY/MM/DD.csv`
- **URL 例**: https://boatracecsv.github.io/data/estimate/v1_basic/2026/05/03.csv

各レース 1 行で、6 枠分の「強さポイント」を `component_keys` ぶんの偏差値で表現したファイルです。予想者の `component_keys` に列挙された各要素を場別に学習した重みで線形結合し、平均 50・標準偏差 10 の偏差値スケールで出力します。

### v1_basic の特徴量(5 成分)

**枠番**・**選手**・**モーター**・**展示**・**気象** の 5 要素を採用。

### v4_motor の特徴量(5 成分)

v1_basic の 5 成分のうち、モーターpt の計算パラメータをエキスパート評価で
チューニングした **motor4** に差し替えた構成。計算式はモーター能力指数 v2
(z 残差 + 時間減衰 + ベイズ収縮)と同一で、以下の 3 点だけが異なる。

| パラメータ | v1_basic (`motor`) | v4_motor (`motor4`) |
| --- | --- | --- |
| スコア表 | [`motor_ability_score.csv`](./motor_ability_score.md)(着順に線形) | [`motor_ability_score_v4.csv`](./motor_ability_score.md#v4-テーブルmotor_ability_score_v4csv)(γ=1.5 凸カーブ = 1着プレミアム) |
| 事故ペナルティ(転/落/沈/エ) | -100 | **-50** |
| 採用節数 | 6 | **5** |

エキスパート評価 4 場(平和島 SS〜E / 唐津 S〜D / 大村 1〜7 点 / 鳴門 金銀銅)を
正解とした場別 Spearman 加重平均が +0.581 → +0.620 に改善し、
leave-one-stadium-out で全場一貫改善を確認したパラメータ
([`notebooks/motor_score_tuning/report.md`](../../notebooks/motor_score_tuning/report.md)、
設計は [`docs/design/motor_score_tuning_v4.md`](../design/motor_score_tuning_v4.md))。
CSV 列名は v1_basic と同じ `N枠_モーターpt`(motor4 のラベルを「モーターpt」に
統一しているため。ファイルは `data/estimate/v4_motor/` に分かれる)。

### v6_course の特徴量(5 成分)※ 2026-08-09 退役

> 2026-08-09 に退役(control 比 **-6.91pt**、p=0.0047)。以下は成分定義の記録で、
> 計算ロジックは残っている。詳細は[現行レジストリ](#現行レジストリ)の退役ノート参照。

v1_basic の 5 成分のうち、**枠番pt (`waku`)** を **コースpt (`course`)** に差し替えた構成。
`course` は **場 × レース番号 × コース**別の収縮済み1着率
([`data/estimate/stadium/course_win_rate.csv`](#dataestimatestadiumcourse_win_ratecsv))を
実進入コース(daily 時点は枠番)で引いた値を場別標準化したもの。CSV 列名は
`N枠_コースpt` / `N枠_寄与_コースpt`。

現行の枠番pt(場×季節×コース)がレース番号の次元を持たないのに対し、コース強度は
レース番号に強く依存する(全国イン1着率: 2〜4R ≒ 47% ⇔ 12R ≒ 71%。1R は企画レースの
影響で 59% と高く、方向は場に依存)ことをテーブル化した。季節次元は意図的に持たない
(場×レース番号セル n≈110〜160 を季節で 1/4 に割るとノイズ優位になるため)。
テーブル定義以外の参照ロジック(realtime = スタート展示の実進入コース、daily =
枠番フォールバック)は waku と同一で、control との差分はテーブル定義のみ。
設計・ホールドアウト検証は [`docs/design/course_strength_v6.md`](../design/course_strength_v6.md)。

### v7_aggregate の特徴量(5 成分)※ 2026-08-09 退役

> 2026-08-09 に退役(control 比 **-7.76pt**、p=0.0040)。同一レシピの `v8_aionly`
> も同日退役(**-10.62pt**、p=0.0001)。詳細は[現行レジストリ](#現行レジストリ)の退役ノート参照。

`v4_motor` / `v5_slit` / `v6_course` の 3 仮説を全て適用した統合構成。
成分は **`course`(v6 由来)+ `racer` + `motor4`(v4 由来)+ `exhibit` + `weather`** で、
control (`v1_basic`) から **`waku`→`course`** と **`motor`→`motor4`** の 2 成分を差し替えたもの。
CSV 列名は `N枠_コースpt`(course)と `N枠_モーターpt`(motor4)を含み、`v6_course` の
CSV から モーターpt 成分だけが motor4 値に変わった形になる(ファイルは
`data/estimate/v7_aggregate/` に分離)。`course` / `motor4` の算出ロジックは
それぞれ「v6_course の特徴量」「v4_motor の特徴量」と同一。

3 仮説目の `v5_slit`(予測 ST の AI 推定 ST 化)は **index / 強さpt には現れない**
(成分は v6 系と同じ)。fun-site 側で `PredictorSpec.useEstimatedST=true` として扱われ、
スタート予想図・1 マーク走行距離計算の予測 ST だけが [Racer ST](#racer-st)(AI 推定 ST)に
切り替わる。設計は [`docs/design/aggregate_v7.md`](../design/aggregate_v7.md)。

### v2_tenkai の特徴量(5 成分, 2026-07-19 退役)

> 退役済み。以下は当時の構成の記録。成分定義・計算ロジックは残してある。

v1_basic の 5 成分のうち、着順ベースの **モーターpt (`motor`)** を **モーター2連率pt (`motor2rate`)** に
置き換えた実験構成。成分数は control と同じ 5 で、モーター能力の指標だけが異なる。

`motor2rate` は **公式モーター2連対率**(`race_cards` の `艇N_モーター2連対率`、生値%)を場別に
偏差値化したもの。着順ベースの `motor`(モーター能力指数)に代わる、より素直な
モーター好不調の指標。新人モーター等で 2連率が空欄の場合は NaN → 下流で 50 補完
([`scripts/boatrace/index_features.py` の `parse_motor_2rate()`](../../scripts/boatrace/index_features.py))。
preview に依存しないため朝バッチ (`state=daily`) でも取得でき、`motor` と違って 50 中立への
フォールバックが少ない。

> **置き換えの根拠**: おかぺん評価(平和島の公開モーター評価)を正解とした順位相関検証で、
> 着順ベースの `motor` は相関ほぼ 0 だったのに対し、公式 2連対率は Spearman ρ≈0.6 と
> 高かった([`notebooks/motor_pt_okapen_validation.ipynb`](../../notebooks/motor_pt_okapen_validation.ipynb))。
> モーター指標だけを差し替えた v2_tenkai を、着順ベース motor のままの control v1_basic と
> 回収率で比較する。

> **展開優位pt (`tenkai`) について**: 2026-05-30〜06-12 の間 v2_tenkai が本成分を採用して
> いたが A君予想を回収率で下回ったため一旦撤去した。その後 2026-06-20 に独立スロット
> `v3_tenkai`(展開予想)として再投入し、現在は v3_tenkai の `component_keys` に含まれる。
> 計算ロジックは [`tenkai_yui_pt()`](../../scripts/boatrace/index_features.py)、ラベルは
> `COMPONENT_LABELS_REGISTRY` に常駐している。

### v3_tenkai の特徴量(6 成分, 2026-07-19 退役)

> 退役済み。以下は当時の構成の記録。成分定義・計算ロジックは残してある。

control (`v1_basic`) の 5 成分(枠番・選手・モーター・展示・気象)に **展開優位pt (`tenkai`)** を
6 番目として加えた構成。モーター指標は control と同じ着順ベース `motor` を使い、`tenkai` の
有無だけが control との差分になる。

展開優位pt は「スタート展示の進入コースと枠番デフォルトコースの **長期勝率差**」を場別標準化したもの。
- 進入変更なし → 偏差値 50 (中立)
- 枠より良いコースに入った (= 進入で前に行けた) → 偏差値 > 50
- 枠より悪いコースに入った (= 沈み込んだ) → 偏差値 < 50

raw 値は `data/estimate/stadium/win_rate.csv` の場×季節×コース別勝率を引いて
`win_rate(進入コース) - win_rate(枠番コース)` で算出する
([`tenkai_yui_pt()`](../../scripts/boatrace/index_features.py))。朝バッチ時点 (展示前) では
進入コース未取得のため枠番=進入扱い → raw=0 → `build_index.py` 側で 50 に上書きされる
([`DAILY_NEUTRAL_COMPONENTS`](../../scripts/build_index.py))。

### 生成パイプライン

1. **日次バッチ** (`scripts/build_index.py --mode daily --all-active`、JST 07:30): 当日のレース全件について、preview 非依存の成分(枠番・選手・モーター能力指数。v4_motor では motor の代わりにチューニング版 motor4、v6_course では waku の代わりにコースpt(枠番フォールバックで計算)、v7_aggregate では waku→course かつ motor→motor4 の両方、v2_tenkai では motor2rate)を計算し、preview 由来の成分(展示・気象・展開優位)は 50 (平均) で補完(`DAILY_NEUTRAL_COMPONENTS`)。状態 = `daily`、暫定の強さpt が入る。
2. **直前バッチ** (`scripts/preview-realtime.py` から内部呼び出し): 各レースの締切 5 分前に preview を取得した直後、対応する index 行の全成分を再計算(展示・気象が実値になるほか、枠番pt / コースpt / 気象pt はスタート展示の**実進入コース**基準に切り替わる)。状態 = `realtime`、強さpt が確定値に更新される。**active な全予想者ぶん**を 1 サイクルで更新。
3. **月次重み学習** (`scripts/build_weights.py --month YYYY-MM --all-active`、毎月 1 日 06:00 JST): 直近 6 ヶ月のデータから 24 場 × `n_components` 要素の重みを学習し、`data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv` を生成。同ジョブは学習前に `scripts/build_course_rate.py` で [`course_win_rate.csv`](#dataestimatestadiumcourse_win_ratecsv) を全履歴から再生成する。

### サンプルデータ(1行目、抜粋)

```
レースコード,レース日,レース場コード,レース回,状態,
1枠_枠番pt,1枠_寄与_枠番pt,1枠_選手pt,1枠_寄与_選手pt,1枠_モーターpt,1枠_寄与_モーターpt,1枠_展示pt,1枠_寄与_展示pt,1枠_気象pt,1枠_寄与_気象pt,1枠_強さpt,
2枠_… (同形式 11 列) … 6枠 まで,
202605030101,2026-05-03,01,1R,realtime,
68.84,30.28,36.59,8.06,50.00,5.94,54.93,3.24,18.63,1.30,51.84,
…
```

### 列の詳細説明

**基本情報**:

- `レースコード` / `レース日` / `レース場コード` / `レース回`: 他ファイルと同じ識別子
- `状態`: `daily`(日次バッチ完了、展示・気象は暫定50)/ `realtime`(直前バッチで展示・気象を実値に更新済み)

**艇 N の 11 列**(N=1..6, 計 66 列):

- `N枠_枠番pt`: 偏差値スケールの 枠番強度。`data/estimate/stadium/win_rate.csv` の場×季節×コース勝率を場別 (μ, σ) で標準化(v1_basic / v4_motor / v5_slit。v6_course / v7_aggregate はこの列の代わりに `N枠_コースpt` を持つ)
- `N枠_コースpt`: 偏差値スケールの 場×レース番号別コース強度(`v6_course` / `v7_aggregate` / `v8_aionly` が持つ。枠番pt の代替。3 者とも 2026-08-09 に退役したため、現在この列を出力する active 予想者はない)。`data/estimate/stadium/course_win_rate.csv` の収縮済み1着率を実進入コース(daily は枠番)で引いて場別標準化
- `N枠_選手pt`: 偏差値スケールの 選手能力指数。`data/programs/recent_national/` + `data/programs/recent_local/` の着順列をグレード別に得点化(算出基準点合計÷出走回数)し場別標準化。式は br-racers.jp の能力指数算出式に準拠
- `N枠_モーターpt`: 偏差値スケールの モーター強度。**モーター能力指数 v2**(直近 6 節の出走実績を「級別×グレード分類×コース」のセル統計で **z 残差**化し、半減期 60 日の **時間減衰**を加重して、サンプル不足モーターを平均(z 残差 0)へ **ベイズ収縮** (k=10) させた値)を場別標準化。`モーター期起算日`(`data/programs/motor_stats/`)で履歴をリセットし、期切替後の新モーターは収縮で平均寄りに引き戻される。スコアテーブルは [`data/estimate/motor_ability_score.csv`](./motor_ability_score.md) 参照。設計詳細は [`docs/design/motor_ability_index_v2.md`](../design/motor_ability_index_v2.md)(v1 設計は [`docs/design/motor_ability_index.md`](../design/motor_ability_index.md))。**`v4_motor` / `v7_aggregate` の CSV も同じ列名**だが、スコア表 v4・ペナルティ -50・直近 5 節で計算した `motor4` 成分の値になる(上記「v4_motor の特徴量」参照)
- `N枠_展示pt`: 偏差値スケールの 展示パフォーマンス。展示タイム + オリジナル展示の3項目をレース内偏差値化して平均、その後場別標準化
- `N枠_気象pt`: 偏差値スケールの 気象有利度。`data/estimate/stadium/sui_params.csv` で当日気象から各コースの有利pt変動を計算し場別標準化(コース固定有利は枠番ptに集約済み)
- `N枠_寄与_{要素}pt`: その要素の重み × 偏差値pt(= 強さptへの寄与の内訳)
- `N枠_強さpt`: 5 つの寄与の合計。Σ重み = 1 のため平均 50 ± 10 のスケールに収まる

### 補完ルール

- 元データが欠損した要素の偏差値ptは **50 で補完**(平均扱い)
- ただし **選手pt** が欠損する場合(新人 / 長期離脱明けで近5節の出走履歴が無いケースが大半)は、平均扱いだと過大評価になりやすいため **30 で補完** する。成分ごとの補完値は [`registry.py`](../../scripts/boatrace/predictors/registry.py) の `COMPONENT_MISSING_FALLBACK` で一元管理
- どの成分が欠損しても 強さpt は計算される
- 重みファイル(`data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv`)が見つからない月のデータは、すべて NaN を出力

> **用途**: 単発レースの予想に直接使えるランキング指標。`強さpt` 順で買い目を組み立てたり、寄与列でなぜ強い/弱いかを分解できる。重みは 6 ヶ月ローリングで学習されるため、季節変動を反映。

---

## Racer ST

**選手別 推定ST**(スリット予想 / 1マーク予想向けの予測 ST)

- **ファイルパス**: `data/estimate/racer_st/YYYY/MM/DD.csv`
- **URL 例**: https://boatracecsv.github.io/data/estimate/racer_st/2026/07/20.csv

各レース 1 行で、6 枠分の「推定 ST」(秒) を出力するファイルです。fun-site では
`useEstimatedST=true` の予想者 — `v5_slit`(スリット予想)と `v7_aggregate`(統合予想)—
のスタート予想図と 1 マーク走行距離計算が、公表の全国平均 ST に代えてこの値を読みます
(他の予想者は従来どおり全国平均 ST)。

### 計算式(M3 構成)

```
推定ST = shrunk_EWMA(選手の実測ST履歴) + コース補正(枠番) + F本数補正
```

- **EWMA**: `data/results/realtime` の実測 ST を半減期 30 日で時間減衰平均。
  対象日より前の日のぶんのみ使用(朝バッチ時点の情報制約)。F(負値 ST)は除外
- **収縮**: 事前分布(公表 全国平均ST。0.00 の実績なし選手は級別平均)へ実効 10 走ぶん収縮
- **コース補正**: 枠番別オフセット(1枠 -0.009 〜 6枠 +0.012)
- **F本数補正**: F0 -0.002 / F1 +0.010 / F2+ +0.038

パラメータは検証(テスト窓で現行比 MAE -4%・スリット順位 Spearman 0.231→0.296・
先頭艇一致 24%→34%)で確定した値をコードに凍結:
[`scripts/boatrace/racer_st.py`](../../scripts/boatrace/racer_st.py)。
検証記録は [`notebooks/st_estimation/phase2_report.md`](../../notebooks/st_estimation/phase2_report.md)、
設計は [`docs/design/st_estimation.md`](../design/st_estimation.md)。

> **推定 ST をどこまで良くする価値があるか**: 実測 ST から組んだスリット隊形は
> 1 着コースを強く規定する(log loss -0.143 / 強さpt 比)が、締切前に取れる ST では
> その **12%** しか回収できず、75% を取るには ST の MAE 0.016 秒(現行 0.053 秒)が
> 必要という逆算結果がある。スタート展示 ST は本番 ST との相関 0.05 で予測に使えない。
> 詳細は [`docs/design/slit_tenkai.md`](../design/slit_tenkai.md)。

### 列構成

- `レースコード` / `レース日` / `レース場コード` / `レース回`: 他ファイルと同じ識別子
- `N枠_登録番号` (N=1..6): その枠の選手登録番号(race_cards 由来)
- `N枠_推定ST` (N=1..6): 推定 ST 秒(小数 4 桁)。欠場等で選手が居ない枠は空欄
- `N枠_推定ST_p25` / `N枠_推定ST_p75` (N=1..6): 推定 ST の **25/75 パーセンタイル**。
  スリット図の帯(不確実性の幅)用。`推定ST` を中心に対称

**予測区間(帯)について**: 幅は `Q75_K(0.6173) × SIGMA_BASE(0.0684) × 選手別σ倍率` の
2 倍。`Q75_K` は Student-t(df 9.14 / scale 0.879)の 75 パーセンタイル係数で、
残差の超過尖度が 2.4 と裾が重いため正規分布(0.6745)では被覆が名目より 3.5pt 過大になる。
選手別σ倍率は実測 ST のばらつきを `K_SIGMA=10` で全体平均へ収縮した相対値
(実データで 0.68〜1.61、中央値 0.98)。テスト窓 47,000 艇走での実被覆は
名目 50/80/95% に対し 49.6/80.7/95.8%。帯の全幅は中央値 0.081 秒 ≒ 1.1m(約 0.4 艇身)。

> **なぜ点ではなく帯なのか**: 予測 ST 1 本でスリットを描くと先頭コースを 7 割外す
> ([`slit_tenkai.md`](../design/slit_tenkai.md) §5.1)。隊形そのものを確率で出す案は
> 検証の結果、事前情報では `in_margin` の分散の 3.9% しか説明できず不成立だった。
> 経緯は [`docs/design/slit_sim_plan.md`](../design/slit_sim_plan.md) と
> [`notebooks/slit_sim/report.md`](../../notebooks/slit_sim/report.md)。

### 生成パイプライン

`scripts/build_racer_st.py --date YYYY-MM-DD` が日次バッチ(run-daily-sync、JST 07:30)で
実行されます。選手別の EWMA 状態は `data/estimate/racer_st/state.csv`
(登録番号 / 重み付き和 / **重み付き二乗和** / 重み計 / 基準日)に永続化され、日次実行は
前日結果だけを増分で取り込みます(同一日での再実行は冪等)。`重み付き二乗和` は帯幅
(σ)用で、平均と同じ減衰・加算で更新できるため全履歴の走査は不要です。
状態が壊れた・60 日超取りこぼした場合は
全履歴が checkout されたローカルで `--rebuild` を実行して state.csv を作り直します。

> **`重み付き二乗和` 導入時の 1 回だけの手順**: この列より前に作られた state.csv には
> 二乗和が無いため、`load_state()` は 0 として読み、選手別σ倍率は 1.0(帯幅一定)に
> 退避します。エラーにはなりませんが帯が全選手同じ幅になるので、
> **リリース時に一度だけ `--rebuild` を実行**して state.csv を作り直してください。
> 全履歴(2025-11-01〜)で約 12 秒です。
>
> ```bash
> python scripts/build_racer_st.py --date $(date +%F) --rebuild
> ```

GCS ミラーには `csv_type=racer_st` でアップロードされます(`gcs_publisher.py`)。
直前バッチ(preview-realtime)では更新しません(検証で展示進入・展示 ST の反映に
効果が無いことを確認済みのため daily のみ)。preview-realtime の sparse-checkout
([`infra/run.sh`](../../infra/run.sh))に `data/estimate/racer_st/` が無いため、
そちらの publisher は `local_file_missing` でスキップします。

> **列を増やしたときの反映は翌朝**: fun-site は GitHub Pages ではなく **GCS ミラー**
> から CSV を読みます(`CSV_SOURCE=gcs`)。ミラーの racer_st を更新するのは
> daily-sync(JST 07:30)だけなので、**日中に列を追加して push しても、その日のうちは
> fun-site に届きません**(リポジトリには入っているのにサイトが変わらない、という形で
> 現れる)。当日中に反映したい場合はミラーへ直接コピーし、fun-site を強制再ビルドします:
>
> ```bash
> gsutil cp data/estimate/racer_st/$(date +%Y/%m/%d).csv gs://boatrace-realtime-data-boatrace-487212/data/estimate/racer_st/$(date +%Y/%m/%d).csv
> ```
>
> **daily-sync の手動再実行で代用しないこと。** `build_index.py --mode daily` は
> 既存 CSV を `atomic_write_csv` で丸ごと上書きし、`--mode realtime` 側にある
> 上書き防止ガードが daily 側には無いため、preview-realtime が積んだ
> `状態=realtime` 行を消してしまいます。

---

## 穴予想 v9_suji: スジ表と買い目

`v9_suji`(スジ予想)が使うファイル群。設計は
[`docs/design/ana_prediction.md`](../design/ana_prediction.md)(§13 A案 / §14 決まり手の表示方式)。

### `data/estimate/suji/tables/suji_table.csv`(静的・月次再生成)

**スジ表** — 1着コースを与えたときの 2-3 着コースの条件付き確率 `P(R2, R3 | R1)`。
`scripts/build_suji_table.py` が `results/realtime` × `previews/stt` の**全履歴**から生成し、
monthly-weights ジョブが毎月 1 日に再生成する。

| 列 | 説明 |
| --- | --- |
| `場コード` | `"00"` = 全場プール。**本番は全場プールのみ**(場別は予測を悪化させる。下記) |
| `1着コース` / `2着コース` / `3着コース` | 1〜6 |
| `n` | そのセルの観測レース数 |
| `確率` | `P(2着, 3着 \| 1着)`。1着コースごとに合計 1 |

行数は 6 通りの 1着コース × 残り 5 コースから 2 つの順列 20 = **120 行**。

> **場の次元を持たない理由**: 場別テーブル(全場プールへベイズ収縮)を試したが、
> どの収縮強度でもプールに追いつかなかった。**場差は「誰が 1着になるか」に出て、
> 「1着が決まった後の 2-3 着の並び」には出ない**(1コース1着率は戸田 42% ⇔ 大村 62% と
> 20pt 違うのに、`P(2着=1c | 1着=3c)` は標本誤差の範囲)。しかも 1着コースは 強さpt で
> 選んでおり、強さpt の `waku` 成分は場×季節×コース勝率で重みも場別に学習済みなので、
> **場の情報はすでに 1着選択側に入っている**。検証は
> [`notebooks/ana_prediction/report.md`](../../notebooks/ana_prediction/report.md)。
> `--by-stadium` フラグと `場コード` 列は将来の再検証のために残してある。

### `data/estimate/suji/tables/kimarite_table.csv`(静的・月次再生成)

**決まり手注釈テーブル** — 出目(1-2-3 着のコース並び)ごとの決まり手分布と最頻値。
買い目 1 点ごとの注釈(「3コースの まくり差し」)に使う。同じく 120 行。

| 列 | 説明 |
| --- | --- |
| `場コード` / `1着コース` / `2着コース` / `3着コース` | 出目の識別 |
| `n` | その出目が出たレース数 |
| `最頻決まり手` | 逃げ / 差し / まくり / まくり差し / 抜き / 恵まれ。観測ゼロなら空欄 |
| `逃げ` 〜 `恵まれ` | 各決まり手の構成比 |

> **これはレース単位の決まり手予測ではない。** 「この出目の並びは実際にはどの決まり手で
> 決まっていることが多いか」を引くだけ。決まり手をレース単位で当てることは
> できていない(条件付きでもベースレートを超えない)が、**出目ごとの注釈としては
> 的中率 63.2% 対 ベースライン 48.7%** で情報がある。決まり手を特定しているのは
> 事前情報ではなく **2着・3着の並びそのもの**(1コースが残っていればまくり差し、
> 外が続いていればまくり)。設計書 §14 を参照。

### `data/estimate/suji/YYYY/MM/DD.csv`(日次 + 直前)

**買い目** — `scripts/build_suji_picks.py` が出力する。レース × 状態 で 1 行。

| 列 | 説明 |
| --- | --- |
| `レースコード` / `レース日` / `レース場コード` / `レース回` | 他ファイルと同じ識別子 |
| `状態` | `daily`(朝バッチ。枠なり + 暫定 強さpt)/ `realtime`(直前バッチ。展示進入 + 確定 強さpt) |
| `1着コース` / `1着艇番` | 1 コース以外で 強さpt が最大だった艇とそのコース |
| `買い目1` 〜 `買い目5` | `"3-1-4"` 形式の出目(**艇番**)。5 点未満なら末尾が空欄 |
| `決まり手1` 〜 `決まり手5` | 各出目の最頻決まり手(`kimarite_table.csv` 由来) |

`daily` 行と `realtime` 行は index CSV と同様に**両方保持**する。
回収率の集計母数になるのは直前買い目(`realtime`)のみ。

---

## Stadium Parameters

**場別パラメータ**

`data/estimate/stadium/` 配下に、index 計算で参照する場別の係数・統計量を保存しています。

### `data/estimate/stadium/win_rate.csv`

場 × 季節 × コース別の長期勝率テーブル。`枠番pt` の生値ソース。

| 列 | 説明 |
| --- | --- |
| `場コード` | "01"〜"24" |
| `季節` | 春(3-5月)/ 夏(6-8月)/ 秋(9-11月)/ 冬(12-2月) |
| `1コース勝率` 〜 `6コース勝率` | コース別の長期1着率(%) |

### `data/estimate/stadium/course_win_rate.csv`

場 × レース番号 × コース別の収縮済み1着率テーブル。`v6_course` の `コースpt` の生値ソース。
`scripts/build_course_rate.py` が `data/results/realtime/` の**全履歴**から生成し、
monthly-weights ジョブが毎月 1 日に再生成する。24 場 × 12 レース番号 = 288 行。

| 列 | 説明 |
| --- | --- |
| `場コード` | "01"〜"24" |
| `レース回` | 1〜12(整数、"R" なし) |
| `n` | セルの集計レース数(収縮前の生 n。診断用) |
| `1コース勝率` 〜 `6コース勝率` | 収縮済みコース別1着率(%) |

セル値はベイズ収縮 `rate = (wins + k·base) / (n + k)`(k=50、base = 場×コースの
全レース番号率)。k はホールドアウト検証(Brier / log-loss)で確定した値
([`docs/design/course_strength_v6.md`](../design/course_strength_v6.md))。季節次元は
持たない。読み込み側(`load_course_table`)はセル欠損 → 場×コース全体率 → NaN
(偏差値 50 補完)の順でフォールバックし、ファイル自体が無い場合も安全に NaN 化する。

### `data/estimate/stadium/sui_params.csv`

24 場分の気象線形回帰パラメータ。波・風(追い/向かい)・気温水温差・天候から各コースの有利pt変動を計算する係数。1 場 1 行、43 列(stadium + 切片6 + 6特徴量×6コース = 36)。

| 列グループ | 内容 |
| --- | --- |
| `base_c1` 〜 `base_c6` | 基準条件(凪・無風・晴・気温=水温)下の有利pt切片 ※index計算では切片は使わず変動分のみを `気象pt` に反映 |
| `wave_cm_c1〜c6` | 波高 1cm あたりの有利pt変化 |
| `temp_diff_c1〜c6` | 気温-水温差 1℃ あたりの有利pt変化 |
| `wind_tail_ms_c1〜c6` | 追い風 1m/s あたりの有利pt変化 |
| `wind_head_ms_c1〜c6` | 向かい風 1m/s あたりの有利pt変化 |
| `is_cloudy_c1〜c6` | 曇り(vs 晴)による有利ptシフト |
| `is_rainy_c1〜c6` | 雨(vs 晴)による有利ptシフト |

風向は各場の `facing_deg`(スタンド方位)で正規化して 追い風/向かい風/横風 のカテゴリに分けます。場ごとの learned R² は概ね 0.05〜0.20。`scripts/build_sui_params.py` で実データから再学習可能。

### `data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv`

毎月 1 日に再学習される 24 場 × `n_components` 要素の重みファイル(予想者ごとに別ディレクトリ)。学習窓は対象月の 6 ヶ月前〜前月末。各場 1 行、`stadium`, `n_samples`, 各要素の `mu_*` / `sigma_*` / `w_*`, `mu_y`, `sigma_y`, `mse`, `r2`, `fallback` を含む。

| 列 | 説明 |
| --- | --- |
| `stadium` | 場名(全角:桐生・戸田 等) |
| `n_samples` | SLSQP fit に使われた行数 |
| `mu_{key}` / `sigma_{key}` | その場の各成分生pt値の平均と標準偏差(偏差値変換に使用) |
| `w_{key}` | その要素の重み(非負・合計 1) |
| `r2` | 着順予測の決定係数 |
| `fallback` | 1 = サンプル不足で均等重み(1/n_components ずつ)に倒した |

build_index.py は実行時に **対象日の月以下で最新の重みファイル** を予想者ごとに自動選択するため、未来日(月)用に重みファイルを事前生成しておく運用も可能。

`SHORT_HISTORY_COMPONENTS` で宣言された成分(現状は `motor` と `motor4`)は backfill が長くできないことを許容するため、SLSQP fit では他成分が欠損していない行で imputation (z=0) して使う。

> **用途**: index 計算の中間成果物。重みの場別比較をすると、たとえば桐生は気象pt の重みが大きい(波が立ちやすいレース場)、福岡は 枠番pt の重みが大きい(イン強度が高い)など、場の性格が数値で見える。

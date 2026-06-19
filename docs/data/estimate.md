# Estimate(派生指標)

予測モデル向けに事前計算した派生データです。

- [予想者(Predictor)レジストリ](#予想者predictorレジストリ) — 複数予想者の管理と CSV パス規約
- [Strength Index](#strength-index) — レース 1 行 × 6 枠の「強さポイント」(偏差値)
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
| `v2_tenkai` | B君予想 | active | 2026-06-13 | waku, racer, **motor2rate**, exhibit, weather (5 成分) |
| `v3_tenkai` | 展開予想 | active | 2026-06-20 | waku, racer, motor, exhibit, weather, **tenkai** (6 成分) |

> `v2_tenkai` は実験スロット。展開優位pt (`tenkai`) を加えた 6 成分版 (2026-05-30〜) は control である A君予想を回収率で下回ったため 2026-06-13 に撤去。同日、次の実験として A君予想の 5 成分のうち着順ベースの **`motor` を公式モーター2連率 `motor2rate` に置き換えた** 5 成分構成を投入した(成分数は control と同じで motor 指標だけを差し替え。おかぺん評価との順位相関検証で有望だった指標。[`notebooks/motor_pt_okapen_validation.ipynb`](../../notebooks/motor_pt_okapen_validation.ipynb))。recipe 変更に伴い `started_at` を当日へリセット(累計回収率を再計測)。`predictor_id` は据え置き。

> `v3_tenkai`(展開予想)は control (`v1_basic`) の 5 成分に **展開優位pt (`tenkai`)** を加えた 6 成分版を独立スロットとして投入したもの(2026-06-20〜)。`tenkai` の計算ロジックは [`scripts/boatrace/index_features.py` の `tenkai_yui_pt()`](../../scripts/boatrace/index_features.py) に常駐している。`tenkai` は展示前(朝バッチ)に進入コース未取得のため [`DAILY_NEUTRAL_COMPONENTS`](../../scripts/build_index.py) で 50 に固定され、preview 反映後に確定する。累計回収率は `started_at` 当日からカウント開始。

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

### v2_tenkai の特徴量(5 成分)

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

### v3_tenkai の特徴量(6 成分)

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

1. **日次バッチ** (`scripts/build_index.py --mode daily --all-active`、JST 07:30): 当日のレース全件について、preview 非依存の成分(枠番・選手・モーター能力指数。v2_tenkai では motor の代わりに motor2rate)を計算し、preview 由来の成分(展示・気象・展開優位)は 50 (平均) で補完(`DAILY_NEUTRAL_COMPONENTS`)。状態 = `daily`、暫定の強さpt が入る。
2. **直前バッチ** (`scripts/preview-realtime.py` から内部呼び出し): 各レースの締切 5 分前に preview を取得した直後、対応する index 行の展示・気象を実値で再計算。状態 = `realtime`、強さpt が確定値に更新される。**active な全予想者ぶん**を 1 サイクルで更新。
3. **月次重み学習** (`scripts/build_weights.py --month YYYY-MM --all-active`、毎月 1 日 06:00 JST): 直近 6 ヶ月のデータから 24 場 × `n_components` 要素の重みを学習し、`data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv` を生成。

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

- `N枠_枠番pt`: 偏差値スケールの 枠番強度。`data/estimate/stadium/win_rate.csv` の場×季節×コース勝率を場別 (μ, σ) で標準化
- `N枠_選手pt`: 偏差値スケールの 選手能力指数。`data/programs/recent_national/` + `data/programs/recent_local/` の着順列をグレード別に得点化(算出基準点合計÷出走回数)し場別標準化。式は br-racers.jp の能力指数算出式に準拠
- `N枠_モーターpt`: 偏差値スケールの モーター強度。**モーター能力指数 v2**(直近 6 節の出走実績を「級別×グレード分類×コース」のセル統計で **z 残差**化し、半減期 60 日の **時間減衰**を加重して、サンプル不足モーターを平均(z 残差 0)へ **ベイズ収縮** (k=10) させた値)を場別標準化。`モーター期起算日`(`data/programs/motor_stats/`)で履歴をリセットし、期切替後の新モーターは収縮で平均寄りに引き戻される。スコアテーブルは [`data/estimate/motor_ability_score.csv`](./motor_ability_score.md) 参照。設計詳細は [`docs/design/motor_ability_index_v2.md`](../design/motor_ability_index_v2.md)(v1 設計は [`docs/design/motor_ability_index.md`](../design/motor_ability_index.md))
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

`SHORT_HISTORY_COMPONENTS` で宣言された成分(現状は `motor`)は backfill が長くできないことを許容するため、SLSQP fit では他成分が欠損していない行で imputation (z=0) して使う。

> **用途**: index 計算の中間成果物。重みの場別比較をすると、たとえば桐生は気象pt の重みが大きい(波が立ちやすいレース場)、福岡は 枠番pt の重みが大きい(イン強度が高い)など、場の性格が数値で見える。

# Motor Ability Score Table

モーター能力指数(`N枠_モーターpt`)を計算するためのスコアテーブル。

- **ファイルパス**: `data/estimate/motor_ability_score.csv`
- **配信 URL**: https://boatracecsv.github.io/data/estimate/motor_ability_score.csv
- **設計書**: [`docs/design/motor_ability_index.md`](../design/motor_ability_index.md)
- **使われる場所**: `scripts/boatrace/index_features.py` の `motor_ability_pt()` から `load_motor_score_table()` 経由で読み込まれる

## スキーマ

| 列 | 型 | 説明 |
| --- | --- | --- |
| `級別` | str | `B2` / `B1` / `A2` / `A1` |
| `グレード分類` | str | `全`(B2/B1)/ `SG_G1` / `G2_G3_一般`(A2/A1) |
| `1着pt` 〜 `6着pt` | int | 各着順の得点 |

行は 6 行(級別 × グレード分類の組み合わせ)。

## 現行データ

```csv
級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,100,75,50,25,0
B1,全,100,80,60,40,20,0
A2,SG_G1,125,100,75,50,25,0
A2,G2_G3_一般,75,60,45,30,15,0
A1,SG_G1,100,80,60,40,20,0
A1,G2_G3_一般,50,40,30,20,10,0
```

## グレード分類の判定

`data/programs/title/YYYY/MM/DD.csv` の `グレード` 列を以下に正規化する(`grade_bucket_for_grade()`)。

| グレード原値 | 分類 |
| --- | --- |
| `SG` / `ＳＧ` / `PG1` / `ＰＧ１` / `G1` / `Ｇ１` / `ＧⅠ` | `SG_G1` |
| `G2` / `Ｇ２` / `ＧⅡ` / `G3` / `Ｇ３` / `ＧⅢ` / `IP`(一般) / その他 | `G2_G3_一般` |

A1 / A2 で `SG_G1` ⇔ `G2_G3_一般` を切り替え、B1 / B2 では分類を見ず `全` を採用する。

## 失格・棄権の扱い

選手pt と異なり、モーターpt は機材起因事故を負の評価として計上する。

| 着順トークン | スコア | 分母(出走数) |
| --- | --- | --- |
| `1`〜`6`(半角・全角) | テーブル値 | +1 |
| `転` / `落` / `沈` / `エ`(機材起因) | **-100** | +1 |
| `F` / `L` / `失` / `妨`(選手起因) | スキップ | +0 |
| `欠` / `不`(無効走) | スキップ | +0 |

定数定義(`scripts/boatrace/index_features.py`):

```python
MOTOR_NEGATIVE_TOKENS = {"転", "落", "沈", "エ"}
MOTOR_NEGATIVE_SCORE  = -100
MOTOR_SKIP_TOKENS     = {"F", "L", "失", "妨", "欠", "不"}
```

## v4 テーブル(`motor_ability_score_v4.csv`)

`v4_motor` 予想者の `motor4` 成分が使うチューニング済みスコア表。

- **ファイルパス**: `data/estimate/motor_ability_score_v4.csv`
- **設計書**: [`docs/design/motor_score_tuning_v4.md`](../design/motor_score_tuning_v4.md)
- **使われる場所**: `index_features.py` の `FeatureContext.motor4_score_table()`(`load_motor_score_table(repo, filename=MOTOR4_SCORE_FILENAME)`)

スキーマは従来表と同一。値は行別スケール(1着pt)を維持したまま、行内の間隔を
**γ=1.5 の凸カーブ**(`A × ((6-着順)/5)^1.5` を整数丸め)にしたもの。エキスパート
評価 4 場との順位相関で最適化した(1着の価値を等間隔より大きく評価する)。

```csv
級別,グレード分類,1着pt,2着pt,3着pt,4着pt,5着pt,6着pt
B2,全,125,89,58,32,11,0
B1,全,100,72,46,25,9,0
A2,SG_G1,125,89,58,32,11,0
A2,G2_G3_一般,75,54,35,19,7,0
A1,SG_G1,100,72,46,25,9,0
A1,G2_G3_一般,50,36,23,13,4,0
```

v4 では失格・棄権のペナルティも `MOTOR4_NEGATIVE_SCORE = -50`(従来 -100)に
緩和され、採用節数も `MOTOR4_HISTORY_SESSIONS = 5`(従来 6)になる。トークン
分類ルール(転/落/沈/エ = ペナルティ、F/L/失/妨/欠/不 = 除外)は共通。

## 更新手順

スコアテーブルを書き換えた場合は以下も更新する(CLAUDE.md ルール):

1. `data/estimate/motor_ability_score.csv` をコミット
2. `docs/data/motor_ability_score.md`(本ファイル)の「現行データ」セクションを同期
3. `docs/design/motor_ability_index.md` のスコアテーブル例も更新
4. ユニットテスト `scripts/tests/unit/test_motor_ability.py` の期待値を更新
5. 影響月の重みファイル `data/estimate/stadium/index_weights/YYYY-MM.csv` を再生成

> **重要**: テーブル変更は `モーターpt` の意味を変えるため、過去日 index の再計算も必要。
> `motor_ability_score_v4.csv` を変更した場合も同様に、`v4_motor` の重み・index の
> 再計算と `docs/design/motor_score_tuning_v4.md` の更新が必要。

# コースpt(場×レース番号別コース強度)v6_course 設計書

control (`v1_basic`) の **枠番pt (`waku`)** を、**場 × レース番号 × コース**の
1着率テーブルに基づく **コースpt (`course`)** に差し替えた予想者 `v6_course` の設計。

- 対象スクリプト: `scripts/boatrace/index_features.py` / `scripts/boatrace/predictors/registry.py` / `scripts/build_course_rate.py`(新規)
- 対象データ追加: `data/estimate/stadium/course_win_rate.csv`(新規)
- 影響範囲: `infra/run-*.sh`(ACTIVE_PREDICTORS)、`infra/run-monthly-weights.sh`(テーブル月次再生成)、fun-site `packages/shared/src/predictors.ts`

---

## 1. 動機

現行の枠番pt は `data/estimate/stadium/win_rate.csv`(**場 × 季節 × コース**の長期1着率)を
コースで引いた値で、**レース番号の次元を持たない**。しかし実データ
(`data/results/realtime/` 2025-11-01〜2026-07-20、39,179 レース)では、
コース強度はレース番号に強く依存する:

| R | 1R | 2R | 3R | 4R | 5R | … | 10R | 11R | 12R |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 全国 1コース1着率 | **59.4%** | 47.5% | 47.0% | 47.6% | 54.2% | … | 60.7% | 68.1% | **70.8%** |

場別ではレンジがさらに大きい(例: 場01 は 2R 30% ⇔ 6R 72%、場23 は 7R 38% ⇔ 1R 83%)。
1R は企画レース(1号艇に A 級配置)の影響で全国的にはむしろイン最強帯であり、
方向・大きさとも**場に強く依存**するため、場×レース番号のテーブル化が必要。

なお現行実装の事実確認として: 枠番pt は realtime 更新時、スタート展示の
**実進入コース**(`data/previews/stt/` の `艇N_コース`、欠損時は枠番フォールバック)で
テーブルを引いている(`compute_features_for_day` → `waku_pt`)。daily 状態のみ
枠番=コースで計算される。v6 でもこの参照方法は変えず、**テーブルの次元だけを変える**
(場×季節 → 場×レース番号)。これにより control との A/B 差分がテーブル定義のみに閉じる。

## 2. 確定パラメータ

| 項目 | 現行 `waku` | `course`(提案) |
| --- | --- | --- |
| テーブル次元 | 場 × 季節 × コース | **場 × レース番号 × コース** |
| 季節次元 | あり(春夏秋冬) | **なし**(全期間プール) |
| ソース | 長期勝率(静的) | `data/results/realtime/` 全履歴(月次再生成) |
| 縮約 | なし | **ベイズ収縮 k=50**(場×コース全体率へ) |
| 参照コース | 実進入コース(daily は枠番) | 同じ |

季節次元は意図的に採用しない(オーナー判断)。セルあたりサンプルは
場×レース番号で n≈110〜160(9ヶ月蓄積)であり、季節で 1/4 に割ると
収縮後もノイズ優位になるため、この判断はデータとも整合する。

### 収縮式

セル (場 j, レース番号 r, コース c) の公表値:

```
rate(j, r, c) = (wins(j,r,c) + k × base(j,c)) / (n(j,r) + k)
  base(j,c)  = 場 j のコース c 全レース番号 1着率
  k = 50
```

セル欠損時のフォールバック: 場×コース全体率 `base(j,c)` → 全国×コース率。

### k の根拠(ホールドアウト検証)

train = 2025-11-01〜2026-06-30(36,105 レース)、test = 2026-07-01〜07-20(3,074 レース)。
勝ちコースの 6 値確率予測を Brier / log-loss で評価:

| 構成 | Brier | log-loss |
| --- | --- | --- |
| k=0(生セル率) | 0.6359 | 1.3654 |
| k=20 | 0.6350 | 1.3357 |
| **k=50(採用)** | **0.6349** | **1.3316** |
| k=100 | 0.6359 | 1.3305 |
| 場のみ(レース番号無視) | 0.6473 | 1.3468 |

レース番号次元の追加は場のみ比で一貫して改善し、k=30〜100 が最適圏。
Brier 最小かつ log-loss がほぼ底の k=50 を採用(k=100 との差は誤差圏、
より軽い収縮を選ぶことでセル情報を活かす)。

## 3. データ: `data/estimate/stadium/course_win_rate.csv`

| 列 | 説明 |
| --- | --- |
| `場コード` | "01"〜"24" |
| `レース回` | 1〜12(整数、"R" なし) |
| `n` | 集計レース数(収縮前の生 n。診断用) |
| `1コース勝率` 〜 `6コース勝率` | 収縮後の 1着率(%、小数 2 桁) |

24 場 × 12 レース番号 = 288 行。生成は新規 CLI:

```sh
python scripts/build_course_rate.py            # data/results/realtime 全履歴から再生成
python scripts/build_course_rate.py --k 50     # 収縮強度(デフォルト 50)
```

勝ちコースの特定は `1着_艇番` と `Nコース_艇番`(実進入順)の突合で行う
(結果が取れなかったレース・艇番不整合行はスキップ)。

**更新運用**: `infra/run-monthly-weights.sh` で毎月 1 日、`build_weights` の**前**に
再生成して commit する(蓄積が増えるほどセルが安定する)。テーブルは全履歴プールのため、
weights 学習窓(直近 6 ヶ月)との重なりによる軽微なリークがあるが、現行 `win_rate.csv`
(長期勝率の静的テーブル)と同性質であり許容する。

## 4. 実装

### index_features.py

- `load_course_table(repo)` — `course_win_rate.csv` を
  `{(場コード2, レース回int): [6 rates]}` + フォールバック `{場コード2: [6 rates]}` に読み込み
- `course_pt(table, stadium_code2, race_no, course)` — セル欠損時は場×コース → NaN
- `FeatureContext.course_table()` — `waku_table` と同様の lazy キャッシュ
- `compute_features_for_day` — long-format 出力に `course` 列を追加。
  参照コースは既存の `course` 変数(realtime = stt 実進入、daily = 枠番)を共用。
  `レース回` は `"1R"` 形式から int へパース(失敗時は場×コースへフォールバック)

### registry.py

```python
COMPONENT_LABELS_REGISTRY["course"] = "コースpt"

PredictorSpec(
    predictor_id="v6_course",
    display_name="コース予想",
    slot=6,
    status=STATUS_ACTIVE,
    started_at=<投入日>,   # デプロイ日に合わせる
    component_keys=("course", "racer", "motor", "exhibit", "weather"),
)
```

control の `waku` を `course` に差し替えた 5 成分。差分は 1 成分のみで、
コースpt と枠番pt の優劣だけを A/B 比較する。ラベルは意味が変わるため
「枠番pt」を流用せず新設(v4_motor の「モーターpt」共用とは逆の判断。
列名は `N枠_コースpt` になり、fun-site 側にも key/label 追加が必要)。

### build_index.py

変更なし。`course` は `DAILY_NEUTRAL_COMPONENTS` に**含めない**
(daily 状態でも枠番フォールバックで実値を出す。現行 waku と同じ扱い)。
欠損補完も既定の 50(`COMPONENT_MISSING_FALLBACK` 追加不要)。

### build_weights.py

変更なし。`course` は 9 ヶ月の結果履歴から全期間 backfill 可能なため
`SHORT_HISTORY_COMPONENTS` に**含めない**。投入月の重みは

```sh
python scripts/build_weights.py --month <投入月> --predictor v6_course
```

で過去 6 ヶ月から通常学習する(v5_slit のような weights コピーは不要。
成分が control と異なるため必ず独自学習が要る)。

### infra / fun-site 同期

- `infra/run.sh` / `run-daily-sync.sh` / `run-monthly-weights.sh` の
  `ACTIVE_PREDICTORS` に `v6_course` を追加
- `run-monthly-weights.sh` に `build_course_rate.py` 実行ステップと
  `course_win_rate.csv` の commit パスを追加
- fun-site `packages/shared/src/predictors.ts`: `ComponentKey` に `"course"`、
  `COMPONENT_LABELS`("コースpt")・`COMPONENT_SHORT_LABELS`("コース")・
  `COMPONENT_COLORS` を追加し、`PREDICTORS` に `v6_course` を追記。
  `PREVIEW_DERIVED_COMPONENTS` には**入れない**(daily でも値を持つ)

## 5. テスト計画

- `test_course_pt.py`(新規): テーブル読み込み・セル欠損フォールバック・
  レース回パース("01R"/"12R"/不正値)・収縮計算の単体テスト
- `test_feature_context.py`: `course` 列が long-format に出ること、
  daily(preview なし)で枠番フォールバックすること
- `build_course_rate.py`: 既知の小型フィクスチャ(手計算できる 2 場 × 2R)で
  収縮値を検証
- 投入前に `build_index.py --date $TODAY --mode daily --predictor v6_course` で
  `N枠_コースpt` の分布(平均 50・SD 10)を目視確認

## 6. 限界・リリース判断

- **ホールドアウト改善 ≒ 回収率改善ではない**(v2/v3_tenkai の教訓)。
  レース番号情報はオッズにも織り込まれている可能性が高く、的中率が上がっても
  回収率が上がるとは限らない。独立スロットとして投入し、**累計回収率で
  control (v1_basic) と A/B 比較**してから判断する。基準は従来どおり
  「有意な回収率差」。
- レース番号効果の源泉の多くは番組編成(企画レースの選手配置)であり、
  **選手pt と部分的に重複**する。SLSQP の非負重み学習が共線性をある程度
  吸収するが、投入月の weights で `w_course` / `w_racer` の配分を確認すること。
- 企画レースの編成は場の改編で変わりうる。全履歴プールのため改編への追従は
  緩やか(月次再生成で徐々に反映)。急な改編検知は将来課題。

## 7. 運用手順

1. `python scripts/build_course_rate.py` でテーブル生成 → commit
2. `python scripts/build_weights.py --month <投入月> --predictor v6_course`
3. `python scripts/build_index.py --date $TODAY --mode daily --predictor v6_course` で分布確認
4. registry の `started_at` をデプロイ日に設定、`infra/run-*.sh` の
   `ACTIVE_PREDICTORS` と fun-site `predictors.ts` を同期
5. 同一 PR で docs 更新: `docs/data/estimate.md`(レジストリ表 + v6 成分説明 +
   Stadium Parameters に `course_win_rate.csv` 追加)、`docs/data/README.md`、
   `docs/development.md`(新 CLI)、`docs/operations.md` / `docs/infrastructure.md`
   (月次ジョブ変更)。fun-site 側は `docs/data-sources.md` / `docs/domain.md`
6. 退役時は `status="retired"`(データ・ロジック残置、ID 再利用しない)

## 8. 将来課題

- 時間減衰(半減期つき加重)による番組改編への追従
- 場×レース番号×コースの **2着率**テーブル化(2連系の買い目精度向上)
- 決まり手(逃げ/差し/まくり)分布のレース番号依存の特徴量化
- 季節×レース番号の交互作用(蓄積が 2 年分を超えたら再検討)

## 9. 実装時検証の記録(2026-07-20)

実装と同時に行った検証の結果。ユニットテストは `scripts/tests/unit/test_course_pt.py`
(全 497 件パス)。

**テーブル単体**(train 2025-11〜2026-06 / test 2026-07、勝ちコースの 6 値確率予測):

| テーブル | Brier | log-loss |
| --- | --- | --- |
| 場×季節(現行 waku 相当、k=50) | 0.6485 | 1.3535 |
| **場×レース回 k=50(v6 採用)** | **0.6349** | **1.3316** |

場×レース回はレース番号次元の分だけ一貫して優れる。

**weights fit**(2026-07、学習窓 2026-01〜06): 全 24 場で `w_course` = 0.28〜0.45 と
最大重み・fallback なし。ただし R² は全場で control (v1_basic) を僅かに下回る
(平均 0.278 → 0.261)。これはレース番号によるコースpt のレベルシフト(イン強い
レース番号では全艇の生値水準が動く)が場内標準化の σ を押し広げる、着順回帰
特有のアーチファクトで、レース内センタリング変種でも結果は変わらないことを確認
(数学的にセンタリングは標準化に吸収される)。

**エンドツーエンド**(2026-07-01〜19 の 2,953 レース、強さpt 1 位 = 勝者の的中率):
v1_basic 55.98% / v6_course 55.71%(勝者の平均予測順位 1.927 / 1.950)。**精度指標では
有意差なし(誤差圏)**。テーブル単体の較正改善が線形結合パイプラインの着順精度には
そのまま乗らない、というのが現時点の観測。したがって §6 のとおり、本予想者の
採否は**累計回収率の A/B 比較のみで判断**する(レース番号情報がオッズに未織込みで
あれば、的中率が同等でも回収率で差が出る余地がある)。

---

## 結果: 2026-08-09 退役

**採否基準どおり累計回収率の A/B 比較で判定し、不採用とした。** control (`v1_basic`)
と同一レースで突き合わせたペア比較(直前買い目・確定レースのみ、2026-07-21〜08-09、
n=3,002)で **79.06% vs 85.97%(-6.91pt、95%CI [-13.1, -0.5]、ペア並べ替え検定
p=0.0047、5 検定の Holm 補正後 0.016)**。

- 差分の大きい上位 20 レースを除外しても差はほぼ不変(外れ値依存ではない)
- 日次でも control を下回った日が **17/20 日**(符号検定 p=0.0026)
- control より買い目点数が多い(13.0 点 vs 11.6 点)が、点数分布を control に揃えて
  標準化しても回収率は 77.5% にとどまる → 点数ではなく選定そのものの問題
- **的中率は v6 のほうが高い**(46.8% vs 46.1%)。堅い決着は当てるが、安いオッズを
  厚く買って EV を落とす負け方に見える

§7 のホールドアウト検証では Brier / log-loss ともテーブル単体は改善していたので、
**較正の改善が回収率に乗らなかった**ことになる。エンドツーエンドの的中率が
「有意差なし(誤差圏)」だった時点の観測とも整合する。同じく `course` を採用した
`v7_aggregate` / `v8_aionly` も同日退役しており、3 者に共通する差分が `course` で
あることから、`course` 成分そのものが回収率を毀損している疑いが濃い。

成分定義と計算ロジック(`index_features.py` のコースpt、`build_course_rate.py`、
`course_win_rate.csv` の月次再生成)は残してある。作り直して再挑戦する場合は、
命名規則どおり `v6_course` の ID は再利用せず新しい ID を立てること。

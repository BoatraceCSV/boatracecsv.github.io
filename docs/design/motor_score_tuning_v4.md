# モーターpt スコア表チューニング(v4_motor)設計書

エキスパート評価を正解データとして、モーター能力指数 v2
([`motor_ability_index_v2.md`](./motor_ability_index_v2.md))の
**スコア表・ペナルティ・採用節数** をチューニングした `motor4` 成分と、
それを採用する予想者 `v4_motor` の設計。

- 分析の全記録: [`notebooks/motor_score_tuning/report.md`](../../notebooks/motor_score_tuning/report.md)
  (正解データ・探索ハーネス・再現スクリプト同梱)
- 対象スクリプト: `scripts/boatrace/index_features.py` / `scripts/boatrace/predictors/registry.py`
- 対象データ追加: `data/estimate/motor_ability_score_v4.csv`
- 影響範囲: `build_weights.py`(SHORT_HISTORY_COMPONENTS)、`infra/run-*.sh`(ACTIVE_PREDICTORS)

---

## 1. 動機

v1 設計書の未決事項に「スコア表は根拠なく決めている」「-100 ペナルティの妥当性未検証」が
残っていた。ドメインエキスパートのモーター評価(場公式サイトが公開する SS〜E 等の
グレード)を正解として、これらのパラメータをデータ駆動で決め直す。

## 2. 正解データ

2026-07-19 取得のスナップショット 4 場・計 183 機
(詳細: [`notebooks/motor_score_tuning/ground_truth/`](../../notebooks/motor_score_tuning/ground_truth/README.md))。

| 場 | 評価 | n | 備考 |
| --- | --- | --- | --- |
| 04 平和島(おかぺん) | SS〜E 11段階 | 43 | |
| 23 唐津 | 素性 S〜D 5段階 | 60 | |
| 24 大村 | 評価平均 1〜7点 | 70 | |
| 14 鳴門 | 金/銀/銅 | 10 | 上位10のみの打ち切りラベル |

目的関数 = 場別 Spearman ρ のラベル数加重平均。

## 3. 確定パラメータ(v2 との差分)

| パラメータ | v2 (`motor`) | v4 (`motor4`) | 定数 |
| --- | --- | --- | --- |
| スコア表 | 着順に線形(等間隔) | **γ=1.5 凸カーブ**(整数丸め) | `MOTOR4_SCORE_FILENAME` |
| 事故ペナルティ | -100 | **-50** | `MOTOR4_NEGATIVE_SCORE` |
| 採用節数 | 6 | **5** | `MOTOR4_HISTORY_SESSIONS` |
| 半減期 / 収縮 k / lane補正 | 60日 / 10 / ON | 変更なし(現行値が最適圏) | — |

スコア表 v4: 行別スケール `A ∈ {125,100,125,75,100,50}` を維持し、
`pt(着順k) = round(A × ((6-k)/5)^1.5)`。

**行間スケール(A)を据え置いた理由**: lane補正(z残差化)が ON のとき、セル内
標準化 `(raw − μ_cell)/σ_cell` がスコア表の行に対する線形変換を吸収するため、
行間の水準差は原理的に出力へ影響しない。効くのは行内の間隔形状(γ)と
ペナルティの相対深さのみ。

## 4. 結果サマリ

| 構成 | 平和島 | 唐津 | 大村 | 鳴門 | 加重平均 |
| --- | --- | --- | --- | --- | --- |
| v2(現行 motor) | +0.623 | +0.590 | +0.589 | +0.290 | +0.581 |
| (参考)公式2連対率のみ | +0.660 | +0.386 | +0.600 | +0.338 | +0.530 |
| **v4(提案)** | **+0.656** | **+0.611** | **+0.641** | **+0.372** | **+0.620** |

- leave-one-stadium-out で全 4 場一貫改善(+0.017〜+0.084、加重平均 +0.037)。
  3 場学習で選ばれる構成は fold 間でほぼ同一(γ=1.5〜1.8, pen=-50, N=5, H=60)。
- スコア表 36 値を自由に振る Optuna 全探索(+0.629)は LOSO で平和島が現行割れ
  → 過学習と判定し、頑健な 3 パラメータのみ採用。
- 1 次元感度分析: ペナルティは -20〜-50 で全場改善(-200 は大幅悪化)、γ は
  1.3〜1.6 に山、節数 5 > 6 > 4、収縮 k は 20 以上で悪化、lane補正 OFF は悪化。

## 5. 実装

計算式は v2 と共通で、パラメータだけを keyword 引数で差し替える:

- `load_motor_score_table(repo, filename=...)` — v4 表の読み込み
- `score_motor_run(table, run, negative_score=...)` — ペナルティ差し替え
- `motor_ability_pt(..., negative_score=..., max_sessions=...)` — 節数スライス
  (履歴は従来どおり 6 節ロードし、先頭 5 節 = 直近 5 節を使う)
- `compute_lane_baseline` / `compute_class_grade_avg` に `negative_score` 引数
- `FeatureContext.motor4_score_table()` / `lane_baselines4(day)` — v4 用の
  スコア表・コース baseline(v4 表 + ペナルティ -50 + 5 節で別計算・別キャッシュ)
- `compute_features_for_day` が long-format 出力に `motor4` 列を追加

予想者 `v4_motor`(モーター予想、slot=4、started_at=2026-07-20)は
control (`v1_basic`) の `motor` を `motor4` に差し替えた 5 成分。ラベルは
「モーターpt」を共用し、CSV 列名を v1_basic と互換に保つ。
`build_weights.py` の `SHORT_HISTORY_COMPONENTS` に `motor4` を追加済み
(motor と同じ履歴ビルダーを使うため backfill 制約も同じ)。

検証: 本番コードパス(`FeatureContext` 経由)の motor4 出力が、チューニングに
使った独立実装ハーネスと最大誤差 4e-15 で一致することを確認済み。
ユニットテストは `scripts/tests/unit/test_motor_ability_v4.py`。

## 6. 限界・リリース判断

- 正解データは **単一時点のスナップショット**。節をまたいだ再現性は未検証。
- **エキスパート評価との相関改善 ≒ 回収率改善ではない**(v2_tenkai/motor2rate の
  教訓: おかぺん相関 ρ≈0.6 でも回収率で control に勝てなかった)。
- したがって v4_motor は独立スロットとして投入し、**累計回収率で control
  (v1_basic) と A/B 比較**してから最終判断する。判断基準は v2_tenkai 退役時と
  同様「有意な回収率差」。

## 7. 運用手順

1. 投入月の重みを学習: `python scripts/build_weights.py --month YYYY-MM --predictor v4_motor`
2. 当日 index を確認: `python scripts/build_index.py --date $TODAY --mode daily --predictor v4_motor`
   で `N枠_モーターpt` の分布(平均 50・SD 10)を目視確認
3. `infra/run-*.sh` の `ACTIVE_PREDICTORS` は同期済み(v1_basic v4_motor)
4. 退役時は registry の `status="retired"` に変更(データ・ロジックは残置)

## 8. 将来課題

- エキスパート評価の**定期スクレイプ&蓄積**(複数節・複数場での再チューニング)
- superboatmotor.com(JS レンダリング必須)の取り込み
- 半減期の場別最適化、prior 平均のデータ駆動化(v2 設計書 §11 から継続)
- lane補正 OFF 構成での行間スケール(A)再調整(現状は原理的に不感)

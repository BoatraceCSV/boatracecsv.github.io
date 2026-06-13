# データファイル一覧

毎日、以下の CSV ファイルが自動生成されます。各ファイルはレースの異なる段階のデータを含みます。
すべて HTTPS でダウンロード可能で、`レースコード` (12桁) を共通キーとして JOIN できます。

## ファイル一覧

| カテゴリ | ファイル | パス | 詳細 |
| --- | --- | --- | --- |
| 事前情報 | Race Title | `data/programs/title/YYYY/MM/DD.csv` | [programs.md#race-title](./programs.md#race-title) |
| 事前情報 | Race Cards | `data/programs/race_cards/YYYY/MM/DD.csv` | [programs.md#race-cards](./programs.md#race-cards) |
| 事前情報 | Recent National Form | `data/programs/recent_national/YYYY/MM/DD.csv` | [programs.md#recent-national-form](./programs.md#recent-national-form) |
| 事前情報 | Recent Local Form | `data/programs/recent_local/YYYY/MM/DD.csv` | [programs.md#recent-local-form](./programs.md#recent-local-form) |
| 事前情報 | Motor Stats | `data/programs/motor_stats/YYYY/MM/DD.csv` | [programs.md#motor-stats](./programs.md#motor-stats) |
| 直前情報 | Realtime Preview (4 sources) | `data/previews/{tkz,stt,sui,original_exhibition}/YYYY/MM/DD.csv` | [previews.md](./previews.md) |
| 結果 | Realtime Results | `data/results/realtime/YYYY/MM/DD.csv` | [results.md#realtime-results](./results.md#realtime-results) |
| 結果 | Realtime Payouts | `data/results/payouts/YYYY/MM/DD.csv` | [results.md#realtime-payouts](./results.md#realtime-payouts) |
| 派生 | Strength Index | `data/estimate/{predictor_id}/YYYY/MM/DD.csv` | [estimate.md#strength-index](./estimate.md#strength-index) |
| 派生 | Stadium Parameters | `data/estimate/stadium/*.csv`, `data/estimate/stadium/weights/{predictor_id}/*.csv` | [estimate.md#stadium-parameters](./estimate.md#stadium-parameters) |
| 派生 | Motor Ability Score | `data/estimate/motor_ability_score.csv` | [motor_ability_score.md](./motor_ability_score.md) |

## ファイル間の関係性

```
Programs              → 選手情報・成績データ(事前情報)
     ⇅
Race Title            → per-race レース名 sidecar(Programs と並行)
     ⇅
Race Cards            → 出走表詳細(Programs と並行: 全国/当地3連率・節間14スロット成績)
     ⇅
Recent National Form  → 全国近況5節(節期間・グレード・着順時系列)
     ⇅
Recent Local Form     → 当地近況5節(同形式、当地ソースのみ)
     ⇅
Motor Stats           → モーター期成績スナップショット(場×モーター 1日1行)
     ↓
Realtime Preview      → 締切5分前の直前情報(tkz / stt / sui / original_exhibition の per-source 追記)
     ↓
Strength Index        → 派生:特徴量を場別重みで線形結合した強さポイント
                         (active 予想者ごとに data/estimate/{predictor_id}/ に出力)
     ↓
Realtime Results      → 締切+3〜30分の準リアルタイム結果(bc_rs1_2 由来)
     ↓
Realtime Payouts      → 締切+3〜30分の払戻金(bc_rs2 由来)

Stadium Parameters    → win_rate.csv / sui_params.csv / weights/{predictor_id}/*.csv (Index 計算の参照テーブル)
```

**基本的な追跡方法**: 同じ `レースコード` で各ファイルを紐付けることで、レースの事前情報から当日の展示・直前情報、最終結果までを一貫して追跡できます。

**例**: レースコード「202602092301」で検索すると:

1. **Programs** から → 参加選手のプロフィール・成績
2. **Race Title** から → per-race レース名(予選 / 優勝戦 等)
3. **Race Cards** から → 全国/当地3連率・節間14スロット成績
4. **Recent National Form / Recent Local Form** から → 全国・当地の直近5節成績
5. **Motor Stats** から → モーター期成績スナップショット
6. **Realtime Preview** から → 締切5分前の直前スナップショット(時系列で複数ソース・展示タイム・気象等)
7. **Strength Index** から → 6 枠分の強さポイント(偏差値)と要素別寄与の内訳
8. **Realtime Results** から → 締切後5〜30分の準リアルタイム結果(着順・決まり手・ST・気象)
9. **Realtime Payouts** から → 同じく締切後5〜30分の払戻金(単勝 / 複勝 / 2連単 / 2連複 / 拡連複 / 3連単 / 3連複)

これらを組み合わせることで、レースの準備段階から当日の直前情報・予測値・実結果までを一貫して追跡でき、特徴量設計や分析に活用できます。

## URL の構造

すべて `https://boatracecsv.github.io/` をルートに、パスを連結すればダウンロード可能です。

例:
- `https://boatracecsv.github.io/data/programs/title/2026/05/03.csv`
- `https://boatracecsv.github.io/data/previews/tkz/2026/05/03.csv`
- `https://boatracecsv.github.io/data/results/realtime/2026/05/03.csv`

更新は 1 日 1 回 (一部のリアルタイム系は 5 分毎)。最新の情報が必要な場合、
[Boatrace OpenAPI](https://github.com/BoatraceOpenAPI) などの別のソースをご利用ください。

# Boatrace Data Automation

ボートレースのデータは、独自フォーマットで分散しており、収集と整形に時間がかかります。
そこで、機械学習で利用しやすいように1レース1行のCSVファイルを作成しました。
HTTPS でダウンロードできるため、Agent からのアクセスにも利用しやすくなっています。
更新は1日1回(リアルタイム系は5分毎)です。
最新の情報が必要な場合、[Boatrace OpenAPI](https://github.com/BoatraceOpenAPI) などの別のソースをご利用ください。

## データダウンロード

すべて `https://boatracecsv.github.io/` をルートに、以下のパスでダウンロードできます。

- `data/programs/title/YYYY/MM/DD.csv` — per-race レース名
- `data/programs/race_cards/YYYY/MM/DD.csv` — 出走表詳細
- `data/programs/recent_national/YYYY/MM/DD.csv` — 全国近況5節
- `data/programs/recent_local/YYYY/MM/DD.csv` — 当地近況5節
- `data/programs/motor_stats/YYYY/MM/DD.csv` — モーター期成績
- `data/previews/{tkz,stt,sui,original_exhibition}/YYYY/MM/DD.csv` — 直前情報
- `data/results/realtime/YYYY/MM/DD.csv` — 締切後5〜30分の準リアルタイム結果
- `data/results/payouts/YYYY/MM/DD.csv` — 締切後5〜30分の払戻金
- `data/estimate/index/YYYY/MM/DD.csv` — 強さポイント(5要素偏差値+寄与+合計)

すべて `レースコード`(12桁、`YYYYMMDDjjrr`)を共通キーとして JOIN できます。

## ドキュメント

詳細なドキュメントは [`docs/`](./docs/README.md) を参照してください。

- [データ仕様](./docs/data/README.md) — 各 CSV のスキーマ・カラム定義・関係性図
- [Development](./docs/development.md) — Quick Start / プロジェクト構造 / スクリプト Usage / Testing
- [Operations](./docs/operations.md) — GitHub Actions Workflows / Configuration / Performance
- [Infrastructure](./docs/infrastructure.md) — Cloud Run Jobs(preview-realtime / daily-sync / monthly-weights)

## License

MIT License

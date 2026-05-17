# Documentation

Boatrace Data Automation のドキュメントです。読みたい内容に応じて以下から選んでください。

## データ仕様(利用者向け)

公開されている CSV ファイルのスキーマ・カラム定義・取得元・利用例。

- [データファイル一覧と関係性図](./data/README.md) — 全 CSV の概要、ファイル間の JOIN 関係、ダウンロード URL
- [Programs(事前情報)](./data/programs.md) — Race Title / Race Cards / Recent National Form / Recent Local Form / Motor Stats
- [Realtime Preview(直前情報)](./data/previews.md) — tkz / stt / sui / original_exhibition
- [Results(レース結果)](./data/results.md) — Realtime Results / Realtime Payouts / Daily Results
- [Estimate(派生指標)](./data/estimate.md) — Strength Index / Stadium Parameters

## 開発者向け

リポジトリのコードを動かす方法、スクリプトの使い方、ディレクトリ構造、テスト。

- [Development](./development.md) — Quick Start / Project Structure / 各スクリプト Usage / Testing

## 運用・インフラ向け

GitHub Actions ワークフロー、設定ファイル、Cloud Run Jobs。

- [Operations](./operations.md) — GitHub Actions Workflows / `.boatrace/config.json` / Performance / Data Source / License
- [Infrastructure](./infrastructure.md) — Cloud Run Jobs(`preview-realtime` / `daily-sync` / `monthly-weights`)のアーキテクチャ、セットアップ、更新手順、トラブルシュート

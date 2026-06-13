# CLAUDE.md

このリポジトリで作業する AI・開発者に向けたルール集です。

## ドキュメント更新ルール

このリポジトリのドキュメントは [`docs/`](./docs/README.md) 配下に集約されています。
ドキュメントの鮮度を保つため、以下の変更を行う際は、対応する `docs/` のドキュメントも
**同じ PR で更新**してください。

| 変更対象 | 更新が必要な docs |
| --- | --- |
| `scripts/*.py` / `scripts/boatrace/*.py` の CLI 仕様変更 | [`docs/development.md`](./docs/development.md) |
| 出力 CSV のスキーマ(列名・列追加・列削除) | [`docs/data/`](./docs/data/README.md) の該当ファイル |
| 新しいデータファイル種別の追加 | [`docs/data/README.md`](./docs/data/README.md) + 新規 `docs/data/*.md` |
| `.github/workflows/*.yml` の改廃・スケジュール変更 | [`docs/operations.md`](./docs/operations.md) |
| `infra/`(Dockerfile / run*.sh / cloudbuild.yaml / Scheduler) | [`docs/infrastructure.md`](./docs/infrastructure.md) |
| `.boatrace/config.json` のキー追加 | [`docs/operations.md`](./docs/operations.md) |
| 派生指標(Strength Index 等)の計算式・特徴量変更 | [`docs/data/estimate.md`](./docs/data/estimate.md) |

`README.md`(リポジトリルート)はプロジェクト概要 + `docs/` へのリンクのみを保持する
ポリシーです。詳細情報を追記する場合は `docs/` 配下に置いてください。

ドキュメントが伴わないコード変更はレビューで差し戻されます。

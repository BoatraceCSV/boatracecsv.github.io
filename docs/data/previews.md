# Realtime Preview(直前情報)

締切5分前のスナップショットをデータソース単位で別ファイルに追記したものです。
データソースを分割することで当日の値の時間変化を保持でき(特に水面気象)、後段の特徴量計算 (`scripts/boatrace/index_features.py`) もこの per-source CSV を直接参照します。

`scripts/preview-realtime.py` が Cloud Run Jobs (`boatrace-487212/asia-northeast1`) で **JST 08:00〜22:59 の 5 分毎** に動作し、各レースの締切5分前のスナップショットを取得します。

## ファイルパス

- `data/previews/tkz/YYYY/MM/DD.csv` — 体重・展示タイム・チルト
- `data/previews/stt/YYYY/MM/DD.csv` — 進入コース・スタート展示
- `data/previews/sui/YYYY/MM/DD.csv` — 水面気象スナップショット
- `data/previews/original_exhibition/YYYY/MM/DD.csv` — オリジナル展示データ

**URL 例**: https://boatracecsv.github.io/data/previews/tkz/2026/05/03.csv

## 共通カラム(4ファイル先頭6列)

- `レースコード` (`YYYYMMDDjjrr`): Programs / Realtime Preview 等と JOIN 可能な12桁識別子
- `レース日` (`YYYY-MM-DD`)
- `レース場` (`01`〜`24`、2桁ゼロ詰め)
- `レース回` (`01R`〜`12R`)
- `締切時刻` (`HH:MM`、`getHoldingList2` 由来)
- `取得日時` (ISO8601, JST、例 `2026-05-03T20:25:03+09:00`)

## ソース固有カラム

`tkz` — `状態` (常に `1`) + 6艇 × {`体重(kg)`, `体重調整(kg)`, `展示タイム`, `チルト`}。

`stt` — 6艇 × {`コース`, `スタート展示`}。`F` 付き行は負値の ST、`L` 付き行は空欄として格納。

`sui` — `気象観測時刻` (HHMM) + `風速(m)` / `風向` / `波の高さ(cm)` / `天候` / `気温(℃)` / `水温(℃)`。

`original_exhibition` — `計測数` / `計測項目1` / `計測項目2` / `計測項目3` + 6艇 × {`選手名`, `値1`, `値2`, `値3`}。場ごとに項目が異なります(多くは「一周／まわり足／直線」、住之江・尼崎・徳山は2項目、桐生は「半周ラップ／まわり足／直線」)。

## 取得・スキップルール

- 中止 / 順延 / 途中中止のレースはスキップ
- ソースファイルがまだ公開されていない、または計測中 (`status=0`) / 計測不能 (`status=2`) の場合は **追記せずスキップ**(次回実行で自動再試行)
- `original_exhibition` は `status=1` のみ追記(旧 `data/original_exhibition/` にあった `ステータス` カラムは廃止)
- 同一 `レースコード` は1日1行のみ(per-source dedup)
- スケジュール実行のため、各レースについて取得は1回のみ

> **用途**: 締切直前のコンディション把握、時系列での風・水温の変化分析、リアルタイム予想モデル特徴量。`レースコード` で他の CSV と JOIN 可能

# Development

開発者向けのセットアップ、プロジェクト構造、スクリプトの使い方をまとめています。

- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Testing](#testing)

---

## Quick Start

### Prerequisites

- Python 3.8+
- git
- pip (included with Python)

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/boatrace-data.git
cd boatrace-data

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r scripts/requirements.txt
```

### Daily Sync (Test Run)

```bash
# 当日 JST のデータを一通り取り込む (daily-sync.yml と同じ並び)
python scripts/race-card.py --date "$(date +%Y-%m-%d)" --force         # bc_j_str3 (race_cards)
python scripts/recent-form.py --date "$(date +%Y-%m-%d)" --force       # bc_zensou (recent_form)
python scripts/motor-stats.py --date "$(date +%Y-%m-%d)" --force       # bc_mst / bc_mdc (motor_stats)
python scripts/race-title.py --date "$(date +%Y-%m-%d)" --force        # getHoldingList2 (title)
python scripts/build_index.py --date "$(date +%Y-%m-%d)" --mode daily --all-active  # 全 active 予想者の強さ index
```

---

## Project Structure

```
scripts/
├── preview-realtime.py          # Realtime preview + realtime result scraper (also updates index)
├── race-title.py                # Per-race レース名 sidecar (data/programs/title/)
├── motor-stats.py               # Motor stats scraper (data/programs/motor_stats/)
├── race-card.py                 # Race-card detail scraper (data/programs/race_cards/)
├── recent-form.py               # Recent national/local form scraper
├── build_index.py               # Strength Index builder (--mode daily/realtime, --update-races, --predictor / --all-active)
├── build_weights.py             # Monthly weight learner (per-stadium per-predictor weights, --predictor / --all-active)
├── build_sui_params.py          # 24-stadium weather coefficient learner
├── boatrace/                    # Python package
│   ├── __init__.py
│   ├── downloader.py            # HTTP downloads with retry
│   ├── extractor.py             # LZH decompression
│   ├── parser.py                # Fixed-width text parsing
│   ├── converter.py             # Text → CSV conversion
│   ├── storage.py               # File I/O operations
│   ├── git_operations.py        # Git commit/push operations
│   ├── index_features.py        # Shared feature computation (build_index/build_weights)
│   ├── predictors/              # Predictor (予想者) レジストリ
│   │   ├── __init__.py
│   │   └── registry.py          # PredictorSpec + active_predictors() — 新規予想者の追加点
│   ├── preview_tsv_scraper.py   # bc_j_tkz/stt/sui/oriten TSV scraper
│   ├── result_realtime.py       # bc_rs1_2 TSV scraper (realtime results)
│   └── logger.py                # Structured JSON logging
├── requirements.txt
└── tests/
    ├── unit/
    └── integration/

.github/workflows/
├── daily-sync.yml               # Daily data sync + daily index batch (07:30 JST)
├── preview-realtime.yml         # Realtime preview — manual fallback only (production runs on Cloud Run Jobs, JST 08:00-22:59 every 5min)
└── monthly-weights.yml          # Monthly weight rebuild (1st of month, 09:00 JST)

infra/                           # Cloud Run Jobs deployment for preview-realtime
├── Dockerfile
├── run.sh
└── cloudbuild.yaml
                                 # (詳細は docs/infrastructure.md 参照)

data/                            # Published data (created at runtime)
├── programs/
│   ├── title/YYYY/MM/DD.csv                # per-race レース名 sidecar (race-title.py)
│   ├── race_cards/YYYY/MM/DD.csv           # bc_j_str3 由来の出走表詳細
│   ├── recent_national/YYYY/MM/DD.csv      # 全国近況5節
│   ├── recent_local/YYYY/MM/DD.csv         # 当地近況5節
│   └── motor_stats/YYYY/MM/DD.csv          # モーター期成績スナップショット
├── previews/
│   ├── tkz/YYYY/MM/DD.csv                  # realtime: 体重・展示タイム・チルト
│   ├── stt/YYYY/MM/DD.csv                  # realtime: 進入コース・スタート展示
│   ├── sui/YYYY/MM/DD.csv                  # realtime: 水面気象スナップショット
│   └── original_exhibition/YYYY/MM/DD.csv  # realtime: オリジナル展示
├── results/
│   ├── realtime/YYYY/MM/DD.csv             # bc_rs1_2 由来の締切後5〜30分スナップショット
│   └── payouts/YYYY/MM/DD.csv              # bc_rs2 由来の締切後5〜30分払戻金スナップショット
└── estimate/
    ├── index/YYYY/MM/DD.csv                # 派生: 強さポイント (5要素偏差値+寄与+合計)
    └── stadium/
        ├── win_rate.csv                    # 場×季節×コース勝率
        ├── sui_params.csv                  # 24場気象線形回帰パラメータ
        └── index_weights/YYYY-MM.csv       # 月次重み(直近6ヶ月で再学習)

docs/                            # ドキュメント (詳細は docs/README.md)
├── README.md
├── data/                        # CSV データ仕様
├── development.md               # 本ファイル
├── operations.md                # GitHub Actions Workflows / Configuration / Performance
└── infrastructure.md            # Cloud Run Jobs (旧 infra/README.md の統合先)

.boatrace/
└── config.json                  # Configuration

logs/
└── boatrace-YYYY-MM-DD.json     # Execution logs
```

---

## Usage

### Fetch Daily Data(手動再実行)

毎日 JST 07:30 に `daily-sync.yml` が自動で同等の処理を実行しますが、特定日を再 fetch したい場合は対応するスクリプトを個別に呼び出します。

```bash
# boatcast 由来のサイドカー (引数 --date で対象日を指定)
python scripts/race-card.py    --date 2026-05-12 --force
python scripts/recent-form.py  --date 2026-05-12 --force
python scripts/motor-stats.py  --date 2026-05-12 --force
python scripts/race-title.py   --date 2026-05-12 --force
```

> 展示会データ (Realtime Preview) は per-race 締切直前にしか取れないため、過去日の単発再 fetch は不可。詳細は次節 *Realtime Preview Scraper* を参照。

### Run Realtime Preview Scraper

```bash
# Default: target today (JST), preview window = [now+1min, now+10min],
# result window = [now-30min, now-3min]
python scripts/preview-realtime.py

# Plan only — log eligible races but write nothing
python scripts/preview-realtime.py --dry-run

# Write CSVs but skip git commit & push
python scripts/preview-realtime.py --no-commit

# Override the reference time (HH:MM JST), useful for testing
python scripts/preview-realtime.py --now 12:30 --no-commit

# Wider preview window (override defaults)
python scripts/preview-realtime.py --window-min 2 --window-max 15

# Skip the realtime-result step (preview only)
python scripts/preview-realtime.py --skip-results

# Skip the realtime-payout step (bc_rs2)
python scripts/preview-realtime.py --skip-payouts

# Custom result polling window (minutes since deadline)
python scripts/preview-realtime.py --result-window-min 5 --result-window-max 45
```

Designed to run every minute via `.github/workflows/preview-realtime.yml`. On each invocation it:

1. Fetches `https://race.boatcast.jp/api_txt/getHoldingList2_{YYYYMMDD}.json` to discover open venues + per-race deadline times (no caching, no persistence).
2. **Preview pass** — selects races whose deadline falls in `[now+window-min, now+window-max]` AND that are not yet recorded in every per-source CSV. Scrapes `bc_j_tkz` / `bc_j_stt` / `bc_sui` / `bc_oriten` for each eligible race and appends one row per source. After appending, also updates the corresponding rows in `data/estimate/{predictor_id}/YYYY/MM/DD.csv` for every active predictor (展示・気象 を実値で再計算 → 状態 = `realtime`).
3. **Result pass** — selects races whose deadline already passed by `[result-window-min, result-window-max]` minutes and whose `レースコード` is not yet in `data/results/realtime/YYYY/MM/DD.csv`. Scrapes `bc_rs1_2` for each candidate and appends one row to the realtime results CSV (skips silently when the file is not yet published).
4. **Payout pass** — same eligibility window as the result pass but keyed off `data/results/payouts/YYYY/MM/DD.csv`. Scrapes `bc_rs2` and appends one row per race. Independent of the result pass: a race may show up in one CSV first and the other a cycle later.
5. Commits & pushes the changes in a single commit (preview + result + payout + index updates batched). Nothing is committed when no rows were appended.

Idempotency is per-source: if `tkz` succeeds but `stt` is still missing for race X, the next minute's run only retries `stt` for X. Likewise, the result / payout passes only retry races still missing from their respective CSV.

### Scrape Race Title Data(per-race レース名 sidecar)

```bash
# Default: scrape today's race-title CSV (JST)
python scripts/race-title.py

# Specific date
python scripts/race-title.py --date 2026-05-03

# Dry run (no file written, no git push)
python scripts/race-title.py --date 2026-05-03 --dry-run

# Force overwrite existing CSV
python scripts/race-title.py --date 2026-05-03 --force

# Write CSV but skip git commit/push
python scripts/race-title.py --date 2026-05-03 --no-push
```

The script fetches `race.boatcast.jp/api_txt/getHoldingList2_YYYYMMDD.json` once and writes one row per scheduled race per open stadium to `data/programs/title/YYYY/MM/DD.csv`. boatcast only exposes the current/upcoming day reliably — backfill of distant past dates may return empty payloads.

### Scrape Race Card Detail Data

```bash
# Default: scrape yesterday's race-card data (JST)
python scripts/race-card.py

# Specific date
python scripts/race-card.py --date 2026-04-25

# Dry run (no file written, no git push)
python scripts/race-card.py --date 2026-04-25 --dry-run

# Force overwrite existing CSV
python scripts/race-card.py --date 2026-04-25 --force
```

Data source: `race.boatcast.jp` の per-race TSV (`/hp_txt/{jo}/bc_j_str3_*.txt`). The script uses the same-day B-file from `mbrace.or.jp` to determine which races are scheduled (matching `original-exhibition.py`'s flow). Available approximately from **2025-05-02 onwards**.

### Scrape Recent Form Data(全国・当地近況5節)

```bash
# Default: scrape yesterday's recent-form data (JST)
python scripts/recent-form.py

# Specific date
python scripts/recent-form.py --date 2026-04-25

# Dry run (no files written, no git push)
python scripts/recent-form.py --date 2026-04-25 --dry-run

# Force overwrite both CSV files
python scripts/recent-form.py --date 2026-04-25 --force
```

A single run produces both `data/programs/recent_national/YYYY/MM/DD.csv` and `data/programs/recent_local/YYYY/MM/DD.csv` from `bc_zensou` and `bc_zensou_touchi` respectively. The B-file from `mbrace.or.jp` is used to look up which racer is in which boat at each race. Per-stadium fetch only — at most ~48 boatcast requests per day even on 24-stadium peak days.

### Scrape Motor Stats Data(モーター期成績)

```bash
# Default: scrape yesterday's motor stats (JST)
python scripts/motor-stats.py

# Specific date
python scripts/motor-stats.py --date 2026-04-25

# Dry run (no file written, no git push)
python scripts/motor-stats.py --date 2026-04-25 --dry-run

# Force overwrite existing CSV
python scripts/motor-stats.py --date 2026-04-25 --force
```

The script fetches `bc_mst` (motor period start date) and `bc_mdc` (per-motor stats) from `race.boatcast.jp` for every stadium that has races on the given date (per the same-day B-file from `mbrace.or.jp`). All motors are written to a single CSV at `data/programs/motor_stats/YYYY/MM/DD.csv`.

**Backfill is not possible** — race.boatcast.jp only exposes the current motor period for each stadium, so historical periods are lost. Run this script daily going forward to accumulate time-series snapshots.

### Build Strength Index(強さポイント)

**依存データ**(`compute_features_for_day` が参照する CSV):

- `data/programs/race_cards/YYYY/MM/DD.csv` — レース集合・モーター番号・級別・節間14スロット成績
- `data/programs/recent_national/` + `recent_local/YYYY/MM/DD.csv` — 選手pt の着順時系列
- `data/programs/motor_stats/YYYY/MM/DD.csv` — モーターpt の **モーター期起算日**(履歴リセット境界)
- `data/programs/title/YYYY/MM/DD.csv` — モーターpt のグレード分類(任意。無い場合は「一般」扱い)
- `data/previews/{sui,tkz,stt,original_exhibition}/YYYY/MM/DD.csv` — 展示・気象
- `data/estimate/motor_ability_score.csv` — **モーターpt のスコアテーブル(必須)**。
  詳細は [`docs/data/motor_ability_score.md`](./data/motor_ability_score.md)
- `data/estimate/stadium/win_rate.csv` / `sui_params.csv` / `weights/{predictor_id}/YYYY-MM.csv`

予想者の宣言は [`scripts/boatrace/predictors/registry.py`](../scripts/boatrace/predictors/registry.py) で行う(`PREDICTORS` タプルに `PredictorSpec` を追加。詳細は [`docs/data/estimate.md`](./data/estimate.md#予想者predictorレジストリ))。

```bash
# 当日朝に走らせる日次バッチ(active な全予想者ぶん):
#   枠番・選手・モーター + 暫定強さpt を埋める。展示・気象は 50 で補完。
python scripts/build_index.py --date 2026-05-03 --mode daily --all-active

# 特定予想者のみ:
python scripts/build_index.py --date 2026-05-03 --mode daily --predictor v1_basic

# 過去日のバックフィル(全要素揃った状態で計算):
python scripts/build_index.py --date 2026-05-03 --mode realtime --all-active

# 一部レースだけ展示・気象を再計算して状態を realtime に更新
# (preview-realtime.py から内部呼び出しされる)
python scripts/build_index.py --date 2026-05-03 \
  --predictor v1_basic \
  --update-races 202605030101,202605030102

# 過去月のバックフィル例(月毎に重みファイルが必要):
for d in $(seq -w 1 31); do
  python scripts/build_index.py --date 2026-05-${d} --mode realtime --all-active
done
```

### Build Monthly Weights(場別重み)

```bash
# 対象月の重みを active な全予想者ぶん、直近6ヶ月のデータから学習
python scripts/build_weights.py --month 2026-05 --all-active

# 特定予想者のみ:
python scripts/build_weights.py --month 2026-05 --predictor v1_basic

# 過去月の重みを生成(walk-forward 検証用)
python scripts/build_weights.py --month 2026-04 --all-active
python scripts/build_weights.py --month 2026-03 --all-active
```

学習窓は `[対象月 - 6ヶ月, 対象月 - 1日]`(対象月のデータは含まない=リーケージなし)。場ごとに非負・合計1の制約で SLSQP 最適化。モーターpt は **v2 ロジック**(直近 6 節 × 級別×グレード×コースの z 残差 × 半減期 60 日の時間減衰 × prior k=10 のベイズ収縮、モーター期起算日でリセット)。フィーチャーフラグ `ENABLE_DECAY` / `ENABLE_LANE_CORRECTION` / `ENABLE_SHRINKAGE` を全 False かつ `MOTOR_HISTORY_SESSIONS=5` にすると v1 と算術等価な単純平均モードに戻る(ablation 検証用)。詳細は [`docs/design/motor_ability_index_v2.md`](./design/motor_ability_index_v2.md)。

`build_weights.py` は 6 ヶ月 ≒ 181 日を直列に処理するため、`boatrace.index_features.FeatureContext` を `build_training_table` で構築して `compute_features_for_day(repo, day, ctx=ctx)` に渡し、静的テーブル(`win_rate.csv` / `motor_ability_score.csv` / `sui_params.csv`)と `race_cards` / `title` 読込、`detect_session_end_days` の節境界検出をバッチ全体で amortize している。単発呼出し(`build_index.py`)は `ctx` を省略するだけで従来通り動く。設計詳細は [`docs/design/feature_context_refactor.md`](./design/feature_context_refactor.md) を参照。

### Build Stadium Weather Params (sui_params.csv)

```bash
# 24場分の気象線形回帰パラメータを実データから再学習
python scripts/build_sui_params.py \
  --start-date 2025-11-01 --end-date 2026-04-30 \
  --out data/estimate/stadium/sui_params.csv
```

`previews + results` を結合して場×コース別に線形回帰し、波・風(追い/向かい)・気温水温差・天候から有利pt変動を推定。結合対象の results は `data/results/realtime/`(`preview-realtime.py` が当日中に追記する準リアルタイム結果)。`--start-date` の下限は realtime CSV の収録開始日 (2025-11-01) に合わせる。

---

## Testing

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_parser.py

# Run with coverage
pytest --cov=boatrace tests/unit/
```

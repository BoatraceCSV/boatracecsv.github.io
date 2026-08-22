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
python scripts/race-card.py --date "$(date +%Y-%m-%d)" --force         # bc_j_str3 (race_cards) + bc_j_waku10 (waku10) + bc_mon_2 (monthly_schedule)
python scripts/recent-form.py --date "$(date +%Y-%m-%d)" --force       # bc_zensou (recent_form)
python scripts/motor-stats.py --date "$(date +%Y-%m-%d)" --force       # bc_mst / bc_mdc (motor_stats) + bc_mrireki (motor_history)
python scripts/race-title.py --date "$(date +%Y-%m-%d)" --force        # getHoldingList2 (title)
python scripts/build_index.py --date "$(date +%Y-%m-%d)" --mode daily --all-active  # 全 active 予想者の強さ index
```

---

## Project Structure

```
scripts/
├── preview-realtime.py          # Realtime preview + odds + realtime result scraper (also updates index)
├── race-title.py                # Per-race レース名 sidecar (data/programs/title/)
├── motor-stats.py               # Motor stats + motor history scraper (data/programs/)
├── race-card.py                 # Race-card detail + waku10 + monthly schedule scraper (data/programs/)
├── recent-form.py               # Recent national/local form scraper
├── build_index.py               # Strength Index builder (--mode daily/realtime, --update-races, --predictor / --all-active)
├── build_weights.py             # Monthly weight learner (per-stadium per-predictor weights, --predictor / --all-active)
├── build_course_rate.py         # 場×レース番号×コース別1着率テーブル (course_win_rate.csv, v6_course 用, stdlib のみ)
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
│   ├── waku10/YYYY/MM/DD.csv               # bc_j_waku10 由来の枠番別過去10走
│   ├── monthly_schedule/YYYY/MM.csv        # bc_mon_2 由来の月間開催日程 (全24場・毎日上書き)
│   ├── recent_national/YYYY/MM/DD.csv      # 全国近況5節
│   ├── recent_local/YYYY/MM/DD.csv         # 当地近況5節
│   ├── motor_stats/YYYY/MM/DD.csv          # モーター期成績スナップショット
│   └── motor_history/YYYY/MM/DD.csv        # bc_mrireki 由来のモーター履歴 (日付=基準節終了日)
├── previews/
│   ├── tkz/YYYY/MM/DD.csv                  # realtime: 体重・展示タイム・チルト
│   ├── stt/YYYY/MM/DD.csv                  # realtime: 進入コース・スタート展示
│   ├── sui/YYYY/MM/DD.csv                  # realtime: 水面気象スナップショット
│   ├── original_exhibition/YYYY/MM/DD.csv  # realtime: オリジナル展示
│   ├── od1/YYYY/MM/DD.csv                  # realtime: 集計中オッズ(3連複・拡連複・単勝・複勝)
│   ├── od2/YYYY/MM/DD.csv                  # realtime: 集計中オッズ(2連単・2連複)
│   └── od3/YYYY/MM/DD.csv                  # realtime: 集計中オッズ(3連単)
├── results/
│   ├── realtime/YYYY/MM/DD.csv             # bc_rs1_2 由来の締切後スナップショット(終日キャッチアップ)
│   └── payouts/YYYY/MM/DD.csv              # bc_rs2 由来の締切後払戻金スナップショット(終日キャッチアップ)
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
# result pass = catch-up mode (締切+3分以降、未記録レースを終日再試行、
# 1回あたり締切の古い順に15件まで)
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

# Custom result polling window (minutes since deadline)。--result-window-max を
# 明示すると固定窓(レガシー動作)。省略時(デフォルト)はキャッチアップモード:
# 未記録レースを終日再試行する
python scripts/preview-realtime.py --result-window-min 5 --result-window-max 45

# キャッチアップモードの1回あたり取得上限(締切の古い順。0 = 無制限)
python scripts/preview-realtime.py --result-catchup-limit 15
```

Designed to run every minute via `.github/workflows/preview-realtime.yml`. On each invocation it:

1. Fetches `https://race.boatcast.jp/api_txt/getHoldingList2_{YYYYMMDD}.json` to discover open venues + per-race deadline times (no caching, no persistence).
2. **Preview pass** — selects races whose deadline falls in `[now+window-min, now+window-max]` AND that are not yet recorded in every per-source CSV. Scrapes `bc_j_tkz` / `bc_j_stt` / `bc_sui` / `bc_oriten` for each eligible race and appends one row per source. After appending, also updates the corresponding rows in `data/estimate/{predictor_id}/YYYY/MM/DD.csv` for every active predictor (展示・気象 を実値で再計算 → 状態 = `realtime`).
3. **Result pass** — selects races whose deadline already passed by at least `result-window-min` minutes and whose `レースコード` is not yet in `data/results/realtime/YYYY/MM/DD.csv`. By default (**catch-up mode**, `--result-window-max` unset) a missing race stays a candidate until end of day — races that run far behind their scheduled deadline (SG 進行遅延・悪天候中断。例: 2026-07-28 びわこ 7R-12R) are recovered once `bc_rs1_2` appears. Candidates are fetched oldest-deadline first, capped per invocation by `--result-catchup-limit` (default 15) so a backlog drains across 5-min ticks without hitting the Cloud Run Job's 300 s timeout. Passing an explicit `--result-window-max N` restores the legacy fixed window. Scrapes `bc_rs1_2` for each candidate and appends one row to the realtime results CSV (skips silently when the file is not yet published).
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

The same run also scrapes, per race, the 枠番別過去10走 TSV
(`/hp_txt/{jo}/bc_j_waku10_*.txt` → `data/programs/waku10/YYYY/MM/DD.csv`)
and, per invocation, the monthly holding schedule for all 24 stadiums
(`/hp_txt/{jo}/bc_mon_2_{YYYYMM}_{jo}.txt` →
`data/programs/monthly_schedule/YYYY/MM.csv`, overwritten daily; use
`--force` to refresh existing files). All created CSVs go into a single
commit.

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

The same run also collects the motor usage history (`bc_mrireki`): for each open stadium it derives the most recent completed 節's end date from `bc_mon_2` and appends that snapshot's rows to `data/programs/motor_history/YYYY/MM/DD.csv` (path date = 節終了日). Stadiums already present in the target CSV are skipped, so the daily re-run is idempotent.

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

### Build Course Win Rate Table (course_win_rate.csv)

```bash
# data/results/realtime の全履歴から 場×レース番号×コース別テーブルを再生成
python scripts/build_course_rate.py

# 収縮強度の明示指定 (デフォルト k=50)
python scripts/build_course_rate.py --k 50
```

`v6_course` の `コースpt` 生値ソース(`data/estimate/stadium/course_win_rate.csv`、
24 場 × 12 レース回 = 288 行)。セル値は場×コース全体率へのベイズ収縮
`(wins + k·base) / (n + k)`。monthly-weights ジョブが `build_weights.py` の前に
毎月再生成する。**意図的に pandas 非依存(stdlib のみ)**で、venv 無しの環境でも
実行できる。設計は [`docs/design/course_strength_v6.md`](./design/course_strength_v6.md)。

### Build Stadium Weather Params (sui_params.csv)

```bash
# 24場分の気象線形回帰パラメータを実データから再学習
python scripts/build_sui_params.py \
  --start-date 2025-11-01 --end-date 2026-04-30 \
  --out data/estimate/stadium/sui_params.csv
```

`previews + results` を結合して場×コース別に線形回帰し、波・風(追い/向かい)・気温水温差・天候から有利pt変動を推定。結合対象の results は `data/results/realtime/`(`preview-realtime.py` が当日中に追記する準リアルタイム結果)。`--start-date` の下限は realtime CSV の収録開始日 (2025-11-01) に合わせる。

---

### `scripts/build_suji_table.py`

静的テーブル 2 枚(スジ表 / 決まり手注釈)を `results/realtime` × `previews/stt` の
全履歴から生成する。monthly-weights ジョブが毎月 1 日に再生成する。
stdlib のみで動く(venv 不要)。

> 元は穴予想 `v9_suji`(2026-08-22 退役)用だが、**決まり手注釈テーブル
> (`kimarite_table.csv`)は `v10_kimarite` の `build_kimarite_picks.py` が読む**ので
> 再生成は続いている。スジ表(`suji_table.csv`)は現在どの active 予想者も使わない。

```sh
python scripts/build_suji_table.py                    # 本番構成 (全履歴・収縮なし)
python scripts/build_suji_table.py --k 50             # 収縮あり (本番では使わない)
python scripts/build_suji_table.py --by-stadium       # 場別テーブルも出す (検証用)
python scripts/build_suji_table.py --from-date 2026-05-01 --to-date 2026-06-25
```

構成選定の記録は [`notebooks/ana_prediction/report.md`](../notebooks/ana_prediction/report.md)。

### `scripts/build_suji_picks.py`

穴予想 `v9_suji` の買い目 CSV(`data/estimate/suji/YYYY/MM/DD.csv`)を生成する。
`build_index.py` の後に走らせる(強さpt を読むため)。

> **`v9_suji` は 2026-08-22 に退役したので、このスクリプトは定期実行されていない**
> (daily-sync のステップは削除、preview-realtime 側は `active_predictors()` の
> ゲートで自動停止)。**モジュールは消さないこと** — `build_kimarite_picks.py` が
> `load_index_rows` / `load_kimarite_table` / `load_stt_courses` /
> `strengths_by_boat` を import している。手動実行は下記のとおり可能。

```sh
# 朝バッチ: 当日の全レースを 状態=daily で出力
python scripts/build_suji_picks.py --date 2026-08-11 --mode daily

# 直前バッチ: 指定レースの 状態=realtime 行を upsert (preview-realtime.py が内部で呼ぶ)
python scripts/build_suji_picks.py --date 2026-08-11 --mode realtime \
    --update-races 202608112301,202608112302
```

### `scripts/build_kimarite.py`

荒れ度メーターの Stage1(決まり手セルの多項ロジスティック回帰)を全履歴で学習し、
係数 CSV を出す。monthly-weights ジョブが毎月 1 日に再学習する。sklearn を使う。

```sh
python scripts/build_kimarite.py                       # 全履歴で学習
python scripts/build_kimarite.py --report              # ホールドアウト評価つき
python scripts/build_kimarite.py --to-date 2026-06-25  # 窓を切る (検証用)
```

### `scripts/build_kimarite_probs.py`

学習済み係数から日次の荒れ度・セル確率を出す。**sklearn 非依存**(係数 CSV から
softmax を直接計算)。180 レースで 0.05 秒程度。

```sh
python scripts/build_kimarite_probs.py --date 2026-08-12 --mode daily
python scripts/build_kimarite_probs.py --date 2026-08-12 --mode realtime \
    --update-races 202608122301
```

### `scripts/build_kimarite_calibration.py`

荒れ度メーターの校正 (予測帯ごとの 予測 vs 実測) と log-loss を集計する。
monthly-weights ジョブが再学習の後に実行する。stdlib のみ。

```sh
python scripts/build_kimarite_calibration.py
python scripts/build_kimarite_calibration.py --from-date 2026-09-01
```

出力は `data/estimate/kimarite/tables/calibration.csv`。集計対象は
**直前予測 (状態=realtime) のみ**で、朝の暫定値は混ぜない。

### `scripts/build_kimarite_pairs.py`

決まり手セル条件付きの 2-3 着テーブル (Stage2)。B案 `v10_kimarite` が
Stage1 の出力に掛けて 3連単 120 通りの分布にする。monthly-weights が生成。

```sh
python scripts/build_kimarite_pairs.py          # 既定 k=150
python scripts/build_kimarite_pairs.py --k 300
```

### `scripts/build_kimarite_picks.py`

穴予想 B案 `v10_kimarite` の買い目。Stage1 の確率 × Stage2 のペア表を合成し、
Plackett-Luce(強さpt)とブレンドして **1 コース頭を除いた上位 5 点**を出す。

```sh
python scripts/build_kimarite_picks.py --date 2026-08-13 --mode daily
python scripts/build_kimarite_picks.py --date 2026-08-13 --mode realtime \
    --update-races 202608132301,202608132302
```

**`build_index.py`(強さpt)と `build_kimarite_probs.py`(Stage1)の両方より後**に
走らせること。どちらかが欠けていれば作り方を示して落ちる。
合成の式と定数 (γ / β / w) は `scripts/boatrace/kimarite_blend.py`。

### `scripts/build_kimarite_logloss.py`

A案 vs B案 の**主判定**。確定した 状態=realtime のレースで 3連単 120 通りの
分布を組み直し、`Plackett-Luce(強さpt, β=1.4)` と log-loss を比べる。

```sh
python scripts/build_kimarite_logloss.py                        # started_at 以降
python scripts/build_kimarite_logloss.py --from-date 2026-09-01
```

出力は `data/estimate/kimarite/tables/logloss.csv`(月次行 + `累計` 行)。

### `notebooks/ana_prediction/kimarite_backtest.py`

γ / β / w の再選定用(numpy / sklearn が要る)。valid で選び、test は報告のみ。

```sh
python notebooks/ana_prediction/kimarite_backtest.py --k-grid 150
```

## Testing

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_parser.py

# Run with coverage
pytest --cov=boatrace tests/unit/
```

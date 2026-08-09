# Operations

GitHub Actions ワークフロー、設定ファイル、運用上のメモをまとめています。Cloud Run Jobs の詳細は [infrastructure.md](./infrastructure.md) を参照してください。

- [Environment Setup for GitHub Actions](#environment-setup-for-github-actions)
- [Workflows](#workflows)
- [Configuration](#configuration)
- [Performance](#performance)
- [Data Source](#data-source)
- [License](#license)

---

## Environment Setup for GitHub Actions

1. Repository secrets (configured in GitHub):
   - `GITHUB_TOKEN` (provided automatically)
   - Optional: `GIT_USER_EMAIL` (defaults to "action@github.com")
   - Optional: `GIT_USER_NAME` (defaults to "GitHub Action")

2. GitHub Pages configuration:
   - Settings → Pages → Source: Deploy from a branch
   - Branch: `main`
   - Folder: `/ (root)`

---

## Workflows

- **`daily-sync.yml`** — Runs every day at 07:30 JST (= 22:30 UTC)。実測で 1 ラン ~22 分かかるため、Cloud Run Jobs の `preview-realtime` (JST 08:00 起動) が当日 title CSV を参照する前に完了するよう 30 分の余裕を取って 07:30 起動としています。Processes Results, Programs, Race Cards (including 枠番別過去10走 `bc_j_waku10` and the 月間開催日程 `bc_mon_2` refresh), Recent Form, Motor Stats, and **Race Title** for the previous/current day. Then runs **Build Daily Index Batch** (`build_index.py --mode daily --all-active`) to populate today's `data/estimate/{predictor_id}/YYYY/MM/DD.csv` for every active predictor with 枠番・選手・モーター + 暫定強さpt(状態 = `daily`、展示・気象は 50 で補完)。Each step uses `if: always()` (and `continue-on-error: true` for third-party-source steps) so a single source outage does not break the rest of the pipeline.
- **`preview-realtime.yml`** — `workflow_dispatch` manual fallback only. The production schedule (JST 08:00-22:59, every 5 minutes) has been migrated to **Cloud Scheduler + Cloud Run Jobs** because GitHub Actions cron was being throttled. Four passes per invocation:
  1. **Preview pass** — scrapes per-source preview data (`tkz` / `stt` / `sui` / `original_exhibition`) for races whose deadline falls in `[now+1min, now+10min]` and updates `data/estimate/{predictor_id}/YYYY/MM/DD.csv` for every active predictor (展示・気象 を実値で再計算 → 状態 = `realtime`).
  2. **Odds pass** — scrapes the aggregating odds `bc_smt_od{1,2,3}` for the same eligibility window and appends one row per source to `data/previews/{od1,od2,od3}/YYYY/MM/DD.csv`(締切約5分前のスナップショット。確定オッズではない)。The three sources are fetched and deduped independently, so a partial success is completed on the next cycle.
  3. **Result pass** — scrapes `bc_rs1_2` for races whose deadline already passed by 3 分以上 and whose row is still missing from `data/results/realtime/YYYY/MM/DD.csv`(**catch-up mode**: 終日再試行。SG 進行遅延・悪天候中断で予定締切から大幅に遅れたレースも公開後に回収する。1 回の実行で締切の古い順に `--result-catchup-limit` 件まで。`--result-window-max` 明示時は従来の固定窓)and appends one row.
  4. **Payout pass** — scrapes `bc_rs2` (払戻金) for the same eligibility window, independent of the result pass, and appends one row to `data/results/payouts/YYYY/MM/DD.csv`.

  All changes (preview + odds + index + result + payout) go in a single commit. Idempotent and resilient to cron drift; commits one batch per invocation only when rows are actually appended.
  - **Cloud Run Jobs 構成**: `boatrace-487212/asia-northeast1` の Cloud Scheduler `preview-realtime-daytime` (`*/5 8-22 * * *`, Asia/Tokyo) が Cloud Run Job `preview-realtime` を発火します。詳細は [`infrastructure.md`](./infrastructure.md) を参照。
- **`monthly-weights.yml`** — Runs on the 1st of each month at 06:00 JST. First rebuilds `data/estimate/stadium/course_win_rate.csv` (`course` 成分の場×レース番号×コース別テーブル、`scripts/build_course_rate.py`、全履歴から再生成。`course` を使う v6_course / v7_aggregate / v8_aionly は 2026-08-09 に退役済みで現在の消費者はいないが、再挑戦に備えて再生成は継続している), then re-learns 24-stadium × n_components weights for every active predictor from the prior 6 months of data and writes `data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv`. `build_index.py` automatically picks up the latest weights ≤ the target month per predictor.

### 予想者(Predictor)の運用

`scripts/boatrace/predictors/registry.py` で active な予想者を宣言し、`infra/run-*.sh` の `ACTIVE_PREDICTORS` 配列を同期して更新します。`build_index.py --all-active` / `build_weights.py --all-active` が registry を参照して全 active 予想者をループします。詳細は [`data/estimate.md`](./data/estimate.md#予想者predictorレジストリ) を参照。

---

## Configuration

Edit `.boatrace/config.json` to customize:

```json
{
  "rate_limit_interval_seconds": 3,
  "max_retries": 3,
  "initial_backoff_seconds": 5,
  "max_backoff_seconds": 30,
  "request_timeout_seconds": 30,
  "log_level": "INFO",
  "log_file": "logs/boatrace-{DATE}.json"
}
```

---

## Performance

- **Daily execution**: ~10-15 seconds (typical)
- **Historical backfill (3 years)**: ~60 minutes
- **CSV file size**: 100-500 KB per file

---

## Data Source

Official Boatrace Races Server: http://www1.mbrace.or.jp/od2/

---

## License

MIT License

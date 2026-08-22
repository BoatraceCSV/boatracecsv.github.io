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
  同じ実行で静的テーブル `data/estimate/suji/tables/*.csv` と
  `data/estimate/kimarite/tables/*.csv` も全履歴から再生成し、weights と同じ
  コミットに含めます。

  > ⚠️ 静的テーブルの出力先は `infra/run-monthly-weights.sh` の
  > sparse-checkout `paths` 配列と末尾の `git add` の**両方**に載っている必要が
  > あります。cone-mode ではどちらかが欠けると生成物が**無言で捨てられます**
  > (実際 `data/estimate/{suji,kimarite}/tables/` は導入時から両方に入っておらず、
  > 2026-08-22 に修正するまで永続化されていませんでした)。詳細は
  > [`infrastructure.md`](./infrastructure.md#sparse-checkout-対象-monthly-weights--run-monthly-weightssh)。

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

## 穴予想 `v9_suji` の運用(2026-08-12〜)

穴予想の買い目は **boatracecsv 側で確定させて CSV に出す**(fun-site は表示と集計のみ)。
そのため他の予想者と違い、index に加えて 2 系統のファイルが要る。

| ジョブ | 追加された処理 | 出力 |
| --- | --- | --- |
| monthly-weights(毎月 1 日 06:00 JST) | `scripts/build_suji_table.py` | `data/estimate/suji/tables/{suji_table,kimarite_table}.csv` |
| daily-sync(07:30 JST) | `scripts/build_suji_picks.py --mode daily` | `data/estimate/suji/YYYY/MM/DD.csv`(状態=daily) |
| preview-realtime(締切 5 分前) | `build_suji_picks.write_day(..., realtime)` を内部呼び出し | 同上(状態=realtime を upsert) |

**依存順序**: `build_suji_picks.py` は `build_index.py`(強さpt)と
`build_suji_table.py`(スジ表)の両方の出力を読む。どちらかが欠けている場合は
**作り方を示すメッセージ付きで落ちる**(素の FileNotFoundError にはしない)。

**sparse-checkout**: 静的テーブルは `data/estimate/suji/tables`、日次の買い目は
`data/estimate/suji/${TODAY_YM}` と**別パスで指定**している。同じ階層に置くと
cone-mode が日次ファイルの全履歴まで checkout してしまうため。

**ロールバック**: `registry.py` / fun-site `predictors.ts` の `v9_suji` を
`status: retired` にすると `active_predictors()` から外れ、preview-realtime /
build_index / GCS ミラー / 集計すべての対象から自動的に落ちる。
`infra/run*.sh` の `ACTIVE_PREDICTORS` も同期すること。

## 穴予想 v10_kimarite の運用(2026-08-13〜)

B案。買い目を CSV で配る点は A案と同じだが、**荒れ度メーターと同じ Stage1
モデルを土台にする**ので依存が 1 段深い。

**依存順序**(これを崩すと買い目が 0 行になる):

```
build_index.py --predictor v10_kimarite   … 強さpt
build_kimarite_probs.py                   … Stage1 の 32 クラス確率
        ↓
build_kimarite_picks.py                   … 合成 → ブレンド → 上位 5 点
```

Stage1 の確率が無いレースは **無言でスキップ**する(買い目を出さない)。
index が無い場合は作り方を示して落ちる。

**sparse-checkout**: 買い目は `data/estimate/kimarite/picks/${TODAY_YM}` を
**独立したパスとして**追加している(`data/estimate/kimarite/${TODAY_YM}` と別)。

**退役判定**: `data/estimate/kimarite/tables/logloss.csv` の `95%CI下限` が
0 を下回ったら退役する。**回収率では判定しない**(A案との差 +4.2pt を有意に
するには 8.2 ヶ月かかる)。詳細は
[`docs/design/ana_prediction.md`](../design/ana_prediction.md) §13.3。

この CSV は monthly-weights が毎月 1 日に再生成する。**初回の生成は 2026-09-01
の実行**(sparse-checkout / `git add` の取りこぼしを 2026-08-22 に修正したため、
それ以前はリポジトリに存在しなかった)。手元で先に見たい場合は
`python scripts/build_kimarite_logloss.py`。

## 荒れ度メーターの運用(2026-08-12〜)

予想者に紐づかない独立の指標なので、`ACTIVE_PREDICTORS` とは無関係に毎回動く。

| ジョブ | 処理 | 出力 |
| --- | --- | --- |
| monthly-weights | `build_kimarite.py`(全履歴で再学習) | `data/estimate/kimarite/tables/cell_coef_*.csv` |
| monthly-weights | `build_kimarite_pairs.py`(Stage2 再生成) | `data/estimate/kimarite/tables/pair_table.csv` |
| monthly-weights | `build_kimarite_calibration.py` | `data/estimate/kimarite/tables/calibration.csv` |
| monthly-weights | `build_kimarite_logloss.py`(**B案の主判定**) | `data/estimate/kimarite/tables/logloss.csv` |
| daily-sync | `build_kimarite_probs.py --mode daily` | `data/estimate/kimarite/YYYY/MM/DD.csv` |
| daily-sync | `build_kimarite_picks.py --mode daily`(**v10_kimarite の買い目**) | `data/estimate/kimarite/picks/YYYY/MM/DD.csv` |
| preview-realtime | `write_day(..., realtime)` を内部呼び出し(probs → picks の順) | 同上(realtime を upsert) |

**学習と推論を分けている理由**: 推論を sklearn 非依存にしておくと、
毎回の直前バッチでモデルを読み込む必要がなく、係数 CSV さえあれば動く。
係数のクラス構成が `boatrace.kimarite.CELLS` とズレていたら
**黙って動かず落ちる**(古い係数で推論して静かに壊れるのを防ぐ)。

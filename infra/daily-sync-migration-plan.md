# daily-sync.yml → Cloud Run Job 移行計画

## 1. 現状と移行の方針

### 既存資産（再利用できるもの）
- `infra/Dockerfile` — `python:3.11-slim` + git + tini。リポジトリは**ランタイムで partial+sparse clone** する設計。依存関係はビルド時に固定。
- `infra/run.sh` — partial clone → sparse-checkout → Python 実行 → 自身が git push する。
- `infra/cloudbuild.yaml` — `preview-realtime` Job を build & deploy。
- Service Account `preview-realtime-runner@boatrace-487212.iam.gserviceaccount.com` と Secret Manager `github-token`、Artifact Registry リポジトリ `containers`。
- GCS `boatrace-realtime-data-boatrace-487212` と Pub/Sub `fun-site-realtime-completed`（fun-site への CSV ミラー）。

### 方針
**1 イメージ・複数 Job 構成**にする。`preview-realtime` と `daily-sync` は同じ Python 依存関係・同じリポジトリを使うため、Docker イメージは共通化し、Cloud Run Job を 2 本（`preview-realtime`、`daily-sync`）並べる。`run.sh` は 2 本に分け、Job ごとに `--command` で切り替える。Cloud Scheduler から JST 07:30 に `daily-sync` Job を `gcloud run jobs execute` で起動する。

GitHub Actions 側の `daily-sync.yml` は最終的に削除するが、移行検証の間は `schedule:` を外して `workflow_dispatch:` 専用のフォールバックとして残す（`preview-realtime.yml` と同じ運用）。

---

## 2. ステップ別の作業

### Step 1. `infra/run-daily-sync.sh` を新規作成
`run.sh` をベースに以下を変更したものを置く。

> **重要**：daily-sync 配下の 6 スクリプトはどれも `boatrace.gcs_publisher` を呼んでいない（調査済み）。`preview-realtime.py` のみが `upload_csvs()` + `publish_realtime_completed()` を呼ぶ実装になっている。**Open Question 1「GCS ミラー有効化」を満たすには、env vars の付与だけでは不足**で、`run-daily-sync.sh` の最終ステップとして publisher を 1 回叩くインライン Python 実行を追加する（後述）。

**sparse-checkout に追加するパス**（preview-realtime より多い）:
```
scripts
.boatrace
data/estimate/stadium
data/estimate/index/<TODAY_YM>
data/results/daily/<TODAY_YM>           # result.py の出力先
data/programs/race_cards/<TODAY_YM>     # race-card.py の出力先
data/programs/recent_national/<TODAY_YM>
data/programs/recent_local/<TODAY_YM>
data/programs/motor_stats/<TODAY_YM>
data/programs/motor_stats/<PREV_YM>     # build_index.py の 7 日 fallback 用
data/programs/title/<TODAY_YM>          # race-title.py の出力先
```

**Python 実行部**：GitHub Actions の `if: always()` / `continue-on-error: true` 相当を bash で再現する。
```bash
set -Eeuo pipefail
EXIT_AGGREGATE=0

run_step() {
  local label="$1"; shift
  log "STEP: ${label}"
  if "$@"; then
    log "STEP OK: ${label}"
  else
    local rc=$?
    log "STEP FAILED (rc=${rc}): ${label} — continuing"
    EXIT_AGGREGATE=1
  fi
}

TODAY_JST=$(TZ=Asia/Tokyo date +%Y-%m-%d)

run_step "result"        python scripts/result.py --force
run_step "race-card"     python scripts/race-card.py --date "$TODAY_JST" --force
run_step "recent-form"   python scripts/recent-form.py --date "$TODAY_JST" --force
run_step "motor-stats"   python scripts/motor-stats.py --date "$TODAY_JST" --force
run_step "race-title"    python scripts/race-title.py --date "$TODAY_JST" --force
run_step "build-index"   python scripts/build_index.py --date "$TODAY_JST" --mode daily

# build_index.py は git にコミットしないため、ここで明示的に push する
# (現行の daily-sync.yml の "Commit Daily Index" ステップ相当)
git add data/estimate/index/
if ! git diff --cached --quiet; then
  git commit -m "Update daily index batch (${TODAY_JST})"
  git push origin "${GIT_BRANCH}"
fi

# === GCS ミラー / Pub/Sub publish (Open Question 1: 有効化) ===
# preview-realtime.py が使っているのと同じ publisher を、daily-sync の
# 全データ取り込みが終わった後に 1 回だけ叩く。upload_csvs() は MD5 dedup
# 済みなので冪等。trigger は "daily-bootstrap" を採用 (preview-realtime が
# title/race_cards 更新時に使うものと同じ。fun-site 側が日次の初回 push
# を区別したい場合のためのフラグ)。
run_step "gcs-publish" python -c '
import os, sys
from datetime import date
from pathlib import Path
sys.path.insert(0, "scripts")
from boatrace.gcs_publisher import upload_csvs, assemble_updated_races, publish_realtime_completed

day = date.fromisoformat(os.environ["TODAY_JST"])
results = upload_csvs(repo=Path("."), day=day)
updated, _ = assemble_updated_races(
    repo=Path("."), day=day, upload_results=results,
    realtime_updated_codes=set(), result_updated_codes=None,
)
publish_realtime_completed(day=day, updated_races=updated, trigger="daily-bootstrap")
'

exit "${EXIT_AGGREGATE}"
```

> publisher 呼び出しの実装詳細（`assemble_updated_races` の引数や `trigger` 文字列）は実装時に `scripts/preview-realtime.py` の `upload_csvs` 周辺ロジックを正本として再確認する。**fun-site 側のディスパッチが trigger 値で分岐していないかも要確認**（現状 `"daily-bootstrap"` / `"realtime"` の 2 値しか出していないため新値は導入しない方針）。

**手動実行用の引数透過**：`workflow_dispatch` の `start_date` / `end_date` / `force_overwrite` 相当を、環境変数 `START_DATE` / `END_DATE` / `FORCE_OVERWRITE` で受けて各スクリプト呼び出しに反映する（範囲指定実行が必要なときのみ追加分岐で対応）。

### Step 2. `infra/Dockerfile` の小改修
現状 `ENTRYPOINT ["/usr/bin/tini", "--", "/app/run.sh"]` で `run.sh` を直接呼んでいる。複数 Job を 1 イメージで運用するため、

- `infra/run-daily-sync.sh` も `COPY --chmod=0755` で `/app/run-daily-sync.sh` に配置する。
- `ENTRYPOINT` は `["/usr/bin/tini", "--"]` だけにし、Job 側の `--command` で `/app/run.sh` か `/app/run-daily-sync.sh` を指定する。`preview-realtime` は `cloudbuild.yaml` に `--command=/app/run.sh` を追記して挙動を維持。

### Step 3. `infra/cloudbuild.yaml` を 2 Job 対応に拡張
末尾に `deploy-job-daily-sync` ステップを追加し、`gcloud run jobs deploy daily-sync` で create-or-update する。`preview-realtime` の deploy ステップに `--command=/app/run.sh` を補い、新規ステップでは `--command=/app/run-daily-sync.sh` を指定する。

`daily-sync` 用の差分パラメータ：
| 項目 | preview-realtime | daily-sync | 理由 |
|---|---|---|---|
| `--task-timeout` | `300s` | `3600s` | 実測 ~22 分。バッファ含めて 60 分。 |
| `--memory` | `1Gi` | `2Gi` | `build_index.py` が pandas + lightgbm + 偏差値計算で広めに使う。OOM の余裕を確保。 |
| `--cpu` | `1` | `2` | 22 分を縮める。並列化はしないが I/O・パース・特徴量計算の合計時間を圧縮。 |
| `--max-retries` | `0` | `0` | 部分失敗は run スクリプト側で握り潰すので Job リトライは不要。冪等保証は GitHub Actions 同様。 |
| `--set-env-vars` | (現状) | `GITHUB_REPO`, `GIT_BRANCH`, `BOATRACE_GCS_CSV_BUCKET=boatrace-realtime-data-boatrace-487212`, `BOATRACE_PUBSUB_TOPIC=projects/boatrace-487212/topics/fun-site-realtime-completed` | preview-realtime と同じ値を流し込む（Open Question 1 で「有効化」決定）。実際の publish は Step 1 末尾で行う。 |
| `--set-secrets` | `GITHUB_TOKEN` | `GITHUB_TOKEN` | 同じ Secret を流用。 |

`substitutions:` には `_JOB_NAME_DAILY=daily-sync` を追加。

### Step 4. Cloud Scheduler ジョブを作成
`gcloud scheduler jobs create http` で Cloud Run Jobs 起動 API を叩くトリガを作る。

```bash
gcloud scheduler jobs create http daily-sync-trigger \
  --project=boatrace-487212 \
  --location=asia-northeast1 \
  --schedule="30 7 * * *" \
  --time-zone="Asia/Tokyo" \
  --uri="https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/boatrace-487212/jobs/daily-sync:run" \
  --http-method=POST \
  --oauth-service-account-email=preview-realtime-runner@boatrace-487212.iam.gserviceaccount.com \
  --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform
```

JST `30 7 * * *` = `30 22 * * *` UTC（現行の cron と同じタイミング）。スケジューラを JST 指定にすることで `time-zone="Asia/Tokyo"` ネイティブに表現でき、運用上わかりやすい。

Service Account には事前に `roles/run.invoker` を Job リソースに対して付与する必要がある。

### Step 5. IAM / 権限まわりの調整
- `preview-realtime-runner` SA に `roles/run.invoker`（`daily-sync` Job スコープ）を追加 → Cloud Scheduler から起動するため。
- 同 SA に `roles/storage.objectAdmin`（GCS バケット `boatrace-realtime-data-boatrace-487212` スコープ）と `roles/pubsub.publisher`（`fun-site-realtime-completed` トピック）を確認。preview-realtime で既に付与済みのはず。
- Cloud Build SA には既に `roles/run.developer` + `iam.serviceAccountUser` が付与済み（preview-realtime 用）。同じ SA を deploy ステップで使うので追加権限は不要。
- **Open Question 3 対応**：`workflow_dispatch` UI は廃止するため、GHA → GCP の認証経路は不要。手動実行は `gcloud run jobs execute daily-sync --region=asia-northeast1 --update-env-vars=START_DATE=...,END_DATE=...,FORCE_OVERWRITE=true` をローカル / 開発者端末から実行する運用に統一する。
- 必要に応じて Cloud Logging のログ保持期間（既定 30 日）を確認。GitHub Actions の `actions/upload-artifact` の 30 日相当が欲しければそのまま。

### Step 6. `daily-sync.yml` を削除（Open Question 3）
`workflow_dispatch` UI は廃止する方針のため、GHA wrapper は作らない。Cloud Run Job が安定稼働した時点で `.github/workflows/daily-sync.yml` を完全削除する。

範囲指定の手動実行（旧 `workflow_dispatch` の `start_date` / `end_date` / `force_overwrite` 相当）が必要な場合は、開発者が gcloud CLI から直接叩く：

```bash
gcloud run jobs execute daily-sync \
  --region=asia-northeast1 \
  --project=boatrace-487212 \
  --update-env-vars=START_DATE=2026-05-01,END_DATE=2026-05-10,FORCE_OVERWRITE=true \
  --wait
```

`run-daily-sync.sh` 側で `START_DATE` / `END_DATE` / `FORCE_OVERWRITE` を読み、空でなければ各 Python スクリプトに `--date` / `--start-date` / `--force` を切り替えて渡す分岐を追加する（範囲指定は build_index.py 等の既存 CLI に合わせる）。これは恒常的に保持して、再バックフィル等の運用で使えるようにする。

### Step 7. ロールアウト手順（カナリア）
1. `infra/run-daily-sync.sh` と `infra/Dockerfile` 改修、`infra/cloudbuild.yaml` 拡張を PR で main にマージ。
2. Cloud Build トリガが発火 → 新イメージ build & 両 Job が deploy される。`preview-realtime` の挙動が変わっていないことを翌日 1 サイクルぶん監視。
3. `gcloud run jobs execute daily-sync --region=asia-northeast1 --project=boatrace-487212 --wait` で**手動 1 回実行**し、git の差分が想定通りに push されることを確認（`data/results/daily/...`、`data/estimate/index/...` などが現行 GHA 実行と同じ形で増えること）。
4. Cloud Scheduler ジョブを `--paused` で作成 → resume → 翌日 JST 07:30 の自動実行を確認。
5. 1 週間問題がなければ `daily-sync.yml` の `schedule:` を削除（`workflow_dispatch:` のみ残してフォールバック化）。さらに数週間後に YAML 自体を削除。

---

## 3. 既存 GHA との相違点・注意

| 観点 | GitHub Actions 現状 | Cloud Run Job 移行後 |
|---|---|---|
| **チェックアウト** | `actions/checkout@v4` で full clone + `git lfs pull` | partial+sparse clone（LFS は実態未使用なので問題なし） |
| **Python 依存** | 毎回 `pip install -r` | イメージにビルド時固定 → 起動時間短縮 |
| **失敗時の継続** | `continue-on-error: true` | bash の `run_step` ラッパーで集約終了コード |
| **ログ保管** | Actions Artifact 30 日 | Cloud Logging（既定 30 日）。詳細ログは `logs/` を GCS にコピーする hook を追加しても良い（任意） |
| **手動実行** | `workflow_dispatch` (3 inputs) | `gcloud run jobs execute daily-sync --update-env-vars=START_DATE=...,FORCE_OVERWRITE=true`（CLI から直接）。GHA UI は廃止。 |
| **同時実行** | GHA は同時起動可能 | Cloud Run Job はデフォルト 1 実行ずつ（`--parallelism=1 --tasks=1`）。重複起動の心配が減る。 |
| **コスト** | GHA Linux runner 22 分 × 30 日 ≒ 660 分/月 (無料枠内) | Cloud Run Job: 2 vCPU × 2 GiB × 22 分 × 30 日 ≒ 数十円/月。Cloud Scheduler は月 3 ジョブまで無料。実質誤差レベル。 |

---

## 4. 影響範囲とロールバック

### 影響範囲
- `infra/Dockerfile`, `infra/cloudbuild.yaml`, 新規 `infra/run-daily-sync.sh`。
- `preview-realtime` Job spec も `--command` 追加で 1 行差分が出る（挙動は不変）。
- リポジトリのデータ書き込み元が GHA runner から Cloud Run Job SA + GitHub PAT に変わる。コミット author は環境変数で揃える（`daily-sync-bot@users.noreply.github.com` 等）。

### ロールバック
- **PR #3 マージ前まで**：GHA `daily-sync.yml` が残っているため、Cloud Scheduler を `--paused` に戻すだけで GHA 側のみ稼働の状態に戻る。リスクは小さい。
- **PR #3 マージ後**：GHA YAML を git revert + Cloud Scheduler を `--paused` に戻す。完全ロールバックには 1 PR 分の手戻りが必要。重要な切り替えポイントなので PR #3 のマージは余裕のある時間帯に行う。
- Cloud Run Job のイメージや spec は両者から独立しているため、Job 自体を削除する必要はない（次回 Cloud Build で再 deploy される）。

---

## 5. Open Questions（決定済み）

| # | 決定 | 計画への反映 |
|---|---|---|
| 1 | **GCS ミラー / Pub/Sub publish を有効化** | Step 1 末尾に publisher 呼び出しを追加 / Step 3 で env vars 注入 / Step 5 で SA に GCS+Pub/Sub 権限を確認。`scripts/boatrace/gcs_publisher.py` を daily-sync スクリプト群は呼んでいないため、`run-daily-sync.sh` 内のインライン Python で `upload_csvs()` + `publish_realtime_completed(trigger="daily-bootstrap")` を 1 回叩く。 |
| 2 | **`monthly-weights.yml` は今回スコープ外** | 本計画では扱わない。完了後に同じ infra パターンで別タスク化。 |
| 3 | **`workflow_dispatch` UI も廃止** | Step 6 で `daily-sync.yml` を完全削除。手動実行が必要な場合は開発者が `gcloud run jobs execute` を直接叩く運用に統一。GHA→GCP の認証 (WIF / SA キー) は不要。 |
| 4 | **並列化しない（6 スクリプトを直列）** | Job spec は `--parallelism=1 --tasks=1`、`run-daily-sync.sh` も `run_step` で逐次。22 分の総時間は許容。 |

---

## 6. 推奨マイルストーン

1. **PR #1（infra）**：`infra/run-daily-sync.sh` 作成 + `Dockerfile` ENTRYPOINT 切替 + `cloudbuild.yaml` に `daily-sync` deploy ステップ追加。**この PR では `preview-realtime` の挙動が変わらないこと**をローカル/CI で確認。
2. **PR #2（IAM・Scheduler）**：手動 or Terraform で
   - `preview-realtime-runner` SA に `roles/run.invoker`（daily-sync スコープ）追加
   - Cloud Scheduler ジョブを `--paused` で作成
3. **動作確認**：`gcloud run jobs execute daily-sync --wait` で手動 1 回実行 → git に想定通りの差分が push され、GCS バケットに当日 5 ファミリの CSV がアップロードされ、Pub/Sub に `daily-bootstrap` メッセージが 1 件出ることを確認。
4. **並走期間**：Scheduler は `--paused` のまま、GHA `daily-sync.yml` の `schedule:` を残して 1 週間並走監視。Cloud Run Job は手動実行のみ。**git push の競合**（同じ日の同じファミリに両方が書く）が起きないよう、Scheduler resume の直前に GHA `schedule:` を削除する。
5. **PR #3（GHA 削除）**：`.github/workflows/daily-sync.yml` を削除（`schedule:` を含む全体）。同 PR に Cloud Scheduler resume を含めず、PR マージ後に手動で `gcloud scheduler jobs resume daily-sync-trigger` を実行（ワンウェイの切り替えポイントを明示）。
6. **Scheduler 起動**：resume → 翌日 JST 07:30 自動実行を観察。08:00 の preview-realtime 初回サイクルが当日 title CSV を読めていることを確認（fun-site 側でも検証）。
7. **+1 ヶ月**：問題なければ `monthly-weights.yml` の同パターン移行に着手（別タスク）。

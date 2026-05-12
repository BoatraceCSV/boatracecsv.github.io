# Boatrace data pipelines on Cloud Run Jobs

`scripts/preview-realtime.py` および `scripts/result.py` ほか daily-sync 系
スクリプトの起動を GitHub Actions schedule から Cloud Scheduler + Cloud Run Jobs
に移すためのインフラ一式です。

GitHub Actions の `schedule:` イベントは混雑時に間引かれ、5 分粒度では
事実上 1 時間に 1 回しか発火しないことがあるため、より精度の高いトリガとして
GCP の Cloud Scheduler から Cloud Run Jobs を直接叩く構成にしています。

現在 Cloud Run Jobs に載っている処理:

| Job 名 | スケジュール | 旧 GHA workflow | 概要 |
| --- | --- | --- | --- |
| `preview-realtime` | JST 08:00–22:55 / 5 分毎 | `.github/workflows/preview-realtime.yml` (`schedule:` 削除済み、`workflow_dispatch` のみフォールバック) | 直前バッチ + index 更新 + 結果取り込み |
| `daily-sync` | JST 07:30 / 1 日 1 回 | `.github/workflows/daily-sync.yml` (移行完了後に削除) | K-file 結果 + 当日 race_cards / recent_form / motor_stats / title 取得 + daily index 生成 |

## アーキテクチャ

```
Cloud Scheduler (Asia/Tokyo)
   │  preview-realtime-daytime: */5 8-22 * * *  → preview-realtime Job
   │  daily-sync:               30 7 * * *      → daily-sync Job
   │  HTTP POST + OIDC token (preview-realtime-invoker SA)
   ▼
Cloud Run Jobs (1 image, 2 jobs — switch by --command):
   ├─ preview-realtime  (--command=/app/run.sh,            cpu=1, mem=1Gi, timeout=300s)
   └─ daily-sync        (--command=/app/run-daily-sync.sh, cpu=2, mem=2Gi, timeout=3600s)
   │
   ▼  (Runner SA: preview-realtime-runner)
git clone --depth 1 --filter=blob:none + sparse-checkout  (PAT in Secret Manager)
   │
   ▼
preview-realtime: python scripts/preview-realtime.py
daily-sync:       6 scripts (result, race-card, recent-form, motor-stats,
                  race-title, build_index) + commit + GCS publish
   │   ├─ git commit && git push origin main   (boatrace.git_operations)
   │   ├─ ★ GCS mirror upload (boatrace.gcs_publisher.upload_csvs)
   │   │     gs://${BOATRACE_GCS_CSV_BUCKET}/data/{programs/title,programs/race_cards,
   │   │                                            previews/stt,estimate/index,results/realtime}/...
   │   └─ ★ Pub/Sub publish (boatrace.gcs_publisher.publish_realtime_completed)
   │         topic: ${BOATRACE_PUBSUB_TOPIC} (e.g. fun-site-realtime-completed)
   │         trigger: "daily-bootstrap" (daily-sync) / "realtime" (preview-realtime)
   ▼
fun-site が Eventarc 経由で Cloud Run Job として起動 → Astro 再ビルド → Cloud Storage 配信
```

* GCP Project: `boatrace-487212` (Project Number: `530399381543`)
* Region: `asia-northeast1`
* GitHub repo: `BoatraceCSV/boatracecsv.github.io`
* CSV mirror bucket: `boatrace-realtime-data-boatrace-487212` (fun-site/infra で provision)
* Pub/Sub topic: `projects/boatrace-487212/topics/fun-site-realtime-completed`
  (fun-site/infra で provision)

## ファイル構成

| ファイル | 役割 |
| --- | --- |
| `infra/Dockerfile` | Python 3.11-slim ベースの実行イメージ (preview-realtime / daily-sync 共用) |
| `infra/run.sh` | preview-realtime Job のエントリポイント (clone → sparse-checkout → python 実行) |
| `infra/run-daily-sync.sh` | daily-sync Job のエントリポイント (clone → sparse-checkout → 6 スクリプト直列 → commit → GCS publish) |
| `infra/cloudbuild.yaml` | Cloud Build パイプライン (build → push → 両 job 更新) |
| `infra/.dockerignore` | ビルドコンテキスト最小化 |
| `infra/daily-sync-migration-plan.md` | daily-sync を GHA から Cloud Run に移行する計画書 (移行完了後に削除可) |

### sparse-checkout 対象 (preview-realtime / `run.sh`)

Cloud Run Job の 1 GiB メモリ制約のためフルクローンせず、`preview-realtime.py`
が実際に読み書きする領域だけを cone-mode sparse-checkout で取得します。
スクリプト側で参照ファイルが増えたら `run.sh` の `git sparse-checkout set`
リストを忘れずに拡張してください(さもないと `index_csv_missing` 等のログを
吐いて該当処理が静かにスキップされます)。

| 取得対象 | 用途 |
| --- | --- |
| `scripts/` | preview-realtime.py / build_index.py / boatrace パッケージ |
| `.boatrace/` | 実行時設定 (load_config) |
| `data/estimate/stadium/` | win_rate.csv, sui_params.csv, index_weights/*.csv |
| `data/estimate/index/<YYYY/MM>/` | 当日 index CSV(直前バッチで一部レースを更新) |
| `data/programs/recent_national/<YYYY/MM>/` | 全国近況5節 |
| `data/programs/recent_local/<YYYY/MM>/` | 当地近況5節 |
| `data/programs/motor_stats/<YYYY/MM>/` + `<前月>/` | モーター期成績(前月分は7日fallback用) |
| `data/previews/{tkz,stt,sui,original_exhibition}/<YYYY/MM>/` | 直前バッチの追記対象 |
| `data/results/realtime/<YYYY/MM>/` | bc_rs1_2 由来の realtime 結果 CSV(締切後の追記対象)。cone 外だと git add が無視され永続化されない |

### sparse-checkout 対象 (daily-sync / `run-daily-sync.sh`)

daily-sync は preview-realtime とは別系統の入出力を扱うため、`run-daily-sync.sh`
側で別の sparse-checkout リストを持っています(`data/previews/` や
`data/results/realtime/` は不要、代わりに `data/results/daily/` や
`data/programs/race_cards/` 等が必要)。

| 取得対象 | 用途 |
| --- | --- |
| `scripts/` | result.py / race-card.py / recent-form.py / motor-stats.py / race-title.py / build_index.py / boatrace パッケージ |
| `.boatrace/` | 実行時設定 (load_config) |
| `data/estimate/stadium/` | win_rate.csv, sui_params.csv, index_weights/*.csv (build_index 入力) |
| `data/estimate/index/<YYYY/MM>/` | build_index --mode daily の出力先 (commit 対象) |
| `data/results/daily/<YYYY/MM>/` | result.py (K-file) の出力先 |
| `data/programs/race_cards/<YYYY/MM>/` | race-card.py の出力先 (GCS ミラー対象) |
| `data/programs/recent_national/<YYYY/MM>/` | recent-form.py の出力先 / build_index 特徴量 |
| `data/programs/recent_local/<YYYY/MM>/` | recent-form.py の出力先 / build_index 特徴量 |
| `data/programs/motor_stats/<YYYY/MM>/` + `<前月>/` | motor-stats.py の出力先 (前月分は build_index の 7 日 fallback 用) |
| `data/programs/title/<YYYY/MM>/` | race-title.py の出力先 (GCS ミラー対象) |

## ワンタイム セットアップ

以下は初回のみ実行する `gcloud` コマンド集です。Cloud Shell か、
`gcloud auth login` 済みのローカル端末から実行してください。

### 0. 共通変数

```bash
export PROJECT_ID=boatrace-487212
export PROJECT_NUMBER=530399381543
export REGION=asia-northeast1
export AR_REPO=containers
export IMAGE=preview-realtime
export JOB_NAME=preview-realtime
export RUNNER_SA=preview-realtime-runner
export INVOKER_SA=preview-realtime-invoker
export SECRET_NAME=github-token
export GITHUB_REPO=BoatraceCSV/boatracecsv.github.io

gcloud config set project "$PROJECT_ID"
```

### 1. API 有効化

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  iam.googleapis.com \
  logging.googleapis.com
```

### 2. Artifact Registry リポジトリ

```bash
gcloud artifacts repositories create "$AR_REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --description="Container images for boatrace automation"
```

### 3. サービスアカウント作成

```bash
# Job 実行用 (ワークロード ID)
gcloud iam service-accounts create "$RUNNER_SA" \
  --display-name="preview-realtime Cloud Run Job runner"

# Scheduler が Job を叩くための ID
gcloud iam service-accounts create "$INVOKER_SA" \
  --display-name="Cloud Scheduler invoker for preview-realtime"
```

### 4. GitHub PAT を Secret Manager に登録

GitHub 側で fine-grained PAT を発行します。

* Repository access: `BoatraceCSV/boatracecsv.github.io` のみ
* Permissions: `Contents` = Read and write
* 推奨有効期限: 90 日 (期限前にローテート)

発行したトークンを貼り付け:

```bash
printf '%s' 'ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx' | \
  gcloud secrets create "$SECRET_NAME" \
    --replication-policy=automatic \
    --data-file=-

# Job 実行 SA に読み取り権限
gcloud secrets add-iam-policy-binding "$SECRET_NAME" \
  --member="serviceAccount:${RUNNER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role=roles/secretmanager.secretAccessor
```

> **更新時:** `gcloud secrets versions add "$SECRET_NAME" --data-file=-`
> Cloud Run Job 側で `--update-secrets=GITHUB_TOKEN=${SECRET_NAME}:latest`
> としていれば自動で最新版が参照されます。

### 4-b. fun-site への CSV ミラーと Pub/Sub publish 用の権限・環境変数

`scripts/boatrace/gcs_publisher.py` が GCS 書込と Pub/Sub publish を行う。
バケットと topic 自体は fun-site/infra で `terraform apply` 済みである前提。

環境変数 `BOATRACE_GCS_CSV_BUCKET` / `BOATRACE_PUBSUB_TOPIC` は
`infra/cloudbuild.yaml` の `substitutions` (`_GCS_CSV_BUCKET` / `_PUBSUB_TOPIC`)
として宣言してあり、`deploy-job` ステップの `--set-env-vars` で毎ビルド注入される。
そのため**手動で `gcloud run jobs update --update-env-vars=...` を実行する必要はない** (値を
変えたい場合は `cloudbuild.yaml` の substitutions を書き換えて再ビルド)。

> `gcloud run jobs deploy` の `--set-env-vars` は**列挙したキーで全置換**するため、
> 過去にこの README では手動 `--update-env-vars` を案内していた。それを毎回手で
> 入れ直す運用になっていたのを cloudbuild.yaml に取り込んで自動化した経緯。
> 既存の Job に `--update-env-vars` で個別追加した値も次回ビルドで上書きされるので、
> 値を変えたい場合は必ず `cloudbuild.yaml` 側を編集すること。

> Runner SA への IAM ロール (`roles/storage.objectAdmin` on the bucket と
> `roles/pubsub.publisher` on the topic) は **fun-site/infra/realtime-pipeline.tf** の
> `google_storage_bucket_iam_member.preview_realtime_csv_writer` /
> `google_pubsub_topic_iam_member.preview_realtime_publisher` で付与される。
> 二重管理を避けるため、本リポジトリ側では IAM 付与の `gcloud` を実行しない。

未設定時は `gcs_publisher` が `gcs_upload_skipped reason=no_bucket_configured` 等の
ログを出して no-op になるため、段階的ロールアウト（コードだけ先に main にマージし、
本番側で環境変数を後から付与）が可能。

### 5. Runner SA / Cloud Build SA に必要な IAM ロール

```bash
RUNNER="serviceAccount:${RUNNER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

# --- 5-a. Runner SA ---
# Cloud Logging への書き込み
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="$RUNNER" \
  --role=roles/logging.logWriter

# --- 5-b. Cloud Build のデフォルト SA ---
# cloudbuild.yaml の deploy-job ステップで Cloud Run Job を作成/更新するため、
# Cloud Build SA に下記の権限が必要。これが無いと
# "build step ... gcr.io/google.com/cloudsdktool/cloud-sdk:slim failed:
#  step exited with non-zero status: 1" でコケる典型ケース。
CB_SA="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
# 旧プロジェクトでは下記のレガシー SA がデフォルト:
#   ${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com
# `gcloud builds list` の "createdBy" でどちらが実際に使われているか確認可。

# Cloud Run Jobs を deploy/update する権限
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="$CB_SA" \
  --role=roles/run.developer

# Job に Runner SA を attach する権限 (Service Account User)
gcloud iam service-accounts add-iam-policy-binding \
  "${RUNNER_SA}@${PROJECT_ID}.iam.gserviceaccount.com" \
  --member="$CB_SA" \
  --role=roles/iam.serviceAccountUser
```

### 6. 初回イメージビルド (Job も同時に作成される)

`cloudbuild.yaml` は `gcloud run jobs deploy` を使っているため、Job が
存在しなければ作成、存在すれば更新します。リポジトリ ルートで実行:

```bash
gcloud builds submit \
  --config infra/cloudbuild.yaml \
  --substitutions=SHORT_SHA=$(git rev-parse --short HEAD) \
  --project "$PROJECT_ID"
```

build 完了後、Job が出来ていることを確認:

```bash
gcloud run jobs describe "$JOB_NAME" --region="$REGION" --format='value(name,template.template.containers[0].image)'
```

### 7. Scheduler から Job を起動できるよう IAM 設定

```bash
INVOKER="serviceAccount:${INVOKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run jobs add-iam-policy-binding "$JOB_NAME" \
  --region="$REGION" \
  --member="$INVOKER" \
  --role=roles/run.invoker
```

### 8. Cloud Scheduler 登録 (JST 08:00〜22:59 を 5 分毎)

`preview-realtime-daytime` 1 本を JST 08:00 起点・5 分毎で登録します
(過去には JST 08:30 系列を別 Scheduler `preview-realtime-morning` として
切っていましたが、開催日朝の `getHoldingList2` で扱える時刻が前倒し
された関係で 08:00 起点に統合し、`preview-realtime-morning` は削除済み)。

```bash
JOB_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"
INVOKER_EMAIL="${INVOKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

# JST 08:00, 08:05, ..., 22:55
gcloud scheduler jobs create http preview-realtime-daytime \
  --location="$REGION" \
  --schedule="*/5 8-22 * * *" \
  --time-zone="Asia/Tokyo" \
  --uri="$JOB_URI" \
  --http-method=POST \
  --oauth-service-account-email="$INVOKER_EMAIL" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --attempt-deadline=60s \
  --description="Preview realtime — JST 08:00-22:59"
```

> `attempt-deadline` は Scheduler が `jobs:run` API のレスポンスを待つ時間
> です。Cloud Run Jobs の `:run` は実行をキックしてすぐ返るので 60 秒で十分。

### 9. 動作確認

```bash
# 手動キック
gcloud run jobs execute "$JOB_NAME" --region="$REGION" --wait

# 直近の実行ログ
gcloud beta run jobs executions list --job="$JOB_NAME" --region="$REGION" --limit=5
gcloud beta run jobs executions describe <EXECUTION_NAME> --region="$REGION"

# Scheduler を即座に発火
gcloud scheduler jobs run preview-realtime-daytime --location="$REGION"

# Cloud Logging で stdout/stderr を見る
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="preview-realtime"' \
  --limit=50 --format='value(timestamp,textPayload)'
```

## daily-sync の追加セットアップ

`preview-realtime` が稼働済みである前提。daily-sync Job は同じイメージ・
同じ Runner SA / Invoker SA / Cloud Build パイプラインを再利用する設計
なので、新規に必要なのは **(a) Job への invoker 権限付与** と
**(b) Cloud Scheduler 1 本** だけ。

### 0. 前提

- `infra/cloudbuild.yaml` に `deploy-job-daily-sync` ステップが入った
  状態で 1 回ビルドが走り、`gcloud run jobs describe daily-sync` で
  Job が存在することを確認済み(無ければ
  `gcloud builds submit --config infra/cloudbuild.yaml --substitutions=SHORT_SHA=$(git rev-parse --short HEAD)`
  を実行して作る)。
- 共通変数(§0)は読み込み済み。
- 旧 `.github/workflows/daily-sync.yml` の `schedule:` はまだ削除していない
  (Cloud Scheduler を `--paused` で作って動作確認 → resume 直前に GHA 側
  `schedule:` を削除する、という順序で進める)。

### 1. Invoker SA に daily-sync Job 起動権限を付与

```bash
INVOKER_EMAIL="${INVOKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run jobs add-iam-policy-binding daily-sync \
  --region="$REGION" \
  --member="serviceAccount:${INVOKER_EMAIL}" \
  --role=roles/run.invoker
```

> Runner SA (`preview-realtime-runner`) の Secret / Logging / GCS / Pub/Sub
> 権限は preview-realtime 用に既に付与済みなので、daily-sync Job も
> そのまま動く。新規付与は不要。

### 2. Cloud Scheduler ジョブを `--paused` で作成

```bash
JOB_URI_DAILY="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/daily-sync:run"

gcloud scheduler jobs create http daily-sync \
  --location="$REGION" \
  --schedule="30 7 * * *" \
  --time-zone="Asia/Tokyo" \
  --uri="$JOB_URI_DAILY" \
  --http-method=POST \
  --oauth-service-account-email="$INVOKER_EMAIL" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --attempt-deadline=60s \
  --description="Daily boatrace data sync — JST 07:30"

# 動作確認まで pause しておく
gcloud scheduler jobs pause daily-sync --location="$REGION"
```

> JST 07:30 = UTC 22:30。`--time-zone=Asia/Tokyo` 指定で cron は
> JST ネイティブに `30 7 * * *` と書ける。旧 GHA の `30 22 * * *` (UTC) と
> 同じタイミング。

### 3. 手動実行で動作確認

```bash
# 1 サイクル手動実行 (実測 ~22 分待機)
gcloud run jobs execute daily-sync --region="$REGION" --wait

# 直近の実行ログ
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="daily-sync"' \
  --limit=80 --freshness=30m \
  --format='value(timestamp,textPayload)'

# git に commit が積まれたか確認
git -C /tmp/boatracecsv.github.io log --oneline -10  # クローンしてある場合

# GCS ミラーが届いたか
gsutil ls "gs://${BOATRACE_GCS_CSV_BUCKET}/data/programs/title/$(TZ=Asia/Tokyo date +'%Y/%m')/"
gsutil ls "gs://${BOATRACE_GCS_CSV_BUCKET}/data/programs/race_cards/$(TZ=Asia/Tokyo date +'%Y/%m')/"

# Pub/Sub 配信履歴 (fun-site 側 Eventarc trigger 起動)
# fun-site のビルド履歴を確認するか、Pub/Sub topic の "Messages" メトリクスを見る
```

期待される stdout の最後付近 (run-daily-sync.sh のログ):

```
[run-daily-sync ...] STEP OK: build-index
[run-daily-sync ...] STEP OK: commit-index
[publisher] uploaded=N updated_races=M trigger=daily-bootstrap
[publisher] published message_id=...
[run-daily-sync ...] STEP OK: gcs-publish
[run-daily-sync ...] Done (aggregate exit=0)
```

### 4. 旧 GHA の停止と Scheduler 再開 (順序重要)

両方が同じ日に同じ CSV ファミリへ書くと git push 競合や CSV ダブリの
原因になるため、**先に GHA 側 schedule を停止**してから Cloud Scheduler を
resume する。

```bash
# (a) GHA 側 schedule を削除する PR を merge (.github/workflows/daily-sync.yml 削除)
#     → PR #3 として別途実施

# (b) Cloud Scheduler を resume
gcloud scheduler jobs resume daily-sync --location="$REGION"

# (c) 翌朝 JST 07:30 の自動実行を観察
gcloud scheduler jobs describe daily-sync --location="$REGION" \
  --format='value(state,scheduleTime,lastAttemptTime)'
```

### 5. ロールバック

問題が出た場合:

```bash
# 1) Cloud Scheduler を一時停止 (即時)
gcloud scheduler jobs pause daily-sync --location="$REGION"

# 2) 必要なら GHA 側 schedule を git revert で復活
#    → PR #3 を revert する PR を merge
```

## 更新手順 (コード/設定の変更を反映)

`scripts/preview-realtime.py` や `scripts/build_index.py`、`infra/Dockerfile`、
`infra/run.sh` などを修正した場合の反映フロー。

### 何が必要かを判断

| 変更したファイル | 必要なアクション |
| --- | --- |
| `scripts/*.py` (Python ソース) | イメージ再ビルド + 両 Job 更新 |
| `scripts/requirements.txt` (新規依存追加) | イメージ再ビルド + 両 Job 更新 |
| `infra/Dockerfile` | イメージ再ビルド + 両 Job 更新 |
| `infra/run.sh` (sparse-checkout 等) | イメージ再ビルド + preview-realtime Job 更新 |
| `infra/run-daily-sync.sh` (sparse-checkout 等) | イメージ再ビルド + daily-sync Job 更新 |
| `infra/cloudbuild.yaml` | 再ビルドだけで反映される(自身が実行される) |
| `data/**` のみ | 不要(Job は実行時に最新 main を pull する) |
| `.github/workflows/preview-realtime.yml` | 不要(Cloud Run 経路は GitHub Actions に依存しない) |

### 1. main に push して自動反映 (CI トリガが有効な場合)

[後段](#ci-連携-任意) で Cloud Build トリガを `infra/**,scripts/**` 監視で
作成済みなら、`git push origin main` だけで自動的にイメージ再ビルドと
Cloud Run Job のロールアウトが走ります。完了は GCP コンソールの
Cloud Build 履歴 / Cloud Run Jobs リビジョンで確認できます。

### 2. 即時反映したいとき (手動ビルド)

```bash
gcloud builds submit \
  --config infra/cloudbuild.yaml \
  --substitutions=SHORT_SHA=$(git rev-parse --short HEAD) \
  --project "$PROJECT_ID"
```

`cloudbuild.yaml` の `deploy-job` ステップが `gcloud run jobs deploy` を
実行するため、Job の `containers[0].image` が新タグ
(`${SHORT_SHA}`) に置き換わります。Cloud Scheduler 側は Job 名で
参照しており、常に最新リビジョンを起動するので変更不要です。

> ビルドは典型的に 2〜4 分。Cloud Build SA に `roles/run.developer`
> と Runner SA への `roles/iam.serviceAccountUser` が要ります(§5-b)。

### 3. 反映確認

```bash
# Job が指している現在のイメージタグを確認
gcloud run jobs describe "$JOB_NAME" \
  --region="$REGION" \
  --format='value(template.template.containers[0].image)'

# 手動キックして 1 サイクル動作確認 (--wait で完了まで待機)
gcloud run jobs execute "$JOB_NAME" --region="$REGION" --wait

# 直近の実行ログ
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="preview-realtime"' \
  --limit=30 --freshness=10m --format='value(timestamp,textPayload)'
```

`run.sh` の sparse-checkout 拡張 (data/estimate/index/, data/estimate/stadium/,
data/programs/recent_*/, data/programs/motor_stats/) を伴う変更後は、
特にログに `preview_realtime_index_skipped reason=index_csv_missing`
が出ていないか確認します。出ている場合は当日の `data/estimate/index/` が
`daily-sync` Job (Cloud Run; JST 07:30) で生成済みかをチェック (詳細は
「## トラブルシュート → preview_realtime_index_skipped」参照)。

### 4. イメージ ロールバック (前リビジョンに戻す)

push したコードに問題があった場合は、Cloud Run Job の image を
直前の SHA に切り戻すだけで済みます。Scheduler は変更不要。

```bash
# 利用可能なタグ一覧 (Artifact Registry)
gcloud artifacts docker images list \
  "${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE}" \
  --include-tags --limit=20 \
  --format='table(tags,createTime,version)' \
  --sort-by=~createTime

# 指定タグへロールバック
PREV_SHA=<前リビジョンの SHORT_SHA>
gcloud run jobs deploy "$JOB_NAME" \
  --region="$REGION" \
  --image="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO}/${IMAGE}:${PREV_SHA}"
```

`:latest` タグは現在のビルドを指したままなので、再ビルド前提なら
`gcloud builds submit ... --substitutions=SHORT_SHA=$PREV_SHA` で
過去コミット時点のイメージを再構築する手もあります。

### 5. 完全停止 (Scheduler 一時停止)

更新で問題が出ていて切り戻しの暇もない場合は Scheduler を停止して
呼び出し自体を止めます(後述の「ロールバック」セクション参照)。

## CI 連携 (任意)

`main` への push で Cloud Build トリガを発火させると、infra/ や
scripts/ の変更後にイメージが自動更新されます。

```bash
gcloud builds triggers create github \
  --name=preview-realtime-build \
  --region="$REGION" \
  --repo-name=boatracecsv.github.io \
  --repo-owner=BoatraceCSV \
  --branch-pattern='^main$' \
  --build-config=infra/cloudbuild.yaml \
  --included-files='infra/**,scripts/**'
```

最初のトリガ作成時に GitHub への OAuth 連携を求められます。

## 運用メモ

* **PAT ローテート**: 期限が切れる前に `gcloud secrets versions add` で
  新バージョンを追加するだけ。Job 側は `:latest` 参照なので再デプロイ不要。
  両 Job が同じ Secret を参照しているため 1 回の更新で済む。
* **想定外の重複実行**: 両 Job とも `parallelism=1 tasks=1 max-retries=0` で動く
  ため同一 Scheduler 内の重複は無い。Scheduler は 2 本:
  `preview-realtime-daytime` (`*/5 8-22 * * *`) と `daily-sync` (`30 7 * * *`)。
  Python 側もレースコードで冪等化されており、CSV 上書きは MD5 dedup される。
* **両 Job の時間帯重なり**: daily-sync は JST 07:30 起動・実測 ~22 分なので
  通常は ~07:52 に終了。preview-realtime の初回 (08:00) より前に完了する想定。
  万一 daily-sync が 08:00 を跨いだ場合、両 Job が同じ git remote へ
  push しようとして競合する可能性があるが、`boatrace.git_operations` の
  rebase ロジックで吸収される(競合しても致命傷にはならない)。
* **タイムアウト**: preview-realtime は 1 実行 5 分(GitHub Actions と同じ)。
  daily-sync は 1 実行 60 分(実測 22 分 + バッファ)。Boatrace 側 API が
  詰まり超過した場合はその回を捨てて次の Scheduler に任せる方針。
* **コスト概算**:
  - preview-realtime: 180 回/日 × 30 日 ≒ 5,400 実行/月、平均 30 秒
  - daily-sync: 30 回/月、~22 分 × 2 vCPU × 2 GiB
  - Cloud Run Jobs / Scheduler とも合算で実質無料枠 + 数十円/月程度。
    Scheduler は 3 ジョブまで無料(現在 2 本)。
* **GitHub Actions 側のフォールバック**: `.github/workflows/preview-realtime.yml`
  は `workflow_dispatch` のみ残してあるので、Cloud Run 側に障害が出たら
  GitHub UI から手動で 1 回起動できる。
  daily-sync は移行完了後 `.github/workflows/daily-sync.yml` を削除予定
  (Open Question 3 で `workflow_dispatch` UI も廃止と決定)。
  代替として `gcloud run jobs execute daily-sync --region="$REGION"
  --update-env-vars=RUN_DATE=2026-05-01 --wait` で開発者端末から起動する。

## ロールバック

Scheduler を一時停止すれば Cloud Run 側は完全停止します:

```bash
# preview-realtime
gcloud scheduler jobs pause preview-realtime-daytime --location="$REGION"

# daily-sync
gcloud scheduler jobs pause daily-sync --location="$REGION"
```

GitHub Actions 側の schedule トリガを復活させたい場合:

- preview-realtime: `.github/workflows/preview-realtime.yml` の `on.schedule`
  ブロックを git revert で復活
- daily-sync: PR #3 で削除済みの `.github/workflows/daily-sync.yml` を
  git revert で復活 (PR #3 マージ前なら schedule ブロックを git revert するだけ)

## トラブルシュート

### Cloud Build が `deploy-job` ステップ (build step 3) で失敗する

```
BUILD FAILURE: Build step failure: build step 3
"gcr.io/google.com/cloudsdktool/cloud-sdk:slim" failed:
step exited with non-zero status: 1
```

実エラーは Cloud Build のログ末尾にあります:

```bash
BUILD_ID=$(gcloud builds list --limit=1 --project="$PROJECT_ID" --format='value(id)')
gcloud builds log "$BUILD_ID" --project="$PROJECT_ID" | tail -60
```

主な原因:

1. **Cloud Build SA に `roles/run.developer` が無い**
   → ログに `Permission 'run.jobs.create' denied` 等が出る。§5-b を実行。
2. **Cloud Build SA に Runner SA への `roles/iam.serviceAccountUser` が無い**
   → ログに `Permission 'iam.serviceaccounts.actAs' denied on service account preview-realtime-runner@...`
   → §5-b の serviceAccountUser を実行。
3. **Runner SA / Secret / AR リポジトリがまだ無い**
   → §2〜§4 が完了していない。順序通り実行する。
4. **Cloud Build SA がプロジェクト切替後の新しい方ではなく旧 SA を使っている**
   → `gcloud builds list --format='table(id,createTime,createdBy)'` で
   実際の起動 SA を確認し、その SA に対して §5-b を実行。

### `gcloud run jobs execute` が `PERMISSION_DENIED` を返す

Runner SA の Secret アクセス権 (§4 末尾の `add-iam-policy-binding`) が
未付与。再度実行する。

### Cloud Run の実行ログに `Authentication failed` (git push)

PAT が失効 / 権限不足。GitHub で fine-grained PAT を再発行し
`gcloud secrets versions add github-token --data-file=-` で更新。
Job 側は `:latest` 参照なので再デプロイ不要。

### `preview_realtime_index_skipped reason=index_csv_missing` がログに出る

`scripts/preview-realtime.py` が `data/estimate/index/YYYY/MM/DD.csv` を更新しよう
として、ファイルが存在しない/sparse-checkout されていない時のメッセージ。
原因は2系統:

1. **当日の daily-batch index がまだ生成されていない**
   `daily-sync` Job (Cloud Run; JST 07:30) の `build-index` ステップが
   当日に成功しているはず。失敗していると当日の index CSV が main に
   存在しない。直近の daily-sync ログを確認:
   ```bash
   gcloud logging read \
     'resource.type="cloud_run_job" AND resource.labels.job_name="daily-sync"' \
     --limit=80 --freshness=24h --format='value(timestamp,textPayload)'
   ```
   移行完了前は GitHub Actions 側 `.github/workflows/daily-sync.yml` の
   Run 履歴を確認(PR #3 マージ後は GHA workflow は存在しない)。
2. **`run.sh` の sparse-checkout に `data/estimate/index/${TODAY_YM}` が無い**
   イメージが古いとこのパスが checkout されず、ファイルが存在するのに
   ローカルでは見えない状態になる。「## 更新手順」に従ってイメージを
   再ビルド+ Job を更新。

### daily-sync の特定ステップだけが失敗している

`run-daily-sync.sh` は `if: always() / continue-on-error: true` 相当の
セマンティクスを bash の `run_step` ラッパーで再現しており、個別ステップの
失敗は `STEP FAILED (rc=N): <label>` ログとして残り、最終的な exit code
は 1 になる(他のステップは続行)。Cloud Logging で grep:

```bash
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="daily-sync" AND textPayload:"STEP FAILED"' \
  --limit=20 --freshness=2d --format='value(timestamp,textPayload)'
```

典型的に boatcast.jp の朝の更新が間に合わない場合は `race-card` /
`recent-form` / `motor-stats` / `race-title` あたりが失敗する。
翌日 07:30 の再実行で吸収される設計なので、単発であれば対応不要。
連日同じステップが失敗する場合はスクリプトのリグレッションを疑う。

### `compute_features_for_day` が NaN だらけ / 想定より少ない結果

`run.sh` の sparse-checkout に必要な data ディレクトリ
(`data/estimate/stadium/`, `data/programs/recent_*/`, `data/programs/motor_stats/` 等)
が漏れている可能性。スクリプト側で新たに参照ファイルを増やしたら
`run.sh` も同時に更新し、イメージを再ビルドする必要がある。

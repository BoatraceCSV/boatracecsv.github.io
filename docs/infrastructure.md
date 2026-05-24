# Infrastructure(Cloud Run Jobs)

`scripts/preview-realtime.py` ほか daily-sync 系スクリプトの起動を
GitHub Actions schedule から Cloud Scheduler + Cloud Run Jobs に移すための
インフラ一式です。

GitHub Actions の `schedule:` イベントは混雑時に間引かれ、5 分粒度では
事実上 1 時間に 1 回しか発火しないことがあるため、より精度の高いトリガとして
GCP の Cloud Scheduler から Cloud Run Jobs を直接叩く構成にしています。

現在 Cloud Run Jobs に載っている処理:

| Job 名 | スケジュール | 旧 GHA workflow | 概要 |
| --- | --- | --- | --- |
| `preview-realtime` | JST 08:00–22:55 / 5 分毎 | `.github/workflows/preview-realtime.yml` (`schedule:` 削除済み、`workflow_dispatch` のみフォールバック) | 直前バッチ + index 更新 + 結果取り込み |
| `daily-sync` | JST 07:30 / 1 日 1 回 | `.github/workflows/daily-sync.yml` (移行完了後に削除) | K-file 結果 + 当日 race_cards / recent_form / motor_stats / title 取得 + daily index 生成 |
| `monthly-weights` | JST 06:00 / 毎月 1 日 | `.github/workflows/monthly-weights.yml` (移行完了後に削除) | active な全予想者について、直近 6 ヶ月の特徴量から場ごとの n_components 要素重みを再計算し `data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv` を更新 (daily-sync 直前に走らせ、境界日の daily/realtime index を同一 weights で計算) |

## アーキテクチャ

```
Cloud Scheduler (Asia/Tokyo)
   │  preview-realtime-daytime: */5 8-22 * * *   → preview-realtime Job
   │  daily-sync:               30 7 * * *       → daily-sync Job
   │  monthly-weights:          0 6 1 * *        → monthly-weights Job
   │  HTTP POST + OIDC token (preview-realtime-invoker SA)
   ▼
Cloud Run Jobs (1 image, 3 jobs — switch by --command):
   ├─ preview-realtime  (--command=/app/run.sh,                 cpu=1, mem=1Gi, timeout=300s)
   ├─ daily-sync        (--command=/app/run-daily-sync.sh,      cpu=2, mem=2Gi, timeout=3600s)
   └─ monthly-weights   (--command=/app/run-monthly-weights.sh, cpu=2, mem=2Gi, timeout=3600s)
   │
   ▼  (Runner SA: preview-realtime-runner)
git clone --depth 1 --filter=blob:none + sparse-checkout  (PAT in Secret Manager)
   │
   ▼
preview-realtime: python scripts/preview-realtime.py
daily-sync:       6 scripts (result, race-card, recent-form, motor-stats,
                  race-title, build_index) + commit + GCS publish
monthly-weights:  python scripts/build_weights.py --month YYYY-MM + commit
   │   ├─ git commit && git push origin main   (boatrace.git_operations / bash)
   │   ├─ ★ GCS mirror upload (boatrace.gcs_publisher.upload_csvs)        ← preview-realtime / daily-sync のみ
   │   │     gs://${BOATRACE_GCS_CSV_BUCKET}/data/{programs/title,programs/race_cards,
   │   │                                            previews/stt,estimate/{predictor_id},
   │   │                                            results/realtime,results/payouts}/...
   │   │     (active 予想者ごとに csv_type=index:{predictor_id} で 1 件ずつ mirror)
   │   └─ ★ Pub/Sub publish (boatrace.gcs_publisher.publish_realtime_completed) ← preview-realtime / daily-sync のみ
   │         topic: ${BOATRACE_PUBSUB_TOPIC} (e.g. fun-site-realtime-completed)
   │         trigger: "daily-bootstrap" (daily-sync) / "realtime" (preview-realtime)
   ▼
fun-site が Eventarc 経由で Cloud Run Job として起動 → Astro 再ビルド → Cloud Storage 配信
(monthly-weights は git push のみ。生成された weights CSV は build_index.py が
 リポジトリから読むため、fun-site への直接配信経路は持たない)
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
| [`../infra/Dockerfile`](../infra/Dockerfile) | Python 3.11-slim ベースの実行イメージ (preview-realtime / daily-sync / monthly-weights 共用) |
| [`../infra/run.sh`](../infra/run.sh) | preview-realtime Job のエントリポイント (clone → sparse-checkout → python 実行) |
| [`../infra/run-daily-sync.sh`](../infra/run-daily-sync.sh) | daily-sync Job のエントリポイント (clone → sparse-checkout → 6 スクリプト直列 → commit → GCS publish) |
| [`../infra/run-monthly-weights.sh`](../infra/run-monthly-weights.sh) | monthly-weights Job のエントリポイント (clone → sparse-checkout 8 ヶ月分 → build_weights.py → commit) |
| [`../infra/cloudbuild.yaml`](../infra/cloudbuild.yaml) | Cloud Build パイプライン (build → push → 3 job 更新) |
| [`../infra/.dockerignore`](../infra/.dockerignore) | ビルドコンテキスト最小化 |

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
| `data/estimate/stadium/` | win_rate.csv, sui_params.csv, weights/{predictor_id}/*.csv |
| `data/estimate/<predictor_id>/<YYYY/MM>/` | 当日 index CSV(直前バッチで一部レースを更新)。`ACTIVE_PREDICTORS` 配列でループ |
| `data/programs/recent_national/<YYYY/MM>/` | 全国近況5節 |
| `data/programs/recent_local/<YYYY/MM>/` | 当地近況5節 |
| `data/programs/motor_stats/<YYYY/MM>/` + `<前月>/` | モーター期成績(前月分は7日fallback用) |
| `data/previews/{tkz,stt,sui,original_exhibition}/<YYYY/MM>/` | 直前バッチの追記対象 |
| `data/results/realtime/<YYYY/MM>/` | bc_rs1_2 由来の realtime 結果 CSV(締切後の追記対象)。cone 外だと git add が無視され永続化されない |
| `data/results/payouts/<YYYY/MM>/` | bc_rs2 由来の払戻金 CSV(締切後の追記対象)。同じく cone 外だと git add が無視され永続化されない |

### sparse-checkout 対象 (daily-sync / `run-daily-sync.sh`)

daily-sync は preview-realtime とは別系統の入出力を扱うため、`run-daily-sync.sh`
側で別の sparse-checkout リストを持っています(`data/previews/` や
`data/results/realtime/` は不要、代わりに `data/programs/race_cards/` 等が必要)。

| 取得対象 | 用途 |
| --- | --- |
| `scripts/` | race-card.py / recent-form.py / motor-stats.py / race-title.py / build_index.py / boatrace パッケージ |
| `.boatrace/` | 実行時設定 (load_config) |
| `data/estimate/stadium/` | win_rate.csv, sui_params.csv, weights/{predictor_id}/*.csv (build_index 入力) |
| `data/estimate/<predictor_id>/<YYYY/MM>/` | build_index --mode daily --all-active の出力先 (各 active 予想者を `ACTIVE_PREDICTORS` 配列でループ。commit 対象) |
| `data/programs/race_cards/<YYYY/MM>/` | race-card.py の出力先 (GCS ミラー対象) |
| `data/programs/recent_national/<YYYY/MM>/` | recent-form.py の出力先 / build_index 特徴量 |
| `data/programs/recent_local/<YYYY/MM>/` | recent-form.py の出力先 / build_index 特徴量 |
| `data/programs/motor_stats/<YYYY/MM>/` + `<前月>/` | motor-stats.py の出力先 (前月分は build_index の 7 日 fallback 用) |
| `data/programs/title/<YYYY/MM>/` | race-title.py の出力先 (GCS ミラー対象) |

### sparse-checkout 対象 (monthly-weights / `run-monthly-weights.sh`)

monthly-weights は `build_weights.py` が**直近 6 ヶ月の全日**について
`compute_features_for_day(repo, day)` を呼ぶため、対象月 (target_month) +
過去 6 ヶ月 + motor_stats の 7 日 fallback 用にさらに 1 ヶ月、計 **8 ヶ月分**
の月単位ディレクトリを sparse-checkout する。月数の計算は
`run-monthly-weights.sh` の bash ループで自動生成される。

| 取得対象 | 用途 |
| --- | --- |
| `scripts/` | build_weights.py / boatrace パッケージ (index_features) |
| `.boatrace/` | 実行時設定 (load_config) |
| `data/estimate/stadium/` | win_rate.csv / sui_params.csv (build_weights 入力) + weights/{predictor_id}/ (出力先) |
| `data/results/realtime/<YM>/` × 8 | 着順 (target = 7 - 着順) |
| `data/programs/race_cards/<YM>/` × 8 | レース宇宙 (universe) |
| `data/programs/recent_national/<YM>/` × 8 | recent 特徴量 (`racer` 列) |
| `data/programs/recent_local/<YM>/` × 8 | recent 特徴量 (`racer` 列) |
| `data/programs/motor_stats/<YM>/` × 8 | motor 特徴量 (8 ヶ月目は月初 7 日 fallback 用) |
| `data/previews/sui/<YM>/` × 8 | 気象 (`weather` 特徴量) |
| `data/previews/tkz/<YM>/` × 8 | 展示タイム (`exhibit` 特徴量) |
| `data/previews/stt/<YM>/` × 8 | 進入コース (course 補正) |
| `data/previews/original_exhibition/<YM>/` × 8 | 展示値 1〜3 (`exhibit` 特徴量) |

> ⚠️ `data/previews/*` を入れ忘れると `_load_realtime_preview_by_code` が空 dict
> を返し、`exhibit` と `weather` 特徴量が全レースで NaN になる。すると
> `fit_one` の `dropna(subset=["waku","racer","exhibit","weather","着順"])` で
> 全行が落ちて n=0 FALLBACK が 24 場分発生する症状になるため、4 ファミリすべて
> 必須。

target_month を `TARGET_MONTH=2026-05` のように上書きすると、その月を起点と
した 8 ヶ月分が動的に cone に入る。月数を変えたい場合は
`run-monthly-weights.sh` の冒頭ループ `for i in 1 2 3 4 5 6 7` を編集する。

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
ログを出して no-op になるため、段階的ロールアウト(コードだけ先に main にマージし、
本番側で環境変数を後から付与)が可能。

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

## monthly-weights の追加セットアップ

`preview-realtime` / `daily-sync` が稼働済みである前提。monthly-weights Job も
同じイメージ・Runner SA / Invoker SA / Cloud Build パイプラインを再利用する
ので、新規に必要なのは **(a) Job への invoker 権限付与** と
**(b) Cloud Scheduler 1 本** だけ。

### 0. 前提

- `infra/cloudbuild.yaml` に `deploy-job-monthly-weights` ステップが入った
  状態で 1 回ビルドが走り、`gcloud run jobs describe monthly-weights` で
  Job が存在することを確認済み (無ければ
  `gcloud builds submit --config infra/cloudbuild.yaml --substitutions=SHORT_SHA=$(git rev-parse --short HEAD)`
  を実行して作る)。
- 共通変数 (§0) は読み込み済み。
- 旧 `.github/workflows/monthly-weights.yml` の `schedule:` はまだ削除して
  いない (Cloud Scheduler を `--paused` で作って動作確認 → resume 直前に
  GHA 側 `schedule:` を削除する、という順序で進める)。

### 1. Invoker SA に monthly-weights Job 起動権限を付与

```bash
INVOKER_EMAIL="${INVOKER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud run jobs add-iam-policy-binding monthly-weights \
  --region="$REGION" \
  --member="serviceAccount:${INVOKER_EMAIL}" \
  --role=roles/run.invoker
```

> Runner SA (`preview-realtime-runner`) の Secret / Logging 権限は
> preview-realtime 用に既に付与済み。monthly-weights は GCS / Pub/Sub publish
> を呼ばないため、これらの追加権限は不要。

### 2. Cloud Scheduler ジョブを `--paused` で作成

```bash
JOB_URI_MONTHLY="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/monthly-weights:run"

gcloud scheduler jobs create http monthly-weights \
  --location="$REGION" \
  --schedule="0 6 1 * *" \
  --time-zone="Asia/Tokyo" \
  --uri="$JOB_URI_MONTHLY" \
  --http-method=POST \
  --oauth-service-account-email="$INVOKER_EMAIL" \
  --oauth-token-scope="https://www.googleapis.com/auth/cloud-platform" \
  --attempt-deadline=60s \
  --description="Monthly index weights — JST 06:00 on the 1st of each month (before daily-sync at 07:30)"

# 動作確認まで pause しておく
gcloud scheduler jobs pause monthly-weights --location="$REGION"
```

> JST 06:00 は daily-sync (JST 07:30) の **前** に終わらせる狙いの時刻。
> 境界日 (毎月 1 日) の daily index と realtime index を同一 weights で
> 計算するための前倒し。
> 訓練ウィンドウから前月末日の K-file (daily-sync が 07:30 に取り込む) が
> 1 日抜けるトレードオフは許容済み。

### 3. 手動実行で動作確認

```bash
# 1 サイクル手動実行 (実測 N 分待機 — 初回ログから timeout を再評価する)
gcloud run jobs execute monthly-weights --region="$REGION" --wait

# 過去月の再計算 (backfill)
gcloud run jobs execute monthly-weights --region="$REGION" \
  --update-env-vars=TARGET_MONTH=2026-03 --wait

# 直近の実行ログ
gcloud logging read \
  'resource.type="cloud_run_job" AND resource.labels.job_name="monthly-weights"' \
  --limit=80 --freshness=1h \
  --format='value(timestamp,textPayload)'

# git に commit が積まれたか確認
gh api /repos/${GITHUB_REPO}/commits?path=data/estimate/stadium/weights --jq '.[0:3] | .[] | {sha, message: .commit.message}'

# 出力 CSV の整合確認 (旧 GHA の出力と diff — 入力データ + アルゴリズムが
# 同じなら原則一致するはず)
```

期待される stdout の最後付近 (`run-monthly-weights.sh` のログ):

```
[run-monthly-weights ...] Building monthly weights for 2026-05
... (build_weights.py の per-stadium fit ログ — stderr 経由)
  FeatureContext stats: race_cards=270 title=270 runs=~1350 period_starts=181
[run-monthly-weights ...] Push attempt 1/5
[run-monthly-weights ...] Push succeeded on attempt 1
[run-monthly-weights ...] Pushed weights for 2026-05
[run-monthly-weights ...] Done
```

差分が無かった場合は `Push attempt` 〜 `Pushed weights ...` のブロックが
無く、代わりに `No weights changes to commit for ...` が出る。

`Push attempt N/5` が複数回出る場合は `preview-realtime` の commit と
レースして fetch + rebase でリトライしている状態。N=5 まで使い切ったら
`ABORT: failed to push weights ...` で fail する(後述のトラブルシュート参照)。

### 4. 旧 GHA の停止と Scheduler 再開 (順序重要)

両方が同月の `data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv` を生成すると
無駄な走行 + git push 競合の原因になるため、**先に GHA 側 schedule を
停止**してから Cloud Scheduler を resume する。

```bash
# (a) GHA 側 schedule を削除する PR を merge (.github/workflows/monthly-weights.yml 削除)
#     → 別 PR として実施

# (b) Cloud Scheduler を resume
gcloud scheduler jobs resume monthly-weights --location="$REGION"

# (c) 翌月 1 日 JST 06:00 の自動実行を観察
gcloud scheduler jobs describe monthly-weights --location="$REGION" \
  --format='value(state,scheduleTime,lastAttemptTime)'
```

### 5. ロールバック

問題が出た場合:

```bash
# 1) Cloud Scheduler を一時停止 (即時)
gcloud scheduler jobs pause monthly-weights --location="$REGION"

# 2) 必要なら GHA 側 schedule を git revert で復活
```

monthly-weights の出力は active な予想者ごとの CSV
(`data/estimate/stadium/weights/{predictor_id}/YYYY-MM.csv`)
なので、誤って壊れた weights が push された場合は GitHub 側で当該コミットを
revert し、過去月の `gcloud run jobs execute monthly-weights --update-env-vars=TARGET_MONTH=...`
で再生成すれば復旧する。

## 更新手順 (コード/設定の変更を反映)

`scripts/preview-realtime.py` や `scripts/build_index.py`、`infra/Dockerfile`、
`infra/run.sh` などを修正した場合の反映フロー。

### 何が必要かを判断

| 変更したファイル | 必要なアクション |
| --- | --- |
| `scripts/*.py` (Python ソース) | イメージ再ビルド + 全 Job 更新 |
| `scripts/requirements.txt` (新規依存追加) | イメージ再ビルド + 全 Job 更新 |
| `infra/Dockerfile` | イメージ再ビルド + 全 Job 更新 |
| `infra/run.sh` (sparse-checkout 等) | イメージ再ビルド + preview-realtime Job 更新 |
| `infra/run-daily-sync.sh` (sparse-checkout 等) | イメージ再ビルド + daily-sync Job 更新 |
| `infra/run-monthly-weights.sh` (sparse-checkout 等) | イメージ再ビルド + monthly-weights Job 更新 |
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

`run.sh` の sparse-checkout 拡張 (data/estimate/<predictor_id>/, data/estimate/stadium/,
data/programs/recent_*/, data/programs/motor_stats/) を伴う変更後は、
特にログに `preview_realtime_index_skipped reason=index_csv_missing`
が出ていないか確認します。出ている場合は当日の
`data/estimate/<predictor_id>/` が `daily-sync` Job (Cloud Run; JST 07:30)
で生成済みかをチェック (詳細は「## トラブルシュート →
preview_realtime_index_skipped」参照)。

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
  3 Job 全てが同じ Secret を参照しているため 1 回の更新で済む。
* **想定外の重複実行**: 3 Job とも `parallelism=1 tasks=1 max-retries=0` で動く
  ため同一 Scheduler 内の重複は無い。Scheduler は 3 本:
  `preview-realtime-daytime` (`*/5 8-22 * * *`) /
  `daily-sync` (`30 7 * * *`) /
  `monthly-weights` (`0 6 1 * *`)。
  Python 側もレースコードで冪等化されており、CSV 上書きは MD5 dedup される。
* **3 Job の時間帯重なり**: 毎月 1 日のタイムライン:
  - JST 06:00: `monthly-weights` 起動 (実測 10〜15 分、FeatureContext 導入後)
  - JST 07:30: `daily-sync` 起動 (実測 ~22 分)
  - JST 08:00–22:55: `preview-realtime` 5 分毎
  3 Job は書き込む path がそれぞれ独立しており **ファイル単位の merge conflict は
  起きない**が、git の non-fast-forward (= 相手が先に push して remote が進んだ)
  reject は普通に起きる。とくに `preview-realtime` 稼働時間帯 (JST 08:00–22:59)
  に monthly-weights / daily-sync の手動再実行を仕掛けると衝突しやすい。
  - `run-monthly-weights.sh`: fetch + rebase + push を最大 5 回リトライ。
    rebase は path 重複なしで成立する想定。詳細は本ファイル末尾の
    トラブルシュート「monthly-weights の git push が rejected で失敗する」参照。
  - `run-daily-sync.sh`: `commit-index` ステップは現状 bare push のまま `run_step`
    で握り潰す設計。push 失敗時は当日の index が main に反映されないが、
    翌朝の daily-sync が同じ index を再生成して上書きする。
  - Python 側 (`boatrace.git_operations.push`) は fetch + rebase + push 内蔵。
    preview-realtime はこれを使うため自前リトライは不要。
* **タイムアウト**: preview-realtime は 1 実行 5 分 / daily-sync は 60 分
  (実測 22 分 + バッファ) / monthly-weights は 60 分 (初回実測後にチューニング)。
  Boatrace 側 API が詰まり超過した場合、preview-realtime / daily-sync は
  次回 Scheduler に任せる方針。monthly-weights は失敗時に翌月まで再実行が
  ないため、手動で `gcloud run jobs execute monthly-weights --wait` で
  追走する。
* **コスト概算**:
  - preview-realtime: 180 回/日 × 30 日 ≒ 5,400 実行/月、平均 30 秒
  - daily-sync: 30 回/月、~22 分 × 2 vCPU × 2 GiB
  - monthly-weights: 1 回/月、~N 分 × 2 vCPU × 2 GiB (誤差レベル)
  - Cloud Run Jobs / Scheduler とも合算で実質無料枠 + 数十円/月程度。
    Scheduler は 3 ジョブまで無料(現在 3 本ジャスト)。さらに Job を
    増やす場合は月 $0.10/job/月の課金が発生する。
* **GitHub Actions 側のフォールバック**: `.github/workflows/preview-realtime.yml`
  は `workflow_dispatch` のみ残してあるので、Cloud Run 側に障害が出たら
  GitHub UI から手動で 1 回起動できる。
  daily-sync / monthly-weights は移行完了後 `.github/workflows/*.yml` を
  それぞれ削除予定 (Open Question 3 で `workflow_dispatch` UI も廃止と決定)。
  代替として `gcloud run jobs execute <JOB> --region="$REGION" ...` で
  開発者端末から起動する:
  - daily-sync の backfill: `--update-env-vars=RUN_DATE=2026-05-01`
  - monthly-weights の backfill: `--update-env-vars=TARGET_MONTH=2026-03`

## ロールバック

Scheduler を一時停止すれば Cloud Run 側は完全停止します:

```bash
# preview-realtime
gcloud scheduler jobs pause preview-realtime-daytime --location="$REGION"

# daily-sync
gcloud scheduler jobs pause daily-sync --location="$REGION"

# monthly-weights
gcloud scheduler jobs pause monthly-weights --location="$REGION"
```

GitHub Actions 側の schedule トリガを復活させたい場合:

- preview-realtime: `.github/workflows/preview-realtime.yml` の `on.schedule`
  ブロックを git revert で復活
- daily-sync: PR #3 で削除済みの `.github/workflows/daily-sync.yml` を
  git revert で復活 (PR #3 マージ前なら schedule ブロックを git revert するだけ)
- monthly-weights: 削除 PR を git revert で復活 (マージ前なら schedule
  ブロックを git revert するだけ)

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

### monthly-weights の git push が rejected で失敗する

```
[run-monthly-weights ...] Push attempt 1/5
[run-monthly-weights ...] Push rejected; remote moved during rebase. Retrying after backoff
[run-monthly-weights ...] Push attempt 2/5
...
[run-monthly-weights ...] ABORT: failed to push weights for 2026-05 after retries
[run-monthly-weights ...] FAILED (exit=1) at line 231
```

`preview-realtime` (5 分毎) と push レースを 5 回連続で負けた状態。通常は
2〜3 回のリトライで通る。連続失敗する場合の原因候補:

1. **`preview-realtime` が極端に頻繁に push している** (5 分間に複数 commit)
   → 一時的なら再実行で吸収。常態化していれば preview-realtime 側の
   `boatrace.git_operations` の動作を要調査。
2. **rebase で conflict が出ている** (`Rebase conflict (unexpected — ...)`
   ログがある場合)
   → weights CSV と preview-realtime 系の path は本来 disjoint なので、
   conflict が出ているなら他ジョブが `data/estimate/stadium/` を触る変更が
   入った可能性。`git log` で該当コミットを特定する。
3. **ネットワーク不調 / GitHub 障害** (`fetch failed` ログが頻発)
   → 翌時間帯に手動再実行 (`gcloud run jobs execute monthly-weights
   --update-env-vars=TARGET_MONTH=2026-05 --wait`)。

リトライ上限 (現状 5 回) は `infra/run-monthly-weights.sh` の
`push_with_rebase` 関数内 `max_attempts=5` で変更可能。バックオフは
`sleep $((attempt * 2))` (2s, 4s, 6s, 8s, 10s) で最大 30 秒待つ。

### `preview_realtime_index_skipped reason=index_csv_missing` がログに出る

`scripts/preview-realtime.py` が `data/estimate/<predictor_id>/YYYY/MM/DD.csv` を
更新しようとして、ファイルが存在しない/sparse-checkout されていない時の
メッセージ。`predictor_id` はログの構造化フィールドに含まれる。原因は2系統:

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
2. **`run.sh` の sparse-checkout に該当 `predictor_id` のパスが無い**
   `infra/run.sh` の `ACTIVE_PREDICTORS` 配列で展開される
   `data/estimate/${predictor}/${TODAY_YM}` が古いイメージで checkout
   されない場合、ファイルが存在してもローカルから見えない状態になる。
   レジストリに新規予想者を追加したのに `ACTIVE_PREDICTORS` を更新
   し忘れたケースが典型。「## 更新手順」に従ってイメージを再ビルド +
   Job を更新。

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

# monthly-weights.yml → Cloud Run Job 移行計画

## 1. 現状と移行の方針

### 既存資産（再利用できるもの）
`daily-sync` 移行で確立済みの infra パターンをそのまま流用する。

- `infra/Dockerfile` — `python:3.11-slim` + git + tini。Python 依存は build 時固定、リポジトリは runtime に partial+sparse clone。`ENTRYPOINT` は tini のみ、エントリスクリプトは Job 側の `--command` で切り替え（1 イメージ・複数 Job 構成）。
- `infra/run.sh` (preview-realtime) / `infra/run-daily-sync.sh` (daily-sync) — 同パターンのエントリスクリプトが揃っている。`run-daily-sync.sh` の方が monthly-weights と構造が近い（git commit + push を bash 側で行う点）。
- `infra/cloudbuild.yaml` — Cloud Build 1 本で複数 Job を create-or-update。`deploy-job` ステップを追加するだけで増設できる。
- Service Account: Runner = `preview-realtime-runner@boatrace-487212.iam.gserviceaccount.com`、Invoker = `preview-realtime-invoker@boatrace-487212.iam.gserviceaccount.com`。両者そのまま流用。
- Secret Manager `github-token`、Artifact Registry `containers` リポジトリ、イメージ名 `preview-realtime`（歴史的経緯で名前据え置き）。

### 方針
**1 イメージ・3 Job 構成**に拡張する。`preview-realtime` / `daily-sync` / **新規 `monthly-weights`** を同じイメージで運用し、Cloud Run Job ごとに `--command` で `/app/run.sh` / `/app/run-daily-sync.sh` / **`/app/run-monthly-weights.sh`** を切り替える。

Cloud Scheduler から **JST 06:00 / 毎月 1 日** に `monthly-weights` Job を `jobs:run` API で起動する。旧 GHA cron `0 0 1 * *` UTC (= JST 09:00) からの**意図的な前倒し**で、狙いは「境界日 (毎月 1 日) の daily index と realtime index を**同一 weights** で計算する」ことにある。詳細は §5 Open Question #3 を参照。

GitHub Actions 側の `monthly-weights.yml` は段階的に削除する：

1. PR で infra 一式をマージ
2. 動作確認のため一度手動 (`gcloud run jobs execute`) で実行
3. Cloud Scheduler を `--paused` で作成 → 翌月 1 日の自動実行を観察
4. 並走監視期間後、`monthly-weights.yml` を完全削除（daily-sync と同じく `workflow_dispatch` も廃止）

---

## 2. ステップ別の作業

### Step 1. `infra/run-monthly-weights.sh` を新規作成

`run-daily-sync.sh` をベースに、monthly-weights 専用に簡略化する。差分は次の通り：

**(a) sparse-checkout が広い**：`build_weights.py` は `compute_features_for_day(repo, day)` を **直近 6 ヶ月の全日**について呼ぶ。`boatrace.index_features` が読むファイルは：

| パス | 用途 |
| --- | --- |
| `scripts/` | `build_weights.py` + `boatrace/index_features.py` |
| `.boatrace/` | 実行時設定 (load_config) |
| `data/estimate/stadium/` | `win_rate.csv` / `sui_params.csv` / `index_weights/` (read + 出力先) |
| `data/results/daily/<YM>/` × 8 ヶ月 | 着順 (target = 7-着順) |
| `data/programs/race_cards/<YM>/` × 8 ヶ月 | レース宇宙 (universe) |
| `data/programs/recent_national/<YM>/` × 8 ヶ月 | recent 特徴量 (`racer` 列) |
| `data/programs/recent_local/<YM>/` × 8 ヶ月 | recent 特徴量 (`racer` 列) |
| `data/programs/motor_stats/<YM>/` × 8 ヶ月 | motor 特徴量 + 月初 7 日 fallback |
| `data/previews/sui/<YM>/` × 8 ヶ月 | 気象 (`weather` 特徴量) |
| `data/previews/tkz/<YM>/` × 8 ヶ月 | 展示タイム (`exhibit` 特徴量) |
| `data/previews/stt/<YM>/` × 8 ヶ月 | 進入コース (course 補正) |
| `data/previews/original_exhibition/<YM>/` × 8 ヶ月 | 展示値 1〜3 (`exhibit` 特徴量) |

> ⚠️ **学んだ教訓**: 初回実装時に `data/previews/*` 4 ファミリを入れ忘れて手動実行したところ、`_load_realtime_preview_by_code` が空 dict を返し `exhibit` と `weather` 特徴量が全行 NaN になった結果、`fit_one` の `dropna(subset=["waku","racer","exhibit","weather","着順"])` で全行ドロップ → 24 場すべて n=0 FALLBACK の症状が出た。4 ファミリは必須。

8 ヶ月分の cone を bash で生成する（target_month を含むため、6 ヶ月前 → target の前月 → target 月の月初 = 6+1 = 7 ヶ月。motor の 7 日 fallback はさらに 1 ヶ月前を含めて計 8 ヶ月）：

```bash
# 月初 1 日に target_month を計算する場合は、TZ=Asia/Tokyo で当月を取り、
# 7 ヶ月分 + motor fallback 用 1 ヶ月 を sparse-checkout に追加する。
TARGET_MONTH="${TARGET_MONTH:-$(TZ=Asia/Tokyo date +%Y-%m)}"
# target_month の月初
TARGET_YM=$(echo "$TARGET_MONTH" | tr - /)            # "2026-05" → "2026/05"
# 直近 6 ヶ月前 〜 target_month の前月、計 6 ヶ月。さらに motor fallback として
# その前月 1 つを追加。target_month 当月の index_weights ファイル書き出し先も含める。
months=()
months+=("$TARGET_YM")
for i in 1 2 3 4 5 6 7; do
  m=$(TZ=Asia/Tokyo date -d "${TARGET_MONTH}-15 -${i} month" +'%Y/%m')
  months+=("$m")
done
```

そのうえで sparse-checkout：

```bash
paths=( scripts .boatrace data/estimate/stadium )
for ym in "${months[@]}"; do
  paths+=( \
    "data/results/daily/${ym}" \
    "data/programs/race_cards/${ym}" \
    "data/programs/recent_national/${ym}" \
    "data/programs/recent_local/${ym}" \
    "data/programs/motor_stats/${ym}" \
    "data/previews/sui/${ym}" \
    "data/previews/tkz/${ym}" \
    "data/previews/stt/${ym}" \
    "data/previews/original_exhibition/${ym}" \
  )
done
git sparse-checkout init --cone
git sparse-checkout set "${paths[@]}"
```

`--filter=blob:none` のおかげで blob は使うときだけ promisor remote からフェッチされるので、不要月を sparse から外せばトラフィックは抑えられる。それでも 6 ヶ月 × 30 日 × 5 ファイル ≒ 900 ファイルが対象なので、preview-realtime や daily-sync よりやや I/O が多い前提で資源を割り当てる（後述 Step 3）。

**(b) Python 実行は 1 ステップ**：

```bash
TARGET_MONTH="${TARGET_MONTH:-$(TZ=Asia/Tokyo date +%Y-%m)}"
log "Building monthly weights for ${TARGET_MONTH}"
python scripts/build_weights.py --month "${TARGET_MONTH}"
```

`build_weights.py` は CSV を 1 本（`data/estimate/stadium/index_weights/${TARGET_MONTH}.csv`）出力するだけで、git に触らない。

**(c) bash 側で commit + push**：旧 GHA workflow の "Commit Weights" ステップを移植する。

```bash
git add data/estimate/stadium/index_weights/
if git diff --cached --quiet; then
  log "No weights changes to commit for ${TARGET_MONTH}"
else
  git commit -m "Update monthly index weights (${TARGET_MONTH})"
  git push origin "${GIT_BRANCH}"
fi
```

**(d) GCS publish は呼ばない**：weights は fun-site が直接読むファミリではない（fun-site が読むのは title / race_cards / stt / index / results。weights は build_index.py の入力としてのみ使われる）。`BOATRACE_GCS_CSV_BUCKET` / `BOATRACE_PUBSUB_TOPIC` の env vars も注入不要。

**(e) `run_step` 集約コードは不要**：steps は 1 本だけなので `set -Eeuo pipefail` で十分。失敗時はそのまま非 0 終了。Cloud Run Job の `--max-retries=0` 設定で自動リトライしない。

**(f) 引数透過**：手動実行で `month` を上書きしたいケース（旧 `workflow_dispatch.month` 相当）は `--update-env-vars=TARGET_MONTH=2026-04` で渡せるようにする（上記 (b) で対応済み）。

### Step 2. `infra/Dockerfile` を 1 行追記

現行：

```dockerfile
COPY --chmod=0755 infra/run.sh            /app/run.sh
COPY --chmod=0755 infra/run-daily-sync.sh /app/run-daily-sync.sh
```

→

```dockerfile
COPY --chmod=0755 infra/run.sh                  /app/run.sh
COPY --chmod=0755 infra/run-daily-sync.sh       /app/run-daily-sync.sh
COPY --chmod=0755 infra/run-monthly-weights.sh  /app/run-monthly-weights.sh
```

`ENTRYPOINT ["/usr/bin/tini", "--"]` の構造は不変。

### Step 3. `infra/cloudbuild.yaml` に `deploy-job-monthly-weights` を追加

`substitutions:` に追加：

```yaml
_JOB_NAME_MONTHLY: monthly-weights
```

末尾に新規ステップを追加：

```yaml
  # monthly-weights Job: 旧 .github/workflows/monthly-weights.yml を置き換える。
  # 毎月 1 日 JST 09:00 に Cloud Scheduler から起動される。
  # daily-sync 同等の重さ (build_weights は 6 ヶ月分の特徴量を一括計算するため
  # pandas + scipy.optimize の総時間が長め)。
  - id: deploy-job-monthly-weights
    name: gcr.io/google.com/cloudsdktool/cloud-sdk:slim
    entrypoint: gcloud
    args:
      - run
      - jobs
      - deploy
      - ${_JOB_NAME_MONTHLY}
      - --image=${_REGION}-docker.pkg.dev/${_PROJECT_ID}/${_AR_REPO}/${_IMAGE}:${SHORT_SHA}
      - --region=${_REGION}
      - --project=${_PROJECT_ID}
      - --service-account=${_RUNNER_SA}@${_PROJECT_ID}.iam.gserviceaccount.com
      - --command=/app/run-monthly-weights.sh
      - --set-secrets=GITHUB_TOKEN=${_SECRET_NAME}:latest
      - --set-env-vars=GITHUB_REPO=${_GITHUB_REPO},GIT_BRANCH=${_GIT_BRANCH}
      - --max-retries=0
      - --task-timeout=3600s
      - --parallelism=1
      - --tasks=1
      - --cpu=2
      - --memory=2Gi
```

`preview-realtime` / `daily-sync` ステップとのリソース・env 差分：

| 項目 | preview-realtime | daily-sync | **monthly-weights** | 理由 |
|---|---|---|---|---|
| `--command` | `/app/run.sh` | `/app/run-daily-sync.sh` | `/app/run-monthly-weights.sh` | エントリスクリプト切り替え |
| `--task-timeout` | `300s` | `3600s` | `3600s` | 6 ヶ月分の特徴量計算 + SLSQP 24 場分。実測 N 分（手動初回実行で確定させる）。1 時間のバッファで安全側 |
| `--memory` | `1Gi` | `2Gi` | `2Gi` | 6 ヶ月分の長表 (ボートレース × ボート行) をメモリに保持。pandas concat の peak が ~1 Gi 超になる懸念 |
| `--cpu` | `1` | `2` | `2` | scipy.optimize.minimize × 24 場 + 特徴量計算の I/O 並走 |
| `--max-retries` | `0` | `0` | `0` | bash 側で集約していないので、ジョブ失敗 = run_step 失敗。冪等性は確保されているが二重 commit を避けるためリトライ off |
| `--set-env-vars` | + GCS/PubSub | + GCS/PubSub | `GITHUB_REPO`, `GIT_BRANCH` のみ | GCS publish を呼ばないため、GCS / Pub/Sub 関連 env は注入不要 |
| `--set-secrets` | `GITHUB_TOKEN` | `GITHUB_TOKEN` | `GITHUB_TOKEN` | 同じ Secret を流用 |

### Step 4. Cloud Scheduler ジョブを作成

```bash
gcloud scheduler jobs create http monthly-weights-trigger \
  --project=boatrace-487212 \
  --location=asia-northeast1 \
  --schedule="0 6 1 * *" \
  --time-zone="Asia/Tokyo" \
  --uri="https://asia-northeast1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/boatrace-487212/jobs/monthly-weights:run" \
  --http-method=POST \
  --oauth-service-account-email=preview-realtime-invoker@boatrace-487212.iam.gserviceaccount.com \
  --oauth-token-scope=https://www.googleapis.com/auth/cloud-platform \
  --paused
```

- **JST 06:00** を採用（旧 GHA cron は JST 09:00 = daily-sync の後）。daily-sync (JST 07:30) の **前** に終わらせることで、当日 (毎月 1 日) の daily index と realtime index を同一 weights で計算する。実測 ~30 分なら 06:30 完了 → 07:30 daily-sync 開始まで 1 時間バッファ。
- 副作用: 訓練ウィンドウから前月末日 (例: 5/1 ジョブで 4/30) の K-file 結果が抜ける。daily-sync (`result.py`) が前日 K-file を取り込むのは 1 日 07:30 のため、06:00 時点では 4/30 が repo にない。180 日中 1 日 (0.5%) の欠落で SLSQP fit には実質影響なし。詳細トレードオフは §5 Open Question #3。
- `--paused` で作成 → 動作確認後に `gcloud scheduler jobs resume monthly-weights-trigger` で解除。daily-sync と同じ二段階展開。

### Step 5. IAM / 権限まわり

- **Runner SA** (`preview-realtime-runner`)：そのまま流用。Secret Manager (`github-token`) read 権限は付与済み。**GCS / Pub/Sub の権限は不要**（publish を呼ばないため、過剰権限を付けないこと）。
- **Invoker SA** (`preview-realtime-invoker`)：そのまま流用。新規 SA は作らない。追加で必要な唯一の binding：

  ```bash
  gcloud run jobs add-iam-policy-binding monthly-weights \
    --region=asia-northeast1 \
    --member="serviceAccount:preview-realtime-invoker@boatrace-487212.iam.gserviceaccount.com" \
    --role=roles/run.invoker
  ```

- **Cloud Build SA**：`roles/run.developer` + Runner SA への `roles/iam.serviceAccountUser` は preview-realtime / daily-sync 用に付与済み。追加付与は不要。
- **PAT (`github-token`)**：daily-sync と同じ fine-grained PAT を使用。`Contents:Write` 権限済み。

### Step 6. `monthly-weights.yml` を削除

Cloud Run Job が安定稼働した時点で `.github/workflows/monthly-weights.yml` を完全削除する（daily-sync と同じ方針で `workflow_dispatch` UI も廃止）。

手動実行が必要な場合（過去月の再計算など）は開発者が gcloud CLI から直接叩く：

```bash
# 当月の再計算
gcloud run jobs execute monthly-weights \
  --region=asia-northeast1 \
  --project=boatrace-487212 \
  --wait

# 過去月の再計算（例: 2026-03）
gcloud run jobs execute monthly-weights \
  --region=asia-northeast1 \
  --project=boatrace-487212 \
  --update-env-vars=TARGET_MONTH=2026-03 \
  --wait
```

過去月を指定した場合、`run-monthly-weights.sh` が `TARGET_MONTH` 環境変数を読んで `--month` フラグに反映するため、sparse-checkout もその月を中心とした 6 ヶ月前 〜 当月のレンジに切り替わる。

### Step 7. ロールアウト手順（カナリア）

1. **PR**：`infra/run-monthly-weights.sh` 新規作成 + `infra/Dockerfile` 1 行追加 + `infra/cloudbuild.yaml` に `deploy-job-monthly-weights` 追加。`infra/README.md` の対応表に monthly-weights 行を追記。
2. main にマージ → Cloud Build トリガが発火 → 新イメージ build & 3 Job (`preview-realtime` / `daily-sync` / `monthly-weights`) が deploy される。`preview-realtime` / `daily-sync` の挙動が変わらないことを翌日 1 サイクル監視。
3. **手動 1 回実行**：当月分を `gcloud run jobs execute monthly-weights --wait` で実行 → `data/estimate/stadium/index_weights/YYYY-MM.csv` が GitHub にコミットされることを確認。Cloud Logging で実測時間とメモリ使用量を確認し、必要なら Job spec を再チューニング。
4. **Cloud Scheduler を `--paused` で作成**。`gcloud scheduler jobs run monthly-weights-trigger` で `--paused` 状態でも 1 回だけ手動発火させ、Scheduler → Cloud Run Jobs のエンドツーエンドが通ることを確認（OIDC 認証含む）。
5. 翌月 1 日の自動実行を待つ前に、GHA `monthly-weights.yml` の `schedule:` を削除（または `if: github.repository == 'never-match'` でガード）して **二重起動を防止**。`schedule:` を残したまま Scheduler を resume すると、JST 06:00 と 09:00 の 2 系統が同月の `index_weights/YYYY-MM.csv` を生成して push 競合する可能性がある（時刻が違うので衝突自体は起きにくいが、2 系統が走ること自体無駄）。
6. `gcloud scheduler jobs resume monthly-weights-trigger`。
7. 翌月 1 日 JST 09:00 の自動実行を観察。問題なければ次月以降に `.github/workflows/monthly-weights.yml` を完全削除。

---

## 3. 既存 GHA との相違点・注意

| 観点 | GitHub Actions 現状 | Cloud Run Job 移行後 |
|---|---|---|
| **チェックアウト** | `actions/checkout@v4` + `fetch-depth: 0` + `git lfs pull` | partial+sparse clone（LFS 実態未使用なので問題なし、daily-sync で実証済み） |
| **Python 依存** | 毎回 `pip install -r` | イメージにビルド時固定 |
| **失敗時の挙動** | 単一ジョブの素直な fail | bash の `set -Eeuo pipefail`。リトライなし（`--max-retries=0`） |
| **ログ保管** | Actions UI で参照 | Cloud Logging（既定 30 日） |
| **手動実行** | `workflow_dispatch` (`month` input) | `gcloud run jobs execute monthly-weights --update-env-vars=TARGET_MONTH=...`。GHA UI は廃止 |
| **同時実行** | GHA 同時起動可能 | Cloud Run Job は `--parallelism=1 --tasks=1` |
| **コスト** | GHA Linux runner 数分 × 月 1 回（無料枠内） | 2 vCPU × 2 GiB × ~30 分 × 月 1 回 ≒ 円単位。Cloud Scheduler は月 3 ジョブまで無料 |

### LFS について
GHA workflow には `lfs: true` + `git lfs pull` があるが、daily-sync 移行計画では「LFS は実態未使用」と整理済み。`monthly-weights` でも同様に LFS なしで動作するはず（`build_weights.py` が読む CSV 群は LFS 管理されていない）。万一 LFS が必要なファイルが混じっていたら Cloud Build / Job ログでフェッチエラーが出るので、その時点で `git lfs install && git lfs pull` を `run-monthly-weights.sh` に追加する。

### sparse-checkout のサイズ感
6 ヶ月分の `data/results/daily/` + `data/programs/{race_cards,recent_*,motor_stats}/` の総ファイル数は概算 6 × 30 × 5 ≒ 900 ファイル。`--filter=blob:none` で初回 blob fetch がオンデマンドになるので、worst case でも数百 MB のダウンロード。1 GiB メモリでも収まるが、daily-sync と揃えて 2 GiB を割り当てる。

---

## 4. 影響範囲とロールバック

### 影響範囲
- `infra/Dockerfile`：1 行 (`COPY --chmod=0755 infra/run-monthly-weights.sh ...`) 追加。既存 2 Job のイメージは新しく再 build されるが、ENTRYPOINT が tini 固定なので挙動は不変。
- `infra/cloudbuild.yaml`：`substitutions:` に 1 行、`steps:` に 1 ステップ (`deploy-job-monthly-weights`) 追加。
- 新規 `infra/run-monthly-weights.sh`。
- `infra/README.md`：「現在 Cloud Run Jobs に載っている処理」表に行追加 + sparse-checkout 対象セクションに `run-monthly-weights.sh` 用のサブセクションを追加。
- `.github/workflows/monthly-weights.yml`：最終的に削除（Step 6）。
- リポジトリへの commit author が `github-actions[bot]` から `monthly-weights-bot@users.noreply.github.com`（または daily-sync と同じ命名規則）に変わる。

### ロールバック
- **PR マージ前**：GHA `monthly-weights.yml` がそのまま動いているので何もしなくてもよい。
- **PR マージ後・Scheduler resume 前**：Cloud Run Job は手動実行のみ。Scheduler を resume しない限り GHA 側のみ稼働。
- **Scheduler resume 後・GHA YAML 削除前**：Scheduler を `--paused` に戻すだけで GHA 側のみ稼働に戻る（ただし Step 7-5 で `schedule:` を外している場合は YAML を git revert する必要あり）。
- **YAML 削除後**：git revert + Scheduler pause で完全ロールバック。1 PR 分の手戻り。

---

## 5. Open Questions（決定済み）

| # | 決定 | 計画への反映 |
|---|---|---|
| 1 | **GCS ミラー / Pub/Sub publish は呼ばない**。weights は fun-site が直接読まない（`build_index.py` が読む中間ファイル） | Step 1 (d), Step 3 で env 注入なし。Runner SA に GCS / Pub/Sub 権限は追加しない |
| 2 | **`workflow_dispatch` UI も廃止**。daily-sync と揃える。手動実行は `gcloud run jobs execute --update-env-vars=TARGET_MONTH=...` で対応 | Step 6 で `.github/workflows/monthly-weights.yml` を完全削除 |
| 3 | **JST 06:00 に変更**（旧 GHA は JST 09:00）。狙いは境界日 (毎月 1 日) の daily index と realtime index を同一 weights で計算すること。訓練ウィンドウから前月末日 1 日分が抜けるトレードオフは許容 | Step 4 で `--schedule="0 6 1 * *"` + `--time-zone="Asia/Tokyo"`。下の詳細比較を参照 |
| 4 | **target を含めて 7 ヶ月 + motor fallback 1 ヶ月** を sparse-checkout 対象にする | Step 1 (a) のループで自動生成 |
| 5 | **daily-sync と同じ 2 vCPU / 2 GiB / 3600s timeout で開始**。初回実行の実測でチューニング | Step 3 の Job spec |
| 6 | **LFS 不要**（daily-sync と同じ判断） | Step 3 注記。動作確認時に LFS フェッチエラーが出たら追加対応 |

### Open Question #3 詳細: スケジュール時刻のトレードオフ

毎月 1 日に走るバッチの時系列を整理すると、monthly-weights が **daily-sync (07:30) の前か後か** で境界日の挙動が変わる。

| 時刻 | JST 06:00 (採用) | JST 09:00 (旧 GHA cron) |
|---|---|---|
| **訓練ウィンドウ** | 6 ヶ月前 〜 **前月末日の前日**まで (例: 5/1 ジョブ → 〜 4/29) | 6 ヶ月前 〜 **前月末日**まで (例: 5/1 ジョブ → 〜 4/30) |
| **欠落** | 前月末日 1 日分 (180 日中 1 日 = 0.5%) — K-file がまだ未取り込み | なし |
| **daily-sync (07:30) が読む weights** | 新月 weights ✓ | 旧月 weights ✗ |
| **preview-realtime (08:00〜) が読む weights** | 新月 weights ✓ (一日中一貫) | 旧月 weights → 09:30 ごろ新月に切り替わる ✗ |
| **境界日の index 一貫性** | ○ daily / realtime とも新 weights | × daily は旧、realtime は途中で切替 |

結論: 訓練データ 0.5% の欠落は SLSQP fit に有意差を生まないが、境界日の index 不整合はユーザー視点で realtime 出力が日中飛ぶため明らかに悪い。**JST 06:00 を採用** し、訓練データ欠落のほうを許容する。

(代替案として JST `0 6 2 * *` = 毎月 2 日 06:00 起動も検討した。前月末日の K-file は取り込まれるが、1 日当日は丸一日「旧 weights」のままで preview-realtime が走り、2 日から新 weights に切り替わる。境界日の不整合がより目立つため不採用。)

---

## 6. 推奨マイルストーン

1. **PR #1（infra）**：`infra/run-monthly-weights.sh` + `Dockerfile` / `cloudbuild.yaml` 差分 + `README.md` 追記。preview-realtime / daily-sync の挙動が変わらないことをローカル / Cloud Build ログで確認。
2. **手動初回実行**：`gcloud run jobs execute monthly-weights --wait` で当月分を流し、コミットされた CSV を旧 GHA の出力と diff 比較（数値は完全一致するはず — 入力データとアルゴリズムは同じ）。
3. **IAM / Scheduler 設定**：Step 5 の `roles/run.invoker` 付与 + Step 4 の `--paused` Scheduler 作成。`gcloud scheduler jobs run monthly-weights-trigger` で `--paused` でも手動発火できることを確認（Scheduler → Cloud Run Jobs の認証経路を実証）。
4. **GHA 側の二重起動防止**：`.github/workflows/monthly-weights.yml` の `schedule:` を削除（PR）。`workflow_dispatch` のみ残してフォールバック化。
5. **Scheduler resume**：`gcloud scheduler jobs resume monthly-weights-trigger`。翌月 1 日 JST 09:00 を観察。
6. **+1 ヶ月後**：問題なければ `.github/workflows/monthly-weights.yml` を完全削除（PR）。

#!/usr/bin/env bash
#
# Cloud Run Jobs entrypoint for monthly-weights.
#
# Replaces .github/workflows/monthly-weights.yml. Runs every month on the 1st
# at JST 06:00 via Cloud Scheduler. Performs:
#   1. Partial+sparse clone of main covering the last 7 months
#      (= target_month + 6 prior months) + 1 extra month for motor_stats
#      fallback (build_weights.py invokes compute_features_for_day for every
#      day in the 6-month training window, and motor_stats has a 7-day
#      fallback that crosses month boundaries).
#   2. Run scripts/build_weights.py --month "${TARGET_MONTH}".
#   3. Commit + push data/estimate/stadium/index_weights/ (build_weights.py
#      writes the CSV but does not self-commit).
#
# The monthly-weights schedule is intentionally placed BEFORE daily-sync
# (07:30 JST) so that the boundary day's daily index and realtime index are
# both computed with the freshly-updated weights. Training results come from
# data/results/realtime/ (appended during each race day at 締切+3〜30分),
# so by 06:00 JST on day-1 of the next month the previous month's last day
# is already fully written — no boundary-day loss.
#
# Required env (injected by the Cloud Run Job spec):
#   GITHUB_TOKEN  - fine-grained PAT with Contents:Write on the repo (from
#                   Secret Manager: github-token).
#
# Optional env (sensible defaults):
#   GITHUB_REPO       owner/name of the GitHub repo
#   GIT_BRANCH        branch to clone and push to
#   GIT_USER_NAME     committer name
#   GIT_USER_EMAIL    committer email
#   TARGET_MONTH      YYYY-MM override for the target month — used for
#                     backfill via `gcloud run jobs execute
#                     --update-env-vars=TARGET_MONTH=2026-03`. When unset,
#                     current month (JST) is used.
#
# Exit codes:
#   0  success (commit pushed, or no diff to commit)
#   1  failure (clone failed, python crashed, git push rejected, etc.)
#
# Idempotency: build_weights.py rewrites the same
# data/estimate/stadium/index_weights/${TARGET_MONTH}.csv each run; the
# subsequent `git diff --cached --quiet` check makes a no-op re-run silent.

set -Eeuo pipefail

log() {
  printf '[run-monthly-weights %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

on_error() {
  local exit_code=$?
  log "FAILED (exit=${exit_code}) at line $1"
  exit "${exit_code}"
}
trap 'on_error $LINENO' ERR

: "${GITHUB_TOKEN:?GITHUB_TOKEN is required (mount Secret Manager secret github-token)}"

GITHUB_REPO="${GITHUB_REPO:-BoatraceCSV/boatracecsv.github.io}"
GIT_BRANCH="${GIT_BRANCH:-main}"
GIT_USER_NAME="${GIT_USER_NAME:-monthly-weights-bot}"
GIT_USER_EMAIL="${GIT_USER_EMAIL:-monthly-weights-bot@users.noreply.github.com}"

# TARGET_MONTH で対象月を上書き。空なら JST 当月。backfill 時は
#   gcloud run jobs execute monthly-weights --update-env-vars=TARGET_MONTH=2026-04
# のように指定する。
if [[ -n "${TARGET_MONTH:-}" ]]; then
  log "TARGET_MONTH override: ${TARGET_MONTH}"
else
  TARGET_MONTH=$(TZ=Asia/Tokyo date +%Y-%m)
fi

# 形式チェック (YYYY-MM)。早期 fail で誤指定の sparse-checkout 計算を防ぐ。
if ! [[ "${TARGET_MONTH}" =~ ^[0-9]{4}-(0[1-9]|1[0-2])$ ]]; then
  log "Invalid TARGET_MONTH='${TARGET_MONTH}' (expected YYYY-MM)"
  exit 1
fi

# Active な予想者の ID リスト。scripts/boatrace/predictors/registry.py の
# ``active_predictors()`` と必ず同期させる (新規予想者追加時は両方更新)。
ACTIVE_PREDICTORS=(v1_basic v2_tenkai)

# ---------------------------------------------------------------------------
# sparse-checkout 対象月の計算
#
# build_weights.py の訓練ウィンドウは [target - 6 ヶ月, target - 1 日]。
# `compute_features_for_day(repo, day)` は各日について以下を読む:
#   - data/programs/race_cards/<YM>/<DD>.csv         (universe)
#   - data/programs/recent_national/<YM>/<DD>.csv    (recent 特徴量)
#   - data/programs/recent_local/<YM>/<DD>.csv       (recent 特徴量)
#   - data/programs/motor_stats/<YM>/<DD>.csv        (motor 特徴量、7日fallback あり)
#   - data/previews/sui/<YM>/<DD>.csv                (気象 = weather 特徴量)
#   - data/previews/tkz/<YM>/<DD>.csv                (展示タイム = exhibit 特徴量)
#   - data/previews/stt/<YM>/<DD>.csv                (進入コース)
#   - data/previews/original_exhibition/<YM>/<DD>.csv (展示値1〜3 = exhibit 特徴量)
#   - data/results/realtime/<YM>/<DD>.csv            (target = 7 - 着順)
# さらに index_features.py が固定で読む:
#   - data/estimate/stadium/win_rate.csv
#   - data/estimate/stadium/sui_params.csv
#
# data/previews/* を入れ忘れると _load_realtime_preview_by_code が空 dict を
# 返し、`exhibit` と `weather` 特徴量が全レースで NaN になる。すると
# fit_one の dropna(subset=["waku","racer","exhibit","weather","着順"]) で
# 全行が落ちて n=0 FALLBACK が 24 場分発生する。
#
# target 当月の出力先 (data/estimate/stadium/index_weights/) は
# data/estimate/stadium/ に含まれるため別途列挙不要。
#
# 対象月リスト:
#   - target_month そのもの (出力 CSV を git add するため cone 内に必須)
#   - target_month - 1 月 〜 target_month - 6 月 (訓練ウィンドウ)
#   - target_month - 7 月 (motor_stats の 7 日 fallback が月境界をまたぐため)
# 合計 8 ヶ月。
# ---------------------------------------------------------------------------
months=()
months+=("$(echo "${TARGET_MONTH}" | tr - /)")          # target_month, e.g. "2026/05"
for i in 1 2 3 4 5 6 7; do
  m=$(TZ=Asia/Tokyo date -d "${TARGET_MONTH}-15 -${i} month" +'%Y/%m')
  months+=("$m")
done

WORKDIR="$(mktemp -d -t monthly-weights.XXXXXX)"
cleanup() {
  rm -rf "${WORKDIR}" 2>/dev/null || true
}
trap cleanup EXIT

cd "${WORKDIR}"

REMOTE_WITH_TOKEN="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"
REMOTE_PUBLIC="https://github.com/${GITHUB_REPO}.git"

log "Cloning ${REMOTE_PUBLIC} (branch=${GIT_BRANCH}, partial+sparse, target=${TARGET_MONTH}, months=${#months[@]})"
git clone \
  --depth 1 \
  --filter=blob:none \
  --no-checkout \
  --no-tags \
  --single-branch \
  --branch "${GIT_BRANCH}" \
  "${REMOTE_WITH_TOKEN}" \
  repo

cd repo

# sparse-checkout: 静的部分 + 月単位データ × N ヶ月。
paths=(
  scripts
  .boatrace
  data/estimate/stadium
)
for ym in "${months[@]}"; do
  paths+=(
    "data/results/realtime/${ym}"
    "data/programs/race_cards/${ym}"
    "data/programs/recent_national/${ym}"
    "data/programs/recent_local/${ym}"
    "data/programs/motor_stats/${ym}"
    "data/previews/sui/${ym}"
    "data/previews/tkz/${ym}"
    "data/previews/stt/${ym}"
    "data/previews/original_exhibition/${ym}"
  )
done

git sparse-checkout init --cone
git sparse-checkout set "${paths[@]}"

git checkout "${GIT_BRANCH}"

# committer / credential 設定。
git config --local user.name "${GIT_USER_NAME}"
git config --local user.email "${GIT_USER_EMAIL}"
git config --local --replace-all "url.${REMOTE_WITH_TOKEN}.insteadOf" "${REMOTE_PUBLIC}"
git remote set-url origin "${REMOTE_PUBLIC}"

mkdir -p logs

# ---------------------------------------------------------------------------
# build_weights.py 実行 (active な全予想者ぶん)
# --all-active で registry の active 予想者を全部ループ。出力先は
# data/estimate/stadium/weights/{predictor_id}/{TARGET_MONTH}.csv。
# ---------------------------------------------------------------------------
log "Building monthly weights for ${TARGET_MONTH} (predictors: ${ACTIVE_PREDICTORS[*]})"
python scripts/build_weights.py --month "${TARGET_MONTH}" --all-active

# ---------------------------------------------------------------------------
# build_weights.py は CSV 出力のみで git にコミットしないため、bash 側で
# commit + push する。旧 GHA workflow の "Commit Weights" ステップ相当。
# 差分が無ければ no-op (再実行時の冪等性)。
#
# preview-realtime ジョブが JST 08:00〜22:59 の間 5 分ごとに main へ push
# しているため、bare push は non-fast-forward で reject されることが多い。
# fetch + rebase で取り込んでから push し、競合があれば最大数回までリトライ。
#
# weights CSV (data/estimate/stadium/index_weights/) は preview-realtime が
# 触る path (data/previews/, data/results/, data/estimate/index/) と完全に
# 別なので、rebase は conflict なしで成立する想定。万一 conflict が出た場合
# は abort して fail する (人手調査が必要)。
# ---------------------------------------------------------------------------
push_with_rebase() {
  local max_attempts=5
  local attempt=1
  while (( attempt <= max_attempts )); do
    log "Push attempt ${attempt}/${max_attempts}"
    if ! git fetch origin "${GIT_BRANCH}"; then
      log "fetch failed on attempt ${attempt}; retrying after backoff"
      attempt=$((attempt + 1))
      sleep $((attempt * 2))
      continue
    fi
    if ! git rebase "origin/${GIT_BRANCH}"; then
      log "Rebase conflict (unexpected — weights path is disjoint from realtime paths)"
      git rebase --abort || true
      return 1
    fi
    if git push origin "${GIT_BRANCH}"; then
      log "Push succeeded on attempt ${attempt}"
      return 0
    fi
    log "Push rejected; remote moved during rebase. Retrying after backoff"
    attempt=$((attempt + 1))
    sleep $((attempt * 2))
  done
  log "Push failed after ${max_attempts} attempts"
  return 1
}

git add data/estimate/stadium/weights/
if git diff --cached --quiet; then
  log "No weights changes to commit for ${TARGET_MONTH}"
else
  git commit -m "Update monthly index weights (${TARGET_MONTH})"
  if ! push_with_rebase; then
    log "ABORT: failed to push weights for ${TARGET_MONTH} after retries"
    exit 1
  fi
  log "Pushed weights for ${TARGET_MONTH}"
fi

log "Done"

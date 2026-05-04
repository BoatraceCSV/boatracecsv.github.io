#!/usr/bin/env bash
#
# Cloud Run Jobs entrypoint for preview-realtime.
#
# Responsibilities:
#   1. Fetch a fresh shallow clone of main.
#   2. Run scripts/preview-realtime.py (which itself appends rows and pushes
#      via boatrace.git_operations).
#
# Required env (injected by the Cloud Run Job spec):
#   GITHUB_TOKEN  - fine-grained PAT with Contents:Write on the repo (from
#                   Secret Manager: github-token).
#
# Optional env (sensible defaults baked in):
#   GITHUB_REPO        owner/name of the GitHub repo
#   GIT_BRANCH         branch to clone and push to
#   GIT_USER_NAME      committer name
#   GIT_USER_EMAIL     committer email
#   PREVIEW_EXTRA_ARGS extra flags forwarded to preview-realtime.py (e.g. --dry-run)
#
# Exit codes:
#   0  success (including "no eligible races, nothing to do")
#   1  unrecoverable failure (clone failed, python crashed, etc.)
#
# Idempotency: the python script dedupes against existing CSV rows, so a
# re-run after a transient failure is safe.

set -Eeuo pipefail

log() {
  printf '[run.sh %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
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
GIT_USER_NAME="${GIT_USER_NAME:-preview-realtime-bot}"
GIT_USER_EMAIL="${GIT_USER_EMAIL:-preview-realtime-bot@users.noreply.github.com}"
PREVIEW_EXTRA_ARGS="${PREVIEW_EXTRA_ARGS:-}"

WORKDIR="$(mktemp -d -t preview-realtime.XXXXXX)"
cleanup() {
  # Best-effort cleanup; never fail the job because of this.
  rm -rf "${WORKDIR}" 2>/dev/null || true
}
trap cleanup EXIT

cd "${WORKDIR}"

# Embed the token in the URL only for the clone; immediately rewrite the
# remote so subsequent fetch/push from the python helper also authenticate
# without ever logging the token in `git remote -v`-style debug output.
REMOTE_WITH_TOKEN="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"
REMOTE_PUBLIC="https://github.com/${GITHUB_REPO}.git"

# The repo is ~5 GB (mostly data/ and models/). A naive `git clone` OOMs on
# the 1 GiB Cloud Run Job. We do a partial clone (commits/trees only, no
# blobs) and then a cone-mode sparse-checkout of just the paths the script
# actually reads or writes:
#   - scripts/             python sources
#   - .boatrace/           runtime config (load_config)
#   - data/previews/{tkz,stt,sui,original_exhibition}/<YYYY>/<MM>/
#                          existing CSVs for today (for dedup) + write target
#
# Today's YYYY/MM is computed in JST because csv_path_for() uses JST dates.
TODAY_YM=$(TZ=Asia/Tokyo date +'%Y/%m')

log "Cloning ${REMOTE_PUBLIC} (branch=${GIT_BRANCH}, partial+sparse, ym=${TODAY_YM})"
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

git sparse-checkout init --cone
git sparse-checkout set \
  scripts \
  .boatrace \
  "data/previews/tkz/${TODAY_YM}" \
  "data/previews/stt/${TODAY_YM}" \
  "data/previews/sui/${TODAY_YM}" \
  "data/previews/original_exhibition/${TODAY_YM}"

# Materialize the working tree. Missing blobs are fetched on demand from the
# promisor remote (origin) thanks to --filter=blob:none.
git checkout "${GIT_BRANCH}"

# Ensure boatrace.git_operations push() can fetch + rebase. With depth=1 the
# rebase normally has nothing to do (we just cloned HEAD) but a credential
# helper covers the case where another job advanced origin in the meantime.
git config --local user.name "${GIT_USER_NAME}"
git config --local user.email "${GIT_USER_EMAIL}"
git config --local --replace-all "url.${REMOTE_WITH_TOKEN}.insteadOf" "${REMOTE_PUBLIC}"
# Prevent the token from leaking via `git remote -v` debug output.
git remote set-url origin "${REMOTE_PUBLIC}"

mkdir -p logs

log "Running scripts/preview-realtime.py ${PREVIEW_EXTRA_ARGS}"
# shellcheck disable=SC2086  # PREVIEW_EXTRA_ARGS is intentionally word-split.
python scripts/preview-realtime.py ${PREVIEW_EXTRA_ARGS}

log "Done"

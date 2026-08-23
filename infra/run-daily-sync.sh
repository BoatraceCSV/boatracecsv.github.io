#!/usr/bin/env bash
#
# Cloud Run Jobs entrypoint for daily-sync.
#
# Replaces .github/workflows/daily-sync.yml. Runs daily at JST 07:30 via
# Cloud Scheduler. Performs:
#   1. Fresh partial+sparse clone of main (paths needed by all 6 scripts).
#   2. Sequentially runs the daily Python scripts. Failures in any
#      individual script are logged but do not abort the run (mirrors the
#      `if: always() / continue-on-error: true` semantics of the original
#      GitHub Actions workflow).
#   3. Commits + pushes data/estimate/{predictor_id}/ for each active
#      predictor (build_index.py does not self-commit, unlike the other
#      scripts).
#   4. Calls boatrace.gcs_publisher to mirror today's CSVs to GCS and
#      emit one Pub/Sub message — same publisher path that
#      preview-realtime.py uses, ensuring fun-site picks up the daily
#      bootstrap immediately.
#
# Required env (injected by the Cloud Run Job spec):
#   GITHUB_TOKEN  - fine-grained PAT with Contents:Write on the repo (from
#                   Secret Manager: github-token).
#
# Optional env (sensible defaults):
#   GITHUB_REPO              owner/name of the GitHub repo
#   GIT_BRANCH               branch to clone and push to
#   GIT_USER_NAME            committer name
#   GIT_USER_EMAIL           committer email
#   RUN_DATE                 YYYY-MM-DD override for "today JST" — used for
#                            backfill via `gcloud run jobs execute
#                            --update-env-vars=RUN_DATE=2026-05-01`. When
#                            unset, today (JST) is used.
#   FORCE_OVERWRITE          "true"/"false" (default true). Currently the
#                            scripts are always invoked with --force to
#                            preserve the existing GHA workflow's behavior;
#                            this env var is a future hook.
#   BOATRACE_GCS_CSV_BUCKET  GCS bucket for fun-site mirror (no-op when unset)
#   BOATRACE_PUBSUB_TOPIC    Pub/Sub topic for fun-site (no-op when unset)
#
# Exit codes:
#   0  all steps succeeded (and publisher succeeded)
#   1  at least one step (or the publisher) failed; remaining steps still ran
#
# Idempotency: each Python script dedupes against the existing CSV row /
# file MD5, so re-execution after a transient failure is safe. The GCS
# publisher uses MD5 dedup as well.

set -Eeuo pipefail

log() {
  printf '[run-daily-sync %s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
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
GIT_USER_NAME="${GIT_USER_NAME:-daily-sync-bot}"
GIT_USER_EMAIL="${GIT_USER_EMAIL:-daily-sync-bot@users.noreply.github.com}"

# fun-site への CSV ミラーと Pub/Sub 通知用の環境変数。未設定なら
# scripts/boatrace/gcs_publisher.py が自動で no-op になる。preview-realtime
# と同じ publisher を末尾で 1 回呼び、daily の title/race_cards/index を
# fun-site に届ける。
export BOATRACE_GCS_CSV_BUCKET="${BOATRACE_GCS_CSV_BUCKET:-}"
export BOATRACE_PUBSUB_TOPIC="${BOATRACE_PUBSUB_TOPIC:-}"

# RUN_DATE で当日上書き。空なら JST 当日。backfill 時は
#   gcloud run jobs execute daily-sync --update-env-vars=RUN_DATE=2026-05-01
# のように指定する。
if [[ -n "${RUN_DATE:-}" ]]; then
  TODAY_JST="${RUN_DATE}"
  log "RUN_DATE override: TODAY_JST=${TODAY_JST}"
else
  TODAY_JST=$(TZ=Asia/Tokyo date +%Y-%m-%d)
fi
TODAY_YM=$(TZ=Asia/Tokyo date -d "${TODAY_JST}" +'%Y/%m')
PREV_YM=$(TZ=Asia/Tokyo date -d "$(TZ=Asia/Tokyo date -d "${TODAY_JST}" +'%Y-%m-15') -1 month" +'%Y/%m')
# モーターpt の素点は各場の直近 6 節 = 過去 90 日ぶんの race_cards / title を舐める
# (index_features.MOTOR_HISTORY_LOOKBACK_DAYS)。その 90 日が触る月を列挙して
# sparse-checkout に渡す。短い月が続くと 4 ヶ月前まで届くので
# (例: 5/1 の 90 日前は 1/31)、固定の月数ではなく実際の窓から求める。
MOTOR_LOOKBACK_DAYS=90
motor_lookback_months=()
_m=$(TZ=Asia/Tokyo date -d "${TODAY_JST} -${MOTOR_LOOKBACK_DAYS} days" +'%Y-%m-01')
while [[ "$(TZ=Asia/Tokyo date -d "${_m}" +'%Y/%m')" < "${TODAY_YM}" ]]; do
  motor_lookback_months+=("$(TZ=Asia/Tokyo date -d "${_m}" +'%Y/%m')")
  _m=$(TZ=Asia/Tokyo date -d "${_m} +1 month" +'%Y-%m-01')
done

# Active な予想者の ID リスト。scripts/boatrace/predictors/registry.py の
# ``active_predictors()`` と必ず同期させる (新規予想者追加時は両方更新)。
# sparse-checkout と commit パス展開、--all-active 後の add 対象に使用。
ACTIVE_PREDICTORS=(v1_basic v10_kimarite)  # 2026-08-22: v9_suji を退役 (B案 v10_kimarite と穴予想スロットが重複。回収率では差を示せず、主判定 log-loss に載る B案を残した) / 2026-08-13: 穴予想 B案 v10_kimarite を投入 (決まり手モデル。判定は 3連単 log-loss) / 2026-08-12: 穴予想 A案 v9_suji を投入 (control と同一成分・買い目のみ差分) / 2026-08-10: v4_motor/v5_slit を退役 (control 比で有意差なし + 買い目が control とほぼ重複) / 2026-08-09: v6_course/v7_aggregate/v8_aionly を退役 (control 比で有意に低回収率) / 2026-07-19: v2_tenkai/v3_tenkai を退役 (registry active_predictors() と同期)

WORKDIR="$(mktemp -d -t daily-sync.XXXXXX)"
cleanup() {
  rm -rf "${WORKDIR}" 2>/dev/null || true
}
trap cleanup EXIT

cd "${WORKDIR}"

REMOTE_WITH_TOKEN="https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPO}.git"
REMOTE_PUBLIC="https://github.com/${GITHUB_REPO}.git"

log "Cloning ${REMOTE_PUBLIC} (branch=${GIT_BRANCH}, partial+sparse, ym=${TODAY_YM}, prev=${PREV_YM}, motor-lookback=${motor_lookback_months[*]})"
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

# sparse-checkout: daily-sync の各スクリプトが読み書きする全パス。
#
#   - scripts/                                python sources
#   - .boatrace/                              runtime config (load_config)
#   - data/estimate/stadium/                  win_rate.csv / sui_params.csv /
#                                              weights/<predictor>/*.csv (build_index 入力)
#   - data/estimate/<predictor>/<YM>/         build_index 出力 (active 予想者ぶん)
#   - data/programs/race_cards/<YM>/          race-card.py 出力 (GCS ミラー対象)
#   - data/programs/recent_national/<YM>/     recent-form.py 出力 (build_index 特徴量)
#   - data/programs/recent_local/<YM>/        recent-form.py 出力
#   - data/programs/motor_stats/<YM>/         motor-stats.py 出力 (build_index 特徴量)
#   - data/programs/motor_stats/<PREV_YM>/    月初の 7 日 fallback 用
#   - data/programs/motor_history/<YM>/       motor-stats.py 出力 (bc_mrireki。
#   - data/programs/motor_history/<PREV_YM>/   パス日付=前節終了日のため前月分も対象)
#   - data/programs/waku10/<YM>/              race-card.py 出力 (bc_j_waku10)
#   - data/programs/monthly_schedule/         race-card.py 出力 (bc_mon_2。月ファイル
#                                              上書きのため dedup 用に既存分ごと取得)
#   - data/programs/title/<YM>/               race-title.py 出力 (GCS ミラー対象)
#   - data/programs/race_cards/<PREV_YM>/     build_racer_st.py の増分取り込みで
#                                              前日 (月初は前月末) の艇番→登録番号
#                                              紐付けに使用
#   - data/programs/{race_cards,title}/       モーターpt の素点が舐める 90 日
#     <motor_lookback_months>/                ルックバック (直近 6 節の検出と、
#                                              各節最終日の節間成績 + グレード)。
#                                              これが欠けると採用節数が落ちて
#                                              モーターpt が平均 50 側に潰れる
#   - data/results/realtime/<YM>,<PREV_YM>/   build_racer_st.py の増分取り込み対象
#                                              (前日結果)
#   - data/estimate/racer_st/                 build_racer_st.py の state.csv + 出力
sparse_paths=(
  scripts
  .boatrace
  data/estimate/stadium
  "data/programs/race_cards/${TODAY_YM}"
  "data/programs/waku10/${TODAY_YM}"
  data/programs/monthly_schedule
  "data/programs/recent_national/${TODAY_YM}"
  "data/programs/recent_local/${TODAY_YM}"
  "data/programs/motor_stats/${TODAY_YM}"
  "data/programs/motor_stats/${PREV_YM}"
  "data/programs/motor_history/${TODAY_YM}"
  "data/programs/motor_history/${PREV_YM}"
  "data/programs/title/${TODAY_YM}"
  "data/programs/race_cards/${PREV_YM}"
  "data/results/realtime/${TODAY_YM}"
  "data/results/realtime/${PREV_YM}"
  data/estimate/racer_st
  # 決まり手注釈テーブル (静的)。v9_suji 退役後も build_kimarite_picks.py が
  # kimarite_table.csv を読むので外さないこと (2026-08-22 退役)。
  data/estimate/suji/tables
  data/estimate/kimarite/tables
  "data/estimate/kimarite/${TODAY_YM}"
  "data/estimate/kimarite/picks/${TODAY_YM}"
  # モーターpt 素点の内訳 (build_motor_pt_breakdown.py 出力)
  "data/estimate/motor_pt/runs/${TODAY_YM}"
  "data/estimate/motor_pt/motors/${TODAY_YM}"
  "data/estimate/motor_pt/baseline/${TODAY_YM}"
)
for predictor in "${ACTIVE_PREDICTORS[@]}"; do
  sparse_paths+=("data/estimate/${predictor}/${TODAY_YM}")
done
# モーターpt の 90 日ルックバックぶん (当月は上で TODAY_YM を取得済み)
for ym in "${motor_lookback_months[@]}"; do
  sparse_paths+=("data/programs/race_cards/${ym}" "data/programs/title/${ym}")
done

git sparse-checkout init --cone
git sparse-checkout set "${sparse_paths[@]}"

git checkout "${GIT_BRANCH}"

# 各 Python スクリプトが boatrace.git_operations 経由で push できるよう
# credential / committer を設定。
git config --local user.name "${GIT_USER_NAME}"
git config --local user.email "${GIT_USER_EMAIL}"
git config --local --replace-all "url.${REMOTE_WITH_TOKEN}.insteadOf" "${REMOTE_PUBLIC}"
git remote set-url origin "${REMOTE_PUBLIC}"

mkdir -p logs

# ---------------------------------------------------------------------------
# run_step: 1 ステップ失敗してもジョブ全体を止めず、最終 exit code に集約する。
# 旧 GHA workflow の `if: always() / continue-on-error: true` の置き換え。
# ---------------------------------------------------------------------------
EXIT_AGGREGATE=0

run_step() {
  local label="$1"; shift
  log "STEP START: ${label}"
  local rc=0
  # `set -e` の影響を受けないよう if で包む。サブシェルにはしない
  # (cd / 環境変数の副作用を保つため)。
  if "$@"; then
    log "STEP OK: ${label}"
  else
    rc=$?
    log "STEP FAILED (rc=${rc}): ${label} — continuing"
    EXIT_AGGREGATE=1
  fi
  return 0
}

# ---------------------------------------------------------------------------
# 1-4. 当日 JST のデータ取得
#      boatcast 側の更新が間に合わない可能性があるが、失敗しても次回
#      (翌日 07:30) で再試行されるので EXIT_AGGREGATE には反映するが
#      後続ステップは続行する。
#
#      前日確定結果 (K-file) の取り込みは 2026-05 に廃止済み。代替の
#      準リアルタイム結果は preview-realtime.py が締切+3〜30 分の窓で
#      data/results/{realtime,payouts}/ に逐次追記している。
#
# 実行順:
#   1. race-title       → data/programs/title/<YM>/<DD>.csv を先に書く。
#                         以降の race-card / recent-form / motor-stats は
#                         getHoldingList2 API を一次ソースに使い、API が
#                         落ちている時のみ title CSV にフォールバックする
#                         (boatrace.holding_list.load_holding_from_title_csv)。
#                         先に title CSV を書いておけばネットワーク無しの
#                         過去日再生成や API 不通時にも完走できる。
#   2. race-card        → race_cards CSV。recent-form が艇↔登録番号マッピングを
#                         この CSV から読むため、recent-form より前に走らせる。
#   3. recent-form      → race-card の出力を消費する。
#   4. motor-stats      → 独立。
# ---------------------------------------------------------------------------
run_step "race-title"  python scripts/race-title.py  --date "${TODAY_JST}" --force
run_step "race-card"   python scripts/race-card.py   --date "${TODAY_JST}" --force
run_step "recent-form" python scripts/recent-form.py --date "${TODAY_JST}" --force
run_step "motor-stats" python scripts/motor-stats.py --date "${TODAY_JST}" --force

# ---------------------------------------------------------------------------
# 5. 当日 daily index バッチ生成 (全 active 予想者ぶん)
#    枠番・選手・モーター・暫定強さpt を埋めた index CSV を生成する。
#    展示・気象は 50 で補完 (状態=daily)。preview-realtime が JST 08:00 に
#    最初のサイクルを開始する前に完了させたい。
#    --all-active で active な予想者を全部ループする (registry 由来)。
# ---------------------------------------------------------------------------
run_step "build-index" python scripts/build_index.py --date "${TODAY_JST}" --mode daily --all-active

# ---------------------------------------------------------------------------
# 5.1. モーターpt 素点の内訳 CSV
#    build_index.py が ``N枠_モーターpt`` を出すときに内部で組み立てている
#    素点の計算過程 (生得点 → コース補正 z → 時間減衰 → ベイズ収縮) を明細として
#    書き出す。fun-site のモーターpt詳細ページが読む。
#
#    素点は全 24 場を横断したコース補正ベースラインに依存するため下流では
#    再現できない。選手pt に対する recent_national/recent_local と同じ
#    「内訳を配る」CSV にあたる。設計: docs/data/motor_pt.md
#
#    build_index と同じ 90 日ルックバックを使うので、上の sparse_paths に
#    ``motor_lookback_months`` ぶんの race_cards / title が入っている必要がある。
# ---------------------------------------------------------------------------
run_step "build-motor-pt-breakdown" \
  python scripts/build_motor_pt_breakdown.py --date "${TODAY_JST}" --force

# ---------------------------------------------------------------------------
# 5.5. 選手別 推定ST (racer_st) の日次生成
#    state.csv を前日結果まで増分更新し、当日レースの推定ST CSV を出力する。
#    fun-site の 1マーク予想が読む (docs/design/st_estimation.md)。
#    初回は state.csv が無く失敗する → ローカルで --rebuild を 1 度実行して
#    state.csv をコミットしてから有効になる (build_racer_st.py docstring 参照)。
#
#    2026-08-10 時点で推定ST を予想に使う予想者 (v5_slit / v7_aggregate) は
#    いずれも退役済み。state.csv は増分更新のため止めると再開時に穴が空くので
#    生成は継続する (再挑戦の余地を残す)。ただし推定 ST の精度改善そのものは
#    投資対効果が低いと結論済み (docs/design/slit_tenkai.md)。
# ---------------------------------------------------------------------------
run_step "build-racer-st" python scripts/build_racer_st.py --date "${TODAY_JST}"

# ---------------------------------------------------------------------------
# 荒れ度メーター (決まり手セルの確率)。学習済み係数から推論するだけなので
# sklearn 非依存・数十ミリ秒。直前バッチが同じ CSV に realtime 行を upsert する。
# 設計: docs/design/ana_prediction.md §14
# ---------------------------------------------------------------------------
run_step "build-kimarite-probs" python scripts/build_kimarite_probs.py --date "${TODAY_JST}" --mode daily

# ---------------------------------------------------------------------------
# 穴予想 v10_kimarite の当日買い目 (状態=daily)。build_index.py と
# build_kimarite_probs.py の**両方**の後に走らせる (強さpt と Stage1 の確率を
# 読むため)。設計: docs/design/ana_prediction.md §4.3
# ---------------------------------------------------------------------------
run_step "build-kimarite-picks" python scripts/build_kimarite_picks.py --date "${TODAY_JST}" --mode daily

# ---------------------------------------------------------------------------
# build_index.py は git にコミットしないため、ここで明示 commit/push する。
# 旧 GHA workflow の "Commit Daily Index" ステップ相当。
# 失敗を握り潰す: コミットが無い場合は no-op、push 競合は次回で吸収。
# 各 active 予想者の出力ディレクトリを add 対象に列挙する。
# ---------------------------------------------------------------------------
commit_and_push_index() {
  for predictor in "${ACTIVE_PREDICTORS[@]}"; do
    git add "data/estimate/${predictor}/"
  done
  # racer_st: 当日出力 + state.csv (build_racer_st.py も git にコミットしない)
  git add data/estimate/racer_st/
  # 荒れ度メーター + 穴予想 v10_kimarite の買い目 (どちらも git にコミットしない)
  git add data/estimate/kimarite/
  # モーターpt 素点の内訳 (build_motor_pt_breakdown.py も git にコミットしない)
  git add data/estimate/motor_pt/
  if git diff --cached --quiet; then
    log "No daily index changes to commit"
    return 0
  fi
  git commit -m "Update daily index batch (${TODAY_JST})"
  git push origin "${GIT_BRANCH}"
}
run_step "commit-index" commit_and_push_index

# ---------------------------------------------------------------------------
# GCS ミラー / Pub/Sub publish (Open Question 1: 有効化)
# preview-realtime.py が呼ぶのと同じ publisher 経路を 1 回叩く。
# - upload_csvs: title/race_cards/recent_national/recent_local/waku10/motor_stats
#   /motor_pt(runs,motors,baseline)
#   /previews(stt,tkz,sui,original_exhibition,tokuten_hayami)/index/results を
#   MD5 dedup でアップロード
# - assemble_updated_races: 更新があった race code を集約 (title/race_cards
#   が変わっていれば trigger は "daily-bootstrap")
# - publish_realtime_completed: 1 件だけ Pub/Sub に publish
#
# 環境変数 (BOATRACE_GCS_CSV_BUCKET / BOATRACE_PUBSUB_TOPIC) が未設定なら
# それぞれ早期 return で no-op。realtime_updated_codes は daily-sync では
# 空集合を渡す (リアルタイム更新は preview-realtime の責務)。
# ---------------------------------------------------------------------------
publish_to_gcs() {
  python <<'PYEOF'
import os
import sys
from datetime import date
from pathlib import Path

# scripts/ をパスに追加して boatrace package を import 可能にする。
sys.path.insert(0, str(Path("scripts").resolve()))

from boatrace.gcs_publisher import (  # noqa: E402
    upload_csvs,
    assemble_updated_races,
    publish_realtime_completed,
)

day = date.fromisoformat(os.environ["TODAY_JST"])
repo = Path(".").resolve()

upload_results = upload_csvs(repo, day)
updated_races, trigger = assemble_updated_races(
    repo,
    day,
    upload_results,
    realtime_updated_codes=set(),
    result_updated_codes=None,
)
print(f"[publisher] uploaded={len(upload_results)} updated_races={len(updated_races)} trigger={trigger}")

if updated_races:
    msg_id = publish_realtime_completed(day, updated_races, trigger)
    print(f"[publisher] published message_id={msg_id}")
else:
    print("[publisher] no race updates to publish")
PYEOF
}
TODAY_JST="${TODAY_JST}" run_step "gcs-publish" publish_to_gcs

log "Done (aggregate exit=${EXIT_AGGREGATE})"
exit "${EXIT_AGGREGATE}"

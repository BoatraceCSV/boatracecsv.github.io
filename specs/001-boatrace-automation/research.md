# Phase 0: Research Findings

**Date**: 2026-01-01
**Purpose**: Resolve NEEDS CLARIFICATION items from plan.md

## 1. CSV Header Mapping

### Results CSV Header (results.csv)

**Total Columns**: 91

```
レースコード,タイトル,日次,レース日,レース場,レース回,レース名,距離(m),天候,風向,風速(m),波の高さ(cm),決まり手,単勝_艇番,単勝_払戻金,複勝_1着_艇番,複勝_1着_払戻金,複勝_2着_艇番,複勝_2着_払戻金,2連単_組番,2連単_払戻金,2連単_人気,2連複_組番,2連複_払戻金,2連複_人気,拡連複_1-2着_組番,拡連複_1-2着_払戻金,拡連複_1-2着_人気,拡連複_1-3着_組番,拡連複_1-3着_払戻金,拡連複_1-3着_人気,拡連複_2-3着_組番,拡連複_2-3着_払戻金,拡連複_2-3着_人気,3連単_組番,3連単_払戻金,3連単_人気,3連複_組番,3連複_払戻金,3連複_人気,1着_着順,1着_艇番,1着_登録番号,1着_選手名,1着_モーター番号,1着_ボート番号,1着_展示タイム,1着_進入コース,1着_スタートタイミング,1着_レースタイム,2着_着順,2着_艇番,2着_登録番号,2着_選手名,2着_モーター番号,2着_ボート番号,2着_展示タイム,2着_進入コース,2着_スタートタイミング,2着_レースタイム,3着_着順,3着_艇番,3着_登録番号,3着_選手名,3着_モーター番号,3着_ボート番号,3着_展示タイム,3着_進入コース,3着_スタートタイミング,3着_レースタイム,4着_着順,4着_艇番,4着_登録番号,4着_選手名,4着_モーター番号,4着_ボート番号,4着_展示タイム,4着_進入コース,4着_スタートタイミング,4着_レースタイム,5着_着順,5着_艇番,5着_登録番号,5着_選手名,5着_モーター番号,5着_ボート番号,5着_展示タイム,5着_進入コース,5着_スタートタイミング,5着_レースタイム,6着_着順,6着_艇番,6着_登録番号,6着_選手名,6着_モーター番号,6着_ボート番号,6着_展示タイム,6着_進入コース,6着_スタートタイミング,6着_レースタイム
```

**Section Breakdown**:
- **Basic Race Info** (13 cols): レースコード ～ 決まり手
- **Betting Results** (26 cols): 単勝_艇番 ～ 3連複_人気
- **Racer Details** (52 cols): 1着_着順 ～ 6着_レースタイム (6 racers × 10 fields + (1-6)着 header fields)

**Decision**: Use this exact header structure in Python. Store as constant in `boatrace/converter.py`.

---

### Program CSV Header (timetable.csv)

**Total Columns**: 218

Structure: 8 basic columns + 6 frames × 35 columns per frame

```
タイトル,日次,レース日,レース場,レース回,レース名,距離(m),電話投票締切予定,[1枠35cols],[2枠35cols],...[6枠35cols]
```

**Per-Frame Columns** (35 each):
- 艇番, 登録番号, 選手名, 年齢, 支部, 体重, 級別, 全国勝率, 全国2連対率, 当地勝率, 当地2連対率, モーター番号, モーター2連対率, ボート番号, ボート2連対率, 今節成績_1-1, 今節成績_1-2, 今節成績_2-1, 今節成績_2-2, 今節成績_3-1, 今節成績_3-2, 今節成績_4-1, 今節成績_4-2, 今節成績_5-1, 今節成績_5-2, 今節成績_6-1, 今節成績_6-2, 早見 (28 cols) + frame prefix for each (7 cols) = 35

**Decision**: Implement dynamic header generation in `boatrace/converter.py` using template strings. Support 6-frame structure directly.

---

## 2. Error Recovery Strategy

### Decision: Exponential Backoff with Circuit Breaker

**Rationale**:
- Official boatrace server may have temporary unavailability or rate limiting
- Need to respect 3+ second interval requirement
- Distinguish between transient (retry) and permanent (skip) failures

**Implementation Parameters**:
- **Max Retries**: 3 attempts per file
- **Initial Backoff**: 5 seconds
- **Backoff Multiplier**: 2x (5s → 10s → 20s)
- **Max Backoff**: 30 seconds
- **Timeout**: 30 seconds per request
- **Retryable HTTP Status Codes**: 408, 429, 500, 502, 503, 504
- **Permanent Failure Status Codes**: 400, 401, 403, 404

**Circuit Breaker**:
- Track consecutive failures across all files in session
- If 5+ consecutive failures: enter "degraded" mode (skip remaining dates, report summary)
- If 10+ consecutive failures: exit with error code

**Alternatives Rejected**:
- No retry at all: Too fragile, would miss valid data on transient failures
- Fixed backoff: Doesn't account for increasing server load

---

## 3. Logging Format

### Decision: Structured JSON Logging

**Rationale**:
- GitHub Actions integrates well with JSON logs
- Easy to parse and analyze in monitoring systems
- Supports both stdout and file-based logging

**Implementation**:
```python
# Format: JSON with timestamp, level, context
{
  "timestamp": "2026-01-01T15:10:05.123Z",
  "level": "INFO|WARNING|ERROR",
  "event": "download_start|download_success|download_failed|parse_error",
  "date": "2025-12-01",
  "file_type": "K|B",
  "url": "http://...",
  "retry_count": 0,
  "error": "Optional error message",
  "duration_ms": 1234
}
```

**Log Destinations**:
- **stdout**: All logs (for GitHub Actions web UI viewing)
- **File**: `logs/boatrace-{YYYY-MM-DD}.json` (for debugging)

**Log Levels**:
- INFO: Download started, file processed, summary
- WARNING: Retry attempt, file skipped, missing data
- ERROR: Download failed permanently, parse error, git operation failed

**Alternatives Rejected**:
- Plain text logs: Harder to parse and analyze at scale
- No logging: Makes debugging failures impossible

---

## 4. Git Configuration in GitHub Actions

### Decision: Use GitHub's Built-in Credentials

**Rationale**:
- GitHub Actions provides automatic `GITHUB_TOKEN` secret
- No need to configure manual git user
- Simplifies workflow setup

**Implementation**:
```bash
# In GitHub Actions workflow:
git config --local user.email "action@github.com"
git config --local user.name "GitHub Action"
git config --local credential.helper store

# Authentication via GITHUB_TOKEN (automatic in checkout action)
```

**Alternative Approach** (if needed for different repository):
```bash
git config --local user.email "${{ secrets.GIT_EMAIL }}"
git config --local user.name "${{ secrets.GIT_USER }}"
```

**Alternatives Rejected**:
- Manual SSH key management: Too complex, unnecessary
- Personal access token: Overkill for automation account

---

## 5. Date/Time Handling: 00:10 JST to UTC Conversion

### Decision: Schedule at 15:10 UTC daily

**Calculation**:
- JST = UTC+9
- 00:10 JST = 00:10 - 09:00 = 15:10 previous day UTC
- Example: 2026-01-01 00:10 JST = 2025-12-31 15:10 UTC

**Implementation in GitHub Actions**:
```yaml
# cron in UTC (GitHub Actions standard)
schedule:
  - cron: '10 15 * * *'  # Every day at 15:10 UTC = 00:10 JST next day
```

**Application Date Logic**:
```python
from datetime import datetime, timezone, timedelta

# Workflow runs at 15:10 UTC (= 00:10 JST next day)
# Fetch "previous day" results in UTC = current day in JST
# Fetch "current day" program in UTC = current day + 1 in JST (confusing!)
#
# CORRECTION: When workflow runs at 15:10 UTC on day D:
# - In JST, it's 00:10 on day D+1
# - "Previous day" results = day D (in UTC) = already complete in JST
# - "Current day" program = day D+1 (in UTC) = scheduled for day D+1 JST

now_utc = datetime.now(timezone.utc)
now_jst = now_utc + timedelta(hours=9)

results_date = (now_utc - timedelta(days=1)).date()  # Yesterday's results
program_date = now_utc.date()  # Today's program
```

**Alternatives Considered**:
- Run at different UTC time: 15:10 is clearest and aligns with JST need
- Run twice daily: Unnecessary complexity

---

## Research Summary

✅ **All NEEDS CLARIFICATION items resolved**:

1. **CSV Headers**: Extracted exact headers (91 cols results, 218 cols program)
2. **Error Recovery**: 3-attempt exponential backoff with circuit breaker
3. **Logging**: JSON structured logs to stdout + file
4. **Git Config**: Use GitHub Actions built-in `GITHUB_TOKEN`
5. **Timezone**: Schedule at 15:10 UTC = 00:10 JST

**Ready for Phase 1: Design & Contracts**

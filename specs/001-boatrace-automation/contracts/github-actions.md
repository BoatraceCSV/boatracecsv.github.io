# GitHub Actions Workflow Contract

**Phase**: 1 (Design & Contracts)
**Purpose**: Define automated workflow for daily data collection and publication

## Workflow Name

`daily-sync.yml` - Daily Boatrace Data Sync

## Triggers

### Schedule Trigger (Primary)

```yaml
schedule:
  - cron: '10 15 * * *'  # Every day at 15:10 UTC = 00:10 JST
```

**Timezone**: UTC (GitHub Actions standard)

**Effective Time**: 00:10 JST (Japan Standard Time) next day

### Manual Trigger (Optional)

```yaml
workflow_dispatch:
  inputs:
    start_date:
      description: 'Start date (YYYY-MM-DD)'
      required: false
      default: ''
    end_date:
      description: 'End date (YYYY-MM-DD)'
      required: false
      default: ''
    force_overwrite:
      description: 'Force overwrite existing files'
      required: false
      default: false
      type: boolean
```

---

## Workflow Steps

### Setup Phase

**Step 1.1: Checkout Code**
```yaml
- uses: actions/checkout@v4
  with:
    fetch-depth: 1
```

**Step 1.2: Setup Python**
```yaml
- uses: actions/setup-python@v4
  with:
    python-version: '3.11'
    cache: 'pip'
```

**Step 1.3: Install Dependencies**
```yaml
- run: |
    pip install -r scripts/requirements.txt
    # Installs: requests, lhafile
```

**Step 1.4: Configure Git**
```yaml
- run: |
    git config --local user.email "action@github.com"
    git config --local user.name "GitHub Action"
```

---

### Execution Phase

**Step 2.1: Run Daily Sync** (Default - Scheduled)
```yaml
- name: Fetch and Convert Daily Data
  run: |
    python scripts/fetch-and-convert.py
  if: github.event_name == 'schedule'
```

**Step 2.2: Run with Manual Parameters** (When Triggered)
```yaml
- name: Fetch and Convert (Manual)
  run: |
    ARGS=""
    if [ -n "${{ github.event.inputs.start_date }}" ]; then
      ARGS="--start-date ${{ github.event.inputs.start_date }}"
    fi
    if [ -n "${{ github.event.inputs.end_date }}" ]; then
      ARGS="$ARGS --end-date ${{ github.event.inputs.end_date }}"
    fi
    if [ "${{ github.event.inputs.force_overwrite }}" = "true" ]; then
      ARGS="$ARGS --force"
    fi
    python scripts/fetch-and-convert.py $ARGS
  if: github.event_name == 'workflow_dispatch'
```

---

### Reporting Phase

**Step 3.1: Capture Workflow Status**
```yaml
- name: Check Job Status
  if: always()
  run: |
    if [ ${{ job.status }} = "success" ]; then
      echo "WORKFLOW_STATUS=SUCCESS" >> $GITHUB_ENV
    else
      echo "WORKFLOW_STATUS=FAILURE" >> $GITHUB_ENV
    fi
```

**Step 3.2: Upload Logs** (If Available)
```yaml
- name: Upload Logs
  if: always()
  uses: actions/upload-artifact@v3
  with:
    name: boatrace-logs-${{ github.run_id }}
    path: logs/
    retention-days: 30
    if-no-files-found: ignore
```

**Step 3.3: Comment on GitHub Issue** (Optional - For Failures)
```yaml
- name: Report Failure
  if: failure()
  uses: actions/github-script@v6
  with:
    script: |
      github.rest.issues.createComment({
        issue_number: context.issue.number,
        owner: context.repo.owner,
        repo: context.repo.repo,
        body: `⚠️ Boatrace data sync failed on ${new Date().toISOString()}. Check logs in workflow artifacts.`
      })
```

---

## Environment Configuration

### Repository Secrets

**Required**:
- `GITHUB_TOKEN` (provided automatically by GitHub Actions)

**Optional**:
- `GIT_USER_EMAIL` (defaults to action@github.com)
- `GIT_USER_NAME` (defaults to GitHub Action)

### Workflow Environment Variables

```yaml
env:
  PYTHONUNBUFFERED: 1
  LOG_LEVEL: INFO
```

---

## GitHub Pages Integration

### Site Configuration (in repository root or gh-pages branch)

**File**: `_config.yml`

```yaml
# GitHub Pages will serve /data directory as static files
include: [".gitignore"]
exclude: ["scripts/", "tests/", ".github/", ".specify/"]
```

### Publishing Strategy

**Option A: Serve from main branch /data directory**

Configured in GitHub repo settings:
- Settings → Pages → Source: Deploy from a branch
- Branch: `main`
- Folder: `/ (root)`

CSV files are served directly from `data/` directory at URLs like:
- `https://owner.github.io/boatrace-data/data/results/2025/12/01.csv`
- `https://owner.github.io/boatrace-data/data/programs/2025/12/02.csv`

---

## Success Criteria

### Workflow Success

✅ Workflow runs at scheduled time (00:10 JST ± 5 minutes)
✅ All steps complete without errors (exit code 0)
✅ CSV files are committed and pushed
✅ GitHub Pages reflects new files within 2 minutes

### Data Quality

✅ CSV files are valid and parseable
✅ No missing columns in output
✅ All 6 racers present per race
✅ At least 90% of races have complete betting information

### Reliability

✅ Workflow succeeds 99% of scheduled runs
✅ Partial failures (some dates) don't block all processing
✅ Failed runs are logged and reportable

---

## Failure Handling

### Retryable Failures (Automatic)

- Network timeouts on download: Retry with exponential backoff (up to 3 times)
- Transient HTTP errors (5xx): Retry with backoff
- Temporary git push failures: Retry once

### Non-Retryable Failures (Skip & Continue)

- HTTP 404: File not available (no data for date)
- HTTP 403: Access forbidden (skip gracefully)
- Parse errors in fixed-width format: Log and skip
- CSV write failures: Log and skip that file

### Critical Failures (Stop & Alert)

- Git authentication failure: Stop, exit with error
- Git push rejection: Stop, exit with error
- Unrecoverable parsing errors (>50% of files): Stop and alert

### Alerting Strategy

**Slack** (Optional integration):
```yaml
- name: Send Slack Notification
  if: failure()
  uses: slackapi/slack-github-action@v1
  with:
    webhook-url: ${{ secrets.SLACK_WEBHOOK }}
    payload: |
      {
        "text": "⚠️ Boatrace data sync failed",
        "blocks": [
          {
            "type": "section",
            "text": {
              "type": "mrkdwn",
              "text": "*Boatrace Data Sync Failed*\nWorkflow: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}"
            }
          }
        ]
      }
```

---

## Performance Targets

| Metric | Target | Constraint |
|--------|--------|-----------|
| Daily execution time | < 5 minutes | For typical 2 files (K+B) |
| CSV file size | 100-500 KB | Per file, depends on races |
| Git push latency | < 2 seconds | After CSV write completes |
| GitHub Pages update | < 2 minutes | After push succeeds |
| Backfill (3 years) | < 60 minutes | 1,095 dates × ~3.3s per date |

---

## Maintenance & Monitoring

### Health Checks

Periodic (weekly) manual run:
```bash
# Test workflow manually
gh workflow run daily-sync.yml --ref main
```

### Log Rotation

Automatic via GitHub Actions:
- Artifact retention: 30 days
- Logs directory: `logs/boatrace-YYYY-MM-DD.json`

### Debugging

When workflow fails:
1. Check GitHub Actions run logs
2. Download artifact logs from workflow
3. Review error messages and timestamps in `logs/`
4. Manually run workflow with specific date range for reproduction

---

## Cost Considerations

### GitHub Actions Usage

- Scheduled runs: 365 runs/year × ~2 minutes = ~730 minutes/year
- Free tier includes 2,000 minutes/month
- **Status**: Well within free tier

### GitHub Pages Hosting

- Static site hosting: Free
- Bandwidth: Unlimited
- Build time: ~30 seconds per push

**Estimated Cost**: $0/month (fully free tier)

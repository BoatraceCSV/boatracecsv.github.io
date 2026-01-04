# CLI Contract Specification

**Phase**: 1 (Design & Contracts)
**Purpose**: Define command-line interface for boatrace data automation

## Main Script: fetch-and-convert.py

Entry point for daily automation and one-time backfill operations.

### Signature

```bash
python scripts/fetch-and-convert.py [--start-date DATE] [--end-date DATE] [--mode MODE] [--force] [--dry-run]
```

### Arguments

| Argument | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `--start-date` | DATE (YYYY-MM-DD) | yesterday | No | Start of date range (inclusive). Default: yesterday for daily mode. |
| `--end-date` | DATE (YYYY-MM-DD) | today | No | End of date range (inclusive). Default: today for daily mode. |
| `--mode` | STRING | "daily" | No | Execution mode: "daily" (fetch yesterday+today) or "backfill" (full range). |
| `--force` | FLAG | false | No | Overwrite existing CSV files instead of skipping. Use with caution. |
| `--dry-run` | FLAG | false | No | Run without writing files or committing. For testing. |
| `--help` | FLAG | - | No | Display help and exit. |
| `--version` | FLAG | - | No | Display version and exit. |

### Exit Codes

```
0   - Success: All files processed, commit pushed (or no new files)
1   - Partial Failure: Some files succeeded, some failed. CSV files written, commit pushed.
2   - Critical Error: Git operations failed, retry needed. CSV may be incomplete.
3   - Configuration Error: Invalid arguments or missing environment variables.
```

### Examples

#### Daily Automation (GitHub Actions scheduled)

```bash
# Fetch yesterday's results and today's program
python scripts/fetch-and-convert.py
# Equivalent to: --start-date [yesterday] --end-date [today] --mode daily
```

#### Historical Backfill

```bash
# Process 3 years of historical data
python scripts/fetch-and-convert.py \
  --start-date 2022-01-01 \
  --end-date 2024-12-31 \
  --mode backfill
```

#### Force Refresh Specific Dates

```bash
# Re-download and overwrite data for specific dates
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-05 \
  --force
```

#### Dry Run (Testing)

```bash
# Test without writing files
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --dry-run
```

---

## Output & Logging

### Standard Output

```
[2025-12-01 15:10:05] Starting fetch-and-convert (mode: daily, dry-run: false)
[2025-12-01 15:10:05] Processing dates: 2025-11-30 to 2025-12-01
[2025-12-01 15:10:05] Downloading K-file for 2025-11-30...
[2025-12-01 15:10:08] Downloaded 2025-11-30 K-file (45 KB)
[2025-12-01 15:10:08] Decompressing 2025-11-30 K-file...
[2025-12-01 15:10:09] Decompressed: K250130.TXT (156 races)
[2025-12-01 15:10:12] Converting K250130.TXT to CSV...
[2025-12-01 15:10:14] Converted: data/results/2025/11/30.csv (156 rows)
...
[2025-12-01 15:10:45] Git: Committing 2 files...
[2025-12-01 15:10:47] Git: Pushing to main...
[2025-12-01 15:10:52] ✓ COMPLETED SUCCESSFULLY
  - Downloaded files: 4
  - Converted files: 4
  - CSV files created: 4
  - Git commit: abc1234
  - Push status: SUCCESS
```

### Log File

**Location**: `logs/boatrace-YYYY-MM-DD.json` (one per execution day)

**Format**: Newline-delimited JSON (NDJSON)

```json
{"timestamp":"2025-12-01T15:10:05.123Z","level":"INFO","event":"start","mode":"daily"}
{"timestamp":"2025-12-01T15:10:05.234Z","level":"INFO","event":"download_start","date":"2025-11-30","file_type":"K"}
{"timestamp":"2025-12-01T15:10:08.456Z","level":"INFO","event":"download_success","date":"2025-11-30","file_type":"K","size_bytes":45000,"duration_ms":3222}
{"timestamp":"2025-12-01T15:10:09.567Z","level":"INFO","event":"decompress_success","date":"2025-11-30","file_type":"K","races":156}
{"timestamp":"2025-12-01T15:10:14.789Z","level":"INFO","event":"convert_success","date":"2025-11-30","file_type":"K","rows":156,"duration_ms":5222}
{"timestamp":"2025-12-01T15:10:47.901Z","level":"INFO","event":"git_push_success","commit":"abc1234","files":4}
{"timestamp":"2025-12-01T15:10:52.012Z","level":"INFO","event":"completed","status":"SUCCESS","total_files":4}
```

---

## Backfill Script: backfill-historical.sh

One-time shell script for historical data initialization.

### Signature

```bash
bash scripts/backfill-historical.sh --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--force]
```

### Arguments

| Argument | Type | Default | Required | Description |
|----------|------|---------|----------|-------------|
| `--start-date` | DATE | - | Yes | Start date (inclusive) |
| `--end-date` | DATE | - | Yes | End date (inclusive) |
| `--force` | FLAG | false | No | Overwrite existing files |

### Exit Codes

```
0   - Success
1   - Partial failure (some dates skipped)
2   - Critical error
```

### Examples

```bash
# Backfill 3 years
bash scripts/backfill-historical.sh \
  --start-date 2022-01-01 \
  --end-date 2024-12-31

# Backfill with force overwrite
bash scripts/backfill-historical.sh \
  --start-date 2025-01-01 \
  --end-date 2025-03-31 \
  --force
```

### Output

```
Starting historical backfill: 2022-01-01 to 2024-12-31
Total dates to process: 1095

Processing: [████████████████████░░░░░░░░░░░░░░░░] 50%  (547/1095)
  - Files downloaded: 547
  - Files failed: 2
  - Files skipped: 0

Processing: [████████████████████████████████████] 100% (1095/1095)
  - Total files processed: 1095
  - Successfully converted: 1093
  - Failed to convert: 2
  - Already existed (skipped): 0
  - CSV files created: 2186 (results + programs)

Committing backfilled data...
Git commit: xyz9876
Git push: SUCCESS

Backfill completed successfully!
```

---

## Environment Variables

### GitHub Actions Secrets (Required in Workflow)

```yaml
GITHUB_TOKEN     # Provided by GitHub Actions automatically
GIT_USER_EMAIL   # Optional: defaults to "action@github.com"
GIT_USER_NAME    # Optional: defaults to "GitHub Action"
```

### Configuration via File (Optional)

**File**: `.boatrace/config.json`

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

## Error Handling

### User-Facing Error Messages

**Invalid Date Format**:
```
Error: Invalid date format '--start-date 2025/12/01'. Use YYYY-MM-DD format.
```

**Date Range Invalid**:
```
Error: Start date (2025-12-05) must be before or equal to end date (2025-12-01).
```

**Missing Required Argument**:
```
Error: Backfill mode requires --start-date and --end-date arguments.
Usage: python scripts/fetch-and-convert.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

**Network Timeout**:
```
Warning: Download for 2025-11-30 K-file failed after 3 retries (timeout). Skipping.
```

**Git Authentication Failed**:
```
Error: Git push failed: Authentication failed. Check GITHUB_TOKEN configuration.
Exit code: 2
```

---

## Data Integrity Checks

### Pre-Write Validation

- CSV row count matches expected races
- All required columns present and non-empty
- No missing racer information (6 racers per race)

### Post-Write Verification

- File size > 0 KB
- CSV parseable by Python csv module
- Header row matches expected structure

### Git Verification

- All CSV files staged before commit
- Commit message follows format: "Update boatrace data: YYYY-MM-DD"
- Push successful (no merge conflicts)

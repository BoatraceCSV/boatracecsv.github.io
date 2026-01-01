# Quickstart Guide for Developers

**Phase**: 1 (Design & Contracts)
**Purpose**: Get developers up to speed quickly on the boatrace automation system

---

## Architecture Overview

```
Official Boatrace Server (http://www1.mbrace.or.jp)
  ↓ (Download K-files & B-files)
  ↓
Python Script: fetch-and-convert.py
  ├─ boatrace/downloader.py      (HTTP downloads, retry logic)
  ├─ boatrace/extractor.py       (LZH decompression)
  ├─ boatrace/parser.py          (Fixed-width text parsing)
  ├─ boatrace/converter.py       (Text → CSV conversion)
  ├─ boatrace/storage.py         (File I/O)
  ├─ boatrace/git_operations.py  (Git commit/push)
  └─ boatrace/logger.py          (Structured logging)
  ↓
CSV Files
  ├─ data/results/YYYY/MM/DD.csv    (Race results)
  └─ data/programs/YYYY/MM/DD.csv   (Race programs)
  ↓ (Git commit & push)
  ↓
GitHub Repository
  ↓ (GitHub Pages serves static files)
  ↓
https://owner.github.io/data/results/2025/12/01.csv
```

---

## Local Development Setup

### Prerequisites

- Python 3.8+
- git
- pip (included with Python)

### Installation

```bash
# Clone repository
git clone https://github.com/your-org/boatrace-data.git
cd boatrace-data

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r scripts/requirements.txt

# Verify installation
python -c "import requests, lhafile; print('✓ Dependencies OK')"
```

---

## Running Locally

### Daily Sync (Test Run)

```bash
# Fetch yesterday's results and today's program
python scripts/fetch-and-convert.py --dry-run

# Output:
# [2025-12-01 15:10:05] Starting fetch-and-convert (mode: daily, dry-run: true)
# [2025-12-01 15:10:05] Processing dates: 2025-11-30 to 2025-12-01
# ... (processing logs)
# [2025-12-01 15:10:52] ✓ COMPLETED SUCCESSFULLY (dry-run - no files written)
```

### Process Specific Dates

```bash
# Convert data for specific dates
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-03

# Force re-process existing files
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --force
```

### Historical Backfill

```bash
# One-time initialization with 3 years of historical data
bash scripts/backfill-historical.sh \
  --start-date 2022-01-01 \
  --end-date 2024-12-31

# This will take ~30-60 minutes depending on network
```

---

## Code Structure

### Main Entry Point

**File**: `scripts/fetch-and-convert.py`

```python
def main():
    args = parse_arguments()
    session = ConversionSession(start_date=args.start_date, end_date=args.end_date)

    for date in session.date_range():
        # Download K-file and B-file
        # Decompress and parse
        # Convert to CSV
        # Write to disk

    if session.csv_files_created:
        git_commit_and_push(session.csv_files_created)

    print_summary(session)
    return session.exit_code()
```

### Core Modules

#### boatrace/downloader.py
- `download_file(url, max_retries=3)` → bytes
- Handles retry logic with exponential backoff
- Rate limiting (3+ second intervals)

#### boatrace/extractor.py
- `extract_lzh(lzh_bytes)` → dict {filename: text_content}
- Decompresses LZH file and returns inner files
- Validates decompression success

#### boatrace/parser.py
- `parse_result_file(text_content)` → List[RaceResult]
- `parse_program_file(text_content)` → List[RaceProgram]
- Fixed-width format parsing from Shift-JIS text

#### boatrace/converter.py
- `races_to_csv(races: List[RaceResult])` → str
- `programs_to_csv(programs: List[RaceProgram])` → str
- Returns CSV string (header + rows)

#### boatrace/storage.py
- `write_csv(path, csv_content)` → bool
- Creates directories if needed
- Validates before writing

#### boatrace/git_operations.py
- `git_commit_and_push(files)` → bool
- Stages, commits, and pushes files
- Handles authentication via GITHUB_TOKEN

#### boatrace/logger.py
- `log_info(event, **context)` → None
- `log_error(event, **context)` → None
- Outputs JSON to stdout + file

---

## Testing

### Unit Tests

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_parser.py

# Run with coverage
pytest --cov=boatrace tests/unit/
```

### Example Test

```python
# tests/unit/test_parser.py
def test_parse_result_file_basic():
    # Use fixture: example K-file content
    text = load_fixture("K250605.txt")

    results = parse_result_file(text)

    assert len(results) == 12  # 12 races per file typically
    assert results[0].stadium == "唐津"
    assert results[0].race_round == "01R"
    assert len(results[0].racers) == 6
```

### Integration Tests

```bash
# End-to-end test with sample data
pytest tests/integration/test_end_to_end.py

# Fetches real data for a single date
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --dry-run
```

---

## Common Tasks

### Add Support for New Boatrace Stadium

Edit `boatrace/parser.py`:
```python
STADIUM_CODE = {
    # ... existing entries ...
    '新しい場所': 'XXX',  # Add new entry
}
```

Update `data-model.md` if new stadium requires different field handling.

### Change CSV Header Order

Edit `boatrace/converter.py`:
```python
RESULTS_HEADER = [
    'race_code', 'title', 'day', ...  # Modify order here
]
```

Update spec in `contracts/cli.md` to reflect new schema.

### Increase Rate Limiting

Edit `.boatrace/config.json`:
```json
{
  "rate_limit_interval_seconds": 5  // Changed from 3
}
```

### Change Logging Level

Via environment variable:
```bash
LOG_LEVEL=DEBUG python scripts/fetch-and-convert.py
```

Or in `config.json`:
```json
{
  "log_level": "DEBUG"
}
```

---

## Troubleshooting

### Issue: "HTTP 404 for date 2025-12-01"

**Cause**: No boatrace events scheduled for that date (weekend/holiday)

**Solution**: This is expected. System logs and skips such dates.

**Verification**:
```bash
grep "404" logs/boatrace-2025-12-01.json
# Should show: "event": "download_failed", "status_code": 404
```

### Issue: "Parse error: Fixed-width format mismatch"

**Cause**: File format changed or corrupted download

**Solution**:
```bash
# Re-download with force flag
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --force
```

### Issue: "Git authentication failed"

**Cause**: Missing or invalid GITHUB_TOKEN

**Solution** (Local testing):
```bash
# Configure git manually
git config user.email "your-email@example.com"
git config user.name "Your Name"
git remote set-url origin https://your-token@github.com/owner/repo.git
```

### Issue: "LZH decompression failed"

**Cause**: Invalid or corrupted .lzh file

**Solution**: Wait for next day's run (tomorrow's data will be available) or manually download from official site for debugging.

---

## Deployment

### To GitHub Actions (Automated)

1. Push code to `main` branch
2. GitHub Actions runs scheduled workflow daily at 00:10 JST
3. No additional setup needed (uses repository's existing GITHUB_TOKEN)

### To Another Repository

1. Fork or clone this repository
2. Update URLs in specification if pointing to different boatrace source
3. Configure GitHub Pages in repository settings
4. Enable GitHub Actions in repository settings
5. Workflow will run on schedule automatically

---

## Performance Characteristics

### Daily Run Timing

| Phase | Duration | Details |
|-------|----------|---------|
| Download K-file | ~2-3 sec | 1 file, ~50 KB |
| Download B-file | ~2-3 sec | 1 file, ~50 KB |
| Decompress | ~0.5 sec | Total for both files |
| Parse | ~1 sec | 20-25 races typical |
| Convert to CSV | ~0.5 sec | 40-50 rows total |
| Write to disk | ~0.1 sec | 2 files |
| Git operations | ~2-3 sec | Commit + push |
| **Total** | **~10-15 sec** | Typical daily run |

### Historical Backfill Timing (3 years)

- 1,095 dates × ~3.3 seconds per date = ~60 minutes
- Can be parallelized in future if needed

---

## Resources & References

- **Official Boatrace Data**: http://www1.mbrace.or.jp/
- **GitHub Actions Documentation**: https://docs.github.com/en/actions
- **Python requests Library**: https://requests.readthedocs.io/
- **lhafile Python Package**: https://pypi.org/project/lhafile/

---

## Getting Help

1. **Check logs**: Review `logs/boatrace-*.json` files
2. **Review contracts**: See `contracts/cli.md` and `contracts/github-actions.md`
3. **Run with debug logging**:
   ```bash
   LOG_LEVEL=DEBUG python scripts/fetch-and-convert.py
   ```
4. **File an issue**: Include logs and date range in issue report

# Boatrace Data Automation

Automated daily collection of boatrace results and race schedules from the official website, with conversion to CSV format and publication via GitHub Pages.

## Quick Start

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
```

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

### Historical Backfill (One-Time)

```bash
# Process 3 years of historical data (takes ~60 minutes)
bash scripts/backfill-historical.sh \
  --start-date 2022-01-01 \
  --end-date 2024-12-31
```

## Project Structure

```
scripts/
├── fetch-and-convert.py         # Main entry point
├── boatrace/                    # Python package
│   ├── __init__.py
│   ├── downloader.py            # HTTP downloads with retry
│   ├── extractor.py             # LZH decompression
│   ├── parser.py                # Fixed-width text parsing
│   ├── converter.py             # Text → CSV conversion
│   ├── storage.py               # File I/O operations
│   ├── git_operations.py        # Git commit/push operations
│   └── logger.py                # Structured JSON logging
├── backfill-historical.sh       # One-time historical data script
├── requirements.txt
└── tests/
    ├── unit/
    │   ├── test_downloader.py
    │   ├── test_extractor.py
    │   ├── test_parser.py
    │   ├── test_converter.py
    │   └── test_storage.py
    ├── integration/
    │   ├── test_end_to_end.py
    │   └── fixtures/
    └── conftest.py

.github/workflows/
└── daily-sync.yml               # GitHub Actions workflow (00:10 JST daily)

data/                            # Published data (created at runtime)
├── results/
│   └── YYYY/MM/DD.csv
└── programs/
    └── YYYY/MM/DD.csv

.boatrace/
└── config.json                  # Configuration

logs/
└── boatrace-YYYY-MM-DD.json    # Execution logs
```

## Documentation

- **[Specification](specs/001-boatrace-automation/spec.md)** - Functional requirements and design decisions
- **[Implementation Plan](specs/001-boatrace-automation/plan.md)** - Technical architecture and project structure
- **[Data Model](specs/001-boatrace-automation/data-model.md)** - Entity definitions and relationships
- **[CLI Contract](specs/001-boatrace-automation/contracts/cli.md)** - Command-line interface specification
- **[GitHub Actions Contract](specs/001-boatrace-automation/contracts/github-actions.md)** - Workflow specification
- **[Quickstart Guide](specs/001-boatrace-automation/quickstart.md)** - Developer guide and troubleshooting

## Usage

### Fetch and Convert Daily Data

```bash
# Default: fetch yesterday's results and today's program
python scripts/fetch-and-convert.py

# Specific date range
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-03

# Force overwrite existing files
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --end-date 2025-12-01 \
  --force

# Dry run (no files written)
python scripts/fetch-and-convert.py \
  --start-date 2025-12-01 \
  --dry-run
```

### Backfill Historical Data

```bash
# One-time initialization with 3 years of historical data
bash scripts/backfill-historical.sh \
  --start-date 2022-01-01 \
  --end-date 2024-12-31

# This will take ~30-60 minutes depending on network
```

## GitHub Pages Access

Once configured, CSV files are published at:

- **Results**: `https://owner.github.io/boatrace-data/data/results/2025/12/01.csv`
- **Programs**: `https://owner.github.io/boatrace-data/data/programs/2025/12/01.csv`

## Testing

```bash
# Run all unit tests
pytest tests/unit/

# Run specific test file
pytest tests/unit/test_parser.py

# Run with coverage
pytest --cov=boatrace tests/unit/
```

## Environment Setup for GitHub Actions

1. Repository secrets (configured in GitHub):
   - `GITHUB_TOKEN` (provided automatically)
   - Optional: `GIT_USER_EMAIL` (defaults to "action@github.com")
   - Optional: `GIT_USER_NAME` (defaults to "GitHub Action")

2. GitHub Pages configuration:
   - Settings → Pages → Source: Deploy from a branch
   - Branch: `main`
   - Folder: `/ (root)`

## Configuration

Edit `.boatrace/config.json` to customize:

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

## Performance

- **Daily execution**: ~10-15 seconds (typical)
- **Historical backfill (3 years)**: ~60 minutes
- **CSV file size**: 100-500 KB per file

## Data Source

Official Boatrace Races Server: http://www1.mbrace.or.jp/od2/

## Troubleshooting

See [Quickstart Guide - Troubleshooting](specs/001-boatrace-automation/quickstart.md#troubleshooting) for common issues and solutions.

## License

[Your License Here]

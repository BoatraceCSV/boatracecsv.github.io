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
python -m venv .venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate

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

## 生成されるデータファイル

毎日、以下の3つのCSVファイルが自動生成されます。各ファイルはレースの異なる段階のデータを含みます。

### Programs (出場艇情報)
**ファイルパス**: `data/programs/YYYY/MM/DD.csv`

レース前に公開される出場艇の情報です。選手のプロフィールと成績データを含みます。

**主な列**:
- `レースコード`, `タイトル`, `レース日`, `レース場`, `レース回`
- 各艇（1枠～6枠）の情報:
  - `艇番`, `登録番号`, `選手名`, `年齢`, `支部`
  - `体重`, `級別`（A1/A2/B1/B2）
  - `全国勝率`, `全国2連対率`, `当地勝率`, `当地2連対率`
  - `モーター番号`, `モーター2連対率`, `ボート番号`, `ボート2連対率`
  - 当節成績（6レースの結果）
  - `早見`（早見表の予想）

**用途**: 選手の実力評価、地元での成績比較、装備情報の分析

### Previews (展示会情報)
**ファイルパス**: `data/previews/YYYY/MM/DD.csv`

展示会での各艇の走行データです。レース当日の朝に実施される展示会での情報を含みます。

**主な列**:
- `レースコード`, `タイトル`, `レース日`, `レース場`, `レース回`
- 環境情報: `風速(m)`, `風向`, `波の高さ(cm)`, `天候`, `気温(℃)`, `水温(℃)`
- 各艇（1艇～6艇）の情報:
  - `艇番`, `コース`, `体重(kg)`, `体重調整(kg)`
  - `展示タイム`
  - `チルト調整`
  - `スタート展示`

**用途**: レース当日のコンディション把握、艇の調整状況確認、展示会での走行速度分析

### Results (レース結果)
**ファイルパス**: `data/results/YYYY/MM/DD.csv`

レース終了後に公開されるレース結果です。順位、払戻金、詳細な走行情報を含みます。

**主な列**:
- `レースコード`, `タイトル`, `日次`, `レース日`, `レース場`, `レース回`
- 環境情報: `天候`, `風向`, `風速(m)`, `波の高さ(cm)`
- `決まり手`（逃げ・差し・まくり等）
- 単勝・複勝・2連単・2連複・拡連複・3連単・3連複の払戻金情報
- 各着順（1着～6着）の詳細:
  - `着順`, `艇番`, `登録番号`, `選手名`
  - `モーター番号`, `ボート番号`
  - `展示タイム`, `進入コース`, `スタートタイミング`, `レースタイム`

**用途**: レース結果の統計分析、投票情報の記録、決着パターンの研究

### ファイル間の関係性

```
Programs   → 選手情報・成績
     ↓
Previews   → 当日の走行テスト
     ↓
Results    → 本レースの結果
```

同じ `レースコード` で3つのファイルを紐付けることで、選手情報から展示会走行、本レース結果まで の完全な追跡が可能です。

## GitHub Pages Access

Once configured, CSV files are published at:

- **Results**: `https://BoatraceCSV.github.io/data/data/results/2025/12/01.csv`
- **Programs**: `https://BoatraceCSV.github.io/data/data/programs/2025/12/01.csv`
- **Previews**: `https://BoatraceCSV.github.io/data/data/previews/2025/12/01.csv`

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

MIT License
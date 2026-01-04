# Implementation Plan: Boatrace Data Automation with GitHub Pages Publishing

**Branch**: `001-boatrace-automation` | **Date**: 2026-01-01 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-boatrace-automation/spec.md`

**Note**: This plan document is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Automate daily collection of boatrace results and race schedules from the official website (K-files and B-files in LZH format), decompress and parse the Shift-JIS text files, convert to UTF-8 CSV format, and publish via GitHub Pages. System runs daily at 00:10 JST using GitHub Actions, with support for one-time historical backfill of 3 years of data.

## Technical Context

**Language/Version**: Python 3.8+ (GitHub Actions default runner)
**Primary Dependencies**: `requests` (HTTP downloads), `lhafile` (LZH decompression), `csv` module (standard library)
**Storage**: File-based - CSV files stored in git repository under `data/results/` and `data/programs/` directories
**Testing**: pytest for unit tests, GitHub Actions for integration/e2e testing
**Target Platform**: Linux (GitHub Actions runner), automated via cron-like scheduling
**Project Type**: Single Python project (CLI + GitHub Actions workflow)
**Performance Goals**: Process 1,095 dates (3 years) of historical data in under 60 minutes; daily execution completes in under 5 minutes
**Constraints**: GitHub server rate limit (3+ second intervals); 95% success rate on available files; file size typically <500KB per CSV
**Scale/Scope**: 24-25 boatrace locations, 8-12 races per location per day, ~50-100 MB total storage for 3 years

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

✅ **PASS** - No violations detected. Project adheres to guidelines:
- Single project scope (Python CLI + workflow)
- No class usage (unnecessary for this domain)
- Standard library dependencies only (requests, lhafile)
- Configuration via parameters and environment variables
- No hard-coded values (dates, URLs are parameterized)

## Project Structure

### Documentation (this feature)

```text
specs/001-boatrace-automation/
├── spec.md                 # Feature specification (source)
├── plan.md                 # This file - implementation plan
├── research.md             # Phase 0 output (research findings)
├── data-model.md           # Phase 1 output (data structures)
├── quickstart.md           # Phase 1 output (developer guide)
├── contracts/              # Phase 1 output (API/CLI contracts)
│   ├── cli.md             # Command-line interface spec
│   └── github-actions.md  # GitHub Actions workflow contract
├── checklists/
│   └── requirements.md    # Specification quality checklist
└── tasks.md               # Phase 2 output (implementation tasks)
```

### Source Code (repository root)

**Structure Decision**: Single Python project with GitHub Actions workflow integration.

```text
scripts/
├── fetch-and-convert.py        # Daily automation script (main entry point)
├── boatrace/                   # Python package
│   ├── __init__.py
│   ├── downloader.py           # Download K-files and B-files
│   ├── extractor.py            # LZH decompression
│   ├── parser.py               # Text file parsing
│   ├── converter.py            # Text → CSV conversion
│   ├── storage.py              # File I/O operations
│   ├── git_operations.py       # Git commit/push operations
│   └── logger.py               # Structured logging
├── backfill-historical.sh      # One-time historical data script
├── requirements.txt            # Python dependencies
└── tests/
    ├── unit/
    │   ├── test_downloader.py
    │   ├── test_extractor.py
    │   ├── test_parser.py
    │   ├── test_converter.py
    │   └── test_storage.py
    ├── integration/
    │   ├── test_end_to_end.py
    │   └── fixtures/            # Sample K-files, B-files for testing
    └── conftest.py

.github/workflows/
└── daily-sync.yml              # GitHub Actions workflow (00:10 JST daily)

data/                           # Published data (created at runtime)
├── results/
│   └── YYYY/MM/DD.csv
└── programs/
    └── YYYY/MM/DD.csv

README.md                       # Setup and usage documentation
```

## Complexity Tracking

No Constitution Check violations. No complexity justifications needed.

## Phase 0: Research Complete ✓

**All NEEDS CLARIFICATION Items Resolved**:

1. ✅ **CSV Header Mapping**: Extracted exact headers (91 cols results, 218 cols program) from notebook
2. ✅ **Error Recovery Strategy**: 3-attempt exponential backoff with circuit breaker pattern
3. ✅ **Logging Format**: JSON structured logs to stdout + file
4. ✅ **Git Configuration**: Use GitHub Actions built-in GITHUB_TOKEN
5. ✅ **Date Handling**: Schedule at 15:10 UTC = 00:10 JST next day

**Research Output**: [research.md](research.md)

---

## Phase 1: Design & Contracts Complete ✓

**Generated Artifacts**:

1. ✅ **Data Model**: [data-model.md](data-model.md)
   - RaceResult entity (91 fields)
   - RaceProgram entity (218 fields)
   - RacerResult and RacerFrame entities
   - ConversionSession state tracking
   - Validation rules and relationships

2. ✅ **CLI Contract**: [contracts/cli.md](contracts/cli.md)
   - fetch-and-convert.py signature and arguments
   - backfill-historical.sh signature
   - Exit codes and error handling
   - Examples and environment variables

3. ✅ **GitHub Actions Contract**: [contracts/github-actions.md](contracts/github-actions.md)
   - daily-sync.yml workflow specification
   - Triggers (scheduled + manual)
   - Workflow steps and GitHub Pages integration
   - Failure handling and monitoring

4. ✅ **Quickstart Guide**: [quickstart.md](quickstart.md)
   - Architecture overview
   - Local development setup
   - Code structure and module descriptions
   - Testing, troubleshooting, and deployment

---

## Phase 1 Gate Check (Constitution Re-evaluation) ✓

✅ **PASS** - No new violations introduced by design:
- Single project scope maintained
- No unnecessary abstractions added
- Logging implementation is straightforward
- Error handling follows standard patterns
- Code structure follows Python conventions

---

## Next Step: Phase 2 - Task Breakdown

Ready to run `/speckit.tasks` to generate:
- Actionable implementation tasks
- Dependency ordering
- Estimated complexity per task
- Implementation sequence for phased delivery

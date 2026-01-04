# Implementation Tasks: Boatrace Data Automation with GitHub Pages Publishing

**Feature**: Boatrace Data Automation with GitHub Pages Publishing
**Feature Branch**: `001-boatrace-automation`
**Date**: 2026-01-01
**Total Tasks**: 37
**Estimated Effort**: 60-80 hours

---

## Overview

This task breakdown is organized by user story priority (P1 → P2 → P3) to enable independent implementation and testing. Each user story can be developed, tested, and deployed independently.

### User Stories & Task Count

| Story | Priority | Title | Tasks | Testable | Dependencies |
|-------|----------|-------|-------|----------|--------------|
| US1 | P1 | Daily Automated Data Collection and Publication | 14 | Yes | None (after foundational) |
| US2 | P2 | One-Time Historical Data Backfill | 8 | Yes | US1 (data conversion logic) |
| US3 | P3 | Error Handling and Recovery | 7 | Yes | US1 (basic pipeline) |

### Parallel Opportunities

- **Phase 1 (Setup)**: All tasks can run in parallel (independent)
- **Phase 2 (Foundational)**: All tasks can run in parallel (different modules)
- **US1 Tasks**: Downloader, Parser, Converter modules can be parallelized (different files)
- **US2 Tasks**: Shell script tasks can be parallelized with US1 (uses US1 modules)
- **US3 Tasks**: Error handling can be parallelized with US1 (decoration pattern)

### Suggested MVP Scope

**Minimum Viable Product**: US1 only (Daily Automation)
- Delivers core value: Daily automated data collection and publication
- Can be tested end-to-end with GitHub Actions
- Enables GitHub Pages access to data
- Estimated: 24-32 hours, deployable after Phase 1 + Phase 2 + US1

---

## Phase 1: Setup & Project Initialization

Initialize project structure and dependencies.

- [ ] T001 Create project directory structure per implementation plan at `/Users/mahiguch/dev/boatrace/data/scripts/`
- [ ] T002 [P] Create Python package directory `boatrace/` with `__init__.py` at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/__init__.py`
- [ ] T003 [P] Create `requirements.txt` with dependencies (requests, lhafile) at `/Users/mahiguch/dev/boatrace/data/scripts/requirements.txt`
- [ ] T004 Create `README.md` with setup and usage instructions at `/Users/mahiguch/dev/boatrace/data/README.md`
- [ ] T005 Create `.boatrace/config.json` with default configuration at `/Users/mahiguch/dev/boatrace/data/.boatrace/config.json`
- [ ] T006 [P] Create `tests/` directory structure at `/Users/mahiguch/dev/boatrace/data/tests/`
- [ ] T007 [P] Create `tests/conftest.py` with pytest fixtures at `/Users/mahiguch/dev/boatrace/data/tests/conftest.py`
- [ ] T008 Create GitHub Actions workflow directory at `/Users/mahiguch/dev/boatrace/data/.github/workflows/`

---

## Phase 2: Foundational Modules (Blocking Prerequisites)

Implement core modules used by all user stories. Can be parallelized.

### Logging Module

- [ ] T009 [P] Implement logger module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/logger.py` with structured JSON logging (stdout + file)

### Storage Module

- [ ] T010 [P] Implement storage module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/storage.py` for CSV file I/O with directory creation

### Data Model Module

- [ ] T011 [P] Implement data model classes at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/models.py` (RaceResult, RaceProgram, RacerResult, RacerFrame, ConversionSession)

---

## Phase 3: US1 - Daily Automated Data Collection and Publication (P1)

Core feature enabling daily automated data fetch, conversion, and publication via GitHub Pages.

**Story Goal**: Daily workflow that downloads boatrace data, converts to CSV, and publishes via GitHub Pages automatically at 00:10 JST.

**Independent Test**: Execute workflow manually, verify CSV files created and GitHub Pages reflects data within 5 minutes.

### US1.1: Downloader Module

- [ ] T012 [US1] [P] Implement downloader module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/downloader.py` with HTTP download, retry logic (3 attempts), exponential backoff (5→10→20s), and rate limiting (3s intervals)
- [ ] T013 [US1] [P] Create unit tests for downloader at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_downloader.py` covering success, 404, timeout, retry logic

### US1.2: Extractor Module

- [ ] T014 [US1] [P] Implement extractor module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/extractor.py` for LZH decompression and Shift-JIS text extraction
- [ ] T015 [US1] [P] Create unit tests for extractor at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_extractor.py` covering valid LZH, corrupted files, missing files

### US1.3: Parser Module

- [ ] T016 [US1] [P] Implement parser module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/parser.py` for fixed-width text parsing of K-files (results) with 91 columns and B-files (programs) with 218 columns
- [ ] T017 [US1] [P] Create unit tests for parser at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_parser.py` covering both K and B file formats, edge cases (invalid races), special characters

### US1.4: Converter Module

- [ ] T018 [US1] [P] Implement converter module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/converter.py` to convert parsed data to CSV with exact headers (91 cols for results, 218 cols for programs) and UTF-8 encoding
- [ ] T019 [US1] [P] Create unit tests for converter at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_converter.py` covering header generation, data serialization, edge cases

### US1.5: Git Operations Module

- [ ] T020 [US1] [P] Implement git_operations module at `/Users/mahiguch/dev/boatrace/data/scripts/boatrace/git_operations.py` for git commit (with message "Update boatrace data: YYYY-MM-DD") and push to main branch
- [ ] T021 [US1] [P] Create unit tests for git_operations at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_git_operations.py` covering authentication, commit, push, error cases

### US1.6: Main Script

- [ ] T022 [US1] Implement main script at `/Users/mahiguch/dev/boatrace/data/scripts/fetch-and-convert.py` with:
  - Argument parsing (--start-date, --end-date, --mode, --force, --dry-run)
  - Daily mode: fetch yesterday's results + today's program
  - Relative date calculation (JST offset handling)
  - Orchestration of downloader → extractor → parser → converter → storage → git operations
  - Session tracking (ConversionSession) for progress and stats
- [ ] T023 [US1] Create integration test for main script at `/Users/mahiguch/dev/boatrace/data/tests/integration/test_end_to_end.py` covering daily mode with sample data

### US1.7: GitHub Actions Workflow

- [ ] T024 [US1] Create GitHub Actions workflow at `/Users/mahiguch/dev/boatrace/data/.github/workflows/daily-sync.yml` with:
  - Schedule trigger: 15:10 UTC daily (= 00:10 JST)
  - Manual trigger (workflow_dispatch) with optional date parameters
  - Checkout, Python setup, dependency installation
  - Git configuration (user/email)
  - Execution of fetch-and-convert.py
  - Artifact upload for logs
  - Status reporting
- [ ] T025 [US1] Create GitHub Pages configuration at `/Users/mahiguch/dev/boatrace/data/_config.yml` to serve `/data` directory

### US1.8: CSV Output Directories

- [ ] T026 [US1] Create output directory structure at `/Users/mahiguch/dev/boatrace/data/data/results/` and `/Users/mahiguch/dev/boatrace/data/data/programs/` with placeholder `.gitkeep` files

---

## Phase 4: US2 - One-Time Historical Data Backfill (P2)

Batch operation to initialize system with 3 years of historical data.

**Story Goal**: Initialize GitHub Pages with 3 years (1,095 dates) of historical boatrace data in single commit.

**Independent Test**: Run backfill script for sample date range, verify all CSV files created in correct structure, git operations succeed.

**Dependency**: Requires US1 core modules (downloader, extractor, parser, converter, storage).

### US2.1: Backfill Script

- [ ] T027 [US2] [P] Create backfill shell script at `/Users/mahiguch/dev/boatrace/data/scripts/backfill-historical.sh` with:
  - Argument parsing (--start-date, --end-date, --force)
  - Date range validation
  - Iteration over each date in range
  - Call to fetch-and-convert.py with mode=backfill
  - Progress display (X/Y files processed)
  - Final summary (successful, failed, skipped counts)

### US2.2: Backfill Integration

- [ ] T028 [US2] [P] Integrate backfill with main script: Add backfill mode to fetch-and-convert.py allowing date range iteration
- [ ] T029 [US2] Create integration test for backfill at `/Users/mahiguch/dev/boatrace/data/tests/integration/test_backfill.py` covering 2-week sample range

### US2.3: Backfill Git Operations

- [ ] T030 [US2] Update git_operations module to handle batch commits for backfill: single commit for all files in range with message "Initial historical data import: YYYY-MM-DD to YYYY-MM-DD"
- [ ] T031 [US2] Create manual backfill execution guide in `/Users/mahiguch/dev/boatrace/data/BACKFILL.md` with examples for 3-year initialization

---

## Phase 5: US3 - Error Handling and Recovery (P3)

Robust error handling for partial failures and recovery mechanisms.

**Story Goal**: System continues processing on file-level failures; all errors logged and reportable.

**Independent Test**: Simulate network failures, corrupted files, parse errors; verify system logs errors and continues processing.

**Dependency**: Requires US1 core pipeline.

### US3.1: Error Recovery in Downloader

- [ ] T032 [US3] [P] Enhance downloader with retry logic: circuit breaker pattern (exit after 5+ consecutive failures)
- [ ] T033 [US3] [P] Create test at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_downloader_errors.py` for retry exhaustion, circuit breaker activation

### US3.2: Error Recovery in Parser

- [ ] T034 [US3] [P] Enhance parser with graceful error handling: log parsing errors, mark race as invalid instead of crashing
- [ ] T035 [US3] [P] Create test at `/Users/mahiguch/dev/boatrace/data/tests/unit/test_parser_errors.py` for malformed files, encoding issues

### US3.3: Session Error Tracking

- [ ] T036 [US3] Enhance ConversionSession to track all errors (ConversionError entities) with timestamp, stage, message, retry count
- [ ] T037 [US3] Update main script to generate error summary report: display failed dates and reasons at end of execution, exit with code 1 for partial failures, code 2 for critical failures

---

## Implementation Dependencies Graph

```
Phase 1 (Setup) ──→
                    │
                    └──→ Phase 2 (Foundational) ──→
                                                   │
                                                   ├──→ Phase 3 (US1) ──→ GitHub Pages Data Available
                                                   │
                                                   ├──→ Phase 4 (US2) ──→ Historical Data Initialized
                                                   │
                                                   └──→ Phase 5 (US3) ──→ Robust Error Handling
```

### Task Dependencies (Critical Path)

**Critical Path** (blocks everything):
1. Phase 1: T001-T008 (any order, parallel OK)
2. Phase 2: T009-T011 (any order, parallel OK)
3. Phase 3 (US1):
   - T012-T021 (parallel OK, all foundational modules)
   - T022-T026 (serial: T012-T021 must complete first)
   - T024 (can run in parallel with T022 after git_operations ready)
4. Phase 4 (US2):
   - T027-T031 (depends on T022, T020 from US1)
5. Phase 5 (US3):
   - T032-T037 (depends on T012-T016, T022 from US1)

---

## Parallel Execution Examples

### Parallel Tracks (Day 1 - Setup & Foundational)

**Track A** (Modules):
- T009 (Logger)
- T010 (Storage)
- T011 (Models)

**Track B** (Project Structure):
- T001, T002, T003, T004, T005, T006, T007, T008

All can run in parallel. Estimated completion: 4-6 hours.

### Parallel Tracks (Day 2-4 - US1 Core Modules)

**Track A** (Download & Extract):
- T012-T013 (Downloader)
- T014-T015 (Extractor)

**Track B** (Parse & Convert):
- T016-T017 (Parser)
- T018-T019 (Converter)

**Track C** (Git & Integration):
- T020-T021 (Git Operations)
- T022-T026 (Main Script & Workflow)

All can run in parallel. Estimated completion: 24-32 hours total.

### Sequential Dependencies Within US1

- T012 (Downloader) MUST complete before T022 (Main Script)
- T014 (Extractor) MUST complete before T022
- T016 (Parser) MUST complete before T022
- T018 (Converter) MUST complete before T022
- T020 (Git Ops) MUST complete before T022

---

## Implementation Strategy: Phased Delivery

### MVP Scope (Phase 1-3: US1 Only)

**Duration**: 28-40 hours
**Deliverables**: Daily automated data collection and GitHub Pages publication
**Deployment**: Push to main branch, enable GitHub Actions

**Customer Value**:
- ✅ Automated daily data fetches (no manual intervention)
- ✅ CSV data accessible via GitHub Pages
- ✅ Reliable for most dates (basic error handling in Phase 2)

**Testable**: End-to-end via manual GitHub Actions trigger

### Phase 2 Addition (US2: Historical Backfill)

**Duration**: +8-12 hours
**Deliverables**: 3-year historical data initialization

**Customer Value**:
- ✅ Historical data available from day 1
- ✅ Users can query trends, no waiting for daily accumulation

**Testable**: Run backfill script locally with date range

### Phase 3 Addition (US3: Error Handling)

**Duration**: +12-16 hours
**Deliverables**: Robust error recovery and detailed reporting

**Customer Value**:
- ✅ Partial failures don't break pipeline
- ✅ All errors logged and reportable
- ✅ System continues on file-level failures

**Testable**: Unit tests with simulated failures

---

## Test Strategy

### Unit Tests (Recommended: Implement with each module)

| Module | Test File | Key Scenarios |
|--------|-----------|---------------|
| Downloader | test_downloader.py | Success, 404, timeout, retry exhaustion |
| Extractor | test_extractor.py | Valid LZH, corrupted, missing |
| Parser | test_parser.py | Valid K-file, B-file, malformed, encoding |
| Converter | test_converter.py | Header generation, serialization, edge cases |
| Git Ops | test_git_operations.py | Commit, push, auth failure |

### Integration Tests (Recommended for US1 & US2)

| Test | File | Scenario |
|------|------|----------|
| End-to-End Daily | test_end_to_end.py | Daily mode with sample date |
| Backfill Range | test_backfill.py | 2-week historical range |

### Manual Testing (Recommended before release)

- [ ] Manual trigger GitHub Actions workflow for specific date
- [ ] Verify CSV files created in correct structure
- [ ] Verify GitHub Pages reflects new data
- [ ] Test backfill with --force flag
- [ ] Simulate network failure and verify retry logic

---

## Acceptance Criteria by User Story

### US1: Daily Automated Data Collection (P1)

**Story Acceptance**:
- [ ] GitHub Actions workflow triggers at 00:10 JST daily
- [ ] CSV files created in `data/results/YYYY/MM/DD.csv` and `data/programs/YYYY/MM/DD.csv`
- [ ] Files committed to main branch with message "Update boatrace data: YYYY-MM-DD"
- [ ] GitHub Pages reflects files within 5 minutes of push
- [ ] Can be tested via manual GitHub Actions trigger
- [ ] At least 95% of available files convert successfully

### US2: Historical Data Backfill (P2)

**Story Acceptance**:
- [ ] Backfill script processes date range without manual intervention
- [ ] All 1,095 dates (3 years) process in under 60 minutes
- [ ] Single commit created for all backfilled data
- [ ] Progress displayed during execution
- [ ] Summary report shows successful/failed/skipped counts
- [ ] All historical data accessible via GitHub Pages

### US3: Error Handling and Recovery (P3)

**Story Acceptance**:
- [ ] Download failures logged with timestamp and date
- [ ] Parse errors logged but don't block remaining files
- [ ] Partial successes (some files fail) commit successfully
- [ ] Existing CSV files skipped on re-run (idempotent)
- [ ] All errors recorded in `logs/boatrace-YYYY-MM-DD.json`
- [ ] Exit code 1 for partial failures, 2 for critical failures

---

## File Structure Checklist

After completing all tasks, verify this structure exists:

```
/Users/mahiguch/dev/boatrace/data/
├── scripts/
│   ├── fetch-and-convert.py            [T022]
│   ├── backfill-historical.sh           [T027]
│   ├── requirements.txt                 [T003]
│   ├── boatrace/
│   │   ├── __init__.py                  [T002]
│   │   ├── logger.py                    [T009]
│   │   ├── storage.py                   [T010]
│   │   ├── models.py                    [T011]
│   │   ├── downloader.py                [T012]
│   │   ├── extractor.py                 [T014]
│   │   ├── parser.py                    [T016]
│   │   ├── converter.py                 [T018]
│   │   └── git_operations.py            [T020]
│   └── tests/
│       ├── conftest.py                  [T007]
│       ├── unit/
│       │   ├── test_downloader.py       [T013]
│       │   ├── test_extractor.py        [T015]
│       │   ├── test_parser.py           [T017]
│       │   └── test_converter.py        [T019]
│       └── integration/
│           ├── test_end_to_end.py       [T023]
│           └── test_backfill.py         [T029]
├── .github/
│   └── workflows/
│       └── daily-sync.yml               [T024]
├── .boatrace/
│   └── config.json                      [T005]
├── data/
│   ├── results/                         [T026]
│   └── programs/                        [T026]
├── logs/                                (created at runtime)
├── README.md                            [T004]
├── BACKFILL.md                          [T031]
├── _config.yml                          [T025]
└── specs/001-boatrace-automation/       (existing)
    ├── spec.md
    ├── plan.md
    ├── research.md
    ├── data-model.md
    ├── quickstart.md
    ├── contracts/
    └── tasks.md                         (this file)
```

---

## Notes

- **Task IDs**: Sequential (T001-T037) in execution order
- **Parallelizable tasks**: Marked with `[P]` where they can run concurrently
- **Story tasks**: Marked with `[US1]`, `[US2]`, or `[US3]` for story association
- **File paths**: All absolute paths within `/Users/mahiguch/dev/boatrace/data/`
- **Testing**: Unit tests are recommended but optional for MVP. Integration tests enable confident deployment.
- **Documentation**: This tasks.md serves as the implementation roadmap

# Feature Specification: Boatrace Data Automation with GitHub Pages Publishing

**Feature Branch**: `001-boatrace-automation`
**Created**: 2026-01-01
**Status**: Draft
**Input**: User description: Automate daily boatrace data download, conversion to CSV, and publication via GitHub Pages with historical backfill capability.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Daily Automated Data Collection and Publication (Priority: P1)

Daily automated collection of boatrace results and race schedules from the official website, automatic conversion to CSV, and publication via GitHub Pages. This enables stakeholders to access up-to-date boatrace data without manual intervention.

**Why this priority**: This is the core functionality. Without daily automation, the entire system has no value. This story alone delivers complete end-to-end value.

**Independent Test**: Can be fully tested by executing the GitHub Actions workflow manually, checking that CSV files are created, committed, and accessible via GitHub Pages within 5 minutes.

**Acceptance Scenarios**:

1. **Given** GitHub Actions workflow is enabled, **When** scheduled time (00:10 JST) is reached, **Then** workflow automatically triggers without manual intervention
2. **Given** official boatrace server is accessible, **When** workflow executes, **Then** K-file (results) and B-file (program) are downloaded for correct dates
3. **Given** files are successfully downloaded, **When** conversion process runs, **Then** valid UTF-8 CSV files are created in correct directory structure (data/results/YYYY/MM/DD.csv, data/programs/YYYY/MM/DD.csv)
4. **Given** CSV files are created, **When** git commit and push occur, **Then** files are committed with descriptive message and pushed to main branch
5. **Given** files are pushed, **When** GitHub Pages rebuilds, **Then** CSV files are accessible via HTTPS at predictable URLs (e.g., boatracecsv.github.io/data/results/2025/12/01.csv)

---

### User Story 2 - One-Time Historical Data Backfill (Priority: P2)

Initialize the system with historical boatrace data spanning 3 years. This enables users to query and analyze historical trends immediately upon launch without waiting for daily collection to accumulate data.

**Why this priority**: Essential for launch readiness and user value on day one. However, it's executed once rather than continuously. Can be developed and tested independently from daily automation.

**Independent Test**: Can be fully tested by running backfill shell script with date range (2022-01-01 to 2024-12-31), verifying all CSV files are created in correct structure, and confirming data commits are pushed to repository.

**Acceptance Scenarios**:

1. **Given** shell script is executable and provided with date range, **When** script is invoked with `--start-date 2022-01-01 --end-date 2024-12-31`, **Then** script executes without manual intervention
2. **Given** script is running, **When** processing historical files, **Then** progress is displayed (X/Y files processed)
3. **Given** all files are processed, **When** conversion completes, **Then** CSV files are organized by year and month (data/results/2022/01/01.csv, etc.)
4. **Given** all files are converted, **When** backfill script completes, **Then** all data is committed and pushed with summary message
5. **Given** backfill is complete, **When** user accesses GitHub Pages, **Then** all historical data from 2022-2024 is queryable via CSV files

---

### User Story 3 - Error Handling and Recovery (Priority: P3)

Robust error handling for partial failures and recovery mechanisms. When downloads fail or parsing encounters issues, system logs errors and either retries or skips affected files, allowing automation to continue and maintain data continuity.

**Why this priority**: Important for system reliability but doesn't block core functionality. If daily automation works 95% of the time, system still delivers significant value. However, robust error handling reduces manual intervention needed.

**Independent Test**: Can be tested independently by simulating network failures, corrupted files, or parsing errors, verifying that system logs errors appropriately, continues processing remaining files, and reports summary of what succeeded/failed.

**Acceptance Scenarios**:

1. **Given** a download fails (network timeout, 404), **When** workflow executes, **Then** error is logged with timestamp and affected date
2. **Given** a single file fails to parse, **When** conversion process runs, **Then** error is logged but remaining files continue processing
3. **Given** some files succeed and some fail, **When** workflow completes, **Then** successful files are committed and failed files are reported
4. **Given** a file was already processed (output CSV exists), **When** workflow re-runs for same date, **Then** file is skipped or reported as duplicate without error

---

### Edge Cases

- What happens when official server has no data for a requested date (weekend, holiday)? (Skip silently, log as "no data available")
- How does system handle files where K-file (results) is available but B-file (program) is missing? (Process available file, skip missing file, continue)
- What if CSV file is corrupted or partially written from previous failed attempt? (Delete and re-process, or skip if file size looks correct)
- How does system handle racer names with unusual characters or encoding issues? (Preserve raw data as-is from source, no normalization)
- What if GitHub push fails due to network issues? (Retry with backoff, log failure, workflow exits with error status)

## Requirements *(mandatory)*

### Functional Requirements

**Data Collection & Conversion**

- **FR-001**: System MUST download K-files (results) and B-files (programs) from official boatrace server at URLs following pattern `http://www1.mbrace.or.jp/od2/{B|K}/YYYYMM/{prefix}YYMMDD.lzh`
- **FR-002**: System MUST decompress LZH files using Python lhafile library and extract Shift-JIS encoded text content
- **FR-003**: System MUST parse fixed-width text format files and extract structured data (headers, betting results, racer details)
- **FR-004**: System MUST convert extracted data to UTF-8 encoded CSV format with consistent column structure
- **FR-005**: System MUST implement rate limiting of minimum 3 seconds between requests to official server to avoid overloading
- **FR-006**: System MUST validate decompression success and skip files that cannot be decompressed

**File Organization & Storage**

- **FR-007**: System MUST store results CSV at path `data/results/YYYY/MM/DD.csv` with headers: Race code, Title, Day, Race date, Stadium, Race round, Distance, Weather, Wind direction, Wind speed, Wave height, Winning method, and betting/racer details
- **FR-008**: System MUST store program CSV at path `data/programs/YYYY/MM/DD.csv` with headers: Title, Day, Race date, Stadium, Race round, Distance, Voting deadline, and 6 racer frames with standardized columns
- **FR-009**: System MUST automatically create missing directories in output path structure
- **FR-010**: System MUST use UTF-8 encoding for all output CSV files

**Incremental Updates**

- **FR-011**: System MUST check if output CSV file already exists before conversion and skip if present (avoid reprocessing)
- **FR-012**: System MUST commit converted CSV files to git with message format "Update boatrace data: YYYY-MM-DD"
- **FR-013**: System MUST push commits to main branch for GitHub Pages publication
- **FR-014**: System MUST skip git operations if no new files were created (no empty commits)

**Scheduled Automation**

- **FR-015**: System MUST execute daily at 00:10 JST via GitHub Actions without manual intervention
- **FR-016**: System MUST fetch results from previous day and program for current day (relative to execution date)
- **FR-017**: System MUST handle timezone conversion correctly (JST = UTC+9)

**Historical Backfill**

- **FR-018**: System MUST provide shell script that accepts `--start-date YYYY-MM-DD` and `--end-date YYYY-MM-DD` parameters
- **FR-019**: System MUST process all dates in range and download/convert available data
- **FR-020**: System MUST display progress during execution (X/Y files processed)
- **FR-021**: System MUST commit backfilled data to repository after processing completes
- **FR-022**: System MUST generate summary report showing successful files, skipped files, and errors

**Error Handling & Logging**

- **FR-023**: System MUST log all errors with timestamps to file or workflow logs
- **FR-024**: System MUST handle download failures gracefully (log error, continue with next file)
- **FR-025**: System MUST handle parsing errors gracefully (log error, skip corrupted file, continue)
- **FR-026**: System MUST exit with non-zero status if critical errors occur (e.g., git push fails)
- **FR-027**: System MUST report summary of processed files, skipped files, and errors at completion

### Key Entities

- **Race Result (K-file)**: Compressed file containing race outcome data from official server. Includes payoff information, finishing positions, and race conditions. Accessed via K-file download pattern.
- **Race Program (B-file)**: Compressed file containing scheduled race information from official server. Includes racer details, boat/motor assignments, and race conditions. Accessed via B-file download pattern.
- **Results CSV**: Processed and structured race results in UTF-8 CSV format with flattened columns for results, payoffs, and racer details. Published at data/results/YYYY/MM/DD.csv
- **Program CSV**: Processed and structured race schedule in UTF-8 CSV format with racer information organized by entry frame. Published at data/programs/YYYY/MM/DD.csv
- **Workflow Run**: Single execution of GitHub Actions workflow triggered daily at 00:10 JST. Contains logs, status, and artifacts (CSV files).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Daily pipeline executes successfully 99% of scheduled runs (allowing 1 failed run per 100 days)
- **SC-002**: 95% of available boatrace files successfully convert to CSV format without corruption
- **SC-003**: New data published via GitHub Pages within 5 minutes of workflow completion (95th percentile latency)
- **SC-004**: Historical backfill processes 3 years of data (1,095 dates) in under 60 minutes
- **SC-005**: All published CSV files are valid and parseable by standard CSV readers (no format errors)
- **SC-006**: Failed or missing data is logged and reportable (maintainers can identify gaps without manual checking)
- **SC-007**: System recovery from single-file failure takes less than 30 minutes (manual re-run of backfill for affected date)

## Assumptions

- Official boatrace server continues serving K-files and B-files in LZH format at documented URLs
- Server is accessible 24/7 with 99.9% uptime; not blocking requests with 3+ second intervals
- GitHub Actions runners have Python 3.8+ and can install pip dependencies (requests, lhafile)
- Git is configured in GitHub Actions environment with user/email for commits
- GitHub Pages is configured to serve static files from `/data` directory or separate branch
- Boatrace events produce 8-12 races per location daily, with 24-25 active locations
- Repository storage is sufficient for 3+ years of CSV data (estimated 50-100 MB)

## Out of Scope

- Real-time data feeds or WebSocket connections
- API server for data access (static CSV files only)
- Data analysis, predictions, or advanced analytics
- Authentication or access control for CSV files
- Web UI or dashboard for browsing data
- Automatic data corrections or modifications after publication
- Support for alternative file formats (JSON, Parquet, Excel)
- Multi-repository deployment or data synchronization

## Design Decisions

**Decision 1: Git Commit Strategy for Backfill**
- **Choice**: (A) Single commit for all backfilled data
- **Rationale**: Simplifies repository history while initializing. Historical data treated as single atomic operation rather than incremental accumulation.
- **Impact**: Single "Initial historical data import" commit contains 3 years of CSV files. Clean history for ongoing daily commits that follow.

**Decision 2: GitHub Pages Branch Configuration**
- **Choice**: (A) `/data` directory on main branch
- **Rationale**: Simplified deployment. Data and workflow code coexist in main branch. No need to manage separate branch.
- **Impact**: GitHub Pages serves from `/{repo}/data/results/`, `/{repo}/data/programs/`. Static site root is repository root, not separate gh-pages branch.

**Decision 3: Duplicate File Handling on Re-runs**
- **Choice**: (A) Skip and report as "already processed"
- **Rationale**: Conservative approach prevents accidental data overwrite. If fresh data needed, user can manually delete CSV file and re-run workflow.
- **Impact**: Workflow idempotent - safe to re-run multiple times for same date without data corruption or duplicate commits.

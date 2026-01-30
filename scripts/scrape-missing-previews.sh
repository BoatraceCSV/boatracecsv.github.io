#!/bin/bash
#
# Scrape missing preview data for all races in missing_previews.csv
#
# This script reads missing_previews.csv and calls preview-race.py for each race code
# to fetch and append preview data.
#
# Usage:
#    bash scrape-missing-previews.sh
#    bash scrape-missing-previews.sh --push     # commit and push results
#

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Input file
INPUT_FILE="${PROJECT_ROOT}/missing_previews_recent.csv"

# Options
PUSH_FLAG=""
if [[ "$1" == "--push" ]]; then
    PUSH_FLAG="--push"
    echo "Git commit & push enabled"
fi

echo "Starting missing preview scraping..."
echo "=========================================="
echo "Input file: $INPUT_FILE"
echo "Project root: $PROJECT_ROOT"
echo "Push flag: ${PUSH_FLAG:-'disabled'}"
echo "=========================================="
echo ""

# Check if input file exists
if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: Input file not found: $INPUT_FILE"
    echo "Run: python3 scripts/find-missing-previews.py --min-date 20260101 --output missing_previews_recent.csv"
    exit 1
fi

# Count total races
TOTAL_RACES=$(tail -n +2 "$INPUT_FILE" | wc -l)
echo "Total races to process: $TOTAL_RACES"
echo ""

# Initialize counters
SUCCEEDED=0
FAILED=0
SKIPPED=0

# Read CSV and process each race
while IFS=',' read -r race_code date stadium race; do
    # Skip header line
    if [[ "$race_code" == "race_code" ]]; then
        continue
    fi

    # Trim whitespace
    race_code=$(echo "$race_code" | xargs)

    # Progress
    CURRENT=$((SUCCEEDED + FAILED + SKIPPED + 1))
    echo -n "[$CURRENT/$TOTAL_RACES] Processing $race_code... "

    # Run preview-race.py (without push, we'll commit all at the end)
    if python3 "$SCRIPT_DIR/preview-race.py" --race-code "$race_code" > /dev/null 2>&1; then
        echo "✓ Success"
        SUCCEEDED=$((SUCCEEDED + 1))
    else
        echo "✗ Failed"
        FAILED=$((FAILED + 1))
    fi

done < "$INPUT_FILE"

echo ""
echo "=========================================="
echo "Scraping complete!"
echo "  Succeeded: $SUCCEEDED"
echo "  Failed:    $FAILED"
echo "  Skipped:   $SKIPPED"
echo "  Total:     $TOTAL_RACES"
echo "=========================================="

# Summary
if [[ $FAILED -eq 0 ]]; then
    echo "✓ All races processed successfully!"
    exit 0
else
    echo "✗ Some races failed. Review errors above."
    exit 1
fi

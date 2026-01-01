#!/bin/bash

# Historical data backfill script
# One-time initialization with historical boatrace data

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
START_DATE=""
END_DATE=""
FORCE_OVERWRITE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --force)
            FORCE_OVERWRITE="--force"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Validate arguments
if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
    echo "Error: --start-date and --end-date are required"
    echo ""
    echo "Usage: bash $0 --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--force]"
    echo ""
    echo "Example:"
    echo "  bash $0 --start-date 2022-01-01 --end-date 2024-12-31"
    exit 1
fi

# Validate date formats
if ! [[ $START_DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid start date format. Use YYYY-MM-DD"
    exit 1
fi

if ! [[ $END_DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid end date format. Use YYYY-MM-DD"
    exit 1
fi

# Validate start <= end
START_EPOCH=$(date -d "$START_DATE" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$START_DATE" +%s)
END_EPOCH=$(date -d "$END_DATE" +%s 2>/dev/null || date -j -f "%Y-%m-%d" "$END_DATE" +%s)

if [ "$START_EPOCH" -gt "$END_EPOCH" ]; then
    echo "Error: Start date must be before or equal to end date"
    exit 1
fi

# Calculate number of dates
DAYS_DIFF=$(( (END_EPOCH - START_EPOCH) / 86400 + 1 ))

echo "Starting historical backfill: $START_DATE to $END_DATE"
echo "Total dates to process: $DAYS_DIFF"
echo ""

# Run main script with backfill mode
cd "$SCRIPT_DIR"

if python fetch-and-convert.py \
    --mode backfill \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    $FORCE_OVERWRITE; then
    echo ""
    echo "✓ Backfill completed successfully!"
    exit 0
else
    echo ""
    echo "⚠ Backfill encountered errors (see above for details)"
    exit 1
fi

#!/bin/bash
# daily_run.sh — Runs every day at 11am automatically
# Fetches today's NBA slate, props, and updates everything

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/Desktop/nba_props/service_account.json"

cd "$HOME/Desktop/nba_props" || exit 1

LOG_FILE="$HOME/Desktop/nba_props/logs/daily_$(date +%Y-%m-%d).log"
mkdir -p "$HOME/Desktop/nba_props/logs"

echo "========================================" >> "$LOG_FILE"
echo "Daily run started: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

/usr/bin/python3 main.py --sheet "NBA Props" >> "$LOG_FILE" 2>&1

EXIT_CODE=$?
echo "Finished: $(date) — Exit code: $EXIT_CODE" >> "$LOG_FILE"

# Optional: open the web app automatically after run
# open "http://localhost:5000"

#!/bin/bash
# kalshi-recorder launcher (macOS).
# Credentials come from the `.env` file in the project root (see `.env.example`).
# Edit PROJECT_DIR below if the project lives somewhere else, then install the
# LaunchAgent plist (see mac/README.md). The private key never leaves this machine.
#
# NOTE: this script redirects the recorder's output to logs/recorder.log itself.
# Do NOT add StandardOutPath/StandardErrorPath pointing at this volume to the
# LaunchAgent plist: macOS's sandbox denies launchd's spawn proxy (xpcproxy)
# writes outside $HOME, so the job then dies instantly with exit 78 (EX_CONFIG)
# and no log file is ever created. (Seen 2026-10-13.)
set -euo pipefail

# --- EDIT ME IF NEEDED ---
PROJECT_DIR="/path/to/kalshi-recorder"
# --- END EDIT ME ---

cd "$PROJECT_DIR"
mkdir -p "$PROJECT_DIR/logs"
PYBIN="$PROJECT_DIR/.venv/bin/python"
if [ ! -x "$PYBIN" ]; then
    PYBIN="python3"
fi

exec "$PYBIN" -u data/recorder.py >> "$PROJECT_DIR/logs/recorder.log" 2>&1

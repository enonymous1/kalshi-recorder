# Mac mini setup — Phase 0 recorder

The Kalshi private key lives **only on this machine**. It is never pasted into
chat, emailed, or uploaded anywhere.

## 1. Copy the project over

```bash
# from this Linux box, or USB / AirDrop / git — your call
cp -r kalshi-recorder ~/kalshi-recorder
cd ~/kalshi-recorder
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

(`uvloop` installs fine on Apple Silicon.)

## 2. Create the API key (read scope)

Kalshi dashboard → Account Settings → API Keys → create key, **read** scope,
**production** (not demo). Save what it gives you:

```bash
mkdir -p ~/.kalshi
# If it's a .txt download, just move it as-is — any extension works:
mv ~/Downloads/kalshi*.txt ~/.kalshi/kalshi-read.txt
chmod 600 ~/.kalshi/kalshi-read.txt
```

(If you downloaded a `.pem`, same thing. The recorder accepts PEM files, raw
base64/hex Ed25519 seeds, and even a PEM bundled with other text.)

## 3. Create the `.env` file (credentials)

```bash
cp .env.example .env
```

Then edit `.env` and fill in:
- `KALSHI_API_KEY_ID` — the UUID from the dashboard
- `KALSHI_PRIVATE_KEY_PATH` — e.g. `/Users/<you>/.kalshi/kalshi-read.pem`

The private key itself stays in the PEM file; only its path goes in `.env`.
`.env` is git-ignored and never leaves this machine. Real environment variables
override `.env` if both are set.

## 4. Smoke test (foreground, one pass)

```bash
.venv/bin/python data/recorder.py --once -v
```

Check `data/recordings/` has fresh parquet, then Ctrl-C. (Credentials come from
`.env`; no need to prefix env vars.)

## 5. Install as a LaunchAgent (restart-safe, survives reboots)

The plist is pre-configured for the external-volume layout
(`/Volumes/JPL-MM-Ext/...`) — no editing needed. **Stop the foreground
VS Code run first** (Ctrl-C), then:

```bash
cp mac/com.kalshi-recorder.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/com.kalshi-recorder.plist
```

Verify:

```bash
launchctl list | grep kalshi-recorder   # want a PID in the first column, not "-"
tail -f ~/Library/Logs/kalshi-recorder.log
```

To stop: `launchctl unload ~/Library/LaunchAgents/com.kalshi-recorder.plist`

Notes:
- `KeepAlive` restarts the recorder if it crashes; `RunAtLoad` starts it at login.
  If the external drive isn't mounted yet at login, the agent retries every 60s
  until it appears — keep the drive plugged in.
- First launch triggers a macOS privacy pop-up ("...would like to access files
  on the removable volume") for the venv python — click Allow. The plist
  deliberately execs python directly with no bash wrapper and keeps its log
  under ~/Library/Logs: each binary a LaunchAgent touches on a removable volume
  needs its own approval, and pointing the log at the volume makes the job die
  instantly (exit 78) with nothing ever written.
- The Mac mini must stay awake: System Settings → Energy → "Prevent automatic
  sleeping when the display is off" (or `sudo pmset -c sleep 0`).
- LaunchAgents start at login, so after a reboot the mini needs to be logged in
  before recording resumes. For a single-purpose box, consider enabling automatic
  login (System Settings → Users & Groups).

## Notes

- The recorder treats the WebSocket feed as optional: without keys it polls REST
  (works fine); with keys it also subscribes to live book/trade channels.
- Recordings land in `data/recordings/{books,trades}/YYYY-MM-DD/`. Back this dir
  up if you care about it — it's the dataset Phase 1 trains on.
- When Phase 2 (live quoting) approaches, mint a **separate write-scoped** key.
  Never reuse the read key for writes.

## Troubleshooting

- **Enumeration hangs:** Some series (e.g. `KXMVECROSSCATEGORY`) hang the `/markets`
  endpoint indefinitely. The recorder wraps each series in a 120s timeout
  (`REC_ENUMERATE_SERIES_TIMEOUT_S`) and logs skips to
  `data/recordings/skipped_series.json`. If enumeration seems stuck, check the log
  for `timed out after 120s` warnings — it's working as designed.
- **Universe empty (0 markets):** Check that `REC_MIN_VOLUME_24H` isn't too high
  and `REC_MAX_MARKETS` isn't 0. The log will show `markets: N across M series`
  followed by `universe: 0 markets` if the filters are too strict.
- **LaunchAgent not starting:** Check `launchctl list | grep kalshi-recorder`.
  If the plist was updated, unload then reload:
  `launchctl unload ~/Library/LaunchAgents/com.kalshi-recorder.plist`
  then `launchctl load ...` again.

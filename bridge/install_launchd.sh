#!/usr/bin/env bash
# Runs dlh-bridge as a macOS launchd agent: starts at login, restarts within 30s if it exits.
#   bridge/install_launchd.sh            install (or update) and start it
#   bridge/install_launchd.sh uninstall  stop and remove it
# Logs: logs/bridge.log. Check: launchctl print gui/$(id -u)/com.dlh.bridge
set -euo pipefail
B="$(cd "$(dirname "$0")" && pwd)"
P="$(cd "$B/.." && pwd)"
LABEL=com.dlh.bridge
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
if [ "${1:-}" = "uninstall" ]; then
  rm -f "$PLIST"; echo "removed $LABEL"; exit 0
fi
mkdir -p "$P/logs" "$(dirname "$PLIST")"
cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key><array><string>/bin/bash</string><string>$B/run.sh</string></array>
  <key>WorkingDirectory</key><string>$P</string>
  <key>EnvironmentVariables</key><dict><key>PATH</key><string>$(dirname "$(command -v java)"):$(dirname "$(command -v mvn)"):/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>30</integer>
  <key>StandardOutPath</key><string>$P/logs/bridge.log</string>
  <key>StandardErrorPath</key><string>$P/logs/bridge.log</string>
</dict>
</plist>
PLIST
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "installed $LABEL ($PLIST); logs in $P/logs/bridge.log"

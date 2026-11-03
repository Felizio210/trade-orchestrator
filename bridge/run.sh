#!/usr/bin/env bash
# Starts dlh-bridge (JForex SDK -> HTTP on 127.0.0.1:$TRADING_BRIDGE_PORT) with the login from .env.
# Builds it first if needed. Ctrl+C to stop.
set -euo pipefail
B="$(cd "$(dirname "$0")" && pwd)"
set -a; [ -f "$B/../.env" ] && . "$B/../.env"; set +a
if [ ! -d "$B/target/classes" ] || [ -n "$(find "$B/src" "$B/pom.xml" -newer "$B/target/classes" -print -quit)" ]; then
  mvn -q -f "$B/pom.xml" package -DskipTests
fi
exec java -Dlog4j.configuration="file:$B/src/main/resources/log4j.properties" -cp "$B/target/classes:$B/target/lib/*" dlh.bridge.Bridge

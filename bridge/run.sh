#!/usr/bin/env bash
# Starts dlh-bridge (JForex SDK -> HTTP on 127.0.0.1:$TRADING_BRIDGE_PORT) with the login from .env.
# Builds it first if needed. Ctrl+C to stop. bridge/install_launchd.sh runs it as a service instead.
#
# The password, first found wins:
#   1. DUKASCOPY_DEMO_PASSWORD in the environment or .env (fine for a demo account)
#   2. the macOS Keychain, service "dlh-dukascopy", account = DUKASCOPY_DEMO_USER. Store it once with
#        security add-generic-password -s dlh-dukascopy -a "<login>" -w
#   3. GCP Secret Manager, secret DUKASCOPY_PASSWORD_SECRET (needs gcloud and access to GCP_PROJECT)
set -euo pipefail
B="$(cd "$(dirname "$0")" && pwd)"
set -a; [ -f "$B/../.env" ] && . "$B/../.env"; set +a
if [ -z "${DUKASCOPY_DEMO_PASSWORD:-}" ] && command -v security >/dev/null && [ -n "${DUKASCOPY_DEMO_USER:-}" ]; then
  DUKASCOPY_DEMO_PASSWORD="$(security find-generic-password -s dlh-dukascopy -a "$DUKASCOPY_DEMO_USER" -w 2>/dev/null || true)"
fi
if [ -z "${DUKASCOPY_DEMO_PASSWORD:-}" ] && [ -n "${DUKASCOPY_PASSWORD_SECRET:-}" ]; then
  DUKASCOPY_DEMO_PASSWORD="$(gcloud secrets versions access latest --secret="$DUKASCOPY_PASSWORD_SECRET" \
    ${GCP_PROJECT:+--project="$GCP_PROJECT"})"
fi
export DUKASCOPY_DEMO_PASSWORD
if [ ! -d "$B/target/classes" ] || [ -n "$(find "$B/src" "$B/pom.xml" -newer "$B/target/classes" -print -quit)" ]; then
  mvn -q -f "$B/pom.xml" package -DskipTests
fi
exec java -Dlog4j.configuration="file:$B/src/main/resources/log4j.properties" -cp "$B/target/classes:$B/target/lib/*" dlh.bridge.Bridge

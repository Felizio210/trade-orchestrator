#!/usr/bin/env bash
# Starts Airflow (scheduler + web UI at http://localhost:8080). Ctrl+C to stop.
source "$(dirname "$0")/env.sh"
exec airflow standalone

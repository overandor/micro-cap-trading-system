#!/usr/bin/env bash
# Lightweight daemon launcher (no systemd needed) — runs the bot detached
# with nohup, writes a PID file, and tails nothing (logs go to the log file).
#
#   ./deploy/run-daemon.sh start     # launch in background
#   ./deploy/run-daemon.sh stop      # graceful SIGINT shutdown
#   ./deploy/run-daemon.sh status
#   ./deploy/run-daemon.sh logs      # tail -f the log
#
# Reads env from deploy/gate-mm.env if present (defaults to DRY RUN).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/gate-mm.pid"
ENV_FILE="$ROOT/deploy/gate-mm.env"
LOG="${MM_LOG_FILE:-$ROOT/gate_micro_mm.log}"

cd "$ROOT"
[ -f "$ENV_FILE" ] && set -a && . "$ENV_FILE" && set +a
export MM_DRY_RUN="${MM_DRY_RUN:-1}"   # default to safe dry run

case "${1:-}" in
  start)
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "Already running (PID $(cat "$PID_FILE"))"; exit 0
    fi
    [ "$MM_DRY_RUN" = "1" ] && echo "Starting in DRY-RUN." || echo "*** LIVE TRADING ***"
    nohup python3 gate_micro_mm.py >>"$LOG" 2>&1 &
    echo $! > "$PID_FILE"
    echo "Started PID $(cat "$PID_FILE"); logging to $LOG"
    ;;
  stop)
    [ -f "$PID_FILE" ] || { echo "Not running"; exit 0; }
    kill -INT "$(cat "$PID_FILE")" 2>/dev/null || true
    echo "Sent graceful shutdown to PID $(cat "$PID_FILE")"
    rm -f "$PID_FILE"
    ;;
  status)
    if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "Running (PID $(cat "$PID_FILE"))"
    else
      echo "Not running"
    fi
    ;;
  logs) tail -f "$LOG" ;;
  *) echo "usage: $0 {start|stop|status|logs}"; exit 1 ;;
esac

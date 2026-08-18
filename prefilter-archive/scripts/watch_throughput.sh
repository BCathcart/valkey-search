#!/bin/bash
# Periodically snapshot the throughput run's progress to disk.
#
# Two failure modes this guards against:
#  1. The benchmark now persists after each corpus size, but a crash *within* a
#     corpus size still loses that size's rows unless they are parsed from the log.
#  2. A resumed run's carried-over rows are never reprinted to the log, so a
#     naive log-only snapshot would overwrite good data with an empty file. That
#     happened once and cost the 160-row partial (recovered from a backup).
#     Hence --merge-with: prior rows are always preserved.
#
# Launch detached:
#   setsid nohup ./scripts/benchmark/watch_throughput.sh LOG OUT INTERVAL \
#       > /tmp/watchdog.log 2>&1 &
set -u
LOG=${1:-/tmp/thr_full.log}
OUT=${2:-prefilter-throughput-partial.json}
INTERVAL=${3:-120}
BASE=${4:-/tmp/thr_resume_base.json}

cd "$(dirname "$0")/../.." || exit 1

snapshot() {
    python3 scripts/benchmark/recover_throughput_log.py \
        --log "$LOG" --out "$OUT" \
        --merge-with "$BASE" \
        --merge-with prefilter-throughput.json 2>&1 | head -2
}

while true; do
    if ! pgrep -f "prefilter_throughput_e2[e].py" >/dev/null; then
        snapshot
        echo "$(date +%H:%M:%S) benchmark process gone; final snapshot taken; exiting"
        exit 0
    fi
    snapshot
    sleep "$INTERVAL"
done

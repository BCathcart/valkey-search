#!/bin/bash
# Unattended driver for dimension 2 (saturated throughput).
#
# Three stages, deliberately ordered:
#   1. Preload all corpus sizes CONCURRENTLY and snapshot each to an RDB.
#      Loading is round-trip-latency bound, so overlapping is safe and collapses
#      ~33 min of sequential loading into ~28 min (the largest corpus).
#   2. Measure SERIALLY, one server at a time. This is a saturation benchmark:
#      concurrent servers would contend for the exact resource under test and
#      would penalise the bandwidth-heavy pre-filter path more than inline.
#   3. Build the per-test-case report and figures.
#
# Crash protection: stage 1 leaves RDB snapshots, so a rerun skips loading
# entirely; stage 2 persists its JSON after every corpus size, so a crash costs
# at most one corpus size of measurement.
#
# Run under setsid so an SSH drop or laptop sleep cannot kill it.
set -u
cd "$(dirname "$0")/../.." || exit 1

NS="${NS:-5000,20000,80000,320000,1280000}"
SELS="${SELS:-0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,0.07,0.10}"
DIM="${DIM:-768}"
WD="${WD:-/tmp/prefilter_thr}"

echo "=== stage 1/3: concurrent preload + RDB snapshots ==="
date
python3 scripts/benchmark/preload_corpora.py \
    --n-values "$NS" --dim "$DIM" --workdir "$WD" --base-port 7700
rc=$?
if [ $rc -ne 0 ]; then
    echo "PRELOAD FAILED (rc=$rc); not measuring on an incomplete corpus set"
    exit $rc
fi

echo
echo "=== stage 2/3: serialized throughput grid ==="
date
python3 scripts/benchmark/prefilter_throughput_e2e.py --mode grid \
    --n-values "$NS" --dim "$DIM" --selectivities "$SELS" \
    --procs 8 --threads-per 4 --duration 2 --warmup 0.5 \
    --workdir "$WD" --port 7521 \
    --out prefilter-throughput
rc=$?
echo "throughput grid rc=$rc"

echo
echo "=== stage 3/3: per-test-case report + figures ==="
date
if [ -f prefilter-throughput.json ]; then
    python3 scripts/benchmark/make_report.py \
        --throughput-json prefilter-throughput.json \
        --outdir figures --out PREFILTER-REPORT.md
    echo "report rc=$?"
else
    echo "no prefilter-throughput.json; attempting recovery from the log"
    python3 scripts/benchmark/recover_throughput_log.py \
        --log /tmp/thr_dim2.log --out prefilter-throughput-partial.json \
        --merge-with /tmp/thr_snapshot_multiproc.json
    python3 scripts/benchmark/make_report.py \
        --throughput-json prefilter-throughput-partial.json \
        --outdir figures --out PREFILTER-REPORT.md
fi

echo
echo "=== driver finished ==="
date

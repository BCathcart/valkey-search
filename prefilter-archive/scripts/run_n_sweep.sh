#!/bin/bash
# N-sensitivity sweep at the production default ef_runtime, dim 768.
#
# Purpose: the planner applies a single global *ratio*. Pre-filter cost is
# O(ratio*N) exact distance computations, while inline HNSW traversal grows far
# more slowly with N. So the crossover ratio should fall as N grows, and if it
# does, no single global ratio can be regression-free at every scale. This
# measures how fast it falls instead of extrapolating from one point.
#
# Wide log-spaced selectivities so the crossover is bracketed at both the small
# and large end of the N range.
set -u
cd "$(dirname "$0")/../.." || exit 1

BIN=./.build-release/tests/indexes_test
SEL=0.0002,0.0005,0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,0.07,0.10

for n in 5000 20000 80000 320000 1280000; do
  log="/tmp/micro_nsweep_${n}.log"
  echo "=== $(date +%H:%M:%S) N=$n starting ==="
  env BENCH_N="$n" BENCH_DIMS=768 BENCH_EFR=10 \
      BENCH_SELECTIVITIES="$SEL" BENCH_QUERIES=100 BENCH_WARMUP=10 \
      timeout 5400 "$BIN" \
      --gtest_also_run_disabled_tests \
      --gtest_filter='*DISABLED_PrefilterCrossoverBenchmark*' > "$log" 2>&1
  echo "=== $(date +%H:%M:%S) N=$n exit=$? ==="
done

echo "=== $(date +%H:%M:%S) N SWEEP COMPLETE ==="

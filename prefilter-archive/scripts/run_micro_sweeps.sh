#!/bin/bash
# Drive the micro-benchmark sweeps unattended.
#
# Run under setsid so an SSH disconnect cannot take the runs down (an earlier
# attempt was lost that way). Each stage writes its own log so a failure part
# way through still leaves the earlier stages' results on disk.
set -u
cd "$(dirname "$0")/../.." || exit 1

BIN=./.build-release/tests/indexes_test
SEL=0.005,0.01,0.02,0.05,0.10,0.20,0.30,0.40

run() {
  local log="$1"; shift
  local filter="$1"; shift
  echo "=== $(date +%H:%M:%S) starting $log ==="
  env "$@" timeout 2400 "$BIN" \
    --gtest_also_run_disabled_tests \
    --gtest_filter="$filter" > "$log" 2>&1
  echo "=== $(date +%H:%M:%S) $log exit=$? ==="
}

# Stage 1: DIM sweep at the production default ef_runtime (10). The earlier
# clustered run used the benchmark's own default of 128, which does ~12.8x more
# graph traversal and therefore reports a higher crossover than production sees.
run /tmp/micro_efr10.log '*DISABLED_PrefilterCrossoverBenchmark*' \
  BENCH_N=20000 BENCH_DIMS=128,768,1536 BENCH_EFR=10 \
  BENCH_SELECTIVITIES="$SEL" BENCH_QUERIES=100 BENCH_WARMUP=10

# Stage 2: same sweep at ef_runtime=128, for the ef sensitivity comparison.
# (Already have this for clustered data, but re-run into a stable filename so
# both halves of the comparison come from one build.)
run /tmp/micro_efr128.log '*DISABLED_PrefilterCrossoverBenchmark*' \
  BENCH_N=20000 BENCH_DIMS=128,768,1536 BENCH_EFR=128 \
  BENCH_SELECTIVITIES="$SEL" BENCH_QUERIES=100 BENCH_WARMUP=10

# Stage 3: filter-type cost comparison (numeric vs tag vs text) at the
# production default ef_runtime, to test whether a more expensive predicate
# moves the crossover.
run /tmp/micro_filtercost_efr10.log '*DISABLED_FilterCostCrossoverBenchmark*' \
  BENCH_N=20000 BENCH_DIM=768 BENCH_EFR=10 \
  BENCH_SELECTIVITIES="$SEL" BENCH_QUERIES=100 BENCH_WARMUP=10

# Stage 4: N sensitivity at the production default. The planner uses a fixed
# ratio, so whether the safe ratio is stable across N decides if one global
# number can be regression-free.
for n in 5000 20000 80000; do
  run "/tmp/micro_n${n}_efr10.log" '*DISABLED_PrefilterCrossoverBenchmark*' \
    BENCH_N="$n" BENCH_DIMS=768 BENCH_EFR=10 \
    BENCH_SELECTIVITIES="$SEL" BENCH_QUERIES=100 BENCH_WARMUP=10
done

echo "=== $(date +%H:%M:%S) ALL MICRO SWEEPS COMPLETE ==="

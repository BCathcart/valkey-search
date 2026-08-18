# Investigation plan & state: raising the HNSW pre-filtering threshold

Resumable state file. Written to disk because an SSH drop already killed one
run; if the session dies again, read this first.

**Goal.** Decide how far `prefiltering-threshold-ratio` (default `0.001` = 0.1%)
can be raised without regressing any workload. Brennan's prior is that ~7% may
be safe (from published work) and that expensive non-vector predicates should
favour pre-filtering.

**Repo.** `/local/home/brenncat/oss-workplace/valkey-search-main-repo-3` (branch
`main`). Build dir `.build-release` (already configured).

---

## Status: BOTH DIMENSIONS COMPLETE (2026-08-18 00:50 UTC)

| dimension | coverage | file |
|---|---|---|
| 1. Serial latency | complete — 400 points (8 patterns x 5 corpus sizes x 10 selectivities) | `prefilter-data/prefilter-matrix.json` / `.md` |
| 2. Saturated throughput | complete — 400 points, same grid | `prefilter-data/prefilter-throughput.json` |

Deliverable: `PREFILTER-REPORT.md` — per test case, the exact query plus a
separate table and graph for each dimension (16 figures in `figures/report/`).

### Final recommendation

```
use_prefilter  <=>  qualified <= 0.48 * sqrt(N * ef_runtime)
```

`C = 0.48` is the largest coefficient holding a >=2x margin at every measured N
against the binding pattern (`num AND tag AND text`, throughput-derived fit
`qualified_crossover = 2.566 * N^0.519`). Margins 2.0-2.6x from N=5k to 10M.

If only the constant may change, pick from the largest N you must protect:
1.01% (<=100k), 0.33% (<=1M), 0.111% (<=10M), 0.036% (<=100M).

**Coefficient history — do not regress this.** 0.6 (latency only) -> 0.4 (sparse
3-point throughput spot-check, over-corrected) -> **0.48** (full 400-point
throughput grid). The full grid puts the throughput/latency crossover ratio at
**0.87x** mean, not the 0.78x the spot-check estimated.

### Two-regime result (the distinction Brennan asked to measure separately)

- **Small corpus / cheap query -> ceiling-bound, the choice barely matters.**
  Both paths converge on ~12-13k qps (gain 1.0-1.1x). Not main-thread
  saturation: main CPU peaks ~41%. Rows are labelled `client-bound` and are
  excluded from fits; they must not be read as a crossover.
- **Larger corpus -> reader-bound, the choice matters enormously.** At N=1.28M,
  `num AND tag AND text` runs 27x in pre-filter's favour at 0.1% selectivity and
  0.007x against it at 10%. Readers pegged at 397-400% of the 400% target.

Exponent is ~sqrt(N) in **both** dimensions (latency b=0.50-0.54, throughput
b=0.52), so a sqrt rule holds its margin as N grows.

### Resume in one command

**Both dimensions are complete**, so this section is only needed to re-run or
extend the grid (more corpus sizes, other `ef_runtime`, cosine, other k).

Verified working: resume correctly skipped completed corpus sizes and, for the
7.7 GB snapshot, logged `still waiting for server readiness (31s/900s)` then
`corpus restored from RDB: dbsize=1280000 num_docs=1280000 backfill=0
(load skipped)` — 0 errors.

Note on row counts: resume loads 176 rows from the 243-row file, not 243. That is
correct — the 67 partial N=80,000 rows are deliberately dropped so that corpus is
re-measured whole rather than stitched across two runs.

All five corpora are already loaded and snapshotted to RDB (11 GB under
`/tmp/prefilter_thr/`). Restore is seconds, so resuming costs measurement time
only (~25 min), not the 30 min of loading.

```bash
cd /local/home/brenncat/oss-workplace/valkey-search-main-repo-3
./scripts/benchmark/stop_bench.sh                 # clear stray servers first
cp prefilter-data/prefilter-throughput.json /tmp/thr_resume.json

setsid nohup python3 scripts/benchmark/prefilter_throughput_e2e.py --mode grid \
  --n-values 5000,20000,80000,320000,1280000 --dim 768 \
  --selectivities 0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,0.07,0.10 \
  --procs 8 --threads-per 4 --duration 2 --warmup 0.5 \
  --workdir /tmp/prefilter_thr --port 7521 \
  --resume-from /tmp/thr_resume.json \
  --out prefilter-throughput > /tmp/thr_full3.log 2>&1 < /dev/null &
disown
```

`--resume-from` skips any corpus size already complete (5k, 20k), so it starts at
N=80,000. It persists to `prefilter-data/prefilter-throughput.json` after **each** corpus size.

**If `/tmp/prefilter_thr/` has been cleared** (reboot, tmp reaper), the RDB
snapshots are gone and the corpora must be rebuilt first — ~30 min, concurrent:

```bash
python3 scripts/benchmark/preload_corpora.py \
  --n-values 5000,20000,80000,320000,1280000 --dim 768 \
  --workdir /tmp/prefilter_thr --base-port 7740
```

The measurement harness also self-heals: `ensure_corpus()` reloads any corpus
whose snapshot is missing, so the resume command works either way — it just
takes the load hit for the sizes it has to rebuild.

Then produce the deliverable:

```bash
python3 scripts/benchmark/make_report.py \
  --throughput-json prefilter-data/prefilter-throughput.json \
  --outdir figures/report --out PREFILTER-REPORT.md
python3 scripts/benchmark/reconcile_dimensions.py     # supersedes sparse correction
```

If it dies mid-corpus, salvage the unpersisted rows from the log first:

```bash
python3 scripts/benchmark/recover_throughput_log.py \
  --log /tmp/thr_full3.log --out /tmp/thr_resume.json \
  --merge-with prefilter-data/prefilter-throughput.json
```

### VERIFY BEFORE TRUSTING ANY RESUMED RUN

**Do not mix instruments.** Dimension 2 was measured two ways. Only thread-based
rows are comparable with each other:

- **thread-based (canonical)**: `prefilter-data/prefilter-throughput.json`, 400 rows,
  `"generator": "threads"`. Use this.
- **multiprocess (abandoned)**: `prefilter-archive/data/prefilter-throughput-multiproc-abandoned.json`, 190 rows.
  Reached higher qps at small N (~52k vs ~12.5k) but the instrument was removed
  after it destabilised the host. Keep only as evidence for the main-bound
  regime; **never merge into the thread-based file** — identical
  (N, pattern, selectivity) keys would silently overwrite.

### Three crashes, all root-caused

**Crash 1 — my bug (fixed).** `mp.Queue()` was created per measurement point and
never closed, leaking POSIX semaphores; it died after ~364 points with
`OSError(38)`. Compounded by `mp.set_start_method("spawn")`, which made every
child re-import the module and fail on `json/scanner.py`. Fix: removed
multiprocessing entirely (`_proc_worker*` deleted, `set_start_method` removed);
`run_saturating()` is threads-only.

**Crash 2 — host-level, NOT my script.** Recurred with the threads-only
generator during N=80,000:
`ConnectionError('Error 38 connecting to 127.0.0.1:7521. Function not implemented')`,
and simultaneously `date` and `python3` failed with
`libc.so.6: cannot open shared object file: Error 38`. A plain `connect()` and
the dynamic loader returning ENOSYS is the host's syscall/filesystem layer
failing, not process churn. It recovers on its own (verified: `date` and
`python3` work again, load 1.23).

**Crash 3 - readiness timeout too short for a large RDB (fixed).** The N=1.28M
server needs >60s to restore its 7.7 GB snapshot, but `start_server()` had a
hard-coded 60s readiness deadline and SIGTERM'd it mid-load
(`Received shutdown signal during loading`). Two fixes in
`prefilter_crossover_e2e.py::start_server`:

1. `ready_timeout` parameter, default **900s**, with a progress line every 30s so
   a slow restore is distinguishable from a hang.
2. Readiness now also requires `INFO persistence -> loading == 0`. valkey answers
   `PING` *while still loading*, so returning early let callers read a partial
   `DBSIZE`; `ensure_corpus()` would have concluded "corpus incomplete", issued
   `FLUSHALL` and re-loaded serially for ~28 minutes. The wait is signalled by a
   private `_StillLoading` exception that is deliberately **not** a
   `RuntimeError`, because the readiness loop treats `RuntimeError` as fatal
   (foreign-server identity mismatch).

Verified: N=1.28M now logs `still waiting for server readiness (31s/900s)` then
`corpus restored from RDB: dbsize=1280000 num_docs=1280000 backfill=0`.

**Implication:** expect this to recur. That is why the harness persists after
every corpus size and why RDB snapshots exist. Do not "fix" it in the script.
If it recurs, wait for the host to recover, then resume with the command above.

### Concurrency policy (deliberate, do not "optimise" this)

- **Loading: concurrent.** All five corpora load in parallel
  (`preload_corpora.py`). Measured 30.2 min vs 33.6 min sequential.
- **Measurement: strictly serialized, one server at a time.** Dimension 2 is a
  saturation measurement. Concurrent servers on this 32-core host would contend
  for the exact resource under test (reader CPU, memory bandwidth).
  Pre-filtering is bandwidth-heavy and inline is not, so contention would
  penalise pre-filtering differentially and corrupt the comparison in a
  direction that cannot be corrected for.

### Infrastructure findings worth keeping

1. **RDB round-trip preserves the vector index.** Verified at N=20k/dim768:
   `SAVE` 1.1 s, 127 MB, reload instant, `backfill_in_progress: 0`, top-10
   identical pre/post. This is what makes resume cheap.
2. **Writes must be serial.** Pipelined `HSET` into an indexed keyspace
   deadlocks permanently (Issue 2). Hence 0.43-1.31 ms/doc and the 28 min
   1.28M load.
3. **`pkill -f <pattern>` signals the invoking shell** when the pattern appears
   in its own command line. Bit me three times. Use
   `scripts/benchmark/stop_bench.sh` / `stop_watchdog.sh`, which exclude `$$`.
4. **The watchdog was removed.** It overwrote a good 160-row file with 0 rows
   because resumed rows come from JSON, not reprinted to the log. Per-corpus
   persist plus RDB snapshots cover the same risk with fewer moving parts. If
   reintroduced, it must merge and refuse to shrink the file.

### Deliverable format Brennan asked for

Per **test case** (= one query varied against corpus size), separately for each
dimension: the exact query string, a table (rows = selectivity, columns = corpus
size), and a graph. 7 test cases x 2 dimensions. Produced by `make_report.py`
into `PREFILTER-REPORT.md`. The older six mixed figures in `figures/fig1..fig6`
are superseded for reporting purposes; `fig2-crossover-vs-n.png` is still the
single best summary picture.

Remaining after the run:
- Compare throughput-derived vs latency-derived crossover per (pattern, N) at
  full resolution; supersede the sparse correction currently in
  `prefilter-crossover-results.md` ("Latency vs throughput" section).
- Re-derive the recommended coefficient from the full throughput grid.

**Partial result already in hand (N=5,000 and 20,000, all 7 valid patterns).**
Full-resolution throughput crossover vs latency crossover:

| N | mean qps_crossover / lat_crossover |
|---|---|
| 5,000 | 0.95x |
| 20,000 | 0.83x |
| overall | 0.89x (median 0.88x) |

The sparse 3-point spot-check used earlier said 0.78x. So the **direction is
confirmed** (throughput crossover sits below latency, i.e. serial latency
flatters pre-filtering) but the **magnitude was overstated**, and the gap
narrows as N grows. This means the `0.6 -> 0.4` coefficient cut may have been an
over-correction; do not finalise the coefficient until 320k and 1.28M land.
Per-pattern at N=20,000: numeric 0.91x, tag 0.78x, text 0.80x, text OR text
0.83x, num AND tag 0.81x, num AND tag AND text 0.84x, num AND NOT tag 0.81x.

### Key numbers so far (latency dimension, unchanged)

Crossover ratio falls as ~1/sqrt(N). Cheap numeric crosses highest, composed
hybrids ~1.9x lower and are the binding constraint:

| N | numeric | binding (num AND tag AND text) |
|---|---|---|
| 5,000 | 8.27% | 4.98% |
| 20,000 | 4.61% | 2.91% |
| 80,000 | 2.22% | 1.40% |
| 320,000 | 1.13% | 0.63% |
| 1,280,000 | 0.70% | 0.35% |

- 7% is safe only around N ~ 5,000.
- Today's 0.1% is ~23x too conservative at N=20k and too aggressive above ~6M.
- Working recommendation: `qualified <= 0.4 * sqrt(N * ef_runtime)`, pending
  re-derivation from the full throughput grid.
- The "complex predicate favours pre-filtering" hypothesis is **not** supported.
- **A correctness bug was found** — see `VALKEY-SEARCH-ISSUES-FOUND.md` Issue 1.

All sweeps finished. Findings, full data tables and the recommendation live in
**`prefilter-crossover-results.md`** (micro + synthesis) and
**`prefilter-archive/superseded-reports/prefilter-crossover-e2e-results.md`** (end-to-end, generated by the harness).
Read those first; this file is retained for the investigation trail.

Verification: `cmake --build .build-release --target indexes_test` clean, and
`./.build-release/tests/indexes_test` → 201/201 pass, 3 disabled (the
pre-existing `DISABLED_PrefetchBenchmark` plus the two new benchmarks).

### Headline result

The crossover ratio falls as ~`1/sqrt(N)`, so a fixed ratio is the wrong shape.
Measured across 7 filter patterns x 5 corpus sizes end to end
(`prefilter-archive/superseded-reports/prefilter-matrix.md`), cross-validated against the in-process micro-benchmark
to within ~10%. Fitting `qualified_crossover = a*N^b` gives `b = 0.50-0.54` for
every pattern.

- Cheap numeric crosses highest (8.27% @ N=5k down to 0.70% @ 1.28M); composed
  hybrids cross ~1.9x lower and are the binding constraint
  (`num AND tag AND text`: 0.349% @ 1.28M).
- 7% is safe only around N ~ 5,000.
- Today's 0.1% is ~23x too conservative at N=20k **and** too aggressive above
  N≈6M. Wrong in both directions depending on index size.
- Preferred fix: `qualified <= 0.4 * sqrt(N * ef_runtime)` (~1.9-2.6x margin).
  Coefficient calibrated on **throughput**, not latency: the throughput-derived
  crossover sits ~20% below the latency-derived one, because serial measurement
  gives pre-filtering the whole memory bandwidth to itself. If only the constant
  may change: 0.95% @ 100k, 0.27% @ 1M, 0.075% @ 10M ceiling (halve for margin).
- The "complex predicate favours pre-filtering" hypothesis is **not** supported:
  numeric crosses highest at every N. Pre-filtering does not amortise predicate
  cost to one evaluation — `EvaluatePrefilteredKeys` re-runs `PrefilterEvaluator`
  per qualified key for unsolved shapes.
- **A correctness bug was found** (inline drops text predicates under a composed
  AND, returning non-matching documents). See `VALKEY-SEARCH-ISSUES-FOUND.md`.

### Artifacts

| path | purpose |
|---|---|
| `VALKEY-SEARCH-ISSUES-FOUND.md` | **defects found** (correctness bug, write stall, test-quality, design gap) |
| `prefilter-crossover-results.md` | micro results, synthesis, recommendation |
| `prefilter-archive/superseded-reports/prefilter-matrix.md` / `.json` / `.csv` | pattern x corpus-size matrix (primary evidence) |
| `figures/fig1..fig5*.png` | graphs for reporting |
| `prefilter-archive/superseded-reports/prefilter-crossover-e2e-results.md` / `.json` | earlier E2E saturating-load sweep |
| `testing/vector_test.cc` | 2 `DISABLED_` benchmarks + `GenerateBenchmarkVectors` |
| `scripts/benchmark/prefilter_matrix_e2e.py` | matrix harness (pattern x N) |
| `scripts/benchmark/plot_matrix.py` | figure generation |
| `scripts/benchmark/prefilter_crossover_e2e.py` | E2E harness (saturating load) |
| `scripts/benchmark/check_filter_honoured.py` | proves the filter-drop bug |
| `prefilter-archive/scripts/probe_text_forms.py` | finds which text forms inline honours |
| `prefilter-archive/scripts/run_micro_sweeps.sh` | dim / ef / filter-type driver |
| `prefilter-archive/scripts/run_n_sweep.sh` | N sensitivity driver |
| `prefilter-archive/scripts/analyse_crossover.py` | log-log crossover interpolation |
| `prefilter-archive/scripts/check_path_agreement.py` | numpy ground-truth correctness |
| `prefilter-archive/scripts/diag_load.py` | load-path bisect (found the write stall) |
| `scripts/benchmark/stop_bench.sh` | safe cleanup |

Raw logs in `/tmp/micro_*.log`, `/tmp/matrix_full.log`, `/tmp/e2e_sweep.log`.
These do not survive a host reboot; re-run via the drivers if needed.

Nothing committed; no git operations were performed. `.vscode/settings.json` is
modified in the working tree but that is a pre-existing IDE change, not part of
this work.

### Follow-ups worth filing separately

1. **Pipelined-write stall** (details below) — a real server bug, independent of
   this investigation.
2. **Concurrency effect on pre-filtering is unmeasured.** E2E could not isolate
   it because inline was pinned at a ~12.4k qps request-rate ceiling. Worth a
   dedicated experiment before shaving the 2x margin.
3. **`ef_runtime` is absent from the planner formula** despite the crossover
   scaling as ~sqrt(ef). This is what the existing `// TODO` in `planner.cc`
   alludes to.

---



## How the two paths work (confirmed by reading source)

- `UsePreFiltering` (`src/query/planner.cc`): for HNSW, chooses pre-filter iff
  `estimated_num_of_keys <= GetPrefilteringThresholdRatio() * N`. FLAT always
  pre-filters. Predicate *cost* is not an input — only estimated cardinality.
  Carries a `// TODO: Tune the threshold ratio`.
- Inline: `InlineVectorFilter` (private to `src/query/search.cc`) is an
  `hnswlib::BaseFilterFunctor`; hnswlib calls it per visited candidate. Each
  call does label→key then `PrefilterEvaluator::Evaluate` over the whole
  predicate tree.
- Pre-filter: `EvaluateFilterAsPrimary` builds entries fetchers, then
  `CalcBestMatchingPrefilteredKeys` → `AddPrefilteredKey` does an exact
  brute-force distance scan into a bounded top-k heap. Exact, so recall 1.0.
- `kDefaultEFRuntime = 10`. This matters a lot — see findings.
- `.Dev()` gating (`vmsdk/src/module_config.h`): Dev configs are
  Hidden+Immutable unless `search.debug-mode yes`, in which case they are
  runtime-mutable. So the threshold can be swept via `CONFIG SET` in debug mode,
  and **shipping a new default requires no un-gating** — but making it tunable
  in production would.

## Instruments built

1. **Micro-benchmark** — `testing/vector_test.cc`, two `DISABLED_` gtests
   following the existing `DISABLED_PrefetchBenchmark` convention. No CMake
   change needed (`vector_test.cc` is already in `INDEXES_TEST_SOURCES`).
   - `DISABLED_PrefilterCrossoverBenchmark` — numeric filter, DIM sweep.
   - `DISABLED_FilterCostCrossoverBenchmark` — numeric vs tag vs text.
   - Env: `BENCH_N BENCH_DIMS BENCH_DIM BENCH_SELECTIVITIES BENCH_K BENCH_M
     BENCH_EFC BENCH_EFR BENCH_QUERIES BENCH_WARMUP`.
   - Build: `cmake --build .build-release --target indexes_test -j $(nproc)`.
   - Exact selectivity: key `i` gets numeric value `i`, so `@num:[0 S-1]`
     matches exactly `S` keys — no estimation error.
   - Mirrors `InlineVectorFilter` and `CalcBestMatchingPrefilteredKeys` because
     both are private to `search.cc`. Commented as mirrors so drift is visible.
   - Reports recall of inline against the exact pre-filter result. **This guard
     matters**: inline can return fewer/worse than k under a selective filter,
     which would look like a speed win but is a quality loss.
2. **E2E harness** — `scripts/benchmark/prefilter_crossover_e2e.py`. Real
   `valkey-server` + module, real `FT.SEARCH`, saturating concurrent load,
   forces each path via `CONFIG SET search.prefiltering-threshold-ratio` 0.0
   (always inline) / 1.0 (always pre-filter). Zero mirroring risk, handles
   arbitrary query shapes including 3-way AND / OR / negation.
3. Helpers: `check_path_agreement.py` (numpy ground truth), `diag_load.py`
   (load-path bisect), `stop_bench.sh` (safe cleanup), `run_micro_sweeps.sh`
   (unattended driver).

## Deliberately not used

The existing memtier infra (`.github/benchmark_configs/`,
`scripts/benchmark/run_endurance_test.sh`) — that answers "did we regress under
load?" with fixed scenarios. This needs filter selectivity as a swept
independent variable and a paired per-query A/B. Different question. Untouched.

---

## Investigation trail (findings in the order they were established)

**Micro, clustered corpus, N=20000, ef_runtime=128** (`/tmp/micro_clustered.log`)
— crossover 10–20% for dims 128 / 768 / 1536, recall 1.000 throughout. Roughly
dim-independent, because both paths scale linearly in dim.

**E2E, clustered, N=20000 dim=768, ef_runtime=10 (default), 4 readers, 16
clients** (`prefilter-archive/superseded-reports/prefilter-crossover-e2e-results.md`) — numeric crosses at 5–10%; all
8 complex shapes cross *lower*, at 2–5%. Recall 1.000.

**This contradicts the "complex predicate favours pre-filtering" hypothesis.**
Cause: the pre-filter path also pays per-key predicate cost — for "unsolved"
shapes (AND-with-numeric/tag, negation) `EvaluatePrefilteredKeys` re-runs
`PrefilterEvaluator` per qualified key, plus dedup for OR/TAG/negate. So a
costlier predicate inflates *both* paths, and the cheap numeric filter turns out
to be the *best* case for pre-filtering, not the worst. Which means calibrating
on cheap numeric is still the right conservative choice — but it yields the
*highest* crossover, so the safe threshold is set by the complex shapes.

**Bugs / methodology traps found and fixed:**
- `DeterministicallyGenerateVectors` sets element `j` of vector `i` to
  `max_value*(i+j)/(size+dim)` — every vector is a linear ramp, so the corpus is
  essentially 1-D. Degenerate; invalidated the first micro numbers (~31%). Added
  `GenerateBenchmarkVectors` (Gaussian mixture) and pointed both crossover
  benchmarks at it. Unrelated correctness tests still use the ramp generator.
- Uniform-random high-dim vectors are adversarial for HNSW: at dim 768 inline
  recall fell to 0.30 and it *missed the exact nearest neighbour*. Clustered
  data restores recall 1.000. Corpus distribution changes the conclusion, so
  the E2E harness defaults to `--data clustered`.
- **valkey-search stall with pipelined writes.** Deep pipelines of `HSET` into
  an indexed keyspace deadlock: `ShouldBlockClient` (`src/index_schema.cc:894`)
  blocks the client per mutation; after unblocking, commands already sitting in
  the query buffer are never re-processed. Observed with `qbuf=548416`,
  `idle=98s`, `search_writer_queue_size:0`, server in `epoll_wait` at 0% CPU.
  Not a benchmark artifact — a real server-side bug worth filing separately.
  Worked around by loading serially.
- E2E `start_server` pinged the port without verifying the responder was the
  process it spawned, so a leftover server (there were stale ASAN-build ones)
  could have been measured instead. Now refuses a busy port and matches
  `INFO server` `process_id`. Leftovers have been cleaned up.
- First E2E `agree` column was bogus (decayed as k/qualified) because it
  compared results from different random query vectors. Replaced with
  `measure_recall`: fixed query vectors, run serially outside the load.
- Inline is pinned at a ~12.4k qps server request-rate ceiling above ~5%
  selectivity while using only 130–200% CPU, so E2E *understates* inline's
  advantage there and hides its shape sensitivity. That is what the
  micro-benchmark is for.

**Reconciliation — RESOLVED.** Micro said 10–20%, E2E said 5–10%. Cause
confirmed: the micro-benchmark ran at `ef_runtime=128` while production defaults
to `kDefaultEFRuntime = 10`. Re-running the micro at ef=10 moved its crossover to
4.01% (dim 768, N=20k), which brackets correctly against E2E's 5–10% once E2E's
~12.4k qps request-rate ceiling is accounted for (the ceiling caps inline, so
E2E reads high). The crossover scales as roughly `sqrt(ef_runtime)` — a 12.8x ef
increase moved it 3.6x. The planner's fixed ratio ignores `ef_runtime` entirely,
exactly as its TODO admits.

**N dependence — the decisive result.** Measured at ef=10, dim 768: 7.50% @
N=5k, 4.01% @ 20k, 1.99% @ 80k, 1.07% @ 320k, 0.795% @ 1.28M. Pre-filter cost is
O(ratio*N) exact distance computations while inline traversal grows far more
slowly, so the crossover *ratio* falls as N grows (`~N^-0.42`). Therefore no
single global ratio is regression-free at every scale, which reframes the whole
question: the fix is the shape of the rule, not the value of the constant. See
`prefilter-crossover-results.md` for the fit and recommendation.

**Inline recall degrades at scale.** At N=1.28M inline recall is 0.978 at 1%
selectivity and 0.842 at 10%, while pre-filtering is exact by construction. So
above the crossover inline's throughput advantage is partly paid for in answer
quality — an independent argument for sitting on the pre-filter side when the two
are close.

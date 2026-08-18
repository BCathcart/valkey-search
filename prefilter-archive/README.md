# Archive: superseded material from the pre-filter threshold investigation

Nothing here is current. Kept only as an evidence trail. The live artifacts are:

| file | what it is |
|---|---|
| `../prefilter-crossover-results.md` | **the answer** — recommendation, reasoning, caveats |
| `../PREFILTER-REPORT.md` | **the evidence** — per test case, table + graph per dimension |
| `../VALKEY-SEARCH-ISSUES-FOUND.md` | **the bugs** — 4 issues, correctness bug first |
| `../PREFILTER-INVESTIGATION-PLAN.md` | resume state and methodology trail |
| `../prefilter-matrix.json`, `../prefilter-throughput.json` | canonical raw data, 400 points each |
| `../figures/fig2-crossover-vs-n.png` | the one-picture summary |
| `../figures/report/` | the 16 per-test-case figures |

## `superseded-reports/`

| file | why superseded |
|---|---|
| `prefilter-matrix.md` | Latency dimension only. `PREFILTER-REPORT.md` covers both dimensions over the same grid. |
| `prefilter-matrix.csv` | Long-form export of the latency matrix. Regenerable from `prefilter-data/prefilter-matrix.json`. |
| `prefilter-crossover-e2e-results.md` | **Actively misleading — do not quote.** The early N=20,000-only run with just 3 selectivity points under load. Its sparse data produced the throughput/latency ratio estimate of `0.78x`, which the full 400-point grid later corrected to `0.87x`. That correction is what moved the recommended coefficient from `0.4` to `0.48`. |
| `prefilter-crossover-e2e-results.json` | Raw data for the above. |

## `data/`

`prefilter-throughput-multiproc-abandoned.json` — 190 rows from the abandoned
multiprocess load generator.

**Never merge this into `prefilter-data/prefilter-throughput.json`.** It shares
`(N, pattern, selectivity)` keys with the thread-based data, so a merge would
silently overwrite good rows with rows from a different instrument. It reached
higher qps at small N (~52k vs ~12.5k, enough to push the main thread to ~88%),
so it is the only evidence for the genuinely main-bound regime — but the
generator spawned ~1120 short-lived processes per corpus size and drove the
host's syscall layer into returning `ENOSYS` on ordinary file opens, so it was
removed.

## `figures/`

`fig1`, `fig3`, `fig4`, `fig5`, `fig6` — the older summary figure set. Redundant
now that `PREFILTER-REPORT.md` carries per-test-case figures for both dimensions.
`fig2-crossover-vs-n.png` was kept at the top level as the single best summary.
All are regenerable: `python3 scripts/benchmark/plot_matrix.py --json prefilter-data/prefilter-matrix.json --outdir figures`.

Note `fig1`/`fig3`..`fig6` were generated before the final coefficient was
settled, so any rule line they draw may show `0.4`/`0.6` rather than `0.48`.

## `scripts/`

One-off diagnostics and drivers whose job is done. Each earned a finding:

| script | what it established |
|---|---|
| `diag_load.py` | Bisected the load path to prove pipelined `HSET` into an indexed keyspace deadlocks permanently (Issue 2). Caught the server idle at 0% CPU with 548 KB unread in the client query buffer. |
| `check_path_agreement.py` | Showed the apparent inline/pre-filter disagreement at dim 768 was distance concentration from uniform random vectors, not a bug — inline recall fell to 0.30 and missed the exact nearest neighbour. Motivated switching the corpus to clustered. |
| `probe_text_forms.py` | Found that `@txt:a @txt:b` (composed AND) is dropped by inline filtering while `text OR text` is honoured, giving a valid text-only complex pattern for the matrix. |
| `analyse_crossover.py` | Early crossover fitting from micro-benchmark logs. Superseded by `reconcile_dimensions.py`, which fits both dimensions. |
| `watch_throughput.sh`, `stop_watchdog.sh` | Snapshot watchdog, **removed deliberately**. It once overwrote a good 160-row file with 0 rows, because resumed rows come from JSON and are not reprinted to the log. Per-corpus persistence plus RDB snapshots cover the same risk with fewer moving parts. If ever reintroduced it must merge and refuse to shrink the file. |
| `run_micro_sweeps.sh`, `run_n_sweep.sh` | Drove the micro-benchmark DIM and N sweeps (complete). |
| `run_dimension2.sh` | Drove preload + throughput grid + report. Superseded by the direct resume command in `PREFILTER-INVESTIGATION-PLAN.md`, which is finer-grained and resumable. |

These import `prefilter_crossover_e2e` from `scripts/benchmark/`, so to re-run one
either copy it back or invoke it with
`PYTHONPATH=scripts/benchmark python3 prefilter-archive/scripts/<name>.py`.

## Still live in `scripts/benchmark/`

The reproducible pipeline, deliberately not archived:
`prefilter_crossover_e2e.py` (shared library), `prefilter_matrix_e2e.py`
(dimension 1), `prefilter_throughput_e2e.py` (dimension 2),
`preload_corpora.py`, `make_report.py`, `reconcile_dimensions.py`,
`recover_throughput_log.py`, `plot_matrix.py`, `stop_bench.sh`, and
`check_filter_honoured.py` (reproduces the Issue 1 correctness bug — kept
because that bug is the most important finding here).

`run_endurance_test.sh` is a pre-existing repo file, untouched.

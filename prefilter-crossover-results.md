# Pre-filter vs inline-filter crossover: measurements and threshold recommendation

**Question.** How far can the HNSW `prefiltering-threshold-ratio` (default
`0.001` = 0.1%) be raised without regressing any workload? Initial prior was
that ~7% might be safe, based on published work.

**Answer, up front.** A single global *ratio* cannot be raised to 7% safely, and
more fundamentally a fixed ratio is the wrong shape. The crossover ratio falls as
roughly `1/sqrt(N)`, so any fixed ratio is simultaneously far too conservative at
small N and too aggressive at large N. Measured crossover ratio for the cheap
numeric filter (production default `ef_runtime=10`, dim 768, clustered corpus,
L2, k=10), from two independent instruments:

| N | micro-benchmark (in-process) | E2E matrix (real `FT.SEARCH`) |
|---|---|---|
| 5,000 | 7.50% | 8.27% |
| 20,000 | 4.01% | 4.61% |
| 80,000 | 1.99% | 2.22% |
| 320,000 | 1.07% | 1.13% |
| 1,280,000 | 0.795% | 0.70% |

The two agree within ~10% across a 256x range of N, which is the main reason to
trust the shape.

Composed hybrid filters cross **lower** than numeric — `num AND tag AND text`
crosses at 0.349% at N=1.28M versus numeric's 0.65% — so composed patterns are
the binding constraint, not the cheap ones.

So 7% is only safe at around N ≈ 5,000. Conversely the current 0.1% default is
~23x too conservative at N = 20k, and becomes *too aggressive* above N ≈ 12M.
**The current default is the wrong shape, and is wrong in both directions
depending on index size.**

Recommended replacement: `qualified <= 0.48 * sqrt(N * ef_runtime)`. Note the
crossover figures above are from serial latency; the coefficient is calibrated
against the ~13% lower **throughput**-derived crossover, since serial measurement
gives pre-filtering the whole memory bandwidth to itself. See "Latency vs
throughput".

> **Separately, and more urgent than the threshold:** this work uncovered a
> correctness bug. Inline filtering silently drops text predicates that sit under
> a composed `AND`, returning documents that do not match the query. See
> `VALKEY-SEARCH-ISSUES-FOUND.md` (Issue 1). It interacts with this topic:
> because inline is chosen whenever `qualified > 0.001 * N`, the current low
> threshold *maximises* exposure, and raising it reduces exposure.

---

## Recommendation

Evidence base: the pattern x corpus-size matrix in `prefilter-archive/superseded-reports/prefilter-matrix.md`
(7 filter patterns x 5 corpus sizes x 10 selectivities, measured end to end
through `FT.SEARCH` on a real server), cross-checked against the in-process
micro-benchmark. Figures in `figures/`.

### The crossover falls as ~1/sqrt(N), for every pattern

Fitting `qualified_crossover = a * N^b` per pattern gives an exponent tightly
clustered at **b = 0.50-0.54**, i.e. the crossover in *qualified keys* grows as
roughly `sqrt(N)`, so the crossover *ratio* falls as `1/sqrt(N)`:

| pattern | a | b | crossover @ N=1.28M | @ N=100M (extrap.) |
|---|---|---|---|---|
| numeric | 4.06 | 0.542 | 0.652% | 0.089% |
| text | 4.79 | 0.514 | 0.513% | 0.062% |
| text OR text | 4.94 | 0.513 | 0.526% | 0.063% |
| tag | 4.01 | 0.504 | 0.375% | 0.043% |
| num AND tag | 3.43 | 0.512 | 0.359% | 0.043% |
| num AND tag AND text | 3.42 | 0.510 | 0.349% | 0.041% |
| num AND NOT tag | 3.91 | 0.501 | 0.349% | 0.040% |

The cheap numeric filter has the **highest** crossover at every N and the
composed hybrids the lowest, so the composed patterns are the binding
constraint — roughly 1.9x below numeric.

### Preferred: replace the fixed ratio with a sqrt rule

```
use_prefilter  <=>  qualified <= 0.48 * sqrt(N * ef_runtime)
```

The coefficient is calibrated against the **throughput**-derived crossover, not
the latency-derived one — see "Latency vs throughput" below, which is the reason
it is 0.48 rather than the 0.6 the latency data alone would suggest. Margins
against the throughput-derived binding pattern (`num AND tag AND text`,
fitted `qualified_crossover = 2.566 * N^0.519` over the full grid):

| N | rule threshold (ef=10) | as a ratio | binding crossover (throughput) | margin |
|---|---|---|---|---|
| 5,000 | 107 | 2.15% | 4.27% *(fit)* | 2.0x |
| 20,000 | 215 | 1.07% | 2.44% *(measured)* | 2.3x |
| 80,000 | 429 | 0.54% | 1.05% *(measured)* | 2.0x |
| 320,000 | 859 | 0.27% | 0.56% *(measured)* | 2.1x |
| 1,280,000 | 1,717 | 0.13% | 0.35% *(measured)* | 2.6x |
| 10,000,000 | 4,800 | 0.048% | 0.110% *(extrap.)* | 2.3x |

`C = 0.48` is the largest coefficient holding a >=2x margin at **every** measured
N (the per-N values that would give exactly 2x are 0.478 / 0.491 / 0.504 / 0.518
/ 0.532, rising with N, so the smallest binds). This supersedes the `0.4` derived
earlier from a sparse 3-selectivity spot-check, which over-corrected.

`ef_runtime` belongs in the rule because the crossover scales as roughly
`sqrt(ef_runtime)`: at N=20,000 it moves from 4.01% (ef=10) to 14.80% (ef=128).
No single ratio can be right for both. Dimensionality does **not** belong —
crossover was 4.68% / 4.09% / 4.54% at dim 128 / 768 / 1536, since both paths
scale linearly in dim.

### What this means for the current 0.1% default

Still the wrong shape, and still wrong in both directions, but the margins are
tighter than the latency data alone implied:

| N | binding crossover (throughput) | vs current 0.1% |
|---|---|---|
| 20,000 | 2.30% | 23x too conservative |
| 80,000 | 1.07% | 11x too conservative |
| 320,000 | 0.50% | 5.0x too conservative |
| 1,280,000 | 0.23% | 2.3x too conservative |
| ~6,000,000 | 0.10% | about right |
| 10,000,000 | 0.075% | 1.3x too aggressive |

So the real win from raising the threshold is concentrated **below ~1M vectors**.
Above ~6M the current default is already at or past the crossover for composed
patterns, and should if anything come down.

### If only the ratio constant may change

Safe value set by the largest N that must not regress, using the
throughput-derived binding pattern:

| N ceiling to protect | max safe ratio | with 2x margin |
|---|---|---|
| 100,000 | 0.95% | 0.47% |
| 1,000,000 | 0.27% | 0.13% |
| 10,000,000 | 0.075% | 0.037% |

So `0.001` → `0.004` (0.4%) is safe to ~100k vectors, and `0.001` → `0.0013` is
about all you can justify if you must protect 1M. **7% is not safe for anything
above about 5,000 vectors.**

### Why the literature's ~7% is not wrong, just differently parameterised

The crossover scales as roughly `sqrt(ef_runtime)`. Papers typically tune for
high recall (ef in the hundreds) on datasets around 1M. At ef=128 the measured
constant is 3.7x the ef=10 constant, and extrapolating to ef≈500 at N≈1M lands
in the neighbourhood of 7%. The number is consistent with this data once ef and
N are accounted for; it just does not transfer to a default of `ef_runtime=10`.

### Latency vs throughput: which motivated the findings, and does it matter

Worth being explicit, because it is the main methodological weakness.

**The shape of the result is latency-derived.** Of the matrix, 400 points are
serial single-connection latency and 120 are saturating throughput — and every
number that drove the original recommendation (the crossover table, the per-pattern
`a`/`b` fits, the `sqrt(N)` law, the `ef_runtime` scaling) came from latency
alone. Throughput was used only for the separate "does it matter at this N"
question.

The justification for that is real but narrow: any fixed per-request overhead adds
the same constant to both paths, so it cancels at the point where the two curves
cross, and serial measurement avoids the ~12-13k qps plateau that compresses gains
toward 1.0. That makes latency a good instrument for *locating* a crossover.

**But it flatters pre-filtering.** A serial query has the whole memory bandwidth
and cache to itself. Pre-filtering streams `qualified x dim` floats and is
bandwidth-heavy; inline traversal is more pointer-chasing and latency-bound. Under
concurrency, bandwidth contention should penalise pre-filtering more — so the
latency crossover is an optimistic upper bound.

Checking that against the throughput points confirms it, and quantifies it. Ratio
of throughput-derived to latency-derived crossover, averaged over patterns:

Measured over the **full grid** (10 selectivities x 7 comparable patterns x 5
corpus sizes, 400 throughput points):

| N | throughput crossover / latency crossover | patterns |
|---|---|---|
| 5,000 | 1.28x *(unreliable: both paths plateau, crossing ill-defined)* | 1 |
| 20,000 | 0.82x | 7 |
| 80,000 | 0.81x | 7 |
| 320,000 | 0.87x | 7 |
| 1,280,000 | 0.92x | 7 |

Overall mean **0.87x**, median 0.84x. Excluding N=5,000, throughput crossover is
consistently **lower** — pre-filtering stops winning earlier under load than
serial latency suggests, by ~13-19% at mid N, narrowing to ~8% at 1.28M as
per-query work comes to dominate fixed overhead in both instruments.

An earlier sparse 3-selectivity spot-check put this ratio at 0.78x; the full grid
says 0.87x. The direction was right, the magnitude was overstated.
Note the ceiling biases this measurement in the *opposite* direction (capping
inline's qps inflates the apparent gain and would push the crossover up), so the
true concurrency penalty on pre-filtering is at least this large.

Consequences, all of which are now folded into the recommendation above:

1. The recommended coefficient is **0.48, not 0.6**. Calibrating on latency
   would have left margins of only ~1.7-1.9x against real throughput behaviour
   rather than the intended 2x.
2. The throughput-derived exponent for the binding pattern is `b ≈ 0.52`,
   essentially the same `sqrt(N)` shape as the latency-derived `0.50`. So a
   `sqrt(N)` rule does **not** lose margin as N grows: margins are 2.0-2.6x
   across the whole measured range. (An earlier sparse fit suggested `b ≈ 0.45`
   and a degrading margin; the full grid does not support that.)
3. The point where today's fixed 0.1% turns *too aggressive* moves from N≈16M
   (latency) to **N≈12M** (throughput).

**Status of this correction:** now measured at full resolution — 400 throughput
points, 10 selectivities x 8 patterns x 5 corpus sizes, so it no longer rests on
a spot-check. What remains
before changing a production default, and is the reason to keep the 2x margin
rather than shave it.

### Does the choice even matter? Only as the corpus grows

Numeric pattern, 16 clients, 4 reader threads:

| N | selectivity | inline qps | pre-filter qps | ratio | inline main-thread CPU |
|---|---|---|---|---|---|
| 5,000 | 0.5% | 3,863 | 12,895 | 3.34x | 12% |
| 5,000 | 2% | 11,840 | 13,063 | **1.10x** | 37% |
| 5,000 | 10% | 12,039 | 12,624 | **1.05x** | 36% |
| 80,000 | 0.5% | 2,522 | 12,608 | 5.00x | 8% |
| 80,000 | 10% | 11,845 | 1,434 | 0.12x | 40% |
| 1,280,000 | 0.5% | 859 | 1,567 | 1.82x | 4% |
| 1,280,000 | 2% | 1,861 | 215 | 0.12x | 7% |
| 1,280,000 | 10% | 5,911 | 35 | 0.01x | 19% |

At small N with a loose filter both paths converge on the same ~12-13k qps
plateau and the ratio collapses to 1.05-1.10x: **the threshold barely matters
there**, because per-query search work is small relative to fixed per-request
overhead. At N=1.28M the plateau is never reached (859-5,911 qps) and the ratio
spans 1.82x to 0.01x, so the choice dominates.

So the threshold matters more as the corpus grows, for two compounding reasons:
the crossover tightens as `1/sqrt(N)`, and the fixed overhead that masks the
difference at small N becomes negligible.

One correction to an earlier working assumption: this plateau is **not**
main-thread saturation. Inline main-thread CPU never exceeds 40% at the plateau,
and one point classifies as client-bound. It is per-request round-trip and
dispatch overhead plus the load generator, not a single saturated server thread.
The practical consequence is the same either way.

### Figures

| file | shows |
|---|---|
| `prefilter-archive/figures/fig1-gain-by-pattern.png` | gain vs selectivity, one panel per pattern, one line per N. Crossing gain=1 is the inflection point. |
| `figures/fig2-crossover-vs-n.png` | crossover vs N per pattern, with the current default, the proposed rule, and the point where the fixed ratio turns too aggressive. |
| `prefilter-archive/figures/fig3-latency-curves.png` | absolute latency of both paths, showing *why* they cross. |
| `prefilter-archive/figures/fig4-recall.png` | inline recall degrading with N. |
| `prefilter-archive/figures/fig5-saturation.png` | saturating throughput and which regime bounds it. |
| `prefilter-archive/figures/fig6-latency-vs-throughput.png` | latency-derived vs throughput-derived crossover — why the recommended coefficient is 0.4 and not 0.6. |

---

## The hypothesis that a complex predicate favours pre-filtering is not supported

The reasoning was: inline re-evaluates the predicate once per HNSW-visited node,
whereas pre-filtering resolves it once up front, so an expensive predicate
should shift the crossover in pre-filtering's favour.

Measured, at N=20,000, dim 768, ef=10:

| filter | crossover ratio | crossover qualified |
|---|---|---|
| numeric (cheapest) | 3.95% | 790 |
| text | 3.62% | 724 |
| tag | 2.85% | 571 |

The crossover moves *down* as the predicate gets more expensive — the opposite
direction. E2E agrees and more strongly: numeric crosses between 5% and 10%
while all eight complex shapes (3-way AND, OR, negation) cross between 2% and
5%.

Cause, from the source: pre-filtering does not actually amortise predicate cost
to one evaluation. For "unsolved" shapes — AND-with-numeric/tag, and negation —
`EvaluatePrefilteredKeys` re-runs `PrefilterEvaluator` per qualified key, plus
deduplication for OR/TAG/negate. An expensive predicate therefore inflates
*both* paths, and it inflates the pre-filter side per qualified key while the
inline side partly offsets it by visiting fewer nodes.

Consequence for calibration: the cheap numeric filter is the **best** case for
pre-filtering, not the worst. So calibrating on it is still the right
conservative choice for a "no regressions" goal — but the binding constraint is
the complex shapes, which cross ~28% lower. The 0.4 coefficient above already
sits below the tag-filter crossover.

---

## Full measurements

### DIM sweep, N=20,000, clustered, ef_runtime=10 (production default)

```
   dim   select qualified  inline_p50  prefil_p50  speedup    winner   recall
   128    0.50%       100       796.9        20.8    38.24x prefilter    0.999
   128    1.00%       200       417.5        35.2    11.87x prefilter    0.998
   128    2.00%       400       221.3        49.1     4.51x prefilter    0.998
   128    5.00%      1000        91.0       102.2     0.89x   inline    0.999
   128   10.00%      2000        45.0       222.6     0.20x   inline    1.000
   128   20.00%      4000        38.0       500.4     0.08x   inline    1.000
   128   30.00%      6000        39.1       746.5     0.05x   inline    1.000
   128   40.00%      8000        34.7      1003.6     0.03x   inline    1.000
   768    0.50%       100      1424.5        36.9    38.55x prefilter    0.998
   768    1.00%       200       826.4        66.2    12.48x prefilter    0.998
   768    2.00%       400       444.5       123.7     3.59x prefilter    0.999
   768    5.00%      1000       199.3       285.9     0.70x   inline    0.999
   768   10.00%      2000        92.5       562.8     0.16x   inline    1.000
   768   20.00%      4000        88.9      1105.2     0.08x   inline    1.000
   768   30.00%      6000        83.2      1648.3     0.05x   inline    1.000
   768   40.00%      8000        71.8      2202.3     0.03x   inline    1.000
  1536    0.50%       100      2790.4        50.7    55.06x prefilter    1.000
  1536    1.00%       200      1457.6        94.6    15.41x prefilter    1.000
  1536    2.00%       400       780.8       177.9     4.39x prefilter    0.997
  1536    5.00%      1000       353.5       421.9     0.84x   inline    1.000
  1536   10.00%      2000       163.2       809.7     0.20x   inline    1.000
  1536   20.00%      4000       152.9      1625.2     0.09x   inline    1.000
  1536   30.00%      6000       130.3      2434.1     0.05x   inline    1.000
  1536   40.00%      8000       117.8      3296.2     0.04x   inline    0.997
```

Interpolated crossovers: dim 128 → 4.68%, dim 768 → 4.09%, dim 1536 → 4.54%.
**Essentially dim-independent**, because both paths scale linearly in dim
(pre-filter does `qualified` full-width distance computations; inline does
fewer but equally wide ones). Dimensionality is therefore not a useful input to
the threshold.

### Same sweep at ef_runtime=128

Crossovers: dim 128 → 16.15%, dim 768 → 14.80%, dim 1536 → 16.42%. A 12.8x
increase in `ef_runtime` raises the crossover ~3.6x, i.e. roughly `sqrt(ef)`.

### N sensitivity, dim 768, ef_runtime=10

Interpolated crossovers: 7.50% @ 5k, 4.01% @ 20k, 1.99% @ 80k, 1.07% @ 320k,
0.795% @ 1.28M. Least-squares fit `qualified_crossover = 2.494 * N^0.581`
(±15% over the 256x range).

Note the recall column degrades for **inline** as N grows: at N=1.28M inline
recall is 0.978 at 1% selectivity and 0.842 at 10%. Pre-filtering is an exact
scan, so it is 1.000 by construction. Above the crossover, inline's throughput
advantage is therefore partly bought with answer quality — which is an
additional, separate argument for sitting on the pre-filter side of the line
when the two are close.

### End-to-end, saturating load

See `prefilter-archive/superseded-reports/prefilter-crossover-e2e-results.md`. Real `valkey-server` + module, real
`FT.SEARCH`, 16 concurrent clients against 4 reader threads, paths forced via
`CONFIG SET search.prefiltering-threshold-ratio` 0.0 / 1.0. Nine query shapes
including `num AND tag AND text`, `num OR tag`, `num AND NOT tag`.

E2E numeric crosses between 5% and 10% versus the micro-benchmark's 4.01% at
the same N. The gap is explained: inline saturates a **server request-rate
ceiling of ~12.4k qps** above ~5% selectivity while consuming only 128–200% CPU
(against a ~400% reader-saturation target). Inline cannot go faster than the
ceiling no matter how cheap the query becomes, so E2E *understates* inline's
advantage and pushes the apparent crossover up. The micro-benchmark is the
better instrument for locating the crossover; E2E is the better instrument for
confirming both paths behave the same way in the real stack, which it does.

---

## Methodology

Two instruments, deliberately different in level:

1. **Micro-benchmark** — `testing/vector_test.cc`, `DISABLED_` gtests following
   the existing `DISABLED_PrefetchBenchmark` convention. Drives
   `VectorHNSW<float>::Search` (inline) and the `AddPrefilteredKey` scan
   (pre-filter) in-process, single-threaded, no server. Real index
   implementations, real entries fetchers, real `PrefilterEvaluator`, real
   `FilterParser` (so field masks and `query_operations` are production
   accurate). Mirrors `InlineVectorFilter` and `CalcBestMatchingPrefilteredKeys`
   because both are private to `search.cc`; both mirrors are commented as such.
2. **E2E harness** — `scripts/benchmark/prefilter_crossover_e2e.py`. Full stack,
   zero mirroring risk, arbitrary query shapes, concurrent saturating load with
   per-thread CPU attribution.

Exact selectivity control: key `i` carries numeric value `i` and markers for
every level `L` where `i < L*N`, so `@num:[0 S-1]` / `@tg:{sL}` / text `sL`
match exactly `S` keys. No estimation error between the intended selectivity and
what the planner sees.

Recall guard: every row reports inline recall against the exact pre-filter
result. Inline HNSW can return fewer or worse than k neighbours under a
selective filter, which looks like a latency win but is a quality loss. Without
this guard the sweep would produce a confidently wrong crossover.

The existing memtier infrastructure (`.github/benchmark_configs/`,
`scripts/benchmark/run_endurance_test.sh`) was deliberately not used and not
modified. It answers "did we regress under load?" on fixed scenarios; this needs
filter selectivity as a swept independent variable and a paired per-query A/B.

---

## Methodology traps found (each changed a conclusion)

1. **Degenerate corpus generator.** `DeterministicallyGenerateVectors` sets
   element `j` of vector `i` to `max_value*(i+j)/(size+dim)`, so every vector is
   a linear ramp and the corpus lies on essentially a 1-D line. The first
   crossover estimate (~31%) came from this and is invalid. Added
   `GenerateBenchmarkVectors` (Gaussian mixture) and pointed both crossover
   benchmarks at it; unrelated correctness tests still use the ramp generator.
2. **Uniform-random high-dim vectors are adversarial for HNSW.** At dim 768,
   uniform data drove inline recall to 0.30 and made it *miss the exact nearest
   neighbour* (verified against numpy ground truth in
   `prefilter-archive/scripts/check_path_agreement.py`). Clustered data restores recall
   to 1.000. Corpus distribution changes the answer, so the E2E harness defaults
   to `--data clustered`.
3. **`ef_runtime` mismatch.** The micro-benchmark defaulted to `ef_runtime=128`
   while production defaults to `kDefaultEFRuntime = 10`. That alone accounted
   for a ~3.6x discrepancy in the crossover and was the reconciliation between
   the micro and E2E numbers.
4. **Bogus agreement metric.** The first E2E `agree` column decayed exactly as
   `k/qualified` because it compared results from *different* random query
   vectors. Replaced with `measure_recall`: fixed query vectors, run serially
   outside the load window.
5. **Harness could measure the wrong server.** `start_server` pinged the port
   without verifying the responder was the process it spawned. Stale ASAN-build
   servers from another workspace were in fact running on this host, and an ASAN
   build is 10–50x slower — that would have invalidated every number rather than
   merely failing. Now refuses a busy port and matches `INFO server`
   `process_id`.

## Separate finding: server-side stall on pipelined writes

Not a benchmark artifact; worth filing independently.

Deep pipelines of `HSET` into an indexed keyspace deadlock.
`ShouldBlockClient` (`src/index_schema.cc:894`) blocks the client per mutation
and a writer thread unblocks it. After unblocking, commands already sitting in
that client's query buffer are never re-processed. Because the client is waiting
for replies it sends nothing further, so epoll never fires again and the
connection stalls permanently.

Observed state: `qbuf=548416`, `idle=98s`, `search_writer_queue_size:0`, all
server threads asleep, main thread in `epoll_wait` having consumed only 80ms of
CPU, 38 of ~200 pipelined `HSET`s applied, socket queues empty and fully ACKed.
Reproduced at N=8,000 / dim 768; not seen at N=2,000. Worked around here by
loading serially (`load_dataset` no longer pipelines).

## Caveats

- Single shared cloud desktop (32 cores, 123 GB). Host verified idle before
  runs (loadavg < 1, no competing servers).
- N ≥ 5M is extrapolated, not measured. The measured ceiling is 1.28M vectors
  (micro-benchmark and E2E matrix alike). Extrapolations use the fitted power law
  and are labelled as such wherever quoted.
- **The crossover location is primarily latency-derived** (400 latency points vs
  120 throughput points in the matrix). Throughput measurement shows the
  crossover sits ~13% lower under load (mean 0.87x); the recommended coefficient
  is calibrated for that. Both dimensions are now measured at equal resolution
  (400 points each), so this is no longer a sparse correction. See "Latency vs
  throughput".
- Only float32 / L2 / k=10 swept. Cosine (which normalises) and other data types
  are unmeasured. Normalisation cost falls on both paths, so the crossover is
  not expected to move much, but this is untested.
- Concurrency effect on pre-filtering is now **partially** measured, where it was
  previously an open risk. Pre-filtering streams `qualified x dim` floats and is
  memory-bandwidth heavy; inline is more pointer-chasing and latency-bound. The
  throughput crossover is measurably ~13% below the latency crossover (mean
  0.87x over the full grid), consistent with that mechanism, and the recommended
  coefficient absorbs it. What remains unmeasured: only one client host and one
  reader-thread count (4) were used, and small-N rows are ceiling-bound
  (`client-bound`, excluded from fits). Wider reader-thread counts and a second
  load host would tighten it further. That is the reason to keep the 2x margin
  rather than shave it.
- The micro-benchmark mirrors two functions private to `search.cc`; they can
  drift from production if that file changes.
- `prefiltering-threshold-ratio` is `.Dev()` gated: hidden and immutable unless
  `search.debug-mode yes`. Changing the *default* needs no un-gating, but making
  it tunable in production for staged rollout/rollback would.

## Reproducing

```bash
cmake --build .build-release --target indexes_test -j "$(nproc)"

# micro sweeps (dim, ef, filter type, N)
prefilter-archive/scripts/run_micro_sweeps.sh
prefilter-archive/scripts/run_n_sweep.sh
python3 prefilter-archive/scripts/analyse_crossover.py /tmp/micro_*.log

# end to end under saturating load
python3 scripts/benchmark/prefilter_crossover_e2e.py \
    --n 20000 --dim 768 --clients 16 --reader-threads 4 \
    --duration 3 --warmup 1 --data clustered \
    --selectivities 0.005,0.01,0.02,0.05,0.10,0.20,0.30,0.40 \
    --out prefilter-archive/superseded-reports/prefilter-crossover-e2e-results.md

# correctness vs numpy ground truth
python3 prefilter-archive/scripts/check_path_agreement.py

./scripts/benchmark/stop_bench.sh   # cleanup
```

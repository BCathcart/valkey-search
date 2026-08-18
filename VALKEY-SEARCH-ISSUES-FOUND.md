# Issues found in valkey-search during the pre-filter threshold investigation

Four issues, found incidentally while benchmarking the HNSW pre-filtering
threshold. Listed worst first. Issues 1 and 2 are user-visible defects; 3 is a
test-quality problem; 4 is a design gap.

All observations are on branch `main`, release build (`.build-release`),
`ef_runtime` at the default of 10, HASH attribute type.

---

## Issue 1 (correctness, high): inline filtering silently drops text predicates under a composed AND, returning documents that do not match the query

### Summary

`FT.SEARCH` with a vector KNN clause plus a filter containing a text predicate
as a child of an AND returns documents that **do not satisfy the filter**,
whenever the inline-filtering path is chosen and the other AND conjuncts are less
restrictive than the text one. Results are wrong, not merely differently ranked.

### Reproduction

Corpus of 3,000 docs. Doc `i` has text field containing marker `s200` iff
`i < 60`, plus the term `all` on every doc. So `@txt:s200` matches exactly 60
docs — ids 0..59. `FT.SEARCH idx "@txt:s200 @txt:all" LIMIT 0 0` correctly
returns 60, so the filter parses and evaluates correctly on its own.

Now add a KNN clause and force each path with
`CONFIG SET search.prefiltering-threshold-ratio` (requires
`search.debug-mode yes`):

```
(@txt:s200 @txt:all)=>[KNN 10 @vec $v AS score]
```

| path | returned | violating the filter | max doc id returned |
|---|---|---|---|
| inline (ratio 0.0) | 10 | **8** | `doc:2670` |
| pre-filter (ratio 1.0) | 10 | 0 | `doc:59` |

`doc:2670` does not contain `s200`. It must not be in the result set.

The same failure occurs whenever the non-text conjunct is broader than the text
conjunct:

| filter | inline violations | pre-filter |
|---|---|---|
| `@txt:s200` (no AND) | 0 | 0 |
| `@txt:s200 @txt:all` | 8 / 10 | 0 |
| `@txt:s200 @txt:s1000` | 8 / 10 | 0 |
| `@num:[0 2999] @txt:s200` (numeric broad) | 8 / 10 | 0 |
| `@tg:{s1000} @txt:s200` (tag broader) | 6 / 10 | 0 |
| `@num:[0 59] @txt:s200` (numeric equally selective) | 0 | 0 |

Script: `scripts/benchmark/check_filter_honoured.py`.

Reproduced independently at every corpus size in the pattern matrix: across
N = 5k / 20k / 80k / 320k / 1.28M and all 10 selectivity levels, the
`@txt:sL @txt:all` pattern violated the filter in **50 of 50** measured points,
typically 24-27 of the 30 returned documents per point, with inline recall
against the exact path sitting at 0.10-0.19. Every other pattern was 1.000. See
`prefilter-archive/superseded-reports/prefilter-matrix.md` (the pattern is marked `INVALID` in the crossover summary,
since its inline latency measures an execution that is not doing the filtering
work).

### Root cause

`ComposedPredicate::EvaluateWithContext` (`src/query/predicate.cc:453`) skips
text children of an AND when the evaluator reports it is a pre-filter evaluator:

```cpp
// In AND: skip text children when in prefilter evaluation because text in
// AND is fully (recursively) resolved in the entries fetcher layer already.
if (evaluator.IsPrefilterEvaluator() &&
    child->GetType() == PredicateType::kText && !from_or &&
    !(evaluator.GetQueryOperations() & QueryOperations::kContainsNegate)) {
  continue;
}
```

The justification in the comment is valid **only for the pre-filter path**, where
the candidate set was produced by the text iterator and therefore already
satisfies the text predicate. It does not hold for the inline path:

- `PrefilterEvaluator::IsPrefilterEvaluator()` returns `true`
  (`src/indexes/vector_base.h:296`).
- `InlineVectorFilter::operator()` constructs an `indexes::PrefilterEvaluator`
  to evaluate the predicate per HNSW candidate
  (`src/query/search.cc:124`).

So the skip fires on the inline path as well. But inline candidates come from the
HNSW graph traversal and have **not** been text-filtered by anything. Skipping
the text child therefore drops the constraint entirely, leaving the filter
enforced only by the remaining conjuncts.

The flag is overloaded. `IsPrefilterEvaluator()` actually means "I am a per-key
evaluator", but it is being used to mean "text has already been resolved
upstream". Those two coincide for pre-filtering and diverge for inline filtering.

### Why existing tests do not catch it

The constraint is only observably lost when a surviving conjunct is less
restrictive than the dropped text predicate. Test and benchmark queries that
combine a text predicate with a numeric or tag predicate of *equal*
selectivity — a natural way to write such a test — leave an equally restrictive
filter, so results stay correct and recall stays 1.000. The bug is masked by
redundancy. It was only exposed here by a text-only AND
(`@txt:sL @txt:all`), where dropping the text children leaves no constraint at
all.

### Impact

Materially higher than it first appears, because of the interaction with the
threshold this investigation was about. Inline filtering is chosen whenever
`qualified > 0.001 * N`, which with the current default is nearly always. So any
hybrid query of the form "text AND (something broader)" is exposed by default.

Note the direction: **raising the pre-filtering threshold reduces exposure**,
because more queries take the pre-filter path, which is correct. The current very
low default maximises exposure.

### Suggested fix

Distinguish "text already resolved upstream" from "per-key evaluator". Options:

1. Add a separate predicate/flag on the evaluator, e.g.
   `TextResolvedUpstream()`, set only by the pre-filter scan path, and gate the
   skip on that instead of `IsPrefilterEvaluator()`.
2. Have `InlineVectorFilter` use an evaluator that reports
   `IsPrefilterEvaluator() == false`, so text children are always evaluated
   inline. Simplest, at the cost of per-candidate text lookups.

Either way a regression test should use a text predicate ANDed with a
*deliberately broader* non-text predicate, and assert filter membership of every
returned key rather than only recall.

---

## Issue 2 (availability, high): deep pipelines of write commands into an indexed keyspace stall permanently

### Summary

Issuing many `HSET`s as a single pipeline against a keyspace covered by a search
index causes the connection to hang indefinitely. The server stops executing the
buffered commands and parks in `epoll_wait`; the client waits forever for
replies. Not a slow path — a permanent stall.

### Reproduction

Create an index over HASH keys with a vector field, then pipeline ~200 `HSET`s
(each ~3 KB, dim 768) on one connection and wait for the replies.

Reproduced at N=8,000 docs / dim 768. Not observed at N=2,000, so it is
load/timing dependent.

Observed state while hung:

- client: `qbuf=548416`, `idle=98s`, `flags=N`
- `INFO SEARCH`: `search_writer_queue_size:0` (nothing left to do)
- socket queues empty, all sent data ACKed — the server received everything
- server: every thread asleep, main thread in `epoll_wait`, having used only
  80 ms of CPU
- `DBSIZE` = 38, i.e. only 38 of ~200 pipelined `HSET`s were applied

Confirmed with `gdb` thread backtraces that no thread is blocked on a lock or
spinning; the server is simply idle with unprocessed data in the client's query
buffer.

### Root cause (probable)

`ShouldBlockClient` (`src/index_schema.cc:894`) returns true for any real user
client outside `MULTI`/`EXEC`, so every mutation blocks the issuing client and a
writer thread unblocks it once the record is indexed. After that unblock,
commands already sitting in that client's query buffer are never re-processed.
Because the client is waiting for replies it sends nothing further, so no new
readable event arrives and the buffer is never drained — a lost wakeup.

This looks like a missing "re-process the input buffer after unblocking"
step (the equivalent of putting the client on the unblocked-clients list so
`processInputBuffer` runs again).

### Workaround

Issue writes serially, one reply per command. Done in
`scripts/benchmark/prefilter_crossover_e2e.py::load_dataset`, which documents the
reason inline.

### Note

This is easy to hit from ordinary client code — bulk loading with a pipeline is
the obvious way to populate an index. Diagnostic script:
`prefilter-archive/scripts/diag_load.py`.

---

## Issue 3 (test quality, medium): `DeterministicallyGenerateVectors` produces a degenerate corpus

### Summary

The shared test helper generates vectors that all lie on essentially a
one-dimensional line, which makes any ANN behaviour measured on it
unrepresentative.

Element `j` of vector `i` is `max_value * (i + j) / (size + dim)`. Every vector
is therefore a linear ramp, and vector `i` is a near-affine function of `i`. The
nearest neighbours of `i` are `i±1`, and the HNSW graph degenerates towards a
chain.

### Impact

Correctness tests are fine — that is what the helper was written for. But it is
also the natural helper to reach for when benchmarking, and on this corpus the
measured pre-filter/inline crossover came out at ~31% selectivity versus ~4% on a
clustered corpus at the same N and `ef_runtime`. Using it for any
performance or recall work produces numbers that do not transfer.

### Suggestion

Keep it for correctness tests, but provide a realistic generator alongside it
(a Gaussian mixture is enough) and use that for anything measuring performance or
recall. Added here as `GenerateBenchmarkVectors` in `testing/vector_test.cc`.

Related, worth knowing when writing such tests: uniform random high-dimensional
vectors are the opposite failure mode — adversarial for HNSW rather than
degenerate. At dim 768, uniform data drove inline recall to 0.30 and caused it to
miss the exact nearest neighbour entirely (verified against numpy ground truth
in `prefilter-archive/scripts/check_path_agreement.py`). Clustered data gives recall
1.000 at the same settings.

---

## Issue 4 (design gap, medium): the pre-filtering threshold is a fixed ratio, but the crossover it approximates is not a fixed ratio

### Summary

`UsePreFiltering` (`src/query/planner.cc`) decides via
`estimated_num_of_keys <= GetPrefilteringThresholdRatio() * N`. The file already
carries `// TODO: Tune the threshold ratio` noting that other factors should be
accounted for. Measurements show the fixed-ratio *shape* is the larger problem,
not the constant.

The actual crossover in qualified-key count grows as roughly `sqrt(N)` and
increases with `ef_runtime`. Fitting `qualified_crossover = a * N^b` per filter
pattern over 5 corpus sizes (5k to 1.28M) gives `b = 0.50-0.54` for all seven
patterns tested, with `a` between 3.4 (composed hybrids) and 4.9 (text):

```
qualified_crossover  ~=  0.6 .. 1.1 * sqrt(N * ef_runtime)
```

Consequences of comparing against `ratio * N`, which grows linearly:

- The crossover *ratio* falls as ~`1/sqrt(N)`: for the cheap numeric filter,
  8.27% at N=5k, 4.61% at 20k, 2.22% at 80k, 1.13% at 320k, 0.70% at 1.28M.
- Composed hybrids cross about 1.9x lower than numeric and are therefore the
  binding constraint: `num AND tag AND text` crosses at 0.349% at N=1.28M.
- So the current 0.1% default is 23x too conservative at N=20k, and becomes
  *too aggressive* above N≈6M, where the fixed line crosses the binding
  pattern's curve. It is wrong in both directions depending on index size.
- `ef_runtime` is not an input at all, yet the crossover moves from 4.01% to
  14.80% between ef=10 and ef=128 at N=20,000. No single ratio can be right for
  both.
- Dimensionality, by contrast, barely matters (crossover 4.68% / 4.09% / 4.54%
  at dim 128 / 768 / 1536), since both paths scale linearly in dim. So it is not
  worth adding as an input.

Note the per-N ratios above come from serial latency; under saturating load the
crossover sits ~20% lower still (pre-filtering is memory-bandwidth heavy and loses
more under concurrency), which is why the coefficient below is calibrated on
throughput rather than latency.

### Suggestion

Replace the fixed ratio with a sqrt rule, e.g.
`qualified <= C * sqrt(N * ef_runtime)`, with `C = 0.48` giving a ~2.0-2.6x margin
below the throughput-derived binding pattern across the measured range.

Full data and derivation: `prefilter-crossover-results.md`,
`prefilter-archive/superseded-reports/prefilter-matrix.md`, figures in `figures/`.

### Secondary note

`prefiltering-threshold-ratio` is `.Dev()` gated, so it is hidden and immutable
unless `search.debug-mode yes` (`vmsdk/src/module_config.h`). Changing the
default needs no un-gating, but exposing it for staged rollout or emergency
rollback in production would.

---

## Investigated and found to be NOT a bug

Recorded so the next person does not re-investigate.

**`FT.SEARCH` filter-only counts cap at 100,000.** At N=1,280,000 a filter-only
query such as `@num:[0 127999] LIMIT 0 0` reports 100,000 rather than 128,000,
for every filter pattern. This is the documented
`max-nonvector-search-results-fetched` config, default 100,000
(`src/valkey_search_options.cc:502`), which bounds the result set on
**non-vector** query paths before content fetching to limit memory use.

It does not affect vector/KNN queries, so it did not affect any latency
measurement here — only the selectivity self-check, which deliberately issues a
filter-only (non-vector) query. Worth knowing if you write a similar check:
either raise the config or cap the verification at 100,000.

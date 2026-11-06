# Design notes: the query context and main-thread revalidation

A query touches the index twice: once in the background to find candidates, and once on the
main thread to confirm each result against its freshly fetched content before the reply.
Both passes evaluate the same predicate tree. This note describes what they share, how
each one reads the index, and the one invariant that keeps the main-thread pass correct
without the time-sliced mutex.

## One resolution per leaf per query

Evaluating a leaf of the query tree against a candidate needs some index data: the postings
of a term and of its stem variants, the bag of keys carrying a tag value, a document's
length and score. Before the `QueryContext` existed, each candidate re-derived that data by
walking the index, so a query with N candidates walked the same trees N times per phase.

`QueryContext` (`src/query/query_context.{h,cc}`) is built once per query, or once per reply
on the main thread, and resolves each leaf on first use. A term leaf becomes its groups of
words with their postings and precomputed IDF. A tag leaf becomes the bags of its exact
values and the prefix count of its prefix values. Expansion leaves (prefix, suffix, fuzzy)
are not resolved globally: each candidate's own per-key tree is walked for the words it
carries, and scoring remembers one representative word so a later candidate that carries it
can be scored with a single probe. Filtering and scoring read the same context, so a leaf
resolved while filtering is reused while scoring.

The evaluator contract is unchanged from before: the evaluator fetches a key's data, the
predicate decides. The context is just what the evaluator fetches from.

## Two lock modes

The background pass holds the time-sliced read phase for the whole search, so the index
cannot change under it. The context is built in `kReadPhase` and reads everything directly.

The main-thread pass must not wait on the time-sliced mutex. A write phase can last tens of
milliseconds, and the main thread blocking on it stalls every client. So the reply path
builds its context in `kShortLocks` and takes only the fine-grained mutexes writers already
take: the text tree lock around a lookup, a word's bucket around a postings probe, the stem
tree lock while reading a root's inflections, the tag index lock around a count, and the
mutated-records mutex for the document count and per-document scores. Every lock decision
by mode lives inside `QueryContext`; predicates and evaluators never choose a lock.

Tag membership on the main thread is not read from the index at all. The fetched record is
the source of truth for the document being revalidated, and its tag field is parsed with the
same tokenizer ingestion uses, so a document whose tags changed is judged by what it says
now. Numeric and vector-range predicates likewise read the fetched record.

## The invariant that makes short locks enough

Short locks protect each structure's internals, not the relationship between a cached
pointer and the tree it came from. A cached `Postings` object stays alive through its
reference count, but if a writer removes the word's last key the tree drops that object, and
a later add installs a fresh one. A context still holding the old object would miss keys
indexed into the new one.

What rules this out is that a result key has no in-flight mutation while it is revalidated.
The asynchronous reply path establishes it with the key contention check: before any
revalidation, every result key with a queued mutation parks the query until that mutation is
applied, after which a fresh context is built. Under that guarantee a result key cannot be
removed from a cached postings object, so the object cannot be detached, so it stays the
current one for every key the reply will probe. Churn on unrelated keys cannot produce a
stale answer for the keys being revalidated.

The synchronous path (MULTI/EXEC, Lua, or `--reader-threads 0`) does not yet run the
contention check; see issue #1602.

## Scoring

Relevance is computed in the background from the context, and a revalidated document whose
content changed is rescored on the main thread through the same `ScoreNode` walk and the
same scorer, with its tag membership taken from the record. The two passes therefore produce
scores on one scale. Which matching word scores an expansion leaf is unspecified: the
representative is whichever word scoring reached first, and a different candidate order can
pick a different word. This is a deliberate trade for not walking the global tree per
expansion leaf.

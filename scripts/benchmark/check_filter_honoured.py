#!/usr/bin/env python3
"""Check whether each execution path honours the filter predicate.

Motivation: in the pattern matrix, `@txt:sL @txt:all` (a text-only composed AND)
showed inline recall ~0.15 against the exact pre-filter path, while every other
pattern was 1.000. Two candidate explanations:

  a) inline filtering is dropping text predicates that sit under a composed AND,
     so it searches effectively unfiltered and returns neighbours that do not
     satisfy the query, or
  b) the recall metric is wrong for this pattern.

This distinguishes them without relying on recall at all: it checks membership
directly. Every returned key is tested against the filter recomputed in Python.
A returned doc that fails the filter is a correctness violation, not a ranking
difference.

Note why the earlier sweep missed this: in shapes like `num AND text`, the
numeric and text predicates were built to select the *same* key set, so dropping
the text child left an equally restrictive filter and recall stayed 1.000. The
redundancy masked it. `@txt:sL @txt:all` breaks the redundancy because `all`
matches everything, so dropping the text children leaves no constraint at all.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prefilter_crossover_e2e as base  # noqa: E402

N = int(os.environ.get("CHK_N", "3000"))
DIM = int(os.environ.get("CHK_DIM", "64"))
K = int(os.environ.get("CHK_K", "10"))
PORT = int(os.environ.get("CHK_PORT", "7511"))
LEVELS = [0.02, 0.10]


def main():
    module = os.path.abspath(".build-release/libsearch.so")
    workdir = "/tmp/prefilter_filtercheck"
    handle, c = base.start_server(module, PORT, workdir, 4, 4)
    try:
        vecs = base.load_dataset(c, N, DIM, LEVELS, data_kind="clustered")

        for level in LEVELS:
            thresh = max(1, int(level * N))
            label = f"s{int(round(level * 10000))}"
            # (name, filter, predicate on doc index i)
            cases = [
                ("text (simple)", f"@txt:{label}", lambda i: i < thresh),
                ("text AND text", f"@txt:{label} @txt:all",
                 lambda i: i < thresh),
                ("text AND text (both selective)",
                 f"@txt:{label} @txt:{label}", lambda i: i < thresh),
                ("num AND text", f"@num:[0 {thresh-1}] @txt:{label}",
                 lambda i: i < thresh),
                # numeric constraint deliberately BROADER than the text one, so
                # dropping the text child leaves a weaker filter and any drop
                # becomes visible.
                ("num(broad) AND text", f"@num:[0 {N-1}] @txt:{label}",
                 lambda i: i < thresh),
                ("tag(broad) AND text", f"@tg:{{s1000}} @txt:{label}",
                 lambda i: i < thresh and i < max(1, int(0.10 * N))),
            ]

            print(f"\n{'='*78}\nlevel {level*100:.1f}%  "
                  f"(docs 0..{thresh-1} qualify)\n{'='*78}")
            print(f"{'case':<32} {'path':<10} {'ret':>4} {'viol':>5} "
                  f"{'max_id':>7}  verdict")

            for name, filt, pred in cases:
                # sanity: filter-only count
                cnt = int(c.execute_command(
                    "FT.SEARCH", "idx", filt, "LIMIT", "0", "0",
                    "DIALECT", "2", "NOCONTENT")[0])
                query = f"({filt})=>[KNN {K} @vec $v AS score]"
                for path, ratio in (("inline", base.FORCE_INLINE),
                                    ("prefilter", base.FORCE_PREFILTER)):
                    base.set_ratio(c, ratio)
                    reply = c.execute_command(
                        "FT.SEARCH", "idx", query, "PARAMS", "2", "v",
                        vecs[0], "LIMIT", "0", str(K), "DIALECT", "2",
                        "NOCONTENT")
                    keys = [k.decode() if isinstance(k, bytes) else str(k)
                            for k in reply[1:]]
                    ids = [int(k.split(":")[1]) for k in keys]
                    viol = [i for i in ids if not pred(i)]
                    verdict = ("OK" if not viol
                               else f"VIOLATES FILTER ({len(viol)}/{len(ids)})")
                    print(f"{name:<32} {path:<10} {len(ids):>4} "
                          f"{len(viol):>5} {max(ids) if ids else -1:>7}  "
                          f"{verdict}")
                print(f"{'':<32} {'(filter-only count = ' + str(cnt) + ')':<10}")
    finally:
        handle.stop()


if __name__ == "__main__":
    main()

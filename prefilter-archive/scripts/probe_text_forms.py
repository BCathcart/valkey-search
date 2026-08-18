#!/usr/bin/env python3
"""Find a text-only composed query form that inline filtering honours.

`@txt:a @txt:b` (composed AND of two text predicates) is dropped by inline
filtering, so it cannot be used to benchmark the inline path. Candidates that
might avoid the AND-child skip in ComposedPredicate::EvaluateWithContext:

  * `@txt:(a b)` - if this parses to a single kText predicate with several terms
    rather than a kComposedAnd of kText children, the skip cannot apply.
  * `@txt:a | @txt:b` - the skip is guarded on `!from_or`, so text under OR
    should still be evaluated.

For each candidate: check the filter-only count is the expected selectivity, then
check every key returned by a KNN search actually satisfies the filter.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prefilter_crossover_e2e as base  # noqa: E402

N = 3000
DIM = 64
K = 10
PORT = 7513
LEVELS = [0.02, 0.10]


def main():
    module = os.path.abspath(".build-release/libsearch.so")
    handle, c = base.start_server(module, PORT, "/tmp/prefilter_textprobe",
                                  4, 4)
    try:
        vecs = base.load_dataset(c, N, DIM, LEVELS, data_kind="clustered")
        big, small = 0.10, 0.02
        tb = max(1, int(big * N))          # 300 docs qualify for s1000
        ts = max(1, int(small * N))        # 60 docs qualify for s200
        lb, ls = "s1000", "s200"

        # (name, filter, expected count, predicate)
        cases = [
            ("AND two text (control, known bad)", f"@txt:{ls} @txt:all", ts,
             lambda i: i < ts),
            ("single field, two terms '(a b)'", f"@txt:({ls} all)", ts,
             lambda i: i < ts),
            ("single field, two terms selective", f"@txt:({ls} {lb})", ts,
             lambda i: i < ts),
            ("OR two text (subset union)", f"@txt:{lb} | @txt:{ls}", tb,
             lambda i: i < tb),
            ("OR two text (self)", f"@txt:{ls} | @txt:{ls}", ts,
             lambda i: i < ts),
        ]

        print(f"{'candidate':<38} {'count':>7} {'exp':>6} {'path':<10} "
              f"{'viol':>5} {'max_id':>7}  verdict")
        for name, filt, exp, pred in cases:
            try:
                cnt = int(c.execute_command(
                    "FT.SEARCH", "idx", filt, "LIMIT", "0", "0",
                    "DIALECT", "2", "NOCONTENT")[0])
            except Exception as exc:  # noqa: BLE001
                print(f"{name:<38} PARSE/EXEC ERROR: {exc}")
                continue
            query = f"({filt})=>[KNN {K} @vec $v AS score]"
            for path, ratio in (("inline", base.FORCE_INLINE),
                                ("prefilter", base.FORCE_PREFILTER)):
                base.set_ratio(c, ratio)
                try:
                    reply = c.execute_command(
                        "FT.SEARCH", "idx", query, "PARAMS", "2", "v",
                        vecs[0], "LIMIT", "0", str(K), "DIALECT", "2",
                        "NOCONTENT")
                except Exception as exc:  # noqa: BLE001
                    print(f"{name:<38} {cnt:>7} {exp:>6} {path:<10} "
                          f"KNN ERROR: {exc}")
                    continue
                ids = [int((k.decode() if isinstance(k, bytes) else str(k))
                           .split(":")[1]) for k in reply[1:]]
                viol = [i for i in ids if not pred(i)]
                ok_count = "count OK" if cnt == exp else f"COUNT WRONG({cnt})"
                verdict = "OK" if not viol else f"VIOLATES ({len(viol)}/{len(ids)})"
                print(f"{name:<38} {cnt:>7} {exp:>6} {path:<10} "
                      f"{len(viol):>5} {max(ids) if ids else -1:>7}  "
                      f"{verdict}; {ok_count}")
    finally:
        handle.stop()


if __name__ == "__main__":
    main()

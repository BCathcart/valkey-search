#!/usr/bin/env python3
"""Pre-filter vs inline crossover matrix: filter pattern x corpus size.

Produces one table per filter pattern, rows = selectivity, columns = corpus
size N, so the inflection point per pattern is directly graphable.

Two measurements per point, because they answer different questions:

  * serial latency (1 connection, no concurrency). This locates the inflection
    point. It is ceiling-free, and it is unbiased for that purpose: any fixed
    per-query overhead (RESP parse, dispatch, reply generation) adds the same
    constant to both paths, so it cannot move the selectivity at which the two
    curves cross.

  * saturating throughput (many clients) with bottleneck attribution. This says
    whether the inflection *matters* at that N. If both paths are pinned to the
    same server request-rate ceiling then the threshold choice is close to
    irrelevant at that corpus size, which is itself a useful finding rather
    than a measurement defect. Run on a coarser selectivity grid since the
    answer is driven mainly by N and absolute per-query cost.

Both paths are forced through the real planner via
`CONFIG SET search.prefiltering-threshold-ratio` 0.0 / 1.0, so this exercises
production code end to end with no mirrored logic.

Outputs: <out>.json (all rows), <out>.csv (long form, for plotting), <out>.md
(per-pattern tables + crossover summary).
"""
import argparse
import json
import math
import os
import statistics
import sys
import time

import valkey

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prefilter_crossover_e2e as base  # noqa: E402


# A marker level smaller than every measured level, loaded but never measured.
# It exists so a text-only OR can be composed against a strict subset term:
# because the markers are nested, "@txt:sL | @txt:s5" selects exactly the same S
# docs as "@txt:sL", keeping selectivity exact while still exercising the
# composed-OR text path with two real text children.
SUBSET_LEVEL = 0.0005
SUBSET_LABEL = f"s{int(round(SUBSET_LEVEL * 10000))}"

# Pattern set: two cheap simple filters, one simple text match, two text-only
# composed queries, and three composed hybrids including a negation.
#
# "text AND text" is included deliberately even though it is known to be
# mis-executed by the inline path (inline drops text predicates that are
# children of a composed AND, so it searches effectively unfiltered and returns
# documents that do not match the query - see the correctness section of the
# report). Its inline numbers are not a valid latency comparison and are
# excluded from the crossover summary, but keeping the row documents the bug.
PATTERNS = [
    ("numeric",              "@num:[0 {S}]"),
    ("tag",                  "@tg:{{{L}}}"),
    ("text",                 "@txt:{L}"),
    ("text OR text",         "@txt:{L} | @txt:" + SUBSET_LABEL),
    ("text AND text",        "@txt:{L} @txt:all"),
    ("num AND tag",          "@num:[0 {S}] @tg:{{{L}}}"),
    ("num AND tag AND text", "@num:[0 {S}] @tg:{{{L}}} @txt:{L}"),
    ("num AND NOT tag",      "@num:[0 {S}] -@tg:{{none}}"),
]

# Patterns whose inline execution is known to violate the filter, so their
# inline latency is meaningless and must not drive a threshold decision.
INLINE_BROKEN = {"text AND text"}


def percentile(values, pct):
    if not values:
        return float("nan")
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(round((pct / 100.0) * (len(s) - 1)))))
    return s[idx]


def verify_selectivity(client, filt, expected, name, level):
    """Confirm the filter alone matches exactly the intended number of docs.

    Guards the whole experiment: if a composed pattern does not select the same
    key set as the simple ones, its rows are not comparable and any crossover
    read off them is meaningless.
    """
    try:
        reply = client.execute_command(
            "FT.SEARCH", "idx", filt, "LIMIT", "0", "0", "DIALECT", "2",
            "NOCONTENT")
        got = int(reply[0])
    except Exception as exc:  # noqa: BLE001
        return None, f"count query failed: {exc}"
    if got != expected:
        return got, (f"SELECTIVITY MISMATCH {name} @ {level*100:.3f}%: "
                     f"expected {expected}, got {got}")
    return got, None


def count_violations(client, query, vecs, k, qualified):
    """Count returned docs that do not satisfy the filter, per path.

    Every pattern here selects exactly docs 0..qualified-1 (the level markers
    are nested and the numeric range is [0, S-1]), so filter satisfaction is
    simply `doc id < qualified`. A non-zero count means that path returned
    documents that do not match the query - a correctness violation, not a
    ranking difference. This is a per-row guard: a path that is not actually
    applying the filter is fast for the wrong reason and its latency must not
    be compared.
    """
    out = {}
    for label, ratio in (("inline", base.FORCE_INLINE),
                         ("prefilter", base.FORCE_PREFILTER)):
        base.set_ratio(client, ratio)
        bad = 0
        total = 0
        for i in range(3):
            reply = client.execute_command(
                "FT.SEARCH", "idx", query, "PARAMS", "2", "v",
                vecs[i % len(vecs)], "LIMIT", "0", str(k),
                "DIALECT", "2", "NOCONTENT")
            for key in reply[1:]:
                key = key.decode() if isinstance(key, bytes) else str(key)
                try:
                    doc_id = int(key.split(":")[1])
                except (IndexError, ValueError):
                    continue
                total += 1
                if doc_id >= qualified:
                    bad += 1
        out[label] = {"violations": bad, "returned": total}
    return out


def serial_latency(client, query, vecs, k, n_queries, warmup):
    """Median per-query latency for each path, measured one query at a time."""
    out = {}
    for label, ratio in (("inline", base.FORCE_INLINE),
                         ("prefilter", base.FORCE_PREFILTER)):
        base.set_ratio(client, ratio)
        for i in range(warmup):
            client.execute_command(
                "FT.SEARCH", "idx", query, "PARAMS", "2", "v",
                vecs[i % len(vecs)], "LIMIT", "0", str(k),
                "DIALECT", "2", "NOCONTENT")
        lat = []
        for i in range(n_queries):
            q = vecs[i % len(vecs)]
            t0 = time.perf_counter()
            client.execute_command(
                "FT.SEARCH", "idx", query, "PARAMS", "2", "v", q,
                "LIMIT", "0", str(k), "DIALECT", "2", "NOCONTENT")
            lat.append((time.perf_counter() - t0) * 1000.0)
        out[label] = {
            "p50": percentile(lat, 50),
            "p90": percentile(lat, 90),
            "mean": statistics.fmean(lat),
        }
    return out


def crossover_from_points(points):
    """Interpolate the selectivity where gain crosses 1.0 (log-log)."""
    pts = sorted(points, key=lambda p: p[0])
    if not pts:
        return None, "no points"
    if pts[0][1] < 1.0:
        return None, f"below range (inline already wins at {pts[0][0]*100:.3f}%)"
    if pts[-1][1] > 1.0:
        return None, f"above range (prefilter still wins at {pts[-1][0]*100:.3f}%)"
    for (s0, g0), (s1, g1) in zip(pts, pts[1:]):
        if g0 >= 1.0 >= g1:
            if g0 == g1:
                return s0, "flat"
            t = (0.0 - math.log(g0)) / (math.log(g1) - math.log(g0))
            return math.exp(math.log(s0) + t * (math.log(s1) - math.log(s0))), \
                f"bracketed [{s0*100:.3f}%, {s1*100:.3f}%]"
    return None, "no bracket"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", default=".build-release/libsearch.so")
    ap.add_argument("--port", type=int, default=7501)
    ap.add_argument("--workdir", default="/tmp/prefilter_matrix")
    ap.add_argument("--n-values", default="5000,20000,80000,320000,1280000")
    ap.add_argument("--dim", type=int, default=768)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--selectivities",
                    default="0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,"
                            "0.07,0.10")
    ap.add_argument("--sat-selectivities", default="0.005,0.02,0.10",
                    help="coarser grid for the saturating-throughput pass")
    ap.add_argument("--queries", type=int, default=25,
                    help="serial timed queries per path per point")
    ap.add_argument("--warmup-queries", type=int, default=5)
    ap.add_argument("--recall-queries", type=int, default=10)
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--reader-threads", type=int, default=4)
    ap.add_argument("--writer-threads", type=int, default=4)
    ap.add_argument("--duration", type=float, default=2.0)
    ap.add_argument("--sat-warmup", type=float, default=0.5)
    ap.add_argument("--data", choices=("clustered", "uniform"),
                    default="clustered")
    ap.add_argument("--patterns", default="",
                    help="comma separated subset of pattern names")
    ap.add_argument("--out", default="prefilter-data/prefilter-matrix")
    ap.add_argument("--skip-saturating", action="store_true")
    args = ap.parse_args()

    n_values = [int(x) for x in args.n_values.split(",") if x]
    levels = [float(x) for x in args.selectivities.split(",") if x]
    sat_levels = [float(x) for x in args.sat_selectivities.split(",") if x]
    patterns = PATTERNS
    if args.patterns:
        want = {p.strip() for p in args.patterns.split(",")}
        patterns = [p for p in PATTERNS if p[0] in want]

    module_path = os.path.abspath(args.module)
    ncores = os.cpu_count() or 1
    sat_target = min(args.reader_threads, ncores) * 100.0

    rows = []
    warnings = []

    for n in n_values:
        base.log(f"\n{'#'*72}\n# N = {n:,}  dim={args.dim}  data={args.data}\n"
                 f"{'#'*72}")
        workdir = os.path.join(args.workdir, f"n{n}")
        handle, admin = base.start_server(
            module_path, args.port, workdir,
            args.reader_threads, args.writer_threads)
        try:
            # Markers must exist for every level either pass measures, plus the
            # subset level used by the text-only OR pattern.
            all_levels = sorted(set(levels) | set(sat_levels))
            load_levels = sorted(set(all_levels) | {SUBSET_LEVEL})
            vecs = base.load_dataset(admin, n, args.dim, load_levels,
                                     data_kind=args.data)
            pid = handle.proc.pid

            hdr = (f"{'pattern':<22} {'select':>7} {'qual':>8} "
                   f"{'in_ms':>8} {'pre_ms':>8} {'gain':>7} {'winner':>10} "
                   f"{'recall':>7}")
            base.log("\n" + hdr)
            base.log("-" * len(hdr))

            for name, template in patterns:
                for level in all_levels:
                    qualified = max(1, int(level * n))
                    label = f"s{int(round(level * 10000))}"
                    filt = template.format(S=qualified - 1, L=label)
                    query = f"({filt})=>[KNN {args.k} @vec $v AS score]"

                    got, warn = verify_selectivity(admin, filt, qualified,
                                                   name, level)
                    if warn:
                        warnings.append(warn)
                        base.log("  !! " + warn)

                    row = {
                        "N": n, "dim": args.dim, "pattern": name,
                        "selectivity": level, "qualified": qualified,
                        "qualified_actual": got, "k": args.k,
                        "data": args.data,
                    }

                    if level in levels:
                        viol = count_violations(admin, query, vecs, args.k,
                                                qualified)
                        lat = serial_latency(admin, query, vecs, args.k,
                                             args.queries,
                                             args.warmup_queries)
                        gain = (lat["inline"]["p50"] / lat["prefilter"]["p50"]
                                if lat["prefilter"]["p50"] else float("nan"))
                        recall = base.measure_recall(admin, query, vecs,
                                                     args.k,
                                                     args.recall_queries)
                        inline_bad = viol["inline"]["violations"]
                        pre_bad = viol["prefilter"]["violations"]
                        row.update({
                            "inline_p50_ms": lat["inline"]["p50"],
                            "prefilter_p50_ms": lat["prefilter"]["p50"],
                            "inline_p90_ms": lat["inline"]["p90"],
                            "prefilter_p90_ms": lat["prefilter"]["p90"],
                            "latency_gain": gain,
                            "recall": recall,
                            "inline_violations": inline_bad,
                            "prefilter_violations": pre_bad,
                            "returned_checked": viol["inline"]["returned"],
                            "valid_comparison": (inline_bad == 0
                                                 and pre_bad == 0),
                        })
                        flag = ""
                        if inline_bad or pre_bad:
                            flag = (f"  <== FILTER VIOLATED "
                                    f"(inline {inline_bad}, "
                                    f"prefilter {pre_bad} of "
                                    f"{viol['inline']['returned']})")
                            warnings.append(
                                f"{name} @ N={n} {level*100:.3f}%: filter "
                                f"violated - inline {inline_bad}, prefilter "
                                f"{pre_bad} of {viol['inline']['returned']} "
                                f"returned docs do not match the query")
                        base.log(f"{name:<22} {level*100:6.3f}% {qualified:>8} "
                                 f"{lat['inline']['p50']:8.3f} "
                                 f"{lat['prefilter']['p50']:8.3f} "
                                 f"{gain:6.2f}x "
                                 f"{'prefilter' if gain > 1 else 'inline':>10} "
                                 f"{recall:7.3f}{flag}")

                    if not args.skip_saturating and level in sat_levels:
                        base.set_ratio(admin, base.FORCE_INLINE)
                        base.run_load(args.port, query, vecs, args.k,
                                      args.clients, args.sat_warmup, admin,
                                      pid)
                        inl = base.run_load(args.port, query, vecs, args.k,
                                            args.clients, args.duration,
                                            admin, pid)
                        base.set_ratio(admin, base.FORCE_PREFILTER)
                        base.run_load(args.port, query, vecs, args.k,
                                      args.clients, args.sat_warmup, admin,
                                      pid)
                        pre = base.run_load(args.port, query, vecs, args.k,
                                            args.clients, args.duration,
                                            admin, pid)
                        bound = base.classify_bottleneck(inl, pre, sat_target)
                        row.update({
                            "inline_qps": inl["qps"],
                            "prefilter_qps": pre["qps"],
                            "qps_gain": (pre["qps"] / inl["qps"]
                                         if inl["qps"] else float("nan")),
                            "inline_cpu_pct": inl["cpu_pct"],
                            "prefilter_cpu_pct": pre["cpu_pct"],
                            "inline_main_cpu_pct": inl["main_cpu_pct"],
                            "prefilter_main_cpu_pct": pre["main_cpu_pct"],
                            "bottleneck": bound,
                            "sat_target_pct": sat_target,
                        })
                        base.log(f"    [sat] {name} @ {level*100:.3f}%: "
                                 f"in {inl['qps']:.0f} qps "
                                 f"(cpu {inl['cpu_pct']:.0f}%, main "
                                 f"{inl['main_cpu_pct']:.0f}%) | "
                                 f"pre {pre['qps']:.0f} qps "
                                 f"(cpu {pre['cpu_pct']:.0f}%, main "
                                 f"{pre['main_cpu_pct']:.0f}%) -> {bound}")

                    rows.append(row)
        finally:
            handle.stop()
            base.log(f"server for N={n} stopped")

    write_outputs(args, rows, warnings, patterns, n_values, levels)


def write_outputs(args, rows, warnings, patterns, n_values, levels):
    with open(args.out + ".json", "w") as fh:
        json.dump({"rows": rows, "warnings": warnings,
                   "config": vars(args)}, fh, indent=1)

    cols = ["N", "dim", "pattern", "selectivity", "qualified",
            "qualified_actual", "inline_p50_ms", "prefilter_p50_ms",
            "inline_p90_ms", "prefilter_p90_ms", "latency_gain", "recall",
            "inline_qps", "prefilter_qps", "qps_gain", "inline_cpu_pct",
            "prefilter_cpu_pct", "inline_main_cpu_pct",
            "prefilter_main_cpu_pct", "bottleneck", "k", "data"]
    with open(args.out + ".csv", "w") as fh:
        fh.write(",".join(cols) + "\n")
        for r in rows:
            fh.write(",".join("" if r.get(c) is None else str(r.get(c, ""))
                              for c in cols) + "\n")

    def lat_rows(pattern, n):
        return sorted(
            [r for r in rows
             if r["pattern"] == pattern and r["N"] == n
             and "latency_gain" in r],
            key=lambda r: r["selectivity"])

    with open(args.out + ".md", "w") as fh:
        fh.write("# Pre-filter vs inline crossover: filter pattern x corpus "
                 "size\n\n")
        fh.write(f"- dim = {args.dim}, k = {args.k}, corpus = {args.data}, "
                 f"`ef_runtime` = server default (10)\n")
        fh.write("- `gain` = inline_p50 / prefilter_p50 from **serial** "
                 "(single-connection) latency. > 1.0 means pre-filtering is "
                 "faster. The crossover is where gain = 1.0.\n")
        fh.write("- Serial latency is used to locate the inflection point "
                 "because it is free of the server request-rate ceiling, and "
                 "any fixed per-query overhead cancels: it adds the same "
                 "constant to both paths and so cannot move the crossing "
                 "point.\n")
        fh.write("- `recall` = inline recall against the exact pre-filter "
                 "result (pre-filter is a brute-force exact scan, so it is "
                 "ground truth).\n")
        fh.write("- Both paths forced via "
                 "`CONFIG SET search.prefiltering-threshold-ratio` 0.0 / 1.0, "
                 "so the real planner, entries fetchers and dedup all run.\n")
        fh.write(f"- Selectivity is verified exactly per pattern with a "
                 f"filter-only `FT.SEARCH ... LIMIT 0 0` count.\n\n")

        if warnings:
            fh.write("## WARNINGS\n\n")
            for w in warnings:
                fh.write(f"- {w}\n")
            fh.write("\n")

        fh.write("## Crossover summary (selectivity where pre-filter stops "
                 "winning)\n\n")
        fh.write("Blank / `INVALID` means the inline path did not honour the "
                 "filter for that pattern, so its latency is not comparable "
                 "and no crossover can be read from it.\n\n")
        fh.write("| pattern | " + " | ".join(f"N={n:,}" for n in n_values)
                 + " |\n")
        fh.write("|" + "---|" * (len(n_values) + 1) + "\n")
        summary = {}
        for name, _ in patterns:
            cells = []
            for n in n_values:
                rs = lat_rows(name, n)
                bad = [r for r in rs if not r.get("valid_comparison", True)]
                if bad:
                    summary[(name, n)] = None
                    cells.append("INVALID")
                    continue
                c, _note = crossover_from_points(
                    [(r["selectivity"], r["latency_gain"]) for r in rs])
                summary[(name, n)] = c
                cells.append(f"{c*100:.2f}%" if c else "n/a")
            marker = " **(inline broken)**" if name in INLINE_BROKEN else ""
            fh.write(f"| {name}{marker} | " + " | ".join(cells) + " |\n")
        fh.write("\n")

        fh.write("## Per-pattern tables\n\n")
        for name, template in patterns:
            fh.write(f"### {name}\n\n")
            fh.write(f"Query: `{template}`\n\n")
            fh.write("gain = inline_p50 / prefilter_p50 (>1 favours "
                     "pre-filter); r = inline recall\n\n")
            fh.write("| selectivity | " + " | ".join(
                f"N={n:,} gain | r" for n in n_values) + " |\n")
            fh.write("|" + "---|" * (1 + 2 * len(n_values)) + "\n")
            for level in levels:
                cells = []
                for n in n_values:
                    match = [r for r in lat_rows(name, n)
                             if abs(r["selectivity"] - level) < 1e-12]
                    if match:
                        cells.append(f"{match[0]['latency_gain']:.2f}x")
                        cells.append(f"{match[0]['recall']:.3f}")
                    else:
                        cells.extend(["-", "-"])
                fh.write(f"| {level*100:.3f}% | " + " | ".join(cells) + " |\n")
            fh.write("\n")
            fh.write("Raw serial latency (ms, p50): inline / prefilter\n\n")
            fh.write("| selectivity | " + " | ".join(
                f"N={n:,}" for n in n_values) + " |\n")
            fh.write("|" + "---|" * (1 + len(n_values)) + "\n")
            for level in levels:
                cells = []
                for n in n_values:
                    match = [r for r in lat_rows(name, n)
                             if abs(r["selectivity"] - level) < 1e-12]
                    cells.append(
                        f"{match[0]['inline_p50_ms']:.2f} / "
                        f"{match[0]['prefilter_p50_ms']:.2f}"
                        if match else "-")
                fh.write(f"| {level*100:.3f}% | " + " | ".join(cells) + " |\n")
            fh.write("\n")

        sat = [r for r in rows if "bottleneck" in r]
        if sat:
            fh.write("## Saturating load: does the choice matter at this N?\n\n")
            fh.write("If both paths pin to the same request-rate ceiling then "
                     "the threshold barely matters at that corpus size. "
                     "`MAIN-BOUND` means valkey's main thread is the limit "
                     "(near 100% of one core), `reader-bound` means the search "
                     "threads are, `client-bound` means the load generator "
                     "was.\n\n")
            fh.write("| pattern | N | selectivity | inline qps | prefilter qps "
                     "| qps gain | in cpu | pre cpu | in main | pre main "
                     "| bound |\n")
            fh.write("|" + "---|" * 11 + "\n")
            for r in sorted(sat, key=lambda r: (r["pattern"], r["N"],
                                                r["selectivity"])):
                fh.write(
                    f"| {r['pattern']} | {r['N']:,} | "
                    f"{r['selectivity']*100:.3f}% | {r['inline_qps']:.0f} | "
                    f"{r['prefilter_qps']:.0f} | {r['qps_gain']:.2f}x | "
                    f"{r['inline_cpu_pct']:.0f}% | "
                    f"{r['prefilter_cpu_pct']:.0f}% | "
                    f"{r['inline_main_cpu_pct']:.0f}% | "
                    f"{r['prefilter_main_cpu_pct']:.0f}% | "
                    f"{r['bottleneck']} |\n")
            fh.write("\n")

    base.log(f"\nwrote {args.out}.json / .csv / .md")
    if warnings:
        base.log(f"{len(warnings)} selectivity warnings - see the md file")


if __name__ == "__main__":
    main()

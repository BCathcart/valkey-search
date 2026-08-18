#!/usr/bin/env python3
"""Reconcile the two dimensions and re-derive the threshold rule.

Dimension 1 (serial latency) and dimension 2 (saturated throughput) are
measured independently over the same grid: 7 filter patterns x 5 corpus sizes x
10 selectivities. This script:

  1. locates the crossover (gain == 1) per (pattern, N) in each dimension,
  2. reports how far the throughput crossover sits below the latency one,
  3. fits qualified_crossover = a * N^b per pattern per dimension,
  4. picks the binding (lowest) pattern and derives the safe coefficient C in
     `qualified <= C * sqrt(N * ef_runtime)`.

Why the throughput dimension governs the recommendation: production cares about
throughput under load, and serial latency flatters pre-filtering because a lone
query owns the whole memory bandwidth and cache, which the bandwidth-heavy
pre-filter scan benefits from more than pointer-chasing inline traversal.

Rows that did not saturate are excluded: a `client-bound` row has both paths
pinned at the load generator's ceiling, so its gain is squashed toward 1.0 and
its crossover is an artefact, not a measurement.
"""

import argparse
import json
import math
import statistics

EF = 10          # production default ef_runtime, used for both grids


def crossover(points):
    """Selectivity where gain crosses 1, by log-log interpolation."""
    pts = sorted(points)
    if len(pts) < 2 or pts[0][1] < 1 or pts[-1][1] > 1:
        return None                      # never crosses inside the measured grid
    for (s0, g0), (s1, g1) in zip(pts, pts[1:]):
        if g0 >= 1 >= g1:
            if g0 == g1:
                return s0
            t = (0 - math.log(g0)) / (math.log(g1) - math.log(g0))
            return math.exp(math.log(s0) + t * (math.log(s1) - math.log(s0)))
    return None


def fit(ns_to_qual):
    """Least squares on log(qualified) = log(a) + b*log(N)."""
    xs = [math.log(n) for n in ns_to_qual]
    ys = [math.log(q) for q in ns_to_qual.values()]
    m = len(xs)
    if m < 3:
        return None
    mx, my = sum(xs) / m, sum(ys) / m
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return None
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
    return math.exp(my - b * mx), b


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency-json", default="prefilter-data/prefilter-matrix.json")
    ap.add_argument("--throughput-json", default="prefilter-data/prefilter-throughput.json")
    args = ap.parse_args()

    lat = json.load(open(args.latency_json))["rows"]
    lat = [r for r in lat
           if "latency_gain" in r and r.get("valid_comparison", True)]

    thr = json.load(open(args.throughput_json))["rows"]
    thr = [r for r in thr
           if r.get("mode") == "grid" and not r.get("inline_broken")]
    saturated = [r for r in thr if r.get("bound") != "client-bound"]
    dropped = len(thr) - len(saturated)

    patterns, ns = [], sorted({r["N"] for r in lat} | {r["N"] for r in thr})
    for r in lat + thr:
        p = r["pattern"]
        if p not in patterns and p != "text AND text":
            patterns.append(p)

    print(f"latency rows: {len(lat)}   throughput rows: {len(thr)} "
          f"({dropped} client-bound excluded, {len(saturated)} usable)\n")

    # ---- 1/2: per-cell crossovers and the throughput/latency ratio ----------
    print("Crossover selectivity per test case")
    print(f"{'pattern':<24} {'N':>10} {'latency':>9} {'throughput':>11} "
          f"{'thr/lat':>8}")
    lat_cross, thr_cross, ratios = {}, {}, []
    for p in patterns:
        for n in ns:
            cl = crossover([(r["selectivity"], r["latency_gain"])
                            for r in lat if r["pattern"] == p and r["N"] == n])
            cs = crossover([(r["selectivity"], r["qps_gain"])
                            for r in saturated
                            if r["pattern"] == p and r["N"] == n])
            if cl:
                lat_cross[(p, n)] = cl
            if cs:
                thr_cross[(p, n)] = cs
            if cl and cs:
                ratios.append((n, cs / cl))
                print(f"{p:<24} {n:>10,} {cl*100:8.3f}% {cs*100:10.3f}% "
                      f"{cs/cl:7.2f}x")
    if ratios:
        by_n = {}
        for n, r in ratios:
            by_n.setdefault(n, []).append(r)
        print("\nthroughput crossover relative to latency crossover:")
        for n in sorted(by_n):
            v = by_n[n]
            print(f"  N={n:>10,}: {statistics.fmean(v):.2f}x  (n={len(v)})")
        allr = [r for _, r in ratios]
        print(f"  overall mean {statistics.fmean(allr):.2f}x, "
              f"median {statistics.median(allr):.2f}x")
        print("  <1 means saturated load favours inline more than serial "
              "latency suggests")

    # ---- 3: fits per pattern, per dimension --------------------------------
    for label, cross in (("LATENCY", lat_cross), ("THROUGHPUT", thr_cross)):
        print(f"\n{label}: qualified_crossover = a * N^b")
        print(f"{'pattern':<24} {'a':>8} {'b':>7} {'pts':>4} "
              f"{'ratio@1.28M':>12} {'ratio@10M(ex)':>14}")
        binding = None
        for p in patterns:
            cells = {n: cross[(p, n)] * n for n in ns if (p, n) in cross}
            f = fit(cells)
            if not f:
                print(f"{p:<24} {'-':>8} {'-':>7} {len(cells):>4}  "
                      f"(insufficient points)")
                continue
            a, b = f
            r128 = 100 * a * 1280000 ** (b - 1)
            r10m = 100 * a * 1e7 ** (b - 1)
            print(f"{p:<24} {a:>8.3f} {b:>7.3f} {len(cells):>4} "
                  f"{r128:>11.3f}% {r10m:>13.4f}%")
            if binding is None or r10m < binding[3]:
                binding = (p, a, b, r10m)
        if binding:
            p, a, b, _ = binding
            print(f"\n  binding pattern: {p}  (a={a:.3f}, b={b:.3f})")
            print("  max safe single global ratio, by the N you must protect:")
            for nmax in (100_000, 1_000_000, 10_000_000, 100_000_000):
                print(f"    N<={nmax:>12,}: {100*a*nmax**(b-1):.4f}%")
            if label == "THROUGHPUT":
                print("\n  coefficient C for `qualified <= C*sqrt(N*ef)`, "
                      "targeting a 2x margin:")
                cs = []
                for n in ns:
                    cross_q = a * n ** b
                    cs.append((cross_q / 2.0) / math.sqrt(n * EF))
                    print(f"    N={n:>10,}: crossover={cross_q:>9.0f} "
                          f"C_for_2x={cs[-1]:.3f}")
                print(f"    -> use C = {min(cs):.2f} "
                      f"(the minimum, so the margin holds at every N)")


if __name__ == "__main__":
    main()

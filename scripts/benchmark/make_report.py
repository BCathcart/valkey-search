#!/usr/bin/env python3
"""Per-test-case report: one table and one graph per query, per dimension.

A "test case" is a single query pattern varied against corpus size. For each one
this emits, separately for the two dimensions:

  * latency    - single-query p50, one connection, no concurrency
  * throughput - queries/sec under saturating multiprocess load

Each gets its own table (rows = filter selectivity, columns = corpus size) and
its own graph, plus the exact query string that was run.

The two dimensions are kept separate on purpose. They can disagree: when the
server's main thread is the ceiling, the pre-filter/inline choice only changes
reader-thread work, so it is hidden and throughput barely moves even though
latency shows a large difference.

Inputs:
  prefilter-data/prefilter-matrix.json      latency grid  (prefilter_matrix_e2e.py)
  prefilter-data/prefilter-throughput.json  throughput grid (prefilter_throughput_e2e.py)

Usage: make_report.py [--outdir figures] [--out PREFILTER-REPORT.md]
"""
import argparse
import json
import math
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

PCT = FuncFormatter(lambda v, _p: f"{v*100:g}%")


def slug(name):
    return name.lower().replace(" ", "-")


def crossover(points):
    """Selectivity where gain crosses 1.0, by log-log interpolation."""
    pts = sorted(points)
    if len(pts) < 2 or pts[0][1] < 1.0 or pts[-1][1] > 1.0:
        return None
    for (s0, g0), (s1, g1) in zip(pts, pts[1:]):
        if g0 >= 1.0 >= g1:
            if g0 == g1:
                return s0
            t = (0.0 - math.log(g0)) / (math.log(g1) - math.log(g0))
            return math.exp(math.log(s0) + t * (math.log(s1) - math.log(s0)))
    return None


def load(path):
    if not os.path.exists(path):
        return [], {}
    with open(path) as fh:
        blob = json.load(fh)
    return blob.get("rows", []), blob.get("config", {})


def pick(rows, pattern, n, field):
    rs = [r for r in rows if r.get("pattern") == pattern and r.get("N") == n
          and field in r]
    return sorted(rs, key=lambda r: r["selectivity"])


def graph(pattern, ns, rows, spec, outdir):
    """Two panels: gain vs selectivity, and the absolute values behind it."""
    gain_f, a_f, b_f, ylab, alab, title, fname = spec
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(12.4, 4.9))
    cmap = plt.get_cmap("viridis")
    any_data = False
    broken = False
    for j, n in enumerate(ns):
        rs = pick(rows, pattern, n, gain_f)
        if not rs:
            continue
        any_data = True
        if any(r.get("inline_broken") or not r.get("valid_comparison", True)
               for r in rs):
            broken = True
        col = cmap(j / max(1, len(ns) - 1))
        xs = [r["selectivity"] for r in rs]
        ax.plot(xs, [r[gain_f] for r in rs], marker="o", ms=4, lw=1.7,
                color=col, label=f"N={n:,}")
        c = crossover([(r["selectivity"], r[gain_f]) for r in rs])
        if c:
            ax.axvline(c, color=col, ls=":", lw=1.0, alpha=0.8)
        ax2.plot(xs, [r[a_f] for r in rs], marker="o", ms=4, lw=1.6, color=col,
                 label=f"N={n:,} inline")
        ax2.plot(xs, [r[b_f] for r in rs], marker="s", ms=4, lw=1.6, ls="--",
                 color=col, alpha=0.8)
    if not any_data:
        plt.close(fig)
        return None

    ax.axhline(1.0, color="crimson", ls="--", lw=1.3)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlabel("filter selectivity (matching rows / corpus size)")
    ax.set_ylabel(ylab)
    ax.set_title("pre-filter advantage; dotted verticals = crossover",
                 fontsize=9)
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=7)
    ax.text(0.02, 0.04, "pre-filter better above the red line",
            transform=ax.transAxes, fontsize=7, color="crimson")

    ax2.set_xscale("log")
    ax2.set_yscale("log")
    ax2.xaxis.set_major_formatter(PCT)
    ax2.set_xlabel("filter selectivity")
    ax2.set_ylabel(alab)
    ax2.set_title("absolute values: solid = inline, dashed = pre-filter",
                  fontsize=9)
    ax2.grid(alpha=0.3, which="both")
    ax2.legend(fontsize=6, ncol=2)

    head = f"{pattern} - {title}"
    if broken:
        head += "\n[INLINE DROPS THE FILTER - results invalid, see Issue 1]"
    fig.suptitle(head, fontsize=12,
                 color="crimson" if broken else "black")
    fig.tight_layout(rect=(0, 0, 1, 0.90 if broken else 0.93))
    p = os.path.join(outdir, f"{slug(pattern)}-{fname}.png")
    fig.savefig(p, dpi=140)
    plt.close(fig)
    return p


def table(fh, pattern, ns, rows, gain_f, a_f, b_f, unit, better):
    levels = sorted({r["selectivity"] for r in rows
                     if r.get("pattern") == pattern})
    if not levels:
        fh.write("_no data_\n\n")
        return
    fh.write(f"| selectivity | " + " | ".join(
        f"N={n:,}" for n in ns) + " |\n")
    fh.write("|" + "---|" * (len(ns) + 1) + "\n")
    for lv in levels:
        cells = []
        for n in ns:
            m = [r for r in pick(rows, pattern, n, gain_f)
                 if abs(r["selectivity"] - lv) < 1e-12]
            if not m:
                cells.append("-")
                continue
            r = m[0]
            cells.append(f"**{r[gain_f]:.2f}x** ({r[a_f]:.{2 if unit=='ms' else 0}f} / "
                         f"{r[b_f]:.{2 if unit=='ms' else 0}f})")
        fh.write(f"| {lv*100:.3f}% | " + " | ".join(cells) + " |\n")
    fh.write(f"\nCells are **gain** (inline {unit} / pre-filter {unit} for "
             f"latency, pre-filter qps / inline qps for throughput) with "
             f"(inline / pre-filter) raw {unit} in brackets. "
             f"Gain > 1 means pre-filtering is {better}.\n\n")
    cross = []
    for n in ns:
        rs = pick(rows, pattern, n, gain_f)
        c = crossover([(r["selectivity"], r[gain_f]) for r in rs])
        cross.append(f"{c*100:.2f}%" if c else "n/a")
    fh.write("Crossover (where pre-filtering stops winning): "
             + ", ".join(f"N={n:,} → {c}" for n, c in zip(ns, cross)) + "\n\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency-json", default="prefilter-data/prefilter-matrix.json")
    ap.add_argument("--throughput-json", default="prefilter-data/prefilter-throughput.json")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--out", default="PREFILTER-REPORT.md")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    lat, lcfg = load(args.latency_json)
    thr, tcfg = load(args.throughput_json)
    lat = [r for r in lat if "latency_gain" in r]
    thr = [r for r in thr if r.get("mode") == "grid" and "qps_gain" in r]

    patterns = []
    for r in lat + thr:
        if r.get("pattern") and r["pattern"] not in patterns:
            patterns.append(r["pattern"])
    lns = sorted({r["N"] for r in lat})
    tns = sorted({r["N"] for r in thr})

    # exact query templates, recovered from the harness definition
    import prefilter_matrix_e2e as mx
    tmpl = dict(mx.PATTERNS)

    made = []
    with open(args.out, "w") as fh:
        fh.write("# Pre-filter vs inline: per-test-case results\n\n")
        fh.write("One section per test case. A test case is a single query "
                 "varied against corpus size. Each has **separate** tables and "
                 "graphs for the two dimensions:\n\n")
        fh.write("1. **Latency** - single query, one connection, no "
                 "concurrency. What the filtering decision does to an "
                 "individual query.\n")
        fh.write("2. **Throughput** - queries/sec under saturating load from "
                 "multiple client processes. What it does to server capacity.\n\n")
        fh.write("They are kept separate because they can disagree. The "
                 "decision only changes work on the reader threads, so when "
                 "the main thread is the ceiling the difference is hidden and "
                 "throughput barely moves even where latency shows a large "
                 "gap. The `bound` column in the throughput tables says which "
                 "resource was saturated.\n\n")
        fh.write(f"Common settings: dim {lcfg.get('dim', '?')}, "
                 f"k {lcfg.get('k', '?')}, corpus "
                 f"{lcfg.get('data', '?')}, `ef_runtime` at the server default "
                 f"(10), L2 distance, HASH keys. Selectivity is exact: doc `i` "
                 f"carries the marker for every level `L` where `i < L*N`, and "
                 f"is verified per point with a filter-only "
                 f"`FT.SEARCH ... LIMIT 0 0` count.\n\n")
        fh.write(f"Throughput load: {tcfg.get('procs','?')} processes x "
                 f"{tcfg.get('threads_per','?')} connections, "
                 f"{tcfg.get('reader_threads','?')} reader threads, "
                 f"{tcfg.get('duration','?')}s per point.\n\n")
        fh.write("---\n\n")

        for p in patterns:
            fh.write(f"## Test case: {p}\n\n")
            t = tmpl.get(p, "?")
            fh.write(f"Query template:\n\n```\n({t})=>[KNN k @vec $v AS score]"
                     f"\n```\n\n")
            ex_n, ex_l = 20000, 0.01
            ex_q = max(1, int(ex_l * ex_n))
            try:
                ex = t.format(S=ex_q - 1, L=f"s{int(round(ex_l*10000))}")
                fh.write(f"Concrete example at N=20,000 and 1% selectivity "
                         f"(200 matching rows):\n\n```\n"
                         f"FT.SEARCH idx \"({ex})=>[KNN 10 @vec $v AS score]\" "
                         f"PARAMS 2 v <vector> LIMIT 0 10 DIALECT 2 NOCONTENT"
                         f"\n```\n\n")
            except Exception:  # noqa: BLE001
                pass
            if p in getattr(mx, "INLINE_BROKEN", set()):
                fh.write("> **This test case is invalid for the inline path.** "
                         "Inline filtering drops text predicates under a "
                         "composed AND, so it returns documents that do not "
                         "match the filter and does almost no filtering work. "
                         "Its numbers below measure a broken execution and must "
                         "not drive a threshold decision. Kept because it "
                         "documents the bug (Issue 1).\n\n")

            fh.write("### Dimension 1: latency\n\n")
            table(fh, p, lns, lat, "latency_gain", "inline_p50_ms",
                  "prefilter_p50_ms", "ms", "faster per query")
            g = graph(p, lns, lat,
                      ("latency_gain", "inline_p50_ms", "prefilter_p50_ms",
                       "gain = inline p50 / pre-filter p50",
                       "single-query p50 latency (ms)",
                       "latency (single query, no concurrency)", "latency"),
                      args.outdir)
            if g:
                made.append(g)
                fh.write(f"![{p} latency]({g})\n\n")

            fh.write("### Dimension 2: throughput under saturating load\n\n")
            if not tns:
                fh.write("_throughput grid not yet available_\n\n")
            else:
                table(fh, p, tns, thr, "qps_gain", "inline_qps",
                      "prefilter_qps", "qps", "higher throughput")
                fh.write("Saturation, per corpus size (inline path):\n\n")
                fh.write("| N | selectivity | main thread CPU | reader pool CPU"
                         " | bound |\n")
                fh.write("|---|---|---|---|---|\n")
                for n in tns:
                    rs = pick(thr, p, n, "qps_gain")
                    for r in rs[::max(1, len(rs)//3)]:
                        fh.write(f"| {n:,} | {r['selectivity']*100:.3f}% | "
                                 f"{r['inline_main_pct']:.0f}% | "
                                 f"{r['inline_reader_pct']:.0f}% "
                                 f"(cap {r['reader_threads']*100}%) | "
                                 f"{r['bound']} |\n")
                fh.write("\n")
                g = graph(p, tns, thr,
                          ("qps_gain", "inline_qps", "prefilter_qps",
                           "gain = pre-filter qps / inline qps",
                           "throughput (queries/sec)",
                           "throughput (saturating load)", "throughput"),
                          args.outdir)
                if g:
                    made.append(g)
                    fh.write(f"![{p} throughput]({g})\n\n")
            fh.write("---\n\n")

    print(f"wrote {args.out}")
    for m in made:
        print("wrote", m)


if __name__ == "__main__":
    main()

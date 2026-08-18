#!/usr/bin/env python3
"""Plot the pre-filter/inline crossover matrix for reporting.

Reads prefilter-data/prefilter-matrix.json (from prefilter_matrix_e2e.py) and optionally the
micro-benchmark logs, and writes PNGs.

Figures produced:
  1. fig1-gain-by-pattern.png     one panel per filter pattern: gain vs
                                  selectivity, one line per corpus size. The
                                  gain=1.0 line is the inflection point.
  2. fig2-crossover-vs-n.png      crossover selectivity vs corpus size, one line
                                  per pattern, against the current 0.1% default
                                  and the proposed rule.
  3. fig3-latency-curves.png      absolute serial latency of both paths vs
                                  selectivity, for a representative pattern at
                                  each N - shows *why* the curves cross.
  4. fig4-recall.png              inline recall vs selectivity per corpus size.
  5. fig5-saturation.png          saturating throughput per path with the
                                  bottleneck regime annotated.

Usage: plot_matrix.py [--json prefilter-data/prefilter-matrix.json] [--outdir figures]
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


def crossover(points):
    pts = sorted(points)
    if not pts or pts[0][1] < 1.0 or pts[-1][1] > 1.0:
        return None
    for (s0, g0), (s1, g1) in zip(pts, pts[1:]):
        if g0 >= 1.0 >= g1:
            if g0 == g1:
                return s0
            t = (0.0 - math.log(g0)) / (math.log(g1) - math.log(g0))
            return math.exp(math.log(s0) + t * (math.log(s1) - math.log(s0)))
    return None


def load(path):
    with open(path) as fh:
        blob = json.load(fh)
    rows = [r for r in blob["rows"] if "latency_gain" in r]
    return rows, blob


def group(rows, key):
    out = {}
    for r in rows:
        out.setdefault(r[key], []).append(r)
    return out


def series(rows, pattern, n, yfield):
    rs = sorted([r for r in rows if r["pattern"] == pattern and r["N"] == n],
                key=lambda r: r["selectivity"])
    return ([r["selectivity"] for r in rs], [r[yfield] for r in rs], rs)


def fig_gain_by_pattern(rows, patterns, ns, outdir):
    ncol = 3
    nrow = math.ceil(len(patterns) / ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(5.2 * ncol, 3.7 * nrow),
                             squeeze=False)
    cmap = plt.get_cmap("viridis")
    for idx, pattern in enumerate(patterns):
        ax = axes[idx // ncol][idx % ncol]
        broken = False
        for j, n in enumerate(ns):
            xs, ys, rs = series(rows, pattern, n, "latency_gain")
            if not xs:
                continue
            if any(not r.get("valid_comparison", True) for r in rs):
                broken = True
            ax.plot(xs, ys, marker="o", ms=3.5, lw=1.6,
                    color=cmap(j / max(1, len(ns) - 1)),
                    label=f"N={n:,}")
        ax.axhline(1.0, color="crimson", ls="--", lw=1.2)
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.xaxis.set_major_formatter(PCT)
        ax.set_xlabel("filter selectivity (qualified / N)")
        ax.set_ylabel("gain = inline p50 / prefilter p50")
        title = pattern + ("  [INLINE DROPS FILTER - INVALID]" if broken else "")
        ax.set_title(title, fontsize=10,
                     color="crimson" if broken else "black")
        ax.grid(alpha=0.3, which="both")
        ax.legend(fontsize=7)
        ax.text(0.02, 0.04, "pre-filter faster above this line",
                transform=ax.transAxes, fontsize=7, color="crimson")
    for k in range(len(patterns), nrow * ncol):
        axes[k // ncol][k % ncol].axis("off")
    fig.suptitle("Pre-filter vs inline: gain by filter pattern and corpus size\n"
                 "crossing the dashed line is the inflection point",
                 fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    p = os.path.join(outdir, "fig1-gain-by-pattern.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def fig_crossover_vs_n(rows, patterns, ns, outdir):
    fig, ax = plt.subplots(figsize=(10, 6.5))
    cmap = plt.get_cmap("tab10")
    fits = {}
    for i, pattern in enumerate(patterns):
        xs, ys = [], []
        for n in ns:
            _s, _g, rs = series(rows, pattern, n, "latency_gain")
            if not rs or any(not r.get("valid_comparison", True) for r in rs):
                continue
            c = crossover([(r["selectivity"], r["latency_gain"]) for r in rs])
            if c:
                xs.append(n)
                ys.append(c)
        if not xs:
            continue
        ax.plot(xs, ys, marker="o", lw=1.8, color=cmap(i % 10), label=pattern)
        if len(xs) >= 3:
            # fit qualified_crossover = a * N^b  (qualified = ratio * N)
            lx = [math.log(n) for n in xs]
            ly = [math.log(c * n) for c, n in zip(ys, xs)]
            m = len(lx)
            mx, my = sum(lx) / m, sum(ly) / m
            b = (sum((x - mx) * (y - my) for x, y in zip(lx, ly))
                 / sum((x - mx) ** 2 for x in lx))
            fits[pattern] = (math.exp(my - b * mx), b)

    # Extrapolate the binding (lowest) pattern so the large-N regime is visible.
    lo = math.log10(min(ns))
    full = [10 ** e for e in
            [lo + (8 - lo) * t / 60 for t in range(61)]]
    if fits:
        worst = min(fits.items(), key=lambda kv: kv[1][0] * 1e8 ** (kv[1][1] - 1))
        wname, (wa, wb) = worst
        ax.plot(full, [wa * n ** (wb - 1) for n in full], color="grey",
                ls="-.", lw=1.6,
                label=f"fit, binding pattern ({wname}):\n"
                      rf"  ${wa:.2f}\,N^{{{wb:.2f}}}$ qualified")
        # where does today's fixed default become unsafe for that pattern?
        # Use the throughput-derived binding fit (a=5.345, b=0.450), since
        # that is the load-relevant curve; the latency fit puts this at ~16M.
        n_unsafe = (5.345 / 0.001) ** (1.0 / (1.0 - 0.450))
        if min(full) < n_unsafe < max(full):
            ax.axvline(n_unsafe, color="crimson", ls=":", lw=1.4)
            ax.annotate(f"above N≈{n_unsafe/1e6:.0f}M the fixed 0.1%\n"
                        f"over-triggers pre-filtering",
                        (n_unsafe, 0.0012), fontsize=8, color="crimson",
                        ha="left", va="bottom",
                        xytext=(6, 0), textcoords="offset points")

    ax.axhline(0.001, color="black", ls=":", lw=2,
               label="current default 0.1%")
    # Proposed rule, exponent 0.5 to match the measured fits, coefficient set
    # for a ~2x margin under the binding pattern.
    # Coefficient 0.4 (not 0.6) because it is calibrated against the
    # throughput-derived crossover, which sits ~20% below the latency-derived
    # curves plotted here. See fig6.
    ax.plot(full, [0.4 * math.sqrt(n * 10) / n for n in full],
            color="green", ls="--", lw=2,
            label=r"proposed $0.4\sqrt{N\cdot ef}$ (ef=10)")

    ax.axvspan(max(ns), 1e8, color="grey", alpha=0.10)
    ax.text(max(ns) * 1.4, 0.055, "extrapolated", fontsize=8, color="grey",
            rotation=90, va="top")

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_ylim(3e-4, 0.12)
    ax.yaxis.set_major_formatter(PCT)
    ax.set_xlabel("corpus size N (vectors)")
    ax.set_ylabel("crossover selectivity")
    ax.set_title("Crossover selectivity falls as ~1/sqrt(N)\n"
                 "a fixed ratio cannot track it: far too conservative at small "
                 "N, too aggressive at large N")
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=7.5, loc="lower left")
    fig.tight_layout()
    p = os.path.join(outdir, "fig2-crossover-vs-n.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def fig_latency_curves(rows, pattern, ns, outdir):
    ncol = min(3, len(ns))
    nrow = math.ceil(len(ns) / ncol)
    fig, axes = plt.subplots(nrow, ncol, figsize=(5.0 * ncol, 3.6 * nrow),
                             squeeze=False)
    for i, n in enumerate(ns):
        ax = axes[i // ncol][i % ncol]
        xs, yi, rs = series(rows, pattern, n, "inline_p50_ms")
        _x, yp, _r = series(rows, pattern, n, "prefilter_p50_ms")
        if not xs:
            ax.axis("off")
            continue
        ax.plot(xs, yi, marker="o", ms=3.5, label="inline (HNSW + predicate)")
        ax.plot(xs, yp, marker="s", ms=3.5, label="pre-filter (exact scan)")
        c = crossover([(r["selectivity"], r["latency_gain"]) for r in rs])
        if c:
            ax.axvline(c, color="crimson", ls="--", lw=1.2)
            ax.text(c, min(min(yi), min(yp)), f" {c*100:.2f}%",
                    color="crimson", fontsize=8, va="bottom")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.xaxis.set_major_formatter(PCT)
        ax.set_xlabel("selectivity")
        ax.set_ylabel("serial p50 latency (ms)")
        ax.set_title(f"N = {n:,}", fontsize=10)
        ax.grid(alpha=0.3, which="both")
        ax.legend(fontsize=7)
    for k in range(len(ns), nrow * ncol):
        axes[k // ncol][k % ncol].axis("off")
    fig.suptitle(f"Why the curves cross - pattern: {pattern}\n"
                 "pre-filter cost grows linearly with qualified keys; "
                 "inline cost falls as the filter loosens", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    p = os.path.join(outdir, "fig3-latency-curves.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def fig_recall(rows, patterns, ns, outdir):
    fig, ax = plt.subplots(figsize=(9, 5.5))
    cmap = plt.get_cmap("viridis")
    valid = [p for p in patterns
             if not any(not r.get("valid_comparison", True)
                        for r in rows if r["pattern"] == p)]
    pattern = "numeric" if "numeric" in valid else (valid or patterns)[0]
    for j, n in enumerate(ns):
        xs, ys, _rs = series(rows, pattern, n, "recall")
        if xs:
            ax.plot(xs, ys, marker="o", ms=4, lw=1.7,
                    color=cmap(j / max(1, len(ns) - 1)), label=f"N={n:,}")
    ax.axhline(1.0, color="green", ls=":", lw=1.5,
               label="pre-filter (exact, by construction)")
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(PCT)
    ax.set_xlabel("filter selectivity")
    ax.set_ylabel("inline recall vs exact pre-filter")
    ax.set_title(f"Inline filtering also loses answer quality at scale "
                 f"(pattern: {pattern})\n"
                 "pre-filtering is exact, so recall gaps are a cost of "
                 "choosing inline")
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=8)
    fig.tight_layout()
    p = os.path.join(outdir, "fig4-recall.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def fig_saturation(blob, outdir):
    sat = [r for r in blob["rows"] if "bottleneck" in r]
    if not sat:
        return None
    patterns = sorted({r["pattern"] for r in sat})
    pattern = "numeric" if "numeric" in patterns else patterns[0]
    rs = [r for r in sat if r["pattern"] == pattern]
    levels = sorted({r["selectivity"] for r in rs})
    ns = sorted({r["N"] for r in rs})

    fig, axes = plt.subplots(1, len(levels),
                            figsize=(4.6 * len(levels), 4.4), squeeze=False)
    for i, level in enumerate(levels):
        ax = axes[0][i]
        sub = sorted([r for r in rs if r["selectivity"] == level],
                     key=lambda r: r["N"])
        xs = [r["N"] for r in sub]
        ax.plot(xs, [r["inline_qps"] for r in sub], marker="o",
                label="inline qps")
        ax.plot(xs, [r["prefilter_qps"] for r in sub], marker="s",
                label="pre-filter qps")
        for r in sub:
            if r["bottleneck"] != "reader-bound":
                ax.annotate(r["bottleneck"], (r["N"], r["inline_qps"]),
                            fontsize=6, rotation=30,
                            textcoords="offset points", xytext=(2, 6))
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("corpus size N")
        ax.set_ylabel("throughput (qps, saturating load)")
        ax.set_title(f"selectivity {level*100:g}%", fontsize=10)
        ax.grid(alpha=0.3, which="both")
        ax.legend(fontsize=7)
    fig.suptitle(f"Saturating throughput - does the choice matter at this N? "
                 f"(pattern: {pattern})\n"
                 "where both paths converge, the server ceiling dominates and "
                 "the threshold barely matters", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.9))
    p = os.path.join(outdir, "fig5-saturation.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def fig_lat_vs_qps(rows, blob, patterns, ns, outdir):
    """Latency-derived vs throughput-derived crossover.

    The headline crossover numbers come from serial latency. Serial measurement
    gives pre-filtering the whole memory bandwidth to itself, so it is an
    optimistic upper bound. This shows the size of that optimism.
    """
    allrows = blob["rows"]
    fig, (ax, ax2) = plt.subplots(1, 2, figsize=(13, 5.4))
    cmap = plt.get_cmap("tab10")
    ratios = {}
    for i, p in enumerate(patterns):
        xs, yl, yq = [], [], []
        for n in ns:
            L = [r for r in allrows
                 if r["pattern"] == p and r["N"] == n and "latency_gain" in r
                 and r.get("valid_comparison", True)]
            S = [r for r in allrows
                 if r["pattern"] == p and r["N"] == n and "qps_gain" in r]
            cl = crossover([(r["selectivity"], r["latency_gain"]) for r in L])
            cq = crossover([(r["selectivity"], r["qps_gain"]) for r in S])
            if cl and cq:
                xs.append(n)
                yl.append(cl)
                yq.append(cq)
                ratios.setdefault(n, []).append(cq / cl)
        if not xs:
            continue
        c = cmap(i % 10)
        ax.plot(xs, yl, marker="o", lw=1.6, color=c, label=f"{p} (latency)")
        ax.plot(xs, yq, marker="s", ls="--", lw=1.6, color=c, alpha=0.75)
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.yaxis.set_major_formatter(PCT)
    ax.set_xlabel("corpus size N")
    ax.set_ylabel("crossover selectivity")
    ax.set_title("solid = serial latency, dashed = saturating throughput\n"
                 "throughput crossover sits lower: pre-filtering loses more "
                 "under load", fontsize=10)
    ax.grid(alpha=0.3, which="both")
    ax.legend(fontsize=6.5, ncol=2)

    ns_sorted = sorted(ratios)
    means = [sum(ratios[n]) / len(ratios[n]) for n in ns_sorted]
    ax2.plot(ns_sorted, means, marker="o", color="crimson", lw=2)
    ax2.axhline(1.0, color="black", ls=":", lw=1.5)
    ax2.set_xscale("log")
    ax2.set_xlabel("corpus size N")
    ax2.set_ylabel("throughput crossover / latency crossover")
    ax2.set_ylim(0.6, 1.6)
    ax2.set_title("below 1.0 means the latency-derived crossover is optimistic\n"
                  "(N=5,000 is unreliable: both paths plateau)", fontsize=10)
    ax2.grid(alpha=0.3)
    for n, m in zip(ns_sorted, means):
        ax2.annotate(f"{m:.2f}x", (n, m), fontsize=8,
                     textcoords="offset points", xytext=(4, 5))
    fig.suptitle("Why the recommended coefficient is calibrated on throughput, "
                 "not latency", fontsize=12)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    p = os.path.join(outdir, "fig6-latency-vs-throughput.png")
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", default="prefilter-data/prefilter-matrix.json")
    ap.add_argument("--outdir", default="figures")
    args = ap.parse_args()
    os.makedirs(args.outdir, exist_ok=True)

    rows, blob = load(args.json)
    patterns = []
    for r in rows:
        if r["pattern"] not in patterns:
            patterns.append(r["pattern"])
    ns = sorted({r["N"] for r in rows})

    made = [
        fig_gain_by_pattern(rows, patterns, ns, args.outdir),
        fig_crossover_vs_n(rows, patterns, ns, args.outdir),
        fig_latency_curves(rows, "numeric" if "numeric" in patterns
                           else patterns[0], ns, args.outdir),
        fig_recall(rows, patterns, ns, args.outdir),
        fig_saturation(blob, args.outdir),
        fig_lat_vs_qps(rows, blob,
                       [x for x in patterns if x != 'text AND text'],
                       ns, args.outdir),
    ]
    for p in made:
        if p:
            print("wrote", p)


if __name__ == "__main__":
    main()

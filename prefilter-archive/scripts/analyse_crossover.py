#!/usr/bin/env python3
"""Compute the pre-filter/inline crossover selectivity from micro-benchmark logs.

The benchmark reports a speedup (prefilter gain) per selectivity point. The
crossover is where speedup == 1. Both latencies are close to power laws in
selectivity over this range, so interpolate log(speedup) against
log(selectivity) between the two points that bracket 1.0 rather than eyeballing
the bracket.

Usage: analyse_crossover.py LOG [LOG ...]
"""
import re
import sys
import math

ROW = re.compile(
    r"^\s*(\S+)\s+([\d.]+)%\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)x\s+"
    r"(\S+)\s+([\d.]+)\s*$")
HDR = re.compile(r"N=(\d+).*?efr=(\d+)")


def parse(path):
    """Return (meta, rows) where rows are dicts keyed by the first column."""
    meta = {}
    rows = []
    with open(path) as fh:
        for line in fh:
            h = HDR.search(line)
            if h and "N" not in meta:
                meta["N"] = int(h.group(1))
                meta["efr"] = int(h.group(2))
            m = ROW.match(line)
            if m:
                rows.append({
                    "group": m.group(1),        # dim or filter name
                    "sel": float(m.group(2)) / 100.0,
                    "qualified": int(m.group(3)),
                    "inline_us": float(m.group(4)),
                    "pre_us": float(m.group(5)),
                    "speedup": float(m.group(6)),
                    "recall": float(m.group(8)),
                })
    return meta, rows


def crossover(points):
    """Interpolate the selectivity where speedup crosses 1.0.

    points: list of (sel, speedup, qualified, recall) sorted by sel.
    Returns (sel, qualified, note) or (None, None, note).
    """
    pts = sorted(points, key=lambda p: p[0])
    if pts[0][1] < 1.0:
        return None, None, f"below range (already inline at {pts[0][0]*100:.3f}%)"
    if pts[-1][1] > 1.0:
        return None, None, f"above range (still prefilter at {pts[-1][0]*100:.3f}%)"
    for (s0, g0, q0, _), (s1, g1, q1, _) in zip(pts, pts[1:]):
        if g0 >= 1.0 >= g1:
            # log-log interpolation for the zero of log(gain)
            if g0 == g1:
                return s0, q0, "flat bracket"
            t = (0.0 - math.log(g0)) / (math.log(g1) - math.log(g0))
            ls = math.log(s0) + t * (math.log(s1) - math.log(s0))
            sel = math.exp(ls)
            lq = math.log(q0) + t * (math.log(max(q1, 1)) - math.log(max(q0, 1)))
            return sel, math.exp(lq), f"bracketed [{s0*100:.3f}%, {s1*100:.3f}%]"
    return None, None, "no bracket found"


def main():
    print(f"{'log':<34} {'N':>9} {'efr':>4} {'group':>8} "
          f"{'crossover':>10} {'qual@cross':>11} {'min_recall':>10}  note")
    for path in sys.argv[1:]:
        meta, rows = parse(path)
        if not rows:
            print(f"{path:<34} (no rows parsed)")
            continue
        groups = {}
        for r in rows:
            groups.setdefault(r["group"], []).append(r)
        for g, rs in groups.items():
            pts = [(r["sel"], r["speedup"], r["qualified"], r["recall"])
                   for r in rs]
            sel, qual, note = crossover(pts)
            min_recall = min(r["recall"] for r in rs)
            sel_s = f"{sel*100:.2f}%" if sel else "n/a"
            qual_s = f"{qual:.0f}" if qual else "n/a"
            print(f"{path.split('/')[-1]:<34} {meta.get('N','?'):>9} "
                  f"{meta.get('efr','?'):>4} {g:>8} {sel_s:>10} {qual_s:>11} "
                  f"{min_recall:>10.3f}  {note}")


if __name__ == "__main__":
    main()

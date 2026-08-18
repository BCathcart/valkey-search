#!/usr/bin/env python3
"""Rebuild the throughput JSON from the run's stdout log.

Why this exists: prefilter_throughput_e2e.py writes its JSON in a single dump
after *all* corpus sizes finish. The largest corpus (1.28M) takes ~28 minutes
just to load, so a crash late in the run would discard every earlier result even
though all of it was already printed. This parser reconstructs the rows from the
log so partial progress is never lost, and emits the same schema
make_report.py consumes.

Usage:
    python3 scripts/benchmark/recover_throughput_log.py \
        --log /tmp/thr_full.log --out prefilter-data/prefilter-throughput-partial.json

Safe to run while the benchmark is still going; it just captures whatever has
been printed so far.
"""
import argparse
import json
import os
import re

# "# N = 320,000  dim=768  readers=4"
N_RE = re.compile(r"^#\s*N\s*=\s*([\d,]+)\s+dim=(\d+)\s+readers=(\d+)")

# "numeric                 0.100%        5     1844    45634  24.75x       6%
#      376%       83%      47% reader-bound/main-heavy"
ROW_RE = re.compile(
    r"^(?P<pattern>\S.*?)\s{2,}"
    r"(?P<sel>[\d.]+)%\s+"
    r"(?P<qual>\d+)\s+"
    r"(?P<in_qps>\d+)\s+"
    r"(?P<pre_qps>\d+)\s+"
    r"(?P<gain>[\d.]+)x\s+"
    r"(?P<in_main>\d+)%\s+"
    r"(?P<in_rdr>\d+)%\s+"
    r"(?P<pre_main>\d+)%\s+"
    r"(?P<pre_rdr>\d+)%\s+"
    r"(?P<bound>\S+)\s*$"
)


def parse(path):
    rows = []
    n = dim = readers = None
    with open(path, errors="replace") as fh:
        for line in fh:
            # The server logs a very chatty "Defined Info Field" line; skip fast.
            if "Defined Info Field" in line:
                continue
            m = N_RE.match(line)
            if m:
                n = int(m.group(1).replace(",", ""))
                dim = int(m.group(2))
                readers = int(m.group(3))
                continue
            m = ROW_RE.match(line.rstrip("\n"))
            if not m or n is None:
                continue
            pattern = m.group("pattern").strip()
            if pattern in ("pattern",):        # header line
                continue
            # Field names must match prefilter_throughput_e2e.py's schema exactly.
            # make_report.py filters on mode == "grid", so omitting "mode" makes
            # every recovered row silently disappear from the report.
            rows.append({
                "mode": "grid",
                "N": n,
                "dim": dim,
                "reader_threads": readers,
                "pattern": pattern,
                "selectivity": float(m.group("sel")) / 100.0,
                "qualified": int(m.group("qual")),
                "inline_qps": float(m.group("in_qps")),
                "prefilter_qps": float(m.group("pre_qps")),
                "qps_gain": float(m.group("gain")),
                "inline_main_pct": float(m.group("in_main")),
                "inline_reader_pct": float(m.group("in_rdr")),
                "prefilter_main_pct": float(m.group("pre_main")),
                "prefilter_reader_pct": float(m.group("pre_rdr")),
                "bound": m.group("bound"),
                # text AND text is the pattern hit by the inline filter-skip bug
                # (Issue 1), so its inline number is not a valid comparison.
                "inline_broken": pattern == "text AND text",
                # p50/p99 and total CPU are not printed in the table, so they are
                # genuinely unavailable here rather than zero. Left absent on
                # purpose: fabricating them would be worse than their absence.
                "recovered_from_log": True,
            })
    return rows


def _dedup_key(r):
    """Identity of a measurement point, robust to float representation.

    Selectivity must be ROUNDED. Parsing "0.700%" from the log yields exactly
    0.007, while the harness computed 0.7/100 = 0.006999999999999999. Keying on
    the raw float made those two look like different points, which silently
    duplicated 16 rows (8 patterns x 2 corpus sizes at the 0.7% level) through a
    merge. They agreed to 0.4%, so it was harmless here, but it corrupts row
    counts and double-counts in fits.
    """
    return (r["N"], r.get("pattern"), round(r.get("selectivity") or 0.0, 6))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="/tmp/thr_full.log")
    ap.add_argument("--out", default="prefilter-data/prefilter-throughput-partial.json")
    ap.add_argument("--merge-with", action="append", default=[],
                    help="existing JSON(s) whose rows should be preserved. "
                         "Repeatable. Without this, a log that has not yet "
                         "printed any rows (e.g. a resumed run) would overwrite "
                         "good data with an empty file.")
    args = ap.parse_args()

    # Merge order matters: prior rows first, freshly parsed rows last, so a
    # re-measured (N, pattern, selectivity) supersedes the older value.
    merged = {}
    preserved = 0
    for path in args.merge_with:
        if not os.path.exists(path):
            continue
        with open(path) as fh:
            for r in json.load(fh).get("rows", []):
                if r.get("mode") != "grid":
                    continue
                merged[_dedup_key(r)] = r
                preserved += 1
    # Also fold in whatever is already at --out, so repeated runs accumulate.
    if os.path.exists(args.out):
        with open(args.out) as fh:
            try:
                for r in json.load(fh).get("rows", []):
                    if r.get("mode") == "grid":
                        merged.setdefault(
                            _dedup_key(r), r)
            except ValueError:
                pass

    for r in parse(args.log):
        merged[_dedup_key(r)] = r

    rows = sorted(merged.values(),
                  key=lambda r: (r["N"], r.get("pattern") or "",
                                 r.get("selectivity") or 0))
    ns = sorted({r["N"] for r in rows})
    pats = sorted({r["pattern"] for r in rows if r.get("pattern")})
    per_n = {n: sum(1 for r in rows if r["N"] == n) for n in ns}

    # Never replace a good file with a strictly worse one.
    tmp = args.out + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({
            "rows": rows,
            "warnings": [],
            "config": {"recovered": True, "source_log": args.log},
        }, fh, indent=1)
    os.replace(tmp, args.out)

    print(f"recovered {len(rows)} rows -> {args.out}")
    if ns:
        print(f"corpus sizes: {', '.join(f'{n:,} ({per_n[n]} rows)' for n in ns)}")
    expected = len(pats) * 10 if pats else 0
    for n in ns:
        state = ("complete" if expected and per_n[n] >= expected
                 else f"partial ({per_n[n]}/{expected})")
        print(f"  N={n:>9,}: {state}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Dimension 2: saturated throughput vs filtering decision.

Treated as a first-class dimension, symmetric with the latency grid: the full
selectivity grid, for every query pattern, at every corpus size.

Two things the earlier harness could not do:

1. **Actually saturate the server.** A threaded Python load generator tops out
   near 12.5k qps (GIL), at which point valkey's main thread is only ~40% busy.
   So the main-bound regime was unreachable and untested. This uses separate
   *processes*, which can push past that.

2. **Attribute saturation to the right thread group.** valkey names its threads,
   so main (`valkey-server`, tid == pid) and the reader pool (`read-worker-N`)
   can be measured separately rather than inferred from a process total.

Why the distinction matters: the pre-filter/inline decision only changes work
done on the reader threads. If the main thread is the bottleneck, that work is
hidden and the decision is close to irrelevant no matter what the latency curves
say. If the readers are the bottleneck, the decision drives throughput directly.

Modes:
  --mode calibrate  ramp client processes at one corpus size and report qps with
                    main vs reader CPU, to locate the main-bound regime.
  --mode grid       full patterns x selectivities x corpus sizes.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

import valkey

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prefilter_crossover_e2e as base  # noqa: E402
from prefilter_matrix_e2e import (PATTERNS, INLINE_BROKEN, SUBSET_LEVEL,  # noqa: E402
                                  percentile, verify_selectivity)

CLK = os.sysconf("SC_CLK_TCK")


def thread_cpu_ticks(pid):
    """(main_ticks, reader_ticks, writer_ticks, total_ticks) for the server."""
    main = readers = writers = total = 0
    base_dir = f"/proc/{pid}/task"
    try:
        tids = os.listdir(base_dir)
    except OSError:
        return (0, 0, 0, 0)
    for tid in tids:
        try:
            with open(f"{base_dir}/{tid}/stat") as fh:
                parts = fh.read().split()
            # comm may contain spaces/parens; utime/stime are the last two of
            # fields 14/15 counting from 1, so index from the closing paren.
            close = fh_close = None
            raw = " ".join(parts)
            close = raw.rindex(")")
            after = raw[close + 2:].split()
            ticks = int(after[11]) + int(after[12])
            name = raw[raw.index("(") + 1:close]
        except (OSError, ValueError, IndexError):
            continue
        total += ticks
        if int(tid) == pid:
            main += ticks
        elif name.startswith("read-worker"):
            readers += ticks
        elif name.startswith("write-worker"):
            writers += ticks
    return (main, readers, writers, total)


# NOTE: the process-based load generator (_proc_worker / _proc_worker_file)
# was removed. It spawned ~1120 short-lived processes per corpus size, which
# drove the host's syscall layer into returning OSError(38) on ordinary file
# opens and wedged the run. run_saturating() below uses threads only.

def ensure_corpus(client, n, dim, levels, data_kind):
    """Return the query vectors, loading the corpus only if not already present.

    Writes must be issued serially (pipelined HSETs into an indexed keyspace
    deadlock -- see VALKEY-SEARCH-ISSUES-FOUND.md issue 2), so loading 1.28M
    docs costs ~28 minutes. That was paid twice after crashes.

    An RDB round-trip preserves the HNSW index itself, not just the schema:
    measured on N=20,000, SAVE took 1.1s, restart restored dbsize and num_docs
    immediately with backfill_in_progress=0, and the top-10 KNN result was
    byte-identical. So a corpus is loaded serially once, snapshotted, and
    restored in seconds on every later run.

    The query vectors are regenerated rather than stored: generate_vectors is
    seeded, so the same (n, dim, data_kind) yields the same corpus and hence
    the same query set that was originally indexed.
    """
    have = client.execute_command("DBSIZE")
    vecs = base.regenerate_query_vectors(n, dim, data_kind)
    if have >= n:
        info = client.execute_command("FT.INFO", "idx")
        info = [x.decode() if isinstance(x, bytes) else x for x in info]
        d = dict(zip(info[0::2], info[1::2]))
        base.log(f"corpus restored from RDB: dbsize={have} "
                 f"num_docs={d.get('num_docs')} "
                 f"backfill={d.get('backfill_in_progress')} (load skipped)")
        return vecs
    base.log(f"no snapshot for N={n}; loading serially (one time)")
    vecs = base.load_dataset(client, n, dim, levels, data_kind=data_kind)
    t0 = time.perf_counter()
    client.execute_command("SAVE")
    base.log(f"snapshot written in {time.perf_counter()-t0:.1f}s; "
             f"later runs at this N will skip the load")
    return vecs


def run_saturating(port, query, vecs, k, duration, pid, procs, threads_per):
    """Drive load from `procs * threads_per` connections, all in this process.

    Threads, not processes. An earlier multiprocess version spawned
    `procs` short-lived processes per measurement point, i.e. 8 x 2 paths x 10
    selectivities x 7 patterns = ~1120 spawns per corpus size. Past roughly a
    thousand spawns the host's syscall layer began returning OSError(38)
    "Function not implemented" for ordinary file opens -- not just in the
    benchmark but in unrelated commands on the box (even loading libc.so.6
    failed). The run wedged with orphaned children. Threads create no processes
    and the threaded generator ran ~470 measurement points earlier with no
    instability.

    Cost of the change: the client tops out near ~12.5k qps, so the MAIN-BOUND
    regime at tiny N is no longer reachable. That is an accepted limitation --
    reader-bound saturation is the regime of interest, and it is reached for
    every real pattern (reader CPU 405-416% against a 400% cap).
    """
    nconn = procs * threads_per
    stop = threading.Event()
    counts = [0] * nconn
    lats = [[] for _ in range(nconn)]
    errors = []

    def worker(idx):
        try:
            conn = valkey.Valkey(host="127.0.0.1", port=port,
                                 socket_timeout=300)
            i = idx
            while not stop.is_set():
                v = vecs[i % len(vecs)]
                i += 1
                t = time.perf_counter()
                conn.execute_command(
                    "FT.SEARCH", "idx", query, "PARAMS", "2", "v", v,
                    "LIMIT", "0", str(k), "NOCONTENT", "DIALECT", "2")
                lats[idx].append((time.perf_counter() - t) * 1000.0)
                counts[idx] += 1
        except Exception as exc:                       # noqa: BLE001
            errors.append(repr(exc))

    ths = [threading.Thread(target=worker, args=(i,), daemon=True)
           for i in range(nconn)]
    m0, r0, w0, t0 = thread_cpu_ticks(pid)
    wall0 = time.perf_counter()
    for t in ths:
        t.start()
    time.sleep(duration)
    stop.set()
    for t in ths:
        t.join(timeout=30)
    wall = time.perf_counter() - wall0
    m1, r1, w1, t1 = thread_cpu_ticks(pid)
    if errors and not any(counts):
        raise RuntimeError(f"all load threads failed: {errors[0]}")

    total_count = sum(counts)
    lats = [x for l in lats for x in l]
    denom = wall * CLK / 100.0 if wall > 0 else 1
    return {
        "qps": total_count / wall if wall else float("nan"),
        "count": total_count,
        "p50": percentile(lats, 50),
        "p90": percentile(lats, 90),
        "p99": percentile(lats, 99),
        "mean": statistics.fmean(lats) if lats else float("nan"),
        "main_cpu_pct": (m1 - m0) / denom,
        "reader_cpu_pct": (r1 - r0) / denom,
        "writer_cpu_pct": (w1 - w0) / denom,
        "total_cpu_pct": (t1 - t0) / denom,
        "conns": procs * threads_per, "threads_per": threads_per,
        "errors": len(errors),
    }


def classify(res, reader_threads):
    """Which resource is the ceiling? Drives whether the decision matters."""
    reader_cap = reader_threads * 100.0
    if res["main_cpu_pct"] >= 90.0:
        return "MAIN-BOUND"
    if res["reader_cpu_pct"] >= 0.90 * reader_cap:
        return "reader-bound"
    if res["main_cpu_pct"] >= 70.0:
        return "main-heavy"
    return "client-bound"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=("calibrate", "grid"), default="grid")
    ap.add_argument("--module", default=".build-release/libsearch.so")
    ap.add_argument("--port", type=int, default=7521)
    ap.add_argument("--workdir", default="/tmp/prefilter_thr")
    ap.add_argument("--n-values", default="5000,20000,80000,320000,1280000")
    ap.add_argument("--dim", type=int, default=768)
    ap.add_argument("--k", type=int, default=10)
    ap.add_argument("--selectivities",
                    default="0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,"
                            "0.07,0.10")
    ap.add_argument("--procs", type=int, default=8)
    ap.add_argument("--threads-per", type=int, default=4)
    ap.add_argument("--ramp", default="1,2,4,8,12,16",
                    help="calibrate mode: process counts to try")
    ap.add_argument("--reader-threads", type=int, default=4)
    ap.add_argument("--writer-threads", type=int, default=4)
    ap.add_argument("--duration", type=float, default=2.0)
    ap.add_argument("--warmup", type=float, default=0.5)
    ap.add_argument("--data", choices=("clustered", "uniform"),
                    default="clustered")
    ap.add_argument("--patterns", default="")
    ap.add_argument("--resume-from",
                    help="JSON of prior rows; corpus sizes already complete "
                         "are skipped (use the log-recovery output)")
    ap.add_argument("--out", default="prefilter-data/prefilter-throughput")
    args = ap.parse_args()

    n_values = [int(x) for x in args.n_values.split(",") if x]
    levels = [float(x) for x in args.selectivities.split(",") if x]
    patterns = PATTERNS
    if args.patterns:
        want = {p.strip() for p in args.patterns.split(",")}
        patterns = [p for p in PATTERNS if p[0] in want]

    module = os.path.abspath(args.module)
    rows, warnings = [], []

    # Resume: reload rows already measured (e.g. recovered from a crashed run's
    # log) and skip any corpus size that is already complete. The 1.28M load
    # alone is ~28 minutes, so re-measuring finished sizes is pure waste.
    done_ns = set()
    if args.resume_from and os.path.exists(args.resume_from):
        with open(args.resume_from) as fh:
            prior = json.load(fh).get("rows", [])
        prior = [r for r in prior if r.get("mode") == "grid"]
        expected = len(patterns) * len(levels)
        per_n = {}
        for r in prior:
            per_n.setdefault(r["N"], []).append(r)
        for pn, rs in per_n.items():
            if len(rs) >= expected:
                done_ns.add(pn)
                rows.extend(rs)
        base.log(f"resume: loaded {len(rows)} rows from {args.resume_from}; "
                 f"skipping complete corpus sizes {sorted(done_ns)}")

    def persist(tag):
        """Write the JSON now, not only at the very end.

        The original version dumped once after all corpus sizes, so a crash in
        the last (largest, slowest) stage discarded everything.
        """
        with open(args.out + ".json", "w") as fh:
            json.dump({"rows": rows, "warnings": warnings,
                       "config": vars(args)}, fh, indent=1)
        base.log(f"[persist:{tag}] {len(rows)} rows -> {args.out}.json")

    for n in n_values:
        if n in done_ns:
            base.log(f"\n# N = {n:,} already complete, skipping")
            continue
        base.log(f"\n{'#'*72}\n# N = {n:,}  dim={args.dim}  "
                 f"readers={args.reader_threads}\n{'#'*72}")
        handle, admin = base.start_server(
            module, args.port, os.path.join(args.workdir, f"n{n}"),
            args.reader_threads, args.writer_threads)
        try:
            load_levels = sorted(set(levels) | {SUBSET_LEVEL})
            vecs = ensure_corpus(admin, n, args.dim, load_levels, args.data)
            pid = handle.proc.pid

            if args.mode == "calibrate":
                # Cheapest realistic query: tiny qualifying set on the numeric
                # filter. Ramp client processes and watch which thread group
                # saturates first.
                qualified = max(1, int(levels[0] * n))
                filt = f"@num:[0 {qualified-1}]"
                query = f"({filt})=>[KNN {args.k} @vec $v AS score]"
                base.log(f"\n{'procs':>6} {'conns':>6} {'qps':>9} "
                         f"{'main%':>7} {'reader%':>8} {'total%':>7} "
                         f"{'p50ms':>7} {'verdict':>13}")
                for p in [int(x) for x in args.ramp.split(",")]:
                    base.set_ratio(admin, base.FORCE_INLINE)
                    run_saturating(args.port, query, vecs, args.k,
                                   args.warmup, pid, p, args.threads_per)
                    r = run_saturating(args.port, query, vecs, args.k,
                                       args.duration, pid, p,
                                       args.threads_per)
                    v = classify(r, args.reader_threads)
                    base.log(f"{p:>6} {p*args.threads_per:>6} {r['qps']:>9.0f} "
                             f"{r['main_cpu_pct']:>6.0f}% "
                             f"{r['reader_cpu_pct']:>7.0f}% "
                             f"{r['total_cpu_pct']:>6.0f}% "
                             f"{r['p50']:>7.2f} {v:>13}")
                    rows.append({"mode": "calibrate", "N": n, "procs": p,
                                 "conns": p * args.threads_per,
                                 "verdict": v, **r})
                continue

            hdr = (f"{'pattern':<22} {'select':>7} {'qual':>8} "
                   f"{'in_qps':>8} {'pre_qps':>8} {'gain':>7} "
                   f"{'in_main%':>8} {'in_rdr%':>8} {'pre_main%':>9} "
                   f"{'pre_rdr%':>8} {'bound':>13}")
            base.log("\n" + hdr)
            base.log("-" * len(hdr))
            for name, template in patterns:
                for level in levels:
                    qualified = max(1, int(level * n))
                    label = f"s{int(round(level*10000))}"
                    filt = template.format(S=qualified - 1, L=label)
                    query = f"({filt})=>[KNN {args.k} @vec $v AS score]"
                    got, warn = verify_selectivity(admin, filt, qualified,
                                                   name, level)
                    if warn:
                        warnings.append(warn)

                    base.set_ratio(admin, base.FORCE_INLINE)
                    run_saturating(args.port, query, vecs, args.k,
                                   args.warmup, pid, args.procs,
                                   args.threads_per)
                    inl = run_saturating(args.port, query, vecs, args.k,
                                         args.duration, pid, args.procs,
                                         args.threads_per)
                    base.set_ratio(admin, base.FORCE_PREFILTER)
                    run_saturating(args.port, query, vecs, args.k,
                                   args.warmup, pid, args.procs,
                                   args.threads_per)
                    pre = run_saturating(args.port, query, vecs, args.k,
                                         args.duration, pid, args.procs,
                                         args.threads_per)

                    gain = pre["qps"] / inl["qps"] if inl["qps"] else float("nan")
                    bound_in = classify(inl, args.reader_threads)
                    bound_pre = classify(pre, args.reader_threads)
                    bound = (bound_in if bound_in == bound_pre
                             else f"{bound_in}/{bound_pre}")
                    rows.append({
                        "mode": "grid", "N": n, "dim": args.dim,
                        "pattern": name, "selectivity": level,
                        "qualified": qualified, "qualified_actual": got,
                        "inline_qps": inl["qps"], "prefilter_qps": pre["qps"],
                        "qps_gain": gain,
                        "inline_p50": inl["p50"], "prefilter_p50": pre["p50"],
                        "inline_p99": inl["p99"], "prefilter_p99": pre["p99"],
                        "inline_main_pct": inl["main_cpu_pct"],
                        "inline_reader_pct": inl["reader_cpu_pct"],
                        "prefilter_main_pct": pre["main_cpu_pct"],
                        "prefilter_reader_pct": pre["reader_cpu_pct"],
                        "inline_total_pct": inl["total_cpu_pct"],
                        "prefilter_total_pct": pre["total_cpu_pct"],
                        "bound_inline": bound_in, "bound_prefilter": bound_pre,
                        "bound": bound,
                        "inline_broken": name in INLINE_BROKEN,
                        "reader_threads": args.reader_threads,
                        "procs": args.procs, "threads_per": args.threads_per,
                    })
                    base.log(f"{name:<22} {level*100:6.3f}% {qualified:>8} "
                             f"{inl['qps']:>8.0f} {pre['qps']:>8.0f} "
                             f"{gain:>6.2f}x {inl['main_cpu_pct']:>7.0f}% "
                             f"{inl['reader_cpu_pct']:>7.0f}% "
                             f"{pre['main_cpu_pct']:>8.0f}% "
                             f"{pre['reader_cpu_pct']:>7.0f}% {bound:>13}")
        finally:
            handle.stop()
            base.log(f"server for N={n} stopped")
        persist(f"N={n}")

    persist("final")
    base.log(f"\nwrote {args.out}.json ({len(rows)} rows)")


if __name__ == "__main__":
    main()

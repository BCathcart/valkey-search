#!/usr/bin/env python3
"""Load every corpus size concurrently and snapshot each to an RDB.

Why this exists
---------------
Loading is the dominant cost of the whole investigation and it cannot be
pipelined: pipelined HSETs into an indexed keyspace deadlock permanently (see
VALKEY-SEARCH-ISSUES-FOUND.md issue 2), so writes are serial round-trips at
0.4-1.3 ms/doc. Sequentially the five corpus sizes cost ~33 minutes, ~28 of
which is the 1.28M corpus alone. That bill was paid twice after crashes.

Two fixes, both applied here:

1. Concurrency. Each corpus gets its own server on its own port and its own
   working directory, and they load in parallel. Wall time collapses to the
   largest corpus (~28 min) instead of the sum.

   This is safe *for loading* because loading is dominated by per-request
   round-trip latency, not by a contended resource, and the box has 32 cores
   for 5 servers. It is emphatically NOT safe for measurement: the throughput
   benchmark measures saturation, so concurrent servers would contend for the
   very resource under test (reader CPU, memory bandwidth) and would penalise
   the bandwidth-heavy pre-filter path more than the inline path. Measurement
   therefore stays strictly serialized in prefilter_throughput_e2e.py.

2. Snapshots. An RDB round-trip preserves the HNSW index, not merely the
   schema: verified on N=20,000 that SAVE took 1.1s and a restart restored
   dbsize and num_docs immediately with backfill_in_progress=0 and a
   byte-identical top-10 KNN result. So every later run restores in seconds.

Usage:
    python3 scripts/benchmark/preload_corpora.py \
        --n-values 5000,20000,80000,320000,1280000 --dim 768
"""

import argparse
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prefilter_crossover_e2e as base                       # noqa: E402

# Must match the levels the measurement runs will query, plus the subset level
# used by the text-only OR pattern. Markers absent from the corpus cannot be
# added later without a full reload, so load a superset.
DEFAULT_LEVELS = "0.0005,0.001,0.002,0.004,0.007,0.01,0.02,0.035,0.05,0.07,0.10"


def load_one(module, n, dim, levels, data_kind, port, workdir, results, lock):
    tag = f"N={n:,}"
    try:
        handle, client = base.start_server(module, port, workdir, 4, 4)
    except Exception as exc:                                  # noqa: BLE001
        with lock:
            results[n] = f"FAILED to start server: {exc!r}"
        return
    try:
        have = client.execute_command("DBSIZE")
        if have >= n:
            with lock:
                results[n] = f"already snapshotted (dbsize={have}); skipped"
            return
        t0 = time.time()
        base.load_dataset(client, n, dim, levels, data_kind=data_kind)
        load_s = time.time() - t0
        t1 = time.time()
        client.execute_command("SAVE")
        save_s = time.time() - t1
        rdb = os.path.join(workdir, "dump.rdb")
        size_mb = os.path.getsize(rdb) / 1e6 if os.path.exists(rdb) else 0
        with lock:
            results[n] = (f"loaded in {load_s/60:.1f} min "
                          f"({1000*load_s/n:.2f} ms/doc), "
                          f"SAVE {save_s:.1f}s, rdb {size_mb:.0f} MB")
    except Exception as exc:                                  # noqa: BLE001
        with lock:
            results[n] = f"FAILED during load: {exc!r}"
    finally:
        try:
            handle.stop()
        except Exception:                                     # noqa: BLE001
            pass
        base.log(f"{tag}: server stopped")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--module", default=".build-release/libsearch.so")
    ap.add_argument("--n-values", default="5000,20000,80000,320000,1280000")
    ap.add_argument("--dim", type=int, default=768)
    ap.add_argument("--levels", default=DEFAULT_LEVELS)
    ap.add_argument("--data", choices=("clustered", "uniform"),
                    default="clustered")
    ap.add_argument("--workdir", default="/tmp/prefilter_thr")
    ap.add_argument("--base-port", type=int, default=7700)
    args = ap.parse_args()

    module = os.path.abspath(args.module)
    ns = [int(x) for x in args.n_values.split(",")]
    levels = sorted({float(x) for x in args.levels.split(",")})

    base.log(f"pre-loading {len(ns)} corpora concurrently: "
             f"{', '.join(f'{n:,}' for n in ns)}")
    base.log(f"levels loaded: {levels}")
    base.log("measurement stays serialized later; only loading is concurrent")

    results, lock = {}, threading.Lock()
    threads = []
    for i, n in enumerate(ns):
        wd = os.path.join(args.workdir, f"n{n}")
        os.makedirs(wd, exist_ok=True)
        t = threading.Thread(
            target=load_one,
            args=(module, n, args.dim, levels, args.data,
                  args.base_port + i, wd, results, lock),
            daemon=False)
        threads.append((n, t))

    t0 = time.time()
    for _, t in threads:
        t.start()
        time.sleep(2)          # stagger so startup logs stay legible
    for _, t in threads:
        t.join()
    wall = time.time() - t0

    base.log(f"\n{'='*66}\nall loads finished in {wall/60:.1f} min\n{'='*66}")
    failures = 0
    for n in ns:
        msg = results.get(n, "no result recorded")
        if "FAILED" in msg:
            failures += 1
        base.log(f"  N={n:>9,}: {msg}")
    if failures:
        base.log(f"\n{failures} corpus size(s) FAILED -- inspect before measuring")
        return 1
    base.log("\nsnapshots ready; measurement runs will restore in seconds")
    return 0


if __name__ == "__main__":
    sys.exit(main())

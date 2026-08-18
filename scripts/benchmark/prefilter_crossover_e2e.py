#!/usr/bin/env python3
"""End-to-end pre-filter vs inline-filter crossover under saturating load.

Spins up a real valkey-server with the search module, loads a hybrid dataset
(vector + numeric + tag + text), then for each query shape and selectivity
drives the server to saturation with concurrent clients twice: once forced
down the inline-filtering path and once forced down the pre-filtering path.
Reports throughput (ops/sec) plus latency percentiles under load.

Forcing works without any code change because the planner predicate in
src/query/planner.cc is

    estimated_num_of_keys <= GetPrefilteringThresholdRatio() * N

and the config is bounded [0.0, 1.0] in src/valkey_search_options.cc:
  ratio = 0.0  -> condition true only when estimated == 0 -> always inline
  ratio = 1.0  -> estimated <= N is always true           -> always pre-filter

The config is registered .Dev() so it is immutable unless debug-mode is on;
"--debug-mode yes" is therefore passed as a *module argument* to loadmodule
(passing it as --search.debug-mode makes the server abort with "Unused Module
Configuration").

Why throughput and not just latency: the pre-filter path is a brute-force scan
and is memory-bandwidth heavy, while HNSW traversal is more latency-bound and
pointer-chasing. Under concurrency, bandwidth saturation should penalise
pre-filtering more, moving the crossover down. Running with several reader
threads all scanning simultaneously is the only way to see that effect, so
--reader-threads is configurable and the interesting comparison is 1 reader
thread (no intra-server contention) versus many.

Saturation is verified rather than assumed: search's query_queue_size is
sampled during each run, and a non-trivial queue depth means clients are
offering more work than the reader threads can retire.

Unlike the C++ micro-benchmark this exercises the full production stack
(RESP, command parsing, EvaluateFilterAsPrimary, entries fetchers, dedup,
reply generation), so complex composed predicates need no mirroring.
"""

import argparse
import json
import os
import random
import signal
import statistics
import struct
import subprocess
import sys
import threading
import time

import numpy as np
import valkey

RATIO_CONFIG = "search.prefiltering-threshold-ratio"
FORCE_INLINE = "0.0"
FORCE_PREFILTER = "1.0"


def log(msg):
    print(msg, flush=True)


class ServerHandle:
    def __init__(self, proc, port, workdir):
        self.proc = proc
        self.port = port
        self.workdir = workdir

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=10)


class _StillLoading(Exception):
    """Server is up but still restoring its RDB; keep waiting.

    Deliberately NOT a RuntimeError: the readiness loop treats RuntimeError
    as fatal (foreign-server identity mismatch), so this must land in the
    generic retry branch instead.
    """


def start_server(module_path, port, workdir, reader_threads, writer_threads,
                 ready_timeout=900):
    os.makedirs(workdir, exist_ok=True)
    logfile = os.path.join(workdir, "server.log")
    stderr_path = os.path.join(workdir, "server.stderr")
    # Start from a clean log so a stale one can't be mistaken for this run.
    for path in (logfile, stderr_path):
        if os.path.exists(path):
            os.remove(path)
    # Use a config file rather than command-line flags. On the command line
    # "--loadmodule" consumes exactly one token as the .so path, so module
    # arguments cannot be passed as separate argv elements (they get treated
    # as server configs, or folded into the filename). In a config file the
    # "loadmodule <path> <module args...>" form is unambiguous.
    conf_path = os.path.join(workdir, "server.conf")
    with open(conf_path, "w") as fh:
        fh.write(f"port {port}\n")
        fh.write("bind 127.0.0.1\n")
        fh.write('save ""\n')
        fh.write("appendonly no\n")
        fh.write("daemonize no\n")
        fh.write(f"logfile {logfile}\n")
        fh.write(f"dir {workdir}\n")
        fh.write(f"loadmodule {module_path} --debug-mode yes"
                 f" --reader-threads {reader_threads}"
                 f" --writer-threads {writer_threads}\n")
    # Refuse to start if the port is already serving. Otherwise a leftover
    # server (possibly a different, much slower build) answers our PINGs, we
    # measure CPU for the pid we spawned while querying someone else's server,
    # and every number produced is garbage. This is not hypothetical: stale
    # ASAN-build servers from another workspace were found running on this
    # host, and an ASAN build is slow enough to look like a hang.
    try:
        stale = valkey.Valkey(host="127.0.0.1", port=port, socket_timeout=2)
        stale.ping()
        raise RuntimeError(
            f"port {port} is already serving. Refusing to start: a leftover "
            f"server would silently answer our queries and invalidate the "
            f"measurement. Kill it or pass a different --port.")
    except RuntimeError:
        raise
    except Exception:
        pass  # nothing listening, which is what we want

    cmd = ["valkey-server", conf_path]
    log(f"starting: {' '.join(cmd)}")
    log(f"  loadmodule {module_path} --debug-mode yes "
        f"--reader-threads {reader_threads} "
        f"--writer-threads {writer_threads}")
    stderr_fh = open(stderr_path, "w")
    proc = subprocess.Popen(cmd, stdout=stderr_fh, stderr=subprocess.STDOUT)
    handle = ServerHandle(proc, port, workdir)

    def failure_detail():
        parts = []
        for path in (logfile, stderr_path):
            if os.path.exists(path):
                with open(path) as fh:
                    tail = fh.read()[-3000:]
                if tail.strip():
                    parts.append(f"--- {path} ---\n{tail}")
        return "\n".join(parts) or "(no output captured)"

    # Restoring a large RDB takes far longer than starting empty: the 1.28M /
    # dim-768 snapshot is 7.7 GB and needs several minutes. A 60s limit killed it
    # mid-load ("Received shutdown signal during loading"), so this is caller
    # controlled and defaults generously.
    deadline = time.time() + ready_timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early:\n{failure_detail()}")
        try:
            client = valkey.Valkey(host="127.0.0.1", port=port,
                                   socket_timeout=120)
            if client.ping():
                # Verify we are talking to the process we launched, not some
                # other server that happens to hold this port.
                info = client.info("server")
                served_pid = int(info.get("process_id", -1))
                if served_pid != proc.pid:
                    handle.stop()
                    raise RuntimeError(
                        f"port {port} is served by pid {served_pid}, but we "
                        f"launched pid {proc.pid}. Aborting rather than "
                        f"measuring a foreign server.")
                # valkey answers PING while still loading an RDB. Returning here
                # would let callers read a partial DBSIZE, and a caller that
                # decides "corpus incomplete -> reload" would then FLUSHALL and
                # re-load serially for ~28 minutes. So wait for loading to end.
                persist = client.info("persistence")
                if int(persist.get("loading", 0)):
                    raise _StillLoading()
                log(f"server is up (pid {served_pid}, verified ours)")
                return handle, client
        except RuntimeError:
            raise  # identity mismatch is fatal, not something to retry
        except Exception:
            waited = time.time() - (deadline - ready_timeout)
            if waited > 30 and int(waited) % 30 < 1:
                log(f"  still waiting for server readiness "
                    f"({waited:.0f}s/{ready_timeout}s; large RDB restore)")
            time.sleep(0.5)
    handle.stop()
    raise RuntimeError(
        f"server did not become ready within {ready_timeout}s:\n"
        f"{failure_detail()}")


def set_ratio(client, value):
    client.execute_command("CONFIG", "SET", RATIO_CONFIG, value)
    got = client.execute_command("CONFIG", "GET", RATIO_CONFIG)
    actual = got[1]
    if isinstance(actual, bytes):
        actual = actual.decode()
    if abs(float(actual) - float(value)) > 1e-12:
        raise RuntimeError(
            f"failed to set {RATIO_CONFIG}: wanted {value}, got {actual}. "
            "Is debug-mode enabled?")


def read_queue_depth(client):
    """Sample search's query_queue_size, used as saturation evidence."""
    try:
        raw = client.execute_command("INFO", "SEARCH")
        if isinstance(raw, bytes):
            raw = raw.decode(errors="replace")
        if not isinstance(raw, str):
            return None
        for line in raw.splitlines():
            if "query_queue_size" in line and ":" in line:
                return float(line.split(":", 1)[1].strip())
    except Exception:
        return None
    return None


CLK_TCK = os.sysconf("SC_CLK_TCK")


def _read_stat_cpu_ticks(path):
    """(utime + stime) in ticks from a /proc .../stat file.

    The comm field is parenthesised and may contain spaces, so fields are
    indexed from the last ')'. After it, fields[0] is state (field 3), so
    utime (field 14) is fields[11] and stime (field 15) is fields[12].
    """
    try:
        with open(path) as fh:
            data = fh.read()
        fields = data[data.rindex(")") + 2:].split()
        return float(fields[11]) + float(fields[12])
    except Exception:
        return None


def read_proc_cpu_ticks(pid):
    """Total CPU ticks for the whole server process (all threads)."""
    return _read_stat_cpu_ticks(f"/proc/{pid}/stat")


def read_main_thread_cpu_ticks(pid):
    """CPU ticks for valkey's main thread only.

    The main thread's tid equals the pid. This is the key measurement for
    attributing a throughput ceiling: the main thread handles RESP parsing,
    command dispatch and reply serialisation, which is roughly constant work
    per query regardless of corpus size, whereas the reader threads do the
    actual search whose cost grows with N and dim. If the main thread is
    pinned near 100% while total CPU is far below the reader-thread target,
    the benchmark is measuring the main thread's ceiling and any pre-filter
    vs inline comparison from it is meaningless.
    """
    return _read_stat_cpu_ticks(f"/proc/{pid}/task/{pid}/stat")


def to_vector_bytes(values):
    return struct.pack(f"{len(values)}f", *values)


def generate_vectors(rng, n, dim, kind):
    """Build the corpus.

    kind="clustered" (default) draws from a mixture of Gaussians. This matters
    for recall: uniform-random high-dimensional vectors are close to the worst
    case for HNSW because all pairwise distances concentrate, so graph search
    has almost no gradient to follow and measured recall is pessimistic in a
    way real embeddings are not. Real embedding corpora are clustered, so
    clustered synthetic data gives recall numbers closer to what a production
    workload would see. "uniform" is kept for comparison.
    """
    if kind == "uniform":
        return rng.uniform(-10.0, 10.0, size=(n, dim)).astype(np.float32)
    n_centroids = max(8, min(256, n // 100))
    centroids = rng.normal(0.0, 10.0, size=(n_centroids, dim))
    assign = rng.integers(0, n_centroids, size=n)
    # Within-cluster spread well below between-cluster spread, so there is real
    # neighbourhood structure for the graph to exploit.
    vecs = centroids[assign] + rng.normal(0.0, 1.0, size=(n, dim))
    return vecs.astype(np.float32)


def measure_recall(client, query, vecs, k, n_queries=20):
    """Recall of the inline path against the exact pre-filter path.

    Run serially and deterministically over the same fixed query vectors for
    both paths, rather than sampling one query from the concurrent load, so the
    number is stable and actually comparable. The pre-filter path is a
    brute-force exact scan of the qualifying set, so it is ground truth for
    "best k among the keys that pass the filter"; recall is therefore the
    fraction of those true neighbours that inline filtering also returns.
    """
    qs = [vecs[i % len(vecs)] for i in range(n_queries)]

    def run(ratio):
        set_ratio(client, ratio)
        out = []
        for q in qs:
            reply = client.execute_command(
                "FT.SEARCH", "idx", query, "PARAMS", "2", "v", q,
                "LIMIT", "0", str(k), "DIALECT", "2", "NOCONTENT")
            out.append({item.decode() if isinstance(item, bytes) else str(item)
                        for item in reply[1:]})
        return out

    exact = run("1.0")
    approx = run("0.0")
    recalls = [len(a & e) / max(len(e), 1) for a, e in zip(approx, exact)]
    return statistics.fmean(recalls) if recalls else float("nan")


def regenerate_query_vectors(n, dim, data_kind="clustered", seed=1234):
    """Reproduce the query vectors for an already-loaded corpus.

    load_dataset() seeds its RNG and derives the query set from the first 64
    generated vectors before issuing any writes, so replaying the same
    (n, dim, data_kind, seed) yields byte-identical query vectors without
    touching the server. Needed when a corpus is restored from an RDB snapshot
    rather than reloaded.
    """
    rng = np.random.default_rng(seed)
    all_vecs = generate_vectors(rng, n, dim, data_kind)
    return [all_vecs[i].tobytes() for i in range(min(64, n))]


def load_dataset(client, n, dim, levels, seed=1234, batch=200,
                 data_kind="clustered"):
    """Load n hash docs with vector + numeric + tag + text fields.

    Selectivity control: key i carries the marker for every level L where
    i < L*n. Markers are nested, so numeric [0, S-1], tag {sL} and text sL all
    select exactly the same S keys. That keeps AND/OR combinations at exactly
    S qualifying docs, so a composed query's true selectivity is still exact.

    Writes are issued SERIALLY, not pipelined, and that is deliberate.
    valkey-search blocks the issuing client for every mutation
    (ShouldBlockClient in src/index_schema.cc returns true for any real user
    client outside MULTI/EXEC) and unblocks it once a writer thread has
    indexed the record. With a deep pipeline this stalls permanently: the
    server consumes the whole batch into the client's query buffer, executes
    the first few HSETs, and after a block/unblock cycle never resumes
    processing the rest of that buffer. Observed directly: client qbuf=548416
    with idle=98s, flags=N, search_writer_queue_size:0, and the main thread
    parked in epoll_wait. No further data arrives (the client is waiting for
    replies), so epoll never fires again and the load hangs forever.
    Serial writes keep at most one command in flight, so the buffer is empty
    at each block/unblock and the stall cannot occur.
    """
    rng = np.random.default_rng(seed)
    log(f"creating index: n={n} dim={dim}")
    client.execute_command("FLUSHALL")
    client.execute_command(
        "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
        "SCHEMA",
        "vec", "VECTOR", "HNSW", "10",
        "DIM", str(dim), "DISTANCE_METRIC", "L2", "TYPE", "FLOAT32",
        "M", "16", "EF_CONSTRUCTION", "200",
        "num", "NUMERIC",
        "tg", "TAG", "SEPARATOR", ",",
        "txt", "TEXT",
    )

    thresholds = [max(1, int(level * n)) for level in levels]
    labels = [f"s{int(round(level * 10000))}" for level in levels]

    # float32 so the bytes can be shipped straight to the index.
    all_vecs = generate_vectors(rng, n, dim, data_kind)
    query_vectors = [all_vecs[i].tobytes() for i in range(min(64, n))]

    start = time.time()
    for i in range(n):
        markers = [labels[j] for j, t in enumerate(thresholds) if i < t]
        if not markers:
            markers = ["none"]
        # "all" is carried by every document. It gives text-only composed
        # queries something exact to intersect against: because the level
        # markers are nested, "@txt:sL @txt:all" selects exactly the same S
        # docs as "@txt:sL" alone, while still forcing the text layer to
        # intersect a selective postings list with an N-long one. That is a
        # realistic text access pattern and keeps selectivity exact.
        client.hset(f"doc:{i}", mapping={
            "vec": all_vecs[i].tobytes(),
            "num": str(i),
            "tg": ",".join(markers),
            "txt": " ".join(markers + ["all"]),
        })
        if (i + 1) % 5000 == 0:
            el = time.time() - start
            log(f"  loaded {i + 1}/{n} ({el:.0f}s, "
                f"{el / (i + 1) * 1000:.2f} ms/doc)")
    log(f"  writes issued in {time.time() - start:.0f}s; waiting for indexing")

    deadline = time.time() + 900
    while time.time() < deadline:
        info = client.execute_command("FT.INFO", "idx")
        info = {
            (k.decode() if isinstance(k, bytes) else k): v
            for k, v in zip(info[::2], info[1::2])
        }
        num_docs = int(info.get("num_docs", 0))
        backfill = info.get("backfill_in_progress")
        backfill = int(backfill) if backfill is not None else 0
        if num_docs >= n and backfill == 0:
            log(f"indexed {num_docs} docs")
            return query_vectors
        time.sleep(1)
    raise RuntimeError("indexing did not complete in 900s")


def run_load(port, query, vecs, k, clients, duration, admin_client, pid):
    """Drive `clients` concurrent connections for `duration` seconds.

    Returns a dict with throughput, latency percentiles, result keys, peak
    query queue depth and server CPU utilisation over the window.
    """
    stop = threading.Event()
    per_thread_lat = [[] for _ in range(clients)]
    counts = [0] * clients
    sample_keys = [None]
    errors = []

    def worker(idx):
        try:
            conn = valkey.Valkey(host="127.0.0.1", port=port,
                                 socket_timeout=300)
            i = 0
            while not stop.is_set():
                # vecs are already float32 bytes, ready to ship.
                vec_bytes = vecs[(idx + i) % len(vecs)]
                start = time.perf_counter()
                reply = conn.execute_command(
                    "FT.SEARCH", "idx", query,
                    "PARAMS", "2", "v", vec_bytes,
                    "LIMIT", "0", str(k),
                    "DIALECT", "2", "NOCONTENT",
                )
                per_thread_lat[idx].append(
                    (time.perf_counter() - start) * 1000.0)
                counts[idx] += 1
                if idx == 0 and i == 0:
                    found = [
                        item.decode() if isinstance(item, bytes) else str(item)
                        for item in reply[1:]
                    ]
                    sample_keys[0] = set(found)
                i += 1
            conn.close()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,), daemon=True)
               for i in range(clients)]
    cpu_before = read_proc_cpu_ticks(pid)
    main_before = read_main_thread_cpu_ticks(pid)
    started = time.perf_counter()
    for t in threads:
        t.start()

    # Sample queue depth mid-run as saturation evidence.
    max_queue = 0.0
    sample_deadline = started + duration
    while time.perf_counter() < sample_deadline:
        time.sleep(max(0.2, duration / 10.0))
        depth = read_queue_depth(admin_client)
        if depth is not None:
            max_queue = max(max_queue, depth)

    stop.set()
    for t in threads:
        t.join(timeout=120)
    elapsed = time.perf_counter() - started
    cpu_after = read_proc_cpu_ticks(pid)
    main_after = read_main_thread_cpu_ticks(pid)

    if errors:
        raise errors[0]

    def pct_of(before, after):
        if before is None or after is None or elapsed <= 0:
            return float("nan")
        return ((after - before) / CLK_TCK) / elapsed * 100.0

    cpu_pct = pct_of(cpu_before, cpu_after)
    main_cpu_pct = pct_of(main_before, main_after)

    all_lat = [x for sub in per_thread_lat for x in sub]
    total = sum(counts)
    if not all_lat:
        return {"qps": 0.0, "p50": float("nan"), "p90": float("nan"),
                "p99": float("nan"), "mean": float("nan"), "keys": set(),
                "max_queue": max_queue, "cpu_pct": cpu_pct,
                "main_cpu_pct": main_cpu_pct, "count": 0}
    all_lat.sort()

    def pct(p):
        return all_lat[min(len(all_lat) - 1, int(p / 100.0 * len(all_lat)))]

    return {
        "qps": total / elapsed,
        "p50": statistics.median(all_lat),
        "p90": pct(90),
        "p99": pct(99),
        "mean": sum(all_lat) / len(all_lat),
        "keys": sample_keys[0] or set(),
        "max_queue": max_queue,
        "cpu_pct": cpu_pct,
        "main_cpu_pct": main_cpu_pct,
        "count": total,
    }


def classify_bottleneck(inl, pre, sat_target):
    """Attribute the throughput ceiling so invalid rows are visible.

    valkey's main thread does roughly constant work per query (RESP parse,
    dispatch, reply serialise) while the reader threads do the search, whose
    cost grows with N and dim. So:

      main-bound   : main thread near 100%. The main thread is the ceiling and
                     both paths get the same qps regardless of search cost, so
                     the pre-filter vs inline comparison here is meaningless.
      reader-bound : total CPU near the reader-thread target. Valid comparison.
      client-bound : neither saturated; the load generator is the limit and the
                     comparison understates any difference.
    """
    main = max(inl.get("main_cpu_pct", 0) or 0, pre.get("main_cpu_pct", 0) or 0)
    total = max(inl.get("cpu_pct", 0) or 0, pre.get("cpu_pct", 0) or 0)
    if main >= 85.0:
        return "MAIN-BOUND"
    if total >= 0.75 * sat_target:
        return "reader-bound"
    return "client-bound"


QUERY_SHAPES = [
    ("numeric", "@num:[0 {S}]"),
    ("tag", "@tg:{{{L}}}"),
    ("text", "@txt:{L}"),
    ("num AND tag", "@num:[0 {S}] @tg:{{{L}}}"),
    ("num AND text", "@num:[0 {S}] @txt:{L}"),
    ("tag AND text", "@tg:{{{L}}} @txt:{L}"),
    ("num AND tag AND text", "@num:[0 {S}] @tg:{{{L}}} @txt:{L}"),
    ("num OR tag", "@num:[0 {S}] | @tg:{{{L}}}"),
    ("num AND NOT tag", "@num:[0 {S}] -@tg:{{none}}"),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--module", default=".build-release/libsearch.so")
    parser.add_argument("--port", type=int, default=7399)
    parser.add_argument("--n", type=int, default=20000)
    parser.add_argument("--dim", type=int, default=768)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--clients", type=int, default=16,
                        help="concurrent client connections offering load")
    parser.add_argument("--reader-threads", type=int, default=4)
    parser.add_argument("--writer-threads", type=int, default=4)
    parser.add_argument("--duration", type=float, default=4.0,
                        help="seconds of load per measurement point")
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--selectivities",
                        default="0.01,0.05,0.10,0.20,0.30,0.40")
    parser.add_argument("--shapes", default="",
                        help="comma-separated subset of query shape names")
    parser.add_argument("--data", choices=("clustered", "uniform"),
                        default="clustered",
                        help="corpus distribution; uniform high-dim data is "
                             "adversarial for HNSW recall")
    parser.add_argument("--recall-queries", type=int, default=20)
    parser.add_argument("--workdir", default="/tmp/prefilter_e2e")
    parser.add_argument("--out", default="prefilter-crossover-e2e-results.md")
    args = parser.parse_args()

    module_path = os.path.abspath(args.module)
    if not os.path.exists(module_path):
        sys.exit(f"module not found: {module_path}")

    levels = [float(x) for x in args.selectivities.split(",") if x]
    shapes = QUERY_SHAPES
    if args.shapes:
        wanted = {s.strip() for s in args.shapes.split(",") if s.strip()}
        shapes = [s for s in QUERY_SHAPES if s[0] in wanted]
        if not shapes:
            sys.exit(f"no query shapes matched {wanted}")

    handle, admin = start_server(module_path, args.port, args.workdir,
                                 args.reader_threads, args.writer_threads)
    rows = []
    try:
        set_ratio(admin, FORCE_INLINE)
        log(f"{RATIO_CONFIG} is settable; debug-mode confirmed")

        vecs = load_dataset(admin, args.n, args.dim, levels,
                            data_kind=args.data)
        log(f"corpus distribution: {args.data}")

        ncores = os.cpu_count() or 1
        pid = handle.proc.pid
        sat_target = min(args.reader_threads, ncores) * 100.0
        log(f"\nload: {args.clients} clients, {args.reader_threads} reader "
            f"threads, {args.duration}s per point, {ncores} cores")
        log(f"saturation target: server CPU near {sat_target:.0f}% "
            f"({args.reader_threads} reader threads busy)")
        header = (f"{'query shape':<22} {'select':>7} {'qual':>7} "
                  f"{'in_qps':>8} {'pre_qps':>8} {'gain':>7} "
                  f"{'in_p50':>8} {'pre_p50':>8} {'in_p99':>8} {'pre_p99':>8} "
                  f"{'in_cpu':>7} {'pre_cpu':>7} {'in_main':>8} "
                  f"{'pre_main':>8} {'bound':>12} {'winner':>10} "
                  f"{'in_recall':>9}")
        log("\n" + header)
        log("-" * len(header))

        for name, template in shapes:
            for level in levels:
                qualified = max(1, int(level * args.n))
                label = f"s{int(round(level * 10000))}"
                filt = template.format(S=qualified - 1, L=label)
                query = f"({filt})=>[KNN {args.k} @vec $v AS score]"

                set_ratio(admin, FORCE_INLINE)
                run_load(args.port, query, vecs, args.k, args.clients,
                         args.warmup, admin, pid)
                inl = run_load(args.port, query, vecs, args.k, args.clients,
                               args.duration, admin, pid)

                set_ratio(admin, FORCE_PREFILTER)
                run_load(args.port, query, vecs, args.k, args.clients,
                         args.warmup, admin, pid)
                pre = run_load(args.port, query, vecs, args.k, args.clients,
                               args.duration, admin, pid)

                gain = pre["qps"] / inl["qps"] if inl["qps"] else float("nan")
                # Deterministic recall over fixed query vectors, run serially
                # outside the load. The pre-filter path is an exact scan of the
                # qualifying set, so it is ground truth here.
                agree = measure_recall(admin, query, vecs, args.k,
                                       args.recall_queries)
                winner = "prefilter" if gain > 1.0 else "inline"
                bound = classify_bottleneck(inl, pre, sat_target)

                log(f"{name:<22} {level*100:6.2f}% {qualified:7d} "
                    f"{inl['qps']:8.1f} {pre['qps']:8.1f} {gain:6.2f}x "
                    f"{inl['p50']:8.2f} {pre['p50']:8.2f} "
                    f"{inl['p99']:8.2f} {pre['p99']:8.2f} "
                    f"{inl['cpu_pct']:6.0f}% {pre['cpu_pct']:6.0f}% "
                    f"{inl['main_cpu_pct']:7.0f}% {pre['main_cpu_pct']:7.0f}% "
                    f"{bound:>12} {winner:>10} {agree:9.3f}")
                rows.append({
                    "shape": name, "query": query, "selectivity": level,
                    "qualified": qualified,
                    "inline_qps": inl["qps"], "prefilter_qps": pre["qps"],
                    "throughput_gain": gain,
                    "inline_p50_ms": inl["p50"], "prefilter_p50_ms": pre["p50"],
                    "inline_p90_ms": inl["p90"], "prefilter_p90_ms": pre["p90"],
                    "inline_p99_ms": inl["p99"], "prefilter_p99_ms": pre["p99"],
                    "inline_mean_ms": inl["mean"],
                    "prefilter_mean_ms": pre["mean"],
                    "inline_cpu_pct": inl["cpu_pct"],
                    "prefilter_cpu_pct": pre["cpu_pct"],
                    "inline_max_queue": inl["max_queue"],
                    "prefilter_max_queue": pre["max_queue"],
                    "inline_main_cpu_pct": inl["main_cpu_pct"],
                    "prefilter_main_cpu_pct": pre["main_cpu_pct"],
                    "bottleneck": bound,
                    "winner": winner, "agreement": agree,
                    "clients": args.clients,
                    "reader_threads": args.reader_threads,
                    "saturation_target_cpu_pct": sat_target,
                })
    finally:
        handle.stop()
        log("server stopped")

    with open(args.out, "w") as fh:
        fh.write("# End-to-end pre-filter vs inline-filter crossover "
                 "(saturating load)\n\n")
        fh.write(f"- N = {args.n}, dim = {args.dim}, k = {args.k}\n")
        fh.write(f"- {args.clients} concurrent clients, "
                 f"{args.reader_threads} reader threads, "
                 f"{args.writer_threads} writer threads, "
                 f"{os.cpu_count()} cores\n")
        fh.write(f"- {args.duration}s of load per point, "
                 f"{args.warmup}s warmup\n")
        fh.write("- Full stack: real valkey-server + module, RESP clients, "
                 "real planner / entries fetchers / dedup / reply gen\n")
        fh.write(f"- Paths forced via `{RATIO_CONFIG}` = 0.0 (inline) "
                 "vs 1.0 (pre-filter)\n")
        fh.write("- `gain` = prefilter_qps / inline_qps; above 1.0 favours "
                 "pre-filtering\n")
        fh.write(f"- corpus distribution: {args.data}\n")
        fh.write(f"- `in_recall` = mean recall of the approximate inline path "
                 f"against the exact pre-filter result, over "
                 f"{args.recall_queries} fixed query vectors run serially. "
                 f"Pre-filter is a brute-force exact scan of the qualifying "
                 f"set, so it is ground truth. Values below 1.0 mean inline "
                 f"is returning worse neighbours, so its higher throughput is "
                 f"partly bought with answer quality.\n")
        fh.write("- `cpu` = server process CPU over the window. Saturation "
                 f"target is ~{min(args.reader_threads, os.cpu_count() or 1)*100:.0f}% "
                 f"({args.reader_threads} reader threads fully busy). Values "
                 "near that target mean the measurement is throughput-bound "
                 "rather than client-bound.\n\n")
        fh.write("| query shape | selectivity | qualified | inline qps | "
                 "prefilter qps | gain | in p50 | pre p50 | in p90 | pre p90 "
                 "| in p99 | pre p99 | in cpu | pre cpu | bound | winner "
                 "| in_recall |\n")
        # 17 columns: shape, select, qual, in/pre qps, gain, in/pre p50,
        # in/pre p90, in/pre p99, in/pre cpu, bound, winner, in_recall
        fh.write("|" + "---|" * 17 + "\n")
        for r in rows:
            fh.write(
                f"| {r['shape']} | {r['selectivity']*100:.2f}% | "
                f"{r['qualified']} | {r['inline_qps']:.1f} | "
                f"{r['prefilter_qps']:.1f} | {r['throughput_gain']:.2f}x | "
                f"{r['inline_p50_ms']:.2f} | {r['prefilter_p50_ms']:.2f} | "
                f"{r['inline_p90_ms']:.2f} | {r['prefilter_p90_ms']:.2f} | "
                f"{r['inline_p99_ms']:.2f} | {r['prefilter_p99_ms']:.2f} | "
                f"{r['inline_cpu_pct']:.0f}% | {r['prefilter_cpu_pct']:.0f}% | "
                f"{r['bottleneck']} | "
                f"{r['winner']} | {r['agreement']:.3f} |\n")
        fh.write("\nAll latencies are client-observed milliseconds under "
                 "concurrent load, so they include queueing delay and are "
                 "expected to be higher than single-query latency.\n")
        fh.write("\n## Crossover by query shape\n\n")
        fh.write("| query shape | last selectivity where prefilter wins | "
                 "first where inline wins |\n|---|---|---|\n")
        for name, _ in shapes:
            shape_rows = [r for r in rows if r["shape"] == name]
            pre_win = [r["selectivity"] for r in shape_rows
                       if r["winner"] == "prefilter"]
            in_win = [r["selectivity"] for r in shape_rows
                      if r["winner"] == "inline"]
            last_pre = f"{max(pre_win)*100:.2f}%" if pre_win else "none"
            first_in = f"{min(in_win)*100:.2f}%" if in_win else "none"
            fh.write(f"| {name} | {last_pre} | {first_in} |\n")

    with open(os.path.splitext(args.out)[0] + ".json", "w") as fh:
        json.dump(rows, fh, indent=2)
    log(f"\nwrote {args.out} and .json")


if __name__ == "__main__":
    main()

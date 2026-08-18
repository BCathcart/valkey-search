#!/usr/bin/env python3
"""Diagnostic: bisect which stage of the E2E dataset load hangs.

Times FT.CREATE and then HSET batches of increasing size, printing elapsed
time per batch so a hang is attributable to a specific stage rather than to
"the load" as a whole. Run standalone against an already-running server.
"""
import os
import struct
import subprocess
import sys
import time

import numpy as np
import valkey

PORT = int(os.environ.get("DIAG_PORT", "7799"))
MODULE = os.environ.get(
    "DIAG_MODULE",
    "/local/home/brenncat/oss-workplace/valkey-search-main-repo-3"
    "/.build-release/libsearch.so")
WORKDIR = "/tmp/diag_load"
DIM = int(os.environ.get("DIAG_DIM", "768"))
N = int(os.environ.get("DIAG_N", "2000"))
SCHEMA = os.environ.get("DIAG_SCHEMA", "full")  # full | vec | vecnum


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def start_server():
    os.makedirs(WORKDIR, exist_ok=True)
    conf = os.path.join(WORKDIR, "server.conf")
    log_path = os.path.join(WORKDIR, "server.log")
    open(log_path, "w").close()
    with open(conf, "w") as fh:
        fh.write(f"port {PORT}\n")
        fh.write("save ''\n")
        fh.write("appendonly no\n")
        fh.write("enable-debug-command yes\n")
        fh.write(f"dir {WORKDIR}\n")
        fh.write(f"logfile {log_path}\n")
        fh.write(f"loadmodule {MODULE} --debug-mode yes "
                 f"--reader-threads 4 --writer-threads 4\n")
    stderr = open(os.path.join(WORKDIR, "server.stderr"), "w")
    proc = subprocess.Popen(["valkey-server", conf],
                            stdout=subprocess.DEVNULL, stderr=stderr)
    for _ in range(100):
        try:
            c = valkey.Valkey(host="127.0.0.1", port=PORT, socket_timeout=5)
            if c.ping():
                log("server up")
                return proc
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server did not start")


def main():
    proc = start_server()
    try:
        c = valkey.Valkey(host="127.0.0.1", port=PORT,
                          socket_timeout=int(os.environ.get(
                              "DIAG_TIMEOUT", "30")))
        c.execute_command("FLUSHALL")

        schema = ["vec", "VECTOR", "HNSW", "10", "DIM", str(DIM),
                  "DISTANCE_METRIC", "L2", "TYPE", "FLOAT32",
                  "M", "16", "EF_CONSTRUCTION", "200"]
        if SCHEMA in ("vecnum", "full"):
            schema += ["num", "NUMERIC"]
        if SCHEMA == "full":
            schema += ["tg", "TAG", "SEPARATOR", ",", "txt", "TEXT"]

        t = time.time()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1",
                          "doc:", "SCHEMA", *schema)
        log(f"FT.CREATE ({SCHEMA}) ok in {time.time()-t:.2f}s")

        rng = np.random.default_rng(7)
        vecs = rng.uniform(-10, 10, size=(N, DIM)).astype(np.float32)

        # Stage 1: single HSET, unpipelined, to time one ingestion.
        t = time.time()
        c.hset("doc:0", mapping={"vec": vecs[0].tobytes(), "num": "0",
                                 "tg": "s10", "txt": "s10"})
        log(f"single HSET ok in {time.time()-t:.3f}s")

        # Stage 2: 10 unpipelined HSETs.
        t = time.time()
        for i in range(1, 11):
            c.hset(f"doc:{i}", mapping={"vec": vecs[i].tobytes(),
                                        "num": str(i), "tg": "s10",
                                        "txt": "s10"})
        log(f"10 serial HSETs ok in {time.time()-t:.3f}s "
            f"({(time.time()-t)/10*1000:.1f} ms each)")

        # Stage 3: full sequential load with progress, to find where (if
        # anywhere) per-doc cost degrades non-linearly as the index grows.
        idx = 11
        batch = 200
        t0 = time.time()
        last = t0
        pipe = c.pipeline(transaction=False)
        pending = 0
        for i in range(idx, N):
            pipe.hset(f"doc:{i}", mapping={"vec": vecs[i].tobytes(),
                                           "num": str(i), "tg": "s10",
                                           "txt": "s10"})
            pending += 1
            if pending >= batch:
                bt = time.time()
                pipe.execute()
                bel = time.time() - bt
                pipe = c.pipeline(transaction=False)
                pending = 0
                if (i + 1) % 1000 < batch or bel > 2.0:
                    log(f"  at doc {i+1}/{N}: batch took {bel:.2f}s "
                        f"({bel/batch*1000:.2f} ms/doc), "
                        f"cum {time.time()-t0:.1f}s")
                    last = time.time()
        if pending:
            pipe.execute()
        log(f"full load of {N} docs issued in {time.time()-t0:.1f}s")
        idx = N

        # Stage 4: how far behind is indexing?
        for _ in range(30):
            info = c.execute_command("FT.INFO", "idx")
            info = {(k.decode() if isinstance(k, bytes) else k): v
                    for k, v in zip(info[::2], info[1::2])}
            nd = int(info.get("num_docs", 0))
            log(f"FT.INFO num_docs={nd} dbsize={c.dbsize()}")
            if nd >= idx:
                break
            time.sleep(1)
        log("DIAG COMPLETE")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())

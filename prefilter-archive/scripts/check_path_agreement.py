#!/usr/bin/env python3
"""Correctness check: compare inline vs pre-filter FT.SEARCH against numpy
ground truth.

The E2E sweep reported zero overlap between the two paths at high filter
selectivity, while the in-process micro-benchmark measured recall ~1.0 for the
same comparison. One of them must be wrong. This computes the true top-k over
the qualifying subset with numpy and checks each path against it, so the
disagreement is attributed rather than guessed at.
"""
import os
import subprocess
import sys
import time

import numpy as np
import valkey

PORT = int(os.environ.get("CHK_PORT", "7801"))
MODULE = os.environ.get(
    "CHK_MODULE",
    "/local/home/brenncat/oss-workplace/valkey-search-main-repo-3"
    "/.build-release/libsearch.so")
WORKDIR = "/tmp/chk_correct"
N = int(os.environ.get("CHK_N", "2000"))
DIM = int(os.environ.get("CHK_DIM", "32"))
K = int(os.environ.get("CHK_K", "10"))
RATIO_CONFIG = "search.prefiltering-threshold-ratio"


def log(m):
    print(m, flush=True)


def start_server():
    os.makedirs(WORKDIR, exist_ok=True)
    conf = os.path.join(WORKDIR, "server.conf")
    logp = os.path.join(WORKDIR, "server.log")
    open(logp, "w").close()
    with open(conf, "w") as fh:
        fh.write(f"port {PORT}\nsave ''\nappendonly no\n")
        fh.write(f"dir {WORKDIR}\nlogfile {logp}\n")
        fh.write(f"loadmodule {MODULE} --debug-mode yes "
                 f"--reader-threads 2 --writer-threads 2\n")
    proc = subprocess.Popen(["valkey-server", conf],
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = valkey.Valkey(host="127.0.0.1", port=PORT, socket_timeout=60)
            if c.ping():
                return proc, c
        except Exception:
            time.sleep(0.2)
    raise RuntimeError("server did not start")


def search(c, query, qvec, k, with_scores=True):
    args = ["FT.SEARCH", "idx", query, "PARAMS", "2", "v", qvec.tobytes(),
            "LIMIT", "0", str(k), "DIALECT", "2"]
    if with_scores:
        args += ["RETURN", "1", "dist"]
    else:
        args += ["NOCONTENT"]
    reply = c.execute_command(*args)
    out = []
    i = 1
    while i < len(reply):
        key = reply[i]
        key = key.decode() if isinstance(key, bytes) else str(key)
        dist = None
        if with_scores and i + 1 < len(reply) and isinstance(reply[i + 1], list):
            fields = reply[i + 1]
            for j in range(0, len(fields) - 1, 2):
                name = fields[j]
                name = name.decode() if isinstance(name, bytes) else name
                if name == "dist":
                    dist = float(fields[j + 1])
            i += 2
        else:
            i += 1
        out.append((key, dist))
    return out


def main():
    proc, c = start_server()
    try:
        c.execute_command("FLUSHALL")
        c.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:", "SCHEMA",
            "vec", "VECTOR", "HNSW", "10", "DIM", str(DIM),
            "DISTANCE_METRIC", "L2", "TYPE", "FLOAT32",
            "M", "16", "EF_CONSTRUCTION", "200",
            "num", "NUMERIC")
        rng = np.random.default_rng(11)
        vecs = rng.uniform(-10, 10, size=(N, DIM)).astype(np.float32)
        # Serial writes: pipelined mutations stall (see load_dataset docstring).
        for i in range(N):
            c.hset(f"doc:{i}", mapping={"vec": vecs[i].tobytes(),
                                        "num": str(i)})
        for _ in range(60):
            info = c.execute_command("FT.INFO", "idx")
            info = {(k.decode() if isinstance(k, bytes) else k): v
                    for k, v in zip(info[::2], info[1::2])}
            if int(info.get("num_docs", 0)) >= N:
                break
            time.sleep(0.5)
        log(f"loaded/indexed {N} docs, dim={DIM}")

        qvec = vecs[0].copy()
        ok = True
        for sel in (0.05, 0.20, 0.40):
            S = int(sel * N)              # qualifying keys are 0..S-1
            query = f"@num:[0 {S - 1}]=>[KNN {K} @vec $v AS dist]"

            # numpy ground truth over the qualifying subset only
            sub = vecs[:S].astype(np.float64)
            d = ((sub - qvec.astype(np.float64)) ** 2).sum(axis=1)
            truth = [f"doc:{i}" for i in np.argsort(d)[:K]]
            truth_set = set(truth)

            c.execute_command("CONFIG", "SET", RATIO_CONFIG, "0.0")
            inline = search(c, query, qvec, K)
            c.execute_command("CONFIG", "SET", RATIO_CONFIG, "1.0")
            pre = search(c, query, qvec, K)

            iset = {k for k, _ in inline}
            pset = {k for k, _ in pre}
            r_in = len(iset & truth_set) / K
            r_pre = len(pset & truth_set) / K
            overlap = len(iset & pset) / K
            max_qual_in = max((int(k.split(":")[1]) for k in iset), default=-1)
            max_qual_pre = max((int(k.split(":")[1]) for k in pset), default=-1)
            log(f"\nselectivity {sel:.0%}  (qualifying = doc:0..doc:{S-1})")
            log(f"  truth : {truth[:5]} ...")
            log(f"  inline: {[k for k,_ in inline][:5]} ... "
                f"recall={r_in:.2f} max_id={max_qual_in} n={len(iset)}")
            log(f"  prefil: {[k for k,_ in pre][:5]} ... "
                f"recall={r_pre:.2f} max_id={max_qual_pre} n={len(pset)}")
            log(f"  inline-vs-prefilter overlap = {overlap:.2f}")

            # Key overlap is only a meaningful quality metric if the top-k
            # distances are actually distinguishable. With high-dimensional
            # uniform-random vectors distances concentrate, so two correct
            # answers can share no keys while being equally good. Compare the
            # distance profiles to tell "wrong" apart from "equally good".
            d_truth = np.sort(d)[:K]
            d_in = sorted(x for _, x in inline if x is not None)
            d_pre = sorted(x for _, x in pre if x is not None)
            if d_in and d_pre:
                spread = (d_truth[-1] - d_truth[0]) / max(d_truth[0], 1e-9)
                log(f"  truth  d[0]={d_truth[0]:.4f} d[k-1]={d_truth[-1]:.4f} "
                    f"(spread {spread*100:.2f}% of d[0])")
                log(f"  inline d[0]={d_in[0]:.4f} d[k-1]={d_in[-1]:.4f}")
                log(f"  prefil d[0]={d_pre[0]:.4f} d[k-1]={d_pre[-1]:.4f}")
                excess = (d_in[-1] - d_pre[-1]) / max(d_pre[-1], 1e-9)
                log(f"  inline worst-neighbour excess distance = "
                    f"{excess*100:+.3f}%")
            if max_qual_in >= S or max_qual_pre >= S:
                log("  !! FILTER VIOLATION: returned a non-qualifying key")
                ok = False
            if r_pre < 0.999:
                log("  !! pre-filter is not exact, which it should be")
                ok = False
            if r_in < 0.5:
                log("  !! inline recall is very low")
                ok = False
        log("\nRESULT: " + ("consistent" if ok else "PROBLEM DETECTED"))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    sys.exit(main())

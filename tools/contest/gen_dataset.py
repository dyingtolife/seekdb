#!/usr/bin/env python3
"""Deterministic float32/L2 dataset + ground truth generator for the AMD CAGRA track.

The contest fixes the evaluation contract to: float32 input, L2 distance,
``type=hnsw, lib=cuvs``.  This generator produces exactly that contract so the
same data can be replayed against the frozen baseline and against a candidate
build, on the CPU (``lib=vsag``) and on the GPU (``lib=cuvs``).

Outputs (little-endian, row major):
    base.f32      n  x dim   float32  database vectors
    query.f32     nq x dim   float32  probe vectors
    gt10.i32      nq x 10    int32    exact L2 top-10 row offsets
    gt100.i32     nq x 100   int32    exact L2 top-100 row offsets
    meta.json     dataset parameters + checksums

Usage:
    python3 gen_dataset.py --out data/n200k_d128 --n 200000 --dim 128
"""

import argparse
import hashlib
import json
import pathlib
import sys

try:
    import numpy as np
except ImportError:  # pragma: no cover
    sys.exit("numpy is required: pip install numpy")


def build_base(n, dim, dist, clusters, seed):
    rng = np.random.default_rng(seed)
    if dist == "uniform":
        base = rng.uniform(-1.0, 1.0, size=(n, dim)).astype(np.float32)
    elif dist == "gauss":
        base = rng.normal(0.0, 1.0, size=(n, dim)).astype(np.float32)
    else:  # clustered: closer to real embeddings, harder for graph indexes
        centers = rng.normal(0.0, 1.0, size=(clusters, dim)).astype(np.float32)
        pick = rng.integers(0, clusters, size=n)
        base = centers[pick] + rng.normal(0.0, 0.18, size=(n, dim)).astype(np.float32)
    return np.ascontiguousarray(base, dtype=np.float32)


def build_queries(base, nq, dim, seed):
    rng = np.random.default_rng(seed + 1)
    idx = rng.choice(base.shape[0], size=nq, replace=False)
    # perturb real rows: the probe distribution stays inside the data manifold
    probes = base[idx] + rng.normal(0.0, 0.05, size=(nq, dim)).astype(np.float32)
    return np.ascontiguousarray(probes, dtype=np.float32)


def brute_force_topk(base, query, k, block=4096):
    """Exact L2 top-k row offsets, computed blockwise to bound host memory."""
    nq = query.shape[0]
    best_idx = np.full((nq, k), -1, dtype=np.int32)
    best_dist = np.full((nq, k), np.inf, dtype=np.float32)
    for start in range(0, base.shape[0], block):
        chunk = base[start:start + block]
        # ||x||^2 - 2 q.x ; the constant ||q||^2 does not change the ranking
        d = np.sum(chunk * chunk, axis=1)[None, :] - 2.0 * (query @ chunk.T)
        take = min(k, chunk.shape[0])
        local = np.argpartition(d, kth=take - 1, axis=1)[:, :take]
        cand_dist = np.take_along_axis(d, local, axis=1)
        cand_idx = (local + start).astype(np.int32)
        merged_dist = np.concatenate([best_dist, cand_dist], axis=1)
        merged_idx = np.concatenate([best_idx, cand_idx], axis=1)
        order = np.argsort(merged_dist, axis=1)[:, :k]
        best_dist = np.take_along_axis(merged_dist, order, axis=1)
        best_idx = np.take_along_axis(merged_idx, order, axis=1)
    return best_idx


def sha256(path, limit=None):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        remaining = limit
        while True:
            if remaining is not None and remaining <= 0:
                break
            chunk = fh.read(1 << 20 if remaining is None else min(1 << 20, remaining))
            if not chunk:
                break
            h.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=200000)
    ap.add_argument("--dim", type=int, default=128)
    ap.add_argument("--nq", type=int, default=1000)
    ap.add_argument("--dist", default="cluster", choices=["uniform", "gauss", "cluster"])
    ap.add_argument("--clusters", type=int, default=256)
    ap.add_argument("--seed", type=int, default=20261028)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    meta_path = out / "meta.json"
    if meta_path.exists() and not args.force:
        print(f"[gen] {out} already populated (use --force to regenerate)")
        print(meta_path.read_text())
        return 0

    print(f"[gen] n={args.n} dim={args.dim} nq={args.nq} dist={args.dist} seed={args.seed}")
    base = build_base(args.n, args.dim, args.dist, args.clusters, args.seed)
    query = build_queries(base, args.nq, args.dim, args.seed)
    print("[gen] computing exact ground truth (numpy, float32 L2)")
    gt100 = brute_force_topk(base, query, 100)
    gt10 = np.ascontiguousarray(gt100[:, :10], dtype=np.int32)

    base.tofile(out / "base.f32")
    query.tofile(out / "query.f32")
    gt10.astype("<i4").tofile(out / "gt10.i32")
    gt100.astype("<i4").tofile(out / "gt100.i32")

    meta = {
        "n": args.n,
        "dim": args.dim,
        "nq": args.nq,
        "dist": args.dist,
        "clusters": args.clusters,
        "seed": args.seed,
        "dtype": "float32",
        "metric": "l2",
        "files": {
            "base.f32": sha256(out / "base.f32"),
            "query.f32": sha256(out / "query.f32"),
            "gt10.i32": sha256(out / "gt10.i32"),
            "gt100.i32": sha256(out / "gt100.i32"),
        },
    }
    meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n")
    print(f"[gen] done -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

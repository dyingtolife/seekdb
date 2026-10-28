# Contest optimisation notes (AMD track)

Branch `amd-cagra-submit` on top of tag `amd-cagra-contest-baseline-v5`
(commit `634888d9b037153fa143d2c913ba4b25503e1d62`).  Two changes, each
independently switchable, no change to the SQL surface, the FP32/L2 contract or
the `lib=vsag` CPU path.

## 1. Reuse per-index device scratch (`tools/hipvs_bridge/seekdb_cuvs_bridge.c`)

The baseline allocated and freed three device buffers (`nq x dim` queries,
`nq x topk` ids, `nq x topk` distances) around every `knn_search`.  That is three
`cuvsRMMAlloc` + three `cuvsRMMFree` per query, i.e. fixed overhead on top of the
GPU work for single-row queries.

Change: each index keeps 8 scratch slots and reuses the largest buffers it has
already allocated.  Slots are handed out under a per-index mutex, so concurrent
clients keep running in parallel; if all slots are busy the search allocates a
private scratch and still runs on the GPU (never serialises, never falls back).

Also added (`OB_CUVS_LOG=0` silences it):

```text
[seekdb_cuvs] build n=... dim=... ms=...      # GPU build wall time
[seekdb_cuvs] search nq=... topk=... ms=...   # GPU search wall time
```

These lines are what the benchmarking harness uses to attribute time to the GPU
stage and to prove the CAGRA graph was really built on the GPU inside the timed
window.

Optional build knobs (defaults reproduce the v5 graph exactly):

| variable | default | meaning |
| --- | --- | --- |
| `OB_CUVS_NN_ITER` | 100 (v5) | nn-descent iterations |
| `OB_CUVS_GRAPH_DEGREE` | 128 (v5) | final CAGRA graph degree |
| `OB_CUVS_INTERMEDIATE_DEGREE` | 256 (v5) | intermediate graph degree |

## 2. Overlap the CAGRA build with the CPU load (`src/oblib/lib/vector/ob_vsag_adaptor.cpp`)

In the baseline the graph is still built at the very end of the DDL, inside
`ob_cuvs_serialize`, i.e. strictly after the CPU-bound VSAG construction has
finished.  The GPU is idle for the whole load and its build time is pure added
latency.

Change: `ob_cuvs_add` now

* reserves `buf_vecs_` / `buf_ids_` geometrically instead of relying on
  `std::vector::insert` growth (the baseline reallocates and copies the whole
  accumulated table O(log n) times), and
* starts a watcher thread that builds the CAGRA graph as soon as the buffered
  vectors stop growing (300 ms quiet, geometric growth) or when the loader is
  quiet for 500 ms, using the same 32 MB-stack pthread the baseline uses.

The build runs with `g_ob_cuvs_mu` held, so it can never observe a reallocated
buffer; the entry is re-looked-up under that lock on every step, so dropping an
index simply stops the watcher; and serving still requires
`built_n_ == buf_ids_.size()`, so a stale graph is never used for results.  When
`ob_cuvs_serialize` runs it finds a ready graph and skips the rebuild.

Runtime switches (all safe defaults):

| variable | default | meaning |
| --- | --- | --- |
| `OB_CUVS_EAGER` | `1` | `0` disables the watcher and the reservation, restoring baseline behaviour |
| `OB_CUVS_SETTLE_MS` | `300` | quiet window for a geometric-growth build |
| `OB_CUVS_FINAL_MS` | `500` | quiet window for the tail build |

## Reproduction

```bash
# bridge
tools/hipvs_bridge/build.sh /work/bridge/libseekdb_cuvs_bridge.so

# seekdb (same flags for baseline and candidate)
./build.sh release -DOB_BUILD_CUVS=ON -DOB_BUILD_CUVS_TRACE=ON \
  -DCUVS_BRIDGE_LIB=/work/bridge/libseekdb_cuvs_bridge.so --make -j64

# measurement: DDL + first query, 3 cache-cleared rounds per version, recall and
# QPS against exact ground truth -- see the harness shipped with the submission
```

The contest scoring compares the end-to-end build window (CREATE VECTOR INDEX
until the first ANN query returns) of this branch against the frozen baseline on
the same machine, dataset and index parameters.

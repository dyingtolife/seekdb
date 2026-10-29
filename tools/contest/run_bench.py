#!/usr/bin/env python3
"""Build/query benchmark driver for the AMD CAGRA track (seekdb x hipVS/cuVS).

It reproduces the contest measurement contract as closely as a local machine
allows:

  * timing window  = ``CREATE VECTOR INDEX`` + the first successful ANN query
    (original vector table already loaded, index absent, caches dropped),
  * per version    3 rounds, cache-cleared, median reported,
  * correctness     returned ids are legal, recall@10/@100 against the exact
    numpy ground truth shipped with the dataset,
  * query path     single-query QPS/P50/P95 and one-call batch QPS,
  * GPU evidence   the bridge prints ``[seekdb_cuvs] build n=.. ms=..``; the
    driver counts those lines *before* the first query, which proves whether the
    CAGRA build really happened inside the timed window and was really executed
    on the GPU (baseline: 0, eager candidate: >= 1).

Examples
--------
  # full A/B: frozen baseline vs candidate, then the CPU (lib=vsag) reference
  python3 run_bench.py --data data/n200k_d128 \
      --baseline-observer /work/base/seekdb --candidate-observer /work/cand/seekdb \
      --bridge /work/bridge/libseekdb_cuvs_bridge.so \
      --port 2981 --base-dir /tmp/seekdb-bench --out results/n200k.json

  # only re-measure the candidate
  python3 run_bench.py --data data/n200k_d128 --variants candidate ...

The output JSON is consumed by ``report.py``.
"""

import argparse
import json
import os
import pathlib
import signal
import statistics
import subprocess
import sys
import time

BRIDGE_BUILD_RE = "[seekdb_cuvs] build n="


# --------------------------------------------------------------------------
# tiny SQL client: pymysql when available (accurate per-query timing),
# otherwise the mysql CLI (QPS measured on batched statement blocks)
# --------------------------------------------------------------------------
class SqlClient:
    def __init__(self, port, db=None, user="root", password=""):
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.conn = None
        try:
            import pymysql  # noqa: F401
        except ImportError:
            self.conn = None
            return
        try:
            import pymysql
            self.conn = pymysql.connect(host="127.0.0.1", port=port, user=user,
                                        password=password, database=db,
                                        autocommit=True, connect_timeout=5)
        except Exception:
            self.conn = None

    # -- low level ---------------------------------------------------------
    def mysql_cmd(self, with_db=True):
        cmd = ["mysql", "--connect-timeout=10", "--protocol=TCP", "-h127.0.0.1",
               "-P", str(self.port), "-u", self.user, "-A", "-N", "-B"]
        if self.password:
            cmd.append(f"-p{self.password}")
        if with_db and self.db:
            cmd.append(self.db)
        return cmd

    def _cli(self, sql, with_db=True, timeout=7200):
        cmd = self.mysql_cmd(with_db)
        proc = subprocess.run(cmd, input=sql.encode(), capture_output=True,
                              timeout=timeout)
        if proc.returncode != 0:
            raise RuntimeError(
                f"mysql failed (rc={proc.returncode}): {proc.stderr.decode(errors='replace')[:800]}\n"
                f"sql: {sql[:400]}")
        return proc.stdout.decode(errors="replace")

    def execute(self, sql, timeout=7200):
        if self.conn is not None:
            with self.conn.cursor() as cur:
                cur.execute(sql)
                try:
                    return cur.fetchall()
                except Exception:
                    return []
        return self._cli(sql, timeout=timeout)

    def script(self, sql, timeout=7200):
        """Run a multi-statement script, return elapsed seconds."""
        start = time.perf_counter()
        if self.conn is not None:
            with self.conn.cursor() as cur:
                for stmt in [s for s in sql.split("\n;\n") if s.strip()]:
                    cur.execute(stmt)
        else:
            self._cli(sql, timeout=timeout)
        return time.perf_counter() - start

    def rows(self, sql, timeout=7200):
        out = self.execute(sql, timeout=timeout)
        if isinstance(out, str):
            return [line.split("\t") for line in out.splitlines() if line]
        return [["" if v is None else str(v) for v in row] for row in out]

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# observer lifecycle
# --------------------------------------------------------------------------
class Observer:
    def __init__(self, binary, port, base_dir, bridge, memory_budget="8G",
                 datafile_size="20G", cpu_count=None, extra_env=None,
                 hide_gpu=False):
        self.binary = binary
        self.port = port
        self.base_dir = pathlib.Path(base_dir)
        self.bridge = bridge
        self.memory_budget = memory_budget
        self.datafile_size = datafile_size
        self.cpu_count = cpu_count or (os.cpu_count() or 8)
        self.extra_env = extra_env or {}
        self.hide_gpu = hide_gpu
        self.proc = None
        self.log_path = self.base_dir / "observer.log"

    def start(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)
        bridge_dir = str(pathlib.Path(self.bridge).parent)
        env = dict(os.environ)
        env["LD_LIBRARY_PATH"] = (f"{bridge_dir}:/opt/hipvs/lib:/opt/rocm/lib:"
                                  + env.get("LD_LIBRARY_PATH", ""))
        env["OB_CUVS_LOG"] = "1"
        env.update(self.extra_env)
        if self.hide_gpu:
            env["HIP_VISIBLE_DEVICES"] = "-1"
            env["ROCR_VISIBLE_DEVICES"] = "-1"
        cmd = [
            self.binary, "--nodaemon", "--port", str(self.port),
            "--data-dir", str(self.base_dir / "store"),
            "--base-dir", str(self.base_dir),
            "--parameter", "__min_full_resource_pool_memory=2147483648",
            "--parameter", f"memory_budget={self.memory_budget}",
            "--parameter", f"datafile_size={self.datafile_size}",
            "--parameter", f"cpu_count={self.cpu_count}",
            "--parameter", "local_ip=127.0.0.1",
        ]
        log = open(self.log_path, "wb")
        self.proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                     env=env, start_new_session=True)
        log.close()

    def wait_ready(self, timeout=300):
        deadline = time.time() + timeout
        client = SqlClient(self.port)
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"observer exited early, see {self.log_path}")
            try:
                client.execute("select 1")
                client.close()
                return
            except Exception:
                time.sleep(1.0)
        raise RuntimeError(f"observer did not become SQL-ready in {timeout}s "
                           f"(log: {self.log_path})")

    def stop(self):
        if self.proc is None:
            return
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except Exception:
            pass
        try:
            self.proc.wait(timeout=120)
        except Exception:
            try:
                os.killpg(os.getpgid(self.proc.pid), signal.SIGKILL)
            except Exception:
                pass
        self.proc = None

    # -- bridge evidence ---------------------------------------------------
    def bridge_builds(self):
        """[(n, ms)] parsed from `[seekdb_cuvs] build n=.. dim=.. ms=..` lines."""
        out = []
        try:
            text = self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return out
        for line in text.splitlines():
            if BRIDGE_BUILD_RE not in line:
                continue
            try:
                n = int(line.split("n=")[1].split()[0])
                ms = int(line.split("ms=")[1].split()[0])
            except Exception:
                continue
            out.append((n, ms))
        return out

    def log_size(self):
        try:
            return self.log_path.stat().st_size
        except FileNotFoundError:
            return 0

    def builds_since(self, offset):
        """Count bridge builds written after byte `offset` of the log."""
        try:
            with open(self.log_path, "rb") as fh:
                fh.seek(offset)
                text = fh.read().decode(errors="replace")
        except FileNotFoundError:
            return []
        out = []
        for line in text.splitlines():
            if BRIDGE_BUILD_RE not in line:
                continue
            try:
                n = int(line.split("n=")[1].split()[0])
                ms = int(line.split("ms=")[1].split()[0])
            except Exception:
                continue
            out.append((n, ms))
        return out


# --------------------------------------------------------------------------
# data loading
# --------------------------------------------------------------------------
def vector_literal(vec, digits=7):
    return "[" + ",".join(f"{float(x):.{digits}f}" for x in vec) + "]"


def load_table(client, table, base_path, n, dim, batch_rows, label):
    """Stream INSERT batches into one mysql process (untimed, memory bounded)."""
    import numpy as np
    mm = np.memmap(base_path, dtype="<f4", mode="r", shape=(n, dim))
    print(f"[load] {label}: {n} rows x {dim} into {table} (untimed)", flush=True)
    start = time.perf_counter()
    proc = subprocess.Popen(client.mysql_cmd(), stdin=subprocess.PIPE,
                            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        proc.stdin.write(f"use {client.db};\n".encode())
        for offset in range(0, n, batch_rows):
            stop = min(offset + batch_rows, n)
            values = ",".join(
                f"({i},'{vector_literal(mm[i])}')" for i in range(offset, stop))
            proc.stdin.write(f"insert into {table} values {values};\n".encode())
            if offset and offset % (batch_rows * 20) == 0:
                print(f"[load] {label}: {offset}/{n}", flush=True)
        proc.stdin.close()
    except BrokenPipeError:
        pass
    rc = proc.wait()
    if rc != 0:
        err = proc.stderr.read().decode(errors="replace")[:800]
        raise RuntimeError(f"data load failed (rc={rc}): {err}")
    print(f"[load] {label}: {time.perf_counter() - start:.1f}s", flush=True)


# --------------------------------------------------------------------------
# measurement
# --------------------------------------------------------------------------
def run_queries(client, table, queries, topk, limit=None, approximate=True):
    """Return (list_of_id_lists, elapsed_seconds) for the SQL ANN path."""
    approx = "APPROXIMATE" if approximate else ""
    sel = [f"use {client.db};"]
    used = queries if limit is None else queries[:limit]
    for q in used:
        sel.append(f"select id from {table} order by l2_distance(v, "
                   f"'{vector_literal(q)}') {approx} limit {topk};")
    if client.conn is not None:
        start = time.perf_counter()
        got = []
        with client.conn.cursor() as cur:
            for q in used:
                cur.execute(f"select id from {table} order by l2_distance(v, "
                            f"'{vector_literal(q)}') {approx} limit {topk}")
                got.append([int(r[0]) for r in cur.fetchall()])
        return got, time.perf_counter() - start
    start = time.perf_counter()
    out = client._cli("\n".join(sel))
    elapsed = time.perf_counter() - start
    got, cur = [], []
    for line in out.splitlines():
        if not line:
            continue
        cur.append(int(line))
        if len(cur) == topk:
            got.append(cur)
            cur = []
    return got, elapsed


def recall_at(got_ids, gt_rows, k):
    hits = 0
    total = 0
    for row, truth in zip(got_ids, gt_rows):
        total += k
        hits += len(set(row[:k]) & set(int(x) for x in truth[:k]))
    return (hits / total) if total else 0.0


def measure_round(observer, client, args, lib, queries, gt10, gt100, table, round_no):
    import numpy as np
    ddl = (f"create vector index {args.index_name} on {table}(v) "
           f"with (distance=l2, type=hnsw, lib={lib}"
           f"{args.index_params})")
    try:
        client.execute(f"alter table {table} drop index {args.index_name}")
    except Exception:
        pass  # first round: the index does not exist yet
    drop_caches(args.drop_caches)

    log_offset = observer.log_size()
    t0 = time.perf_counter()
    client.execute(ddl)
    t_ddl = time.perf_counter() - t0
    builds_during_ddl = observer.builds_since(log_offset)

    # first query is part of the timed window (prevents moving lazy builds out)
    q0 = queries[0]
    t1 = time.perf_counter()
    first = client.rows(f"select id from {table} order by l2_distance(v, "
                        f"'{vector_literal(q0)}') APPROXIMATE limit 10")
    t_first = time.perf_counter() - t1
    if not first:
        raise RuntimeError("first ANN query returned no rows")

    round_rec = {
        "round": round_no,
        "ddl_s": round(t_ddl, 3),
        "first_query_s": round(t_first, 3),
        "build_s": round(t_ddl + t_first, 3),
        "bridge_builds_in_ddl": [[n, ms] for n, ms in builds_during_ddl],
        "bridge_build_ms_total": sum(ms for _, ms in builds_during_ddl),
    }

    # ---- recall -------------------------------------------------------
    n_recall = min(args.recall_queries, len(queries))
    got, _ = run_queries(client, table, queries, 100, limit=n_recall)
    got = [row for row in got if row]
    round_rec["recall@10"] = round(recall_at(got, gt10[:len(got)], 10), 5)
    round_rec["recall@100"] = round(recall_at(got, gt100[:len(got)], 100), 5)
    round_rec["ids_valid"] = bool(all(0 <= i < args.n for row in got for i in row))

    # ---- single-query QPS --------------------------------------------
    qps_q = min(args.qps_queries, len(queries))
    _, elapsed = run_queries(client, table, queries, 10, limit=qps_q)
    round_rec["single_qps"] = round(qps_q / elapsed, 2) if elapsed > 0 else 0.0

    # ---- batch QPS (one-call dbms_vector.batch_knn) -------------------
    if args.probe_table:
        try:
            client.execute("drop table if exists batch_out")
            client.execute("create table batch_out(probe_id bigint, "
                           "neighbor_id bigint, distance float, rk int)")
            start = time.perf_counter()
            client.execute(
                f'call dbms_vector.batch_knn("{args.probe_table}","{table}",'
                f'{args.batch_topk},"batch_out")', timeout=7200)
            elapsed = time.perf_counter() - start
            rows = client.rows("select count(*) from batch_out")
            produced = int(rows[0][0]) if rows else 0
            probes = max(1, produced // max(1, args.batch_topk))
            round_rec["batch_qps"] = round(probes / elapsed, 2) if elapsed else 0.0
            round_rec["batch_rows"] = produced
        except Exception as exc:  # batch API is optional for this track
            round_rec["batch_qps"] = None
            round_rec["batch_error"] = str(exc)[:300]
    return round_rec


def drop_caches(enabled):
    if not enabled:
        return
    try:
        subprocess.run(["sync"], check=False, timeout=120)
        with open("/proc/sys/vm/drop_caches", "w") as fh:
            fh.write("3\n")
    except Exception:
        pass


def median(values):
    values = [v for v in values if isinstance(v, (int, float))]
    return statistics.median(values) if values else None


# --------------------------------------------------------------------------
# variants
# --------------------------------------------------------------------------
def run_variant(name, observer_binary, lib, rounds, args, queries, gt10, gt100):
    print(f"\n=== variant {name} (lib={lib}) ===", flush=True)
    base_dir = pathlib.Path(args.base_dir) / name
    if base_dir.exists():
        # a fresh data dir per variant keeps every round truly cold; the table is
        # reloaded once per variant, which is untimed by the contest rules
        import shutil
        shutil.rmtree(base_dir, ignore_errors=True)
    observer = Observer(observer_binary, args.port, base_dir, args.bridge,
                        memory_budget=args.memory_budget,
                        datafile_size=args.datafile_size,
                        extra_env=dict(e.split("=", 1) for e in args.extra_env))
    observer.start()
    observer.wait_ready()
    client = SqlClient(args.port, db=args.database)
    try:
        client.execute(f"create database if not exists {args.database}")
        client.execute("alter system set ob_vector_memory_limit_percentage=30")
        client.execute(f"drop table if exists {args.table}")
        client.execute(f"create table {args.table}(id bigint primary key, "
                       f"v vector({args.dim}))")
        load_table(client, args.table, pathlib.Path(args.data) / "base.f32",
                   args.n, args.dim, args.load_batch, name)
        # probe table for the one-call batch API
        client.execute(f"drop table if exists {args.probe_table}")
        client.execute(f"create table {args.probe_table}(id bigint primary key, "
                       f"v vector({args.dim}))")
        load_table(client, args.probe_table, pathlib.Path(args.data) / "query.f32",
                   args.nq, args.dim, args.load_batch, "probes")

        records = []
        for rnd in range(1, rounds + 1):
            rec = measure_round(observer, client, args, lib, queries, gt10,
                                gt100, args.table, rnd)
            records.append(rec)
            print(f"[{name}] round {rnd}: build={rec['build_s']}s "
                  f"(ddl={rec['ddl_s']}s first_query={rec['first_query_s']}s) "
                  f"gpu_builds_in_ddl={len(rec['bridge_builds_in_ddl'])} "
                  f"recall@10={rec['recall@10']} recall@100={rec['recall@100']} "
                  f"single_qps={rec['single_qps']}", flush=True)
    finally:
        client.close()
        observer.stop()

    summary = {
        "variant": name,
        "lib": lib,
        "rounds": records,
        "median_build_s": median([r["build_s"] for r in records]),
        "median_ddl_s": median([r["ddl_s"] for r in records]),
        "median_first_query_s": median([r["first_query_s"] for r in records]),
        "median_recall@10": median([r["recall@10"] for r in records]),
        "median_recall@100": median([r["recall@100"] for r in records]),
        "median_single_qps": median([r["single_qps"] for r in records]),
        "median_batch_qps": median([r.get("batch_qps") for r in records]),
        "gpu_builds_in_ddl": sum(len(r["bridge_builds_in_ddl"]) for r in records),
        "ids_valid": all(r["ids_valid"] for r in records),
    }
    print(f"[{name}] median build={summary['median_build_s']}s "
          f"recall@10={summary['median_recall@10']} "
          f"single_qps={summary['median_single_qps']}", flush=True)
    return summary


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="dataset directory (gen_dataset.py)")
    ap.add_argument("--baseline-observer", help="frozen-baseline seekdb binary")
    ap.add_argument("--candidate-observer", help="candidate seekdb binary")
    ap.add_argument("--bridge", default="/work/bridge/libseekdb_cuvs_bridge.so")
    ap.add_argument("--port", type=int, default=2981)
    ap.add_argument("--base-dir", default="/tmp/seekdb-bench")
    ap.add_argument("--database", default="amd_bench")
    ap.add_argument("--table", default="t_vec")
    ap.add_argument("--probe-table", default="t_probe")
    ap.add_argument("--index-name", default="idx_vec")
    ap.add_argument("--index-params", default=",m=16,ef_construction=200,ef_search=200",
                    help="extra DDL params; '' mirrors a bare (type=hnsw, lib=cuvs)")
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--cpu-rounds", type=int, default=1)
    ap.add_argument("--load-batch", type=int, default=500)
    ap.add_argument("--recall-queries", type=int, default=200)
    ap.add_argument("--qps-queries", type=int, default=200)
    ap.add_argument("--batch-topk", type=int, default=10)
    ap.add_argument("--memory-budget", default="8G")
    ap.add_argument("--datafile-size", default="20G")
    ap.add_argument("--drop-caches", action="store_true",
                    help="sync + drop page cache between rounds (needs root)")
    ap.add_argument("--variants", default="baseline,candidate,cpu",
                    help="subset of: baseline,candidate,cpu")
    ap.add_argument("--extra-env", default=[], action="append",
                    help="extra NAME=VALUE env for the observer; repeatable")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="")
    args = ap.parse_args()

    import numpy as np
    data = pathlib.Path(args.data)
    meta = json.loads((data / "meta.json").read_text())
    args.n = int(meta["n"])
    args.dim = int(meta["dim"])
    args.nq = int(meta["nq"])
    queries = np.fromfile(data / "query.f32", dtype="<f4").reshape(args.nq, args.dim)
    gt10 = np.fromfile(data / "gt10.i32", dtype="<i4").reshape(args.nq, 10)
    gt100 = np.fromfile(data / "gt100.i32", dtype="<i4").reshape(args.nq, 100)

    wanted = [v.strip() for v in args.variants.split(",") if v.strip()]
    results = {"label": args.label, "data": str(data), "meta": meta,
               "index_params": args.index_params, "variants": {}}

    if "baseline" in wanted:
        if not args.baseline_observer:
            sys.exit("--baseline-observer is required for the baseline variant")
        results["variants"]["baseline"] = run_variant(
            "baseline", args.baseline_observer, "cuvs", args.rounds, args,
            queries, gt10, gt100)
    if "candidate" in wanted:
        if not args.candidate_observer:
            sys.exit("--candidate-observer is required for the candidate variant")
        results["variants"]["candidate"] = run_variant(
            "candidate", args.candidate_observer, "cuvs", args.rounds, args,
            queries, gt10, gt100)
    if "cpu" in wanted:
        if not args.candidate_observer:
            sys.exit("--candidate-observer is required for the cpu variant")
        results["variants"]["cpu"] = run_variant(
            "cpu", args.candidate_observer, "vsag", args.cpu_rounds, args,
            queries, gt10, gt100)

    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    print(f"\n[bench] wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

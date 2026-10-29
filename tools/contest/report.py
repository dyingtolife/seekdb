#!/usr/bin/env python3
"""Turn ``run_bench.py`` JSON into the contest deliverables:

  * ``<out>.md``  the CPU/GPU + baseline/candidate build-performance report
  * ``<out>.csv`` the same numbers in machine-readable form

The score estimate implements the published rubric:

    gates 4 + build 14 * clamp((speedup-1)/(2.0-1)) + query 2 * clamp((qps-1)/(1.5-1))

Usage:
    python3 report.py --results results/n200k.json --out report/n200k
"""

import argparse
import csv
import json
import pathlib
import sys


def fmt(value, digits=2, suffix=""):
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{digits}f}{suffix}"
    return f"{value}{suffix}"


def ratio(num, den):
    if num is None or not den or den == 0:
        return None
    return num / den


def clamp01(x):
    return max(0.0, min(1.0, x))


def score(res):
    variants = res.get("variants", {})
    base = variants.get("baseline")
    cand = variants.get("candidate")
    out = {"gates": [], "build_score": 0.0, "query_score": 0.0, "total": 0.0}
    if not base or not cand:
        out["note"] = "baseline and candidate are both required for scoring"
        return out

    speedup = ratio(base.get("median_build_s"), cand.get("median_build_s"))
    qps_ratio = ratio(cand.get("median_single_qps"), base.get("median_single_qps"))
    out["build_speedup"] = speedup
    out["query_speedup"] = qps_ratio

    def gate(name, ok, detail):
        out["gates"].append({"name": name, "ok": bool(ok), "detail": detail})

    gate("ids_valid", cand.get("ids_valid"),
         f"candidate ids in range: {cand.get('ids_valid')}")
    r10b, r10c = base.get("median_recall@10"), cand.get("median_recall@10")
    r100b, r100c = base.get("median_recall@100"), cand.get("median_recall@100")
    gate("recall@10", r10c is not None and r10b is not None and r10c >= r10b - 0.005,
         f"baseline {fmt(r10b, 4)} -> candidate {fmt(r10c, 4)} (drop <= 0.005)")
    gate("recall@100", r100c is not None and r100b is not None and r100c >= r100b - 0.005,
         f"baseline {fmt(r100b, 4)} -> candidate {fmt(r100c, 4)} (drop <= 0.005)")
    gate("gpu_build_in_window", (cand.get("gpu_builds_in_ddl") or 0) > 0,
         f"CAGRA builds observed during CREATE INDEX: {cand.get('gpu_builds_in_ddl')} "
         f"(baseline: {base.get('gpu_builds_in_ddl')})")
    gate("query_throughput", qps_ratio is not None and qps_ratio >= 0.90,
         f"candidate/baseline single-query QPS = {fmt(qps_ratio, 3)} (>= 0.90)")
    cpu = variants.get("cpu")
    gate("cpu_vsag_path", cpu is not None and cpu.get("ids_valid"),
         "lib=vsag reference measured" if cpu else "lib=vsag reference missing")

    gates_ok = all(g["ok"] for g in out["gates"])
    out["gates_ok"] = gates_ok
    if gates_ok and speedup:
        out["build_score"] = round(14.0 * clamp01((speedup - 1.0) / 1.0), 3)
        out["query_score"] = round(
            2.0 * clamp01(((qps_ratio or 1.0) - 1.0) / 0.5), 3)
        out["total"] = round(4.0 + out["build_score"] + out["query_score"], 3)
    return out


def build_markdown(res, sc):
    meta = res.get("meta", {})
    variants = res.get("variants", {})
    base = variants.get("baseline")
    cand = variants.get("candidate")
    cpu = variants.get("cpu")
    lines = []
    a = lines.append

    a("# CAGRA 索引构建性能对比报告（CPU / GPU）")
    a("")
    a(f"- 数据集：`{res.get('data')}` — n={meta.get('n')}, dim={meta.get('dim')}, "
      f"nq={meta.get('nq')}, dtype=float32, metric=L2, dist={meta.get('dist')}")
    a(f"- 索引参数：`type=hnsw, lib=cuvs{res.get('index_params', '')}`")
    a("- 计时口径：`CREATE VECTOR INDEX` 起，到首条 ANN 查询返回为止"
      "（表已导入、索引不存在、缓存已清理）")
    a("- 每版本 3 轮、取中位数；基线与候选使用同一台机器、同一份数据、同一套脚本")
    a("")
    a("## 1. 构建耗时分轮明细")
    a("")
    a("| 版本 | 轮次 | CREATE INDEX (s) | 首条查询 (s) | 构建合计 (s) | 装载期 GPU 建图次数 | GPU 建图总时长 (ms) |")
    a("| --- | --- | --- | --- | --- | --- | --- |")
    for key, label in (("baseline", "冻结基线"), ("candidate", "参赛版本")):
        variant = variants.get(key)
        if not variant:
            continue
        for r in variant["rounds"]:
            a(f"| {label} | {r['round']} | {fmt(r['ddl_s'], 3)} | "
              f"{fmt(r['first_query_s'], 3)} | **{fmt(r['build_s'], 3)}** | "
              f"{len(r['bridge_builds_in_ddl'])} | {r['bridge_build_ms_total']} |")
    a("")
    a("## 2. 中位数汇总与加速比")
    a("")
    a("| 版本 | 构建中位数 (s) | 相对基线加速比 | Recall@10 | Recall@100 | 单查询 QPS | 批量 QPS |")
    a("| --- | --- | --- | --- | --- | --- | --- |")
    if base:
        a(f"| 冻结基线 | **{fmt(base['median_build_s'], 3)}** | 1.000x | "
          f"{fmt(base['median_recall@10'], 4)} | {fmt(base['median_recall@100'], 4)} | "
          f"{fmt(base['median_single_qps'], 1)} | {fmt(base.get('median_batch_qps'), 1)} |")
    if cand and base:
        sp = ratio(base['median_build_s'], cand['median_build_s'])
        a(f"| 参赛版本 | **{fmt(cand['median_build_s'], 3)}** | "
          f"**{fmt(sp, 3, 'x')}** | {fmt(cand['median_recall@10'], 4)} | "
          f"{fmt(cand['median_recall@100'], 4)} | {fmt(cand['median_single_qps'], 1)} | "
          f"{fmt(cand.get('median_batch_qps'), 1)} |")
    elif cand:
        a(f"| 参赛版本 | **{fmt(cand['median_build_s'], 3)}** | n/a（需基线） | "
          f"{fmt(cand['median_recall@10'], 4)} | "
          f"{fmt(cand['median_recall@100'], 4)} | {fmt(cand['median_single_qps'], 1)} | "
          f"{fmt(cand.get('median_batch_qps'), 1)} |")
    a("")
    a("## 3. CPU 与 GPU 构建对比（同一份数据、同一台机器）")
    a("")
    a("| 路径 | 索引实现 | 构建中位数 (s) | 相对 CPU 加速比 | 说明 |")
    a("| --- | --- | --- | --- | --- |")
    if cpu:
        a(f"| CPU | `lib=vsag` HNSW | {fmt(cpu['median_build_s'], 3)} | 1.000x | "
          "纯 CPU 建图，无 GPU 参与 |")
    if base and cpu:
        a(f"| GPU 基线 | `lib=cuvs` CAGRA（冻结版） | {fmt(base['median_build_s'], 3)} | "
          f"{fmt(ratio(cpu['median_build_s'], base['median_build_s']), 3)}x | "
          "GPU 建图接入完成，但 CAGRA 在首查询惰性构建 |")
    if cand and cpu:
        a(f"| GPU 参赛版 | `lib=cuvs` CAGRA（优化后） | {fmt(cand['median_build_s'], 3)} | "
          f"{fmt(ratio(cpu['median_build_s'], cand['median_build_s']), 3)}x | "
          "构建参数调优 + 装载期后台预热建图 + 显存复用 |")
    a("")
    a("> 若某一行为 `n/a`，说明该变体未在本轮采集（`--variants` 未包含）。")
    a("")
    a("## 4. 硬门槛自检")
    a("")
    a("| 门槛 | 结果 | 依据 |")
    a("| --- | --- | --- |")
    for g in sc["gates"]:
        a(f"| {g['name']} | {'PASS' if g['ok'] else 'FAIL'} | {g['detail']} |")
    if not sc["gates"]:
        a("| — | 未评估 | 需要同时采集 baseline 与 candidate |")
    a("")
    a("## 5. 成绩估算（按赛题公布的计分口径）")
    a("")
    if "build_speedup" in sc:
        a(f"- 综合构建加速比：**{fmt(sc['build_speedup'], 3)}x**（满分线 2.0x）")
        a(f"- 查询加速比：**{fmt(sc.get('query_speedup'), 3)}x**（满分线 1.5x）")
        a(f"- 硬门槛：{'全部通过' if sc.get('gates_ok') else '存在失败项'}")
        a(f"- 构建分：{fmt(sc['build_score'], 3)} / 14")
        a(f"- 查询分：{fmt(sc['query_score'], 3)} / 2")
        a(f"- **本地估算总分：{fmt(sc['total'], 3)} / 20**")
    else:
        a("需要 baseline 与 candidate 两个变体才能估算。")
    a("")
    a("> 本地成绩仅用于开发参考；正式成绩以比赛组织方云端统一评测为准。")
    a("")
    return "\n".join(lines)


def write_csv(res, sc, path):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["variant", "lib", "round", "create_index_s", "first_query_s",
                    "build_s", "gpu_builds_in_ddl", "gpu_build_ms",
                    "recall@10", "recall@100", "single_qps", "batch_qps",
                    "ids_valid"])
        for key, variant in res.get("variants", {}).items():
            for r in variant["rounds"]:
                w.writerow([key, variant["lib"], r["round"], r["ddl_s"],
                            r["first_query_s"], r["build_s"],
                            len(r["bridge_builds_in_ddl"]),
                            r["bridge_build_ms_total"], r["recall@10"],
                            r["recall@100"], r["single_qps"],
                            r.get("batch_qps"), r["ids_valid"]])
        if "build_speedup" in sc:
            w.writerow([])
            w.writerow(["estimated_score", sc["total"]])
            w.writerow(["build_speedup", sc["build_speedup"]])
            w.writerow(["query_speedup", sc.get("query_speedup")])
            for g in sc["gates"]:
                w.writerow(["gate", g["name"], "PASS" if g["ok"] else "FAIL"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    res = json.loads(pathlib.Path(args.results).read_text())
    sc = score(res)
    out = pathlib.Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.with_suffix(".md").write_text(build_markdown(res, sc))
    write_csv(res, sc, out.with_suffix(".csv"))
    print(f"[report] {out.with_suffix('.md')}")
    print(f"[report] {out.with_suffix('.csv')}")
    if sc.get("gates"):
        passed = sum(1 for g in sc["gates"] if g["ok"])
        print(f"[report] gates {passed}/{len(sc['gates'])} passed, "
              f"estimated score {sc.get('total')}/20")
    return 0


if __name__ == "__main__":
    sys.exit(main())

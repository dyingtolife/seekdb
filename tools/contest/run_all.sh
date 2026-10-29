#!/usr/bin/env bash
# One-command reproduction of the AMD CAGRA build benchmark:
#
#   dataset -> bridge+seekdb builds -> baseline/candidate/cpu rounds -> report
#
# Everything is driven by environment variables so the same script can be
# replayed on the contest cloud box:
#
#   N=200000 DIM=128 tools/run_all.sh
#
# Required inputs (when --no-build is not used):
#   BASELINE_SRC   checkout of the frozen contest commit
#   CANDIDATE_SRC  checkout of the frozen commit + patch/*.patch applied
#
# Optional GPU smoke validation of the candidate (reuses the upstream runner):
#   tools/run_all.sh --verify-gpu
set -Eeuo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PKG=$(dirname "$HERE")
N=${N:-200000}
DIM=${DIM:-128}
NQ=${NQ:-1000}
ROUNDS=${ROUNDS:-3}
PORT=${PORT:-2981}
BASE_DIR=${BASE_DIR:-/tmp/seekdb-amd-bench}
DATA=${DATA:-$PKG/data/n${N}_d${DIM}}
RESULTS=${RESULTS:-$PKG/results}
LABEL=${LABEL:-n${N}_d${DIM}}
BRIDGE=${BRIDGE:-/work/bridge/libseekdb_cuvs_bridge.so}
BASELINE_SRC=${BASELINE_SRC:-}
CANDIDATE_SRC=${CANDIDATE_SRC:-}
BASELINE_OBSERVER=${BASELINE_OBSERVER:-}
CANDIDATE_OBSERVER=${CANDIDATE_OBSERVER:-}
MEMORY_BUDGET=${MEMORY_BUDGET:-8G}
DATAFILE_SIZE=${DATAFILE_SIZE:-20G}
DO_BUILD=1
VERIFY_GPU=0

while (($# > 0)); do
  case "$1" in
    --no-build) DO_BUILD=0; shift ;;
    --verify-gpu) VERIFY_GPU=1; shift ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

command -v mysql >/dev/null || { echo "mysql client is required" >&2; exit 2; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 2; }
[[ -c /dev/kfd ]] || { echo "/dev/kfd is missing: this benchmark needs a ROCm GPU" >&2; exit 2; }

echo "=============================================================="
echo " seekdb x hipVS/cuVS CAGRA build benchmark"
echo " dataset : ${N} x ${DIM} float32, L2, ${NQ} probes"
echo " rounds  : ${ROUNDS} per version (median reported)"
echo " output  : ${RESULTS}/${LABEL}.{json,md,csv}"
echo "=============================================================="

mkdir -p "$DATA" "$RESULTS"

echo "--- [1/4] dataset"
if [[ -f "$DATA/meta.json" ]]; then
  echo "    reusing $DATA"
else
  python3 "$HERE/gen_dataset.py" --out "$DATA" --n "$N" --dim "$DIM" --nq "$NQ"
fi

echo "--- [2/4] builds"
if [[ "$DO_BUILD" == "1" ]]; then
  [[ -n "$CANDIDATE_SRC" ]] || { echo "CANDIDATE_SRC is required (or pass --no-build)" >&2; exit 2; }
  args=(--candidate-src "$CANDIDATE_SRC" --bridge "$BRIDGE")
  [[ -n "$BASELINE_SRC" ]] && args+=(--baseline-src "$BASELINE_SRC")
  [[ -z "$BASELINE_SRC" ]] && args+=(--skip-baseline)
  "$HERE/build_all.sh" "${args[@]}"
  BASELINE_OBSERVER=${BASELINE_OBSERVER:-$(find "$BASELINE_SRC/build_release" -maxdepth 4 -type f -name seekdb -perm -u+x 2>/dev/null | head -n 1)}
  CANDIDATE_OBSERVER=${CANDIDATE_OBSERVER:-$(find "$CANDIDATE_SRC/build_release" -maxdepth 4 -type f -name seekdb -perm -u+x 2>/dev/null | head -n 1)}
else
  [[ -n "$CANDIDATE_OBSERVER" ]] || { echo "CANDIDATE_OBSERVER is required with --no-build" >&2; exit 2; }
fi
[[ -n "$CANDIDATE_OBSERVER" ]] || { echo "candidate observer not found" >&2; exit 1; }
[[ -n "$BASELINE_OBSERVER" ]] || { echo "baseline observer not found (baseline variant will be skipped)" >&2; }

export LD_LIBRARY_PATH="$(dirname "$BRIDGE"):${HIPVS_ROOT:-/opt/hipvs}/lib:${ROCM_ROOT:-/opt/rocm}/lib:${LD_LIBRARY_PATH:-}"

echo "--- [3/4] benchmark"
variants="candidate,cpu"
[[ -n "$BASELINE_OBSERVER" ]] && variants="baseline,$variants"
python3 "$HERE/run_bench.py" \
  --data "$DATA" \
  --baseline-observer "$BASELINE_OBSERVER" \
  --candidate-observer "$CANDIDATE_OBSERVER" \
  --bridge "$BRIDGE" \
  --port "$PORT" \
  --base-dir "$BASE_DIR" \
  --rounds "$ROUNDS" \
  --cpu-rounds 1 \
  --memory-budget "$MEMORY_BUDGET" \
  --datafile-size "$DATAFILE_SIZE" \
  --variants "$variants" \
  --label "$LABEL" \
  --out "$RESULTS/$LABEL.json"

echo "--- [4/4] report"
python3 "$HERE/report.py" --results "$RESULTS/$LABEL.json" --out "$RESULTS/$LABEL"

if [[ "$VERIFY_GPU" == "1" ]]; then
  echo "--- extra: upstream GPU smoke validation (candidate)"
  runner="$CANDIDATE_SRC/docs/gpu-vector-index-hipvs-cuvs/validation/run_gpu_smoke.sh"
  if [[ -x "$runner" ]]; then
    "$runner" --observer "$CANDIDATE_OBSERVER" --bridge "$BRIDGE" \
      --base-dir /tmp/seekdb-cuvs-smoke --port $((PORT + 50)) \
      --render "${RENDER_NODE:-/dev/dri/renderD128}" \
      --evidence "$RESULTS/gpu-smoke"
  else
    echo "    runner not found: $runner" >&2
  fi
fi

echo
echo "done:"
echo "  report : $RESULTS/$LABEL.md"
echo "  numbers: $RESULTS/$LABEL.json"

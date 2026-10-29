#!/usr/bin/env python3
"""Run the complete contest benchmark on an AMD Radeon Cloud `seekdb-hipvs`
workspace in ONE paste.  Idempotent: re-running skips finished stages.

Stage 0  environment self-check (GPU, hipVS, ROCm, toolchain, disk)
Stage 1  fetch code: candidate branch + frozen baseline + benchmark tools
Stage 2  build both hipVS bridges
Stage 3  build both seekdb binaries (skip if binary already exists)
Stage 4  functional validation (upstream run_gpu_smoke.sh on the candidate)
Stage 5  benchmark: 200k smoke first, then the 1M run (pass STAGE=full)
Stage 6  reports written to /workspace/reports/

Usage (in a JupyterLab terminal on the Radeon Cloud workspace):
    bash setup_cloud_run.sh            # all stages
    STAGES=0,1,2 bash setup_cloud_run.sh   # selected stages only
"""
set -e

### ---- configuration -------------------------------------------------------
WORK=/workspace
CAND_REPO=$WORK/seekdb-candidate
BASE_REPO=$WORK/seekdb-baseline
CAND_BRANCH=amd-cagra-redo-v1
BASE_REF=amd-cagra-contest-baseline-v1          # frozen contest baseline
TOOLS_REPO=https://github.com/dyingtolife/seekdb.git
BRIDGE_DIR=$WORK/bridge
OUT=$WORK/reports
JOBS=$(nproc)
PORT=2981
SMOKE_N=200000
FULL_N=${FULL_N:-1000000}
DIM=${DIM:-128}
ROUNDS=${ROUNDS:-3}
STAGES=${STAGES:-0,1,2,3,4,5,6}

want() { case ",$STAGES," in *",$1,"*) return 0 ;; *) return 1 ;; esac }

say() { printf '\n\033[1;36m==> [%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }

### ---- stage 0: self-check --------------------------------------------------
if want 0; then
  say "stage 0: environment self-check"
  command -v mysql >/dev/null || { echo "mysql client missing"; MISSING_MYSQL=1; }
  python3 -c 'import numpy' 2>/dev/null || { echo "numpy missing"; MISSING_NUMPY=1; }
  rocm-smi --showproductname 2>/dev/null | head -5 || rocminfo | grep -m1 gfx1100 || echo "WARN: no gfx1100"
  ls /opt/hipvs/lib/libcuvs_c.so || echo "ERROR: /opt/hipvs missing -> ask organizers for the contest image"
  ls /opt/rocm/bin/hipcc
  df -h $WORK | tail -1
  free -g | head -2
  nproc
fi

### ---- stage 1: fetch code --------------------------------------------------
if want 1; then
  say "stage 1: fetch candidate + baseline + tools"
  [ -d $CAND_REPO/.git ] || git clone https://github.com/dyingtolife/seekdb.git $CAND_REPO
  git -C $CAND_REPO fetch origin $CAND_BRANCH
  git -C $CAND_REPO switch $CAND_BRANCH
  git -C $CAND_REPO pull --ff-only origin $CAND_BRANCH
  echo "candidate commit: $(git -C $CAND_REPO rev-parse HEAD)"
  [ -d $BASE_REPO/.git ] || git clone https://github.com/zihaomu/seekdb.git $BASE_REPO
  git -C $BASE_REPO fetch origin $BASE_REF --tags
  git -C $BASE_REPO switch -d $BASE_REF
  echo "baseline commit:  $(git -C $BASE_REPO rev-parse HEAD)"
fi

### ---- stage 2: bridges -----------------------------------------------------
if want 2; then
  say "stage 2: build bridges"
  mkdir -p $BRIDGE_DIR/base $BRIDGE_DIR/cand
  bash $BASE_REPO/tools/hipvs_bridge/build.sh $BRIDGE_DIR/base/libseekdb_cuvs_bridge.so
  bash $CAND_REPO/tools/hipvs_bridge/build.sh $BRIDGE_DIR/cand/libseekdb_cuvs_bridge.so
fi

### ---- stage 3: seekdb builds ----------------------------------------------
if want 3; then
  say "stage 3: build seekdb (baseline first, then candidate)"
  find_observer() { find $1/build_release -maxdepth 4 -type f -name seekdb -perm -u+x 2>/dev/null | head -1; }
  for repo in $BASE_REPO $CAND_REPO; do
    if [ -n "$(find_observer $repo)" ]; then
      echo "skip: $(find_observer $repo) already built"
      continue
    fi
    case $repo in
      $BASE_REPO) B=$BRIDGE_DIR/base/libseekdb_cuvs_bridge.so ;;
      $CAND_REPO) B=$BRIDGE_DIR/cand/libseekdb_cuvs_bridge.so ;;
    esac
    (cd $repo && ./build.sh release -DOB_BUILD_CUVS=ON -DOB_BUILD_CUVS_TRACE=ON \
        -DCUVS_BRIDGE_LIB=$B --make -j$JOBS)
  done
  echo "baseline:  $(find_observer $BASE_REPO)"
  echo "candidate: $(find_observer $CAND_REPO)"
fi

### ---- stage 4: functional smoke -------------------------------------------
if want 4; then
  say "stage 4: GPU smoke validation (candidate)"
  CAND_BIN=$(find_observer $CAND_REPO)
  (cd $CAND_REPO && LD_LIBRARY_PATH=$BRIDGE_DIR/cand:/opt/hipvs/lib:/opt/rocm/lib \
    docs/gpu-vector-index-hipvs-cuvs/validation/run_gpu_smoke.sh \
    --observer "$CAND_BIN" --bridge $BRIDGE_DIR/cand/libseekdb_cuvs_bridge.so \
    --base-dir /tmp/seekdb-cuvs-smoke --port $((PORT + 40)) \
    --render "$(ls /dev/dri/renderD* | head -1)" \
    --evidence $OUT/gpu-smoke)
  echo "smoke summary:"; cat $OUT/gpu-smoke/summary.json
fi

### ---- stage 5: benchmark ---------------------------------------------------
if want 5; then
  say "stage 5: benchmark"
  python3 $CAND_REPO/tools/gen_dataset.py --out $WORK/data/n${SMOKE_N}_d${DIM} \
      --n $SMOKE_N --dim $DIM --nq 1000
  BASE_BIN=$(find_observer $BASE_REPO); CAND_BIN=$(find_observer $CAND_REPO)
  python3 $CAND_REPO/tools/run_bench.py --data $WORK/data/n${SMOKE_N}_d${DIM} \
      --baseline-observer "$BASE_BIN" --candidate-observer "$CAND_BIN" \
      --bridge $BRIDGE_DIR/cand/libseekdb_cuvs_bridge.so --port $PORT \
      --base-dir $WORK/bench-smoke --rounds $ROUNDS \
      --out $OUT/n${SMOKE_N}_d${DIM}.json
  python3 $CAND_REPO/tools/report.py --results $OUT/n${SMOKE_N}_d${DIM}.json \
      --out $OUT/n${SMOKE_N}_d${DIM}
  if [ "${FULL_N:-0}" -gt 0 ] && [ ! -f $OUT/n${FULL_N}_d${DIM}.json ]; then
    python3 $CAND_REPO/tools/gen_dataset.py --out $WORK/data/n${FULL_N}_d${DIM} \
        --n $FULL_N --dim $DIM --nq 1000
    python3 $CAND_REPO/tools/run_bench.py --data $WORK/data/n${FULL_N}_d${DIM} \
        --baseline-observer "$BASE_BIN" --candidate-observer "$CAND_BIN" \
        --bridge $BRIDGE_DIR/cand/libseekdb_cuvs_bridge.so --port $PORT \
        --base-dir $WORK/bench-full --rounds $ROUNDS --drop-caches \
        --out $OUT/n${FULL_N}_d${DIM}.json
    python3 $CAND_REPO/tools/report.py --results $OUT/n${FULL_N}_d${DIM}.json --out $OUT/n${FULL_N}_d${DIM}
  fi
fi

say "done. reports in $OUT/"

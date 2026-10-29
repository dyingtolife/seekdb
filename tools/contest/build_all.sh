#!/usr/bin/env bash
# Build the hipVS bridge plus two seekdb binaries:
#   * baseline  = frozen contest commit (amd-cagra-contest-baseline-v1)
#   * candidate = baseline + the optimisation patches in ./patch
#
# Both are built with the same fixed toolchain/flags so that the only difference
# is the code under test.  Source build time is excluded from the score.
#
# Usage:
#   tools/build_all.sh --baseline-src DIR --candidate-src DIR \
#                      [--bridge OUT] [--jobs N] [--skip-baseline]
set -Eeuo pipefail

BASELINE_SRC=""
CANDIDATE_SRC=""
BRIDGE=""
JOBS=$(nproc 2>/dev/null || echo 16)
SKIP_BASELINE=0
HIPVS_ROOT=${HIPVS_ROOT:-/opt/hipvs}
ROCM_ROOT=${ROCM_ROOT:-/opt/rocm}

while (($# > 0)); do
  case "$1" in
    --baseline-src) BASELINE_SRC=$2; shift 2 ;;
    --candidate-src) CANDIDATE_SRC=$2; shift 2 ;;
    --bridge) BRIDGE=$2; shift 2 ;;
    --jobs) JOBS=$2; shift 2 ;;
    --skip-baseline) SKIP_BASELINE=1; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

[[ -n "$CANDIDATE_SRC" ]] || { echo "--candidate-src is required" >&2; exit 2; }
[[ -d "$CANDIDATE_SRC" ]] || { echo "no such tree: $CANDIDATE_SRC" >&2; exit 2; }
if [[ "$SKIP_BASELINE" == "0" ]]; then
  [[ -n "$BASELINE_SRC" && -d "$BASELINE_SRC" ]] || {
    echo "--baseline-src is required unless --skip-baseline" >&2; exit 2; }
fi
BRIDGE=${BRIDGE:-$(dirname "$CANDIDATE_SRC")/bridge/libseekdb_cuvs_bridge.so}

find_observer()
{
  local root=$1
  find "$root/build_release" -maxdepth 4 -type f -name seekdb -perm -u+x 2>/dev/null | head -n 1
}

build_bridge()
{
  echo "==> building hipVS bridge -> $BRIDGE"
  mkdir -p "$(dirname "$BRIDGE")"
  HIPVS_ROOT="$HIPVS_ROOT" ROCM_ROOT="$ROCM_ROOT" \
    "$CANDIDATE_SRC/tools/hipvs_bridge/build.sh" "$BRIDGE"
}

build_seekdb()
{
  local src=$1 label=$2
  echo "==> building seekdb ($label) in $src"
  ( cd "$src" && ./build.sh release \
      -DOB_BUILD_CUVS=ON \
      -DOB_BUILD_CUVS_TRACE=ON \
      -DCUVS_BRIDGE_LIB="$BRIDGE" \
      --make -j"$JOBS" )
  local bin
  bin=$(find_observer "$src")
  [[ -n "$bin" ]] || { echo "cannot locate the seekdb binary under $src/build_release" >&2; exit 1; }
  echo "    $label observer: $bin"
}

build_bridge
if [[ "$SKIP_BASELINE" == "0" ]]; then
  build_seekdb "$BASELINE_SRC" baseline
fi
build_seekdb "$CANDIDATE_SRC" candidate

cat <<EOF

Build finished.  Use these paths for run_bench.py:

  --bridge             $BRIDGE
  --baseline-observer  $( [[ "$SKIP_BASELINE" == "0" ]] && find_observer "$BASELINE_SRC" || echo "<not built>" )
  --candidate-observer $(find_observer "$CANDIDATE_SRC")

Remember to export the runtime library path before running the observer:

  export LD_LIBRARY_PATH=$(dirname "$BRIDGE"):$HIPVS_ROOT/lib:$ROCM_ROOT/lib
EOF

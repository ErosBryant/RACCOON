#!/usr/bin/env bash

# Load the template DB with YCSB, then run five alternating 30M-operation
# readrandom/overwrite pairs for every configured mode. overwrite updates the
# existing DB without deleting it.

set -euo pipefail
ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"


script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)

export YCSB_OPERATIONS=${YCSB_OPERATIONS:-50000000}
export YCSB_WORKLOADS=${YCSB_WORKLOADS:-"readrandom overwrite readrandom overwrite"}
export YCSB_RESULTS_ROOT=${YCSB_RESULTS_ROOT:-"${script_dir}/../motivation/ycsb-read-overwrite-alternating"}
export YCSB_COMBINE_MODE_LOGS=${YCSB_COMBINE_MODE_LOGS:-1}

exec "${script_dir}/ycsb.sh" "$@"

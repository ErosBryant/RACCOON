#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
# This source code is licensed under both the GPLv2 (found in the
# COPYING file in the root directory) and Apache 2.0 License
# (found in the LICENSE.Apache file in the root directory).

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/.." && pwd)

DB_BENCH=${DB_BENCH:-${repo_dir}/db_bench}
MODE=${MODE:-rac}
DB_ROOT=${DB_ROOT:-${repo_dir}/benchmark-data}
OUT_DIR=${OUT_DIR:-${repo_dir}/benchmark-results/manual}
NUM=${NUM:-1000000}
READS=${READS:-${NUM}}
THREADS=${THREADS:-1}
KEY_SIZE=${KEY_SIZE:-16}
VALUE_SIZE=${VALUE_SIZE:-500}
CACHE_SIZE=${CACHE_SIZE:-33554432}
SEED=${SEED:-1000}
db_path=${DB_ROOT}/${MODE}

if [[ ! -x ${DB_BENCH} ]]; then
  echo "Error: db_bench is not executable: ${DB_BENCH}" >&2
  echo 'Build it with: AUTO_CLEAN=1 make -j"$(nproc)" DEBUG_LEVEL=0 db_bench' >&2
  exit 1
fi

mode_args=()
case ${MODE} in
  baseline)
    mode_args=(--enable_rac=false --read_triggered_compaction_threshold=0)
    ;;
  rtc)
    mode_args=(--enable_rac=false --read_triggered_compaction_threshold=0.01
      --max_compaction_trigger_wakeup_seconds=1)
    ;;
  rac)
    mode_args=(--enable_rac=true --read_triggered_compaction_threshold=0
      --rac_design_option=3 --rac2_ablation=0
      --rac_window_size=100000 --rac_top_k=4
      --rac_option2_dual_purpose_selection=true)
    ;;
  *)
    echo "Error: MODE must be baseline, rtc, or rac (got: ${MODE})." >&2
    exit 1
    ;;
esac

common_args=(
  "--db=${db_path}"
  "--num=${NUM}"
  "--reads=${READS}"
  "--threads=${THREADS}"
  "--key_size=${KEY_SIZE}"
  "--value_size=${VALUE_SIZE}"
  "--cache_size=${CACHE_SIZE}"
  "--seed=${SEED}"
  --compression_type=none
  --statistics --histogram
)

run_benchmark() {
  local workload=$1
  shift
  local log_file
  mkdir -p -- "${OUT_DIR}"
  log_file=$(mktemp "${OUT_DIR}/${MODE}_${workload}.XXXXXX.log")
  local command=("${DB_BENCH}" "${common_args[@]}" "${mode_args[@]}" "$@")

  {
    printf 'Date (UTC): %s\n' "$(date -u +%FT%TZ)"
    printf 'Revision: %s\n' "$(git -C "${repo_dir}" rev-parse HEAD 2>/dev/null || printf unknown)"
    printf 'Mode: %s\nDatabase: %s\n' "${MODE}" "${db_path}"
    "${DB_BENCH}" --version
    printf 'Command: '
    printf '%q ' "${command[@]}"
    printf '\n'
  } | tee "${log_file}"
  "${command[@]}" 2>&1 | tee -a "${log_file}"
  printf 'Log: %s\n' "${log_file}"
}

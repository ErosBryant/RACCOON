#!/usr/bin/env bash

# Compare baseline RocksDB, RTC, and RAC with the same fillrandom/readrandom
# workload. Each mode uses its own fresh database.

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)
run_id=$(date +%Y%m%d-%H%M%S)

db_bench=${DB_BENCH:-"${repo_dir}/db_bench"}
db_root=${DB_ROOT:-"/mnt/new1/rocksdb_compare_${run_id}"}
result_dir=${RESULT_DIR:-"${repo_dir}/trace-zhao/simple-results/${run_id}"}

num=${NUM:-32000000}
value_size=${VALUE_SIZE:-500}
threads=${THREADS:-1}
seed=${SEED:-1000}
rtc_threshold=${RTC_THRESHOLD:-0.01}

if [[ ! -x "${db_bench}" ]]; then
  echo "Error: db_bench is not executable: ${db_bench}" >&2
  exit 1
fi

mkdir -p "${db_root}" "${result_dir}"

common_args=(
  "--num=${num}"
  "--value_size=${value_size}"
  "--compression_type=none"
  "--statistics"
  "--histogram"
  # "--report_bg_io_stats=true"
)

mode_args=()
set_mode_args() {
  case "$1" in
    baseline)
      mode_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=0"
      )
      ;;
    rtc)
      mode_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=${rtc_threshold}"
        "--max_compaction_trigger_wakeup_seconds=1"
      )
      ;;
    rac)
      mode_args=(
        "--enable_rac=true"
        "--read_triggered_compaction_threshold=0"
        "--rac_design_option=2"
        "--rac_window_size=100000"
        "--rac_top_k=4"
        "--rac_benefit_efficiency_threshold=1.0"
        "--rac_high_miss_ratio_threshold=0.95"
        "--rac_bottom_overlap_threshold=8"
        "--rac_cooldown_windows=1"
        "--rac_option2_dual_purpose_selection=1"
        "--rac_option2_read_benefit_slack=0.80"
        "--rac_option2_emerging_score_floor=0.80"
        "--compaction_pri=3"
        "--num_bottom_pri_threads=2"
      )
      ;;
  esac
}

run_bench() {
  local log_file=$1
  shift

  printf '%q ' "$@" | tee "${log_file}"
  printf '\n' | tee -a "${log_file}"
  "$@" 2>&1 | tee -a "${log_file}"
}

# baseline
for mode in rtc rac; do
  db_path="${db_root}/${mode}"
  set_mode_args "${mode}"

  echo "===== ${mode}: fillrandom ====="
  run_bench "${result_dir}/${mode}_fillrandom.log" \
    "${db_bench}" \
    "--db=${db_path}" \
    "--benchmarks=fillrandom,waitforcompaction,stats" \
    "${common_args[@]}" \
    "${mode_args[@]}"

# "--reads=${num}" \
  echo "===== ${mode}: readrandom ====="
  run_bench "${result_dir}/${mode}_readrandom.log" \
    "${db_bench}" \
    "--db=${db_path}" \
    "--benchmarks=readrandom,waitforcompaction,stats" \
    "--use_existing_db=true" \
    "${common_args[@]}" \
    "${mode_args[@]}"
done

echo "===== summary ====="
grep -hE '^(fillrandom|readrandom)[[:space:]]*:' "${result_dir}"/*.log \
  | tee "${result_dir}/summary.txt"

echo "Done. Logs: ${result_dir}"
echo "Databases: ${db_root}"

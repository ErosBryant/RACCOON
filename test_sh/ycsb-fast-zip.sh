#!/usr/bin/env bash

# YCSB comparison built on the rtc_motivation_ratio.sh configuration.
#
# Execution layout:
#   1. Load one immutable template DB with db_bench ycsbfill.
#   2. For each mode, clone that template once.
#   3. Run the configured workload phases sequentially on the same clone.
#   4. Delete the completed mode clone.
#   5. Clone the original template again for the next mode.
#
# Thus every mode sees the same initial DB and phase order, while later phases
# intentionally observe changes made by earlier phases. Workloads a-f use the
# YCSB implementations. readrandom, overwrite, and fillrandom are also accepted
# so small wrapper scripts can define custom phase sequences.

set -euo pipefail
ulimit -n 1048576 2>/dev/null || echo "[WARN] Could not raise open file limit; current limit: $(ulimit -n)"

  
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/../.." && pwd)

# shellcheck source=trace-zhao/test_sh/monitor.sh
source "${script_dir}/monitor.sh"

db_bench=${DB_BENCH:-"${repo_dir}/db_bench"}
run_db_root=${YCSB_RUN_DB_ROOT:-${RTC_MOTIVATION_RUN_DB_ROOT:-/mnt/nvme/rtc_motivation_runs}}
results_root=${YCSB_RESULTS_ROOT:-"${repo_dir}/trace-zhao/motivation/ycsb-fast"}
result_dir=${YCSB_RESULT_DIR:-"${results_root}/$(date +%Y%m%d-%H%M%S)"}

entries=${YCSB_ENTRIES:-${RTC_MOTIVATION_ENTRIES:-200000000}}
operations=${YCSB_OPERATIONS:-${RTC_MOTIVATION_OPERATIONS:-32000000}}
threads=${YCSB_THREADS:-${RTC_MOTIVATION_THREADS:-1}}
key_size=${YCSB_KEY_SIZE:-${RTC_MOTIVATION_KEY_SIZE:-48}}
value_size=${YCSB_VALUE_SIZE:-${RTC_MOTIVATION_VALUE_SIZE:-1000}}
compression=${YCSB_COMPRESSION:-${RTC_MOTIVATION_COMPRESSION:-snappy}}
uniforms=${YCSB_UNIFORM:-${RTC_MOTIVATION_UNIFORM:-"0"}}
seed=${YCSB_SEED:-${RTC_MOTIVATION_SEED:-1000}}
max_background_jobs=${YCSB_MAX_BACKGROUND_JOBS:-${RTC_MOTIVATION_MAX_BACKGROUND_JOBS:-2}}
ycsb_read_then_write=${YCSB_READ_THEN_WRITE:-false}

workloads=${YCSB_WORKLOADS:-"a b c d f"}
modes=${YCSB_MODES:-"rac baseline rtc"}

rtc_threshold=${YCSB_RTC_THRESHOLD:-${RTC_MOTIVATION_RTC_THRESHOLD:-0.01}}
rtc_wakeup_seconds=${YCSB_RTC_WAKEUP_SECONDS:-${RTC_MOTIVATION_RTC_WAKEUP_SECONDS:-10}}

rac_top_k=${YCSB_RAC_TOP_K:-${RTC_MOTIVATION_RAC_TOP_K:-4}}
rac_dual_purpose=${YCSB_RAC_DUAL_PURPOSE_SELECTION:-${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-1}}
rac_window_size=${YCSB_RAC_WINDOW_SIZE:-${RTC_MOTIVATION_RAC_WINDOW_SIZE:-100000}}
rac_efficiency_threshold=${YCSB_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-3}}
rac2_efficiency_threshold=${YCSB_RAC2_BENEFIT_EFFICIENCY_THRESHOLD:-0.05}
rac_high_miss_ratio_threshold=${YCSB_RAC_HIGH_MISS_RATIO_THRESHOLD:-${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD:-1}}
rac_allow_l0_with_writes=${YCSB_RAC_ALLOW_L0_WITH_WRITES:-${RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES:-1}}
rac_l0_efficiency_threshold=${YCSB_RAC_L0_EFFICIENCY_THRESHOLD:-${RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD:-7}}
rac_l0_cooldown_windows=${YCSB_RAC_L0_COOLDOWN_WINDOWS:-${RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS:-4}}
rac_bottom_overlap_threshold=${YCSB_RAC_BOTTOM_OVERLAP_THRESHOLD:-${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD:-4}}
rac_cooldown_windows=${YCSB_RAC_COOLDOWN_WINDOWS:-${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS:-4}}
rac_read_benefit_slack=${YCSB_RAC_READ_BENEFIT_SLACK:-${RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK:-0.5}}
rac_emerging_score_floor=${YCSB_RAC_EMERGING_SCORE_FLOOR:-${RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR:-0.4}}
rac_num_bottom_pri_threads=${YCSB_RAC_NUM_BOTTOM_PRI_THREADS:-${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS:-1}}

rac2_hotspot_persistence=${YCSB_RAC2_HOTSPOT_PERSISTENCE:-${RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE:-0.8}}
rac2_prediction_horizon=${YCSB_RAC2_PREDICTION_HORIZON:-${RTC_MOTIVATION_RAC2_PREDICTION_HORIZON:-4}}
rac2_compaction_bandwidth_mib_per_sec=${YCSB_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC:-${RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC:-200}}

# seq
# /mnt/nvme/rtc_motivation_runs/ycsb_template_e200000000_v500_uni0_3172490

#  random 
# /mnt/nvme/rtc_motivation_runs/ycsb_template_e200000000_v500_uni0_3352618
reuse_template_db=${YCSB_REUSE_TEMPLATE_DB:-"/mnt/nvme/fast_template_2196275"}
cleanup_run_db=${YCSB_CLEANUP_RUN_DB:-1}
cleanup_template_db=${YCSB_CLEANUP_TEMPLATE_DB:-0}
drop_caches=${YCSB_DROP_CACHES:-${RTC_MOTIVATION_DROP_CACHES:-1}}
extra_args_text=${YCSB_EXTRA_DB_BENCH_ARGS:---cache_size=0}
dry_run=0
active_monitor_prefix=

usage() {
  cat <<'EOF'
Usage: trace-zhao/test_sh/ycsb-fast.sh [--dry-run]

Default flow:
  load once -> clone baseline -> A..F -> delete clone
            -> clone RTC      -> A..F -> delete clone
            -> clone RAC      -> A..F -> delete clone
            -> clone RAC2     -> A..F -> delete clone

Useful overrides:
  YCSB_MODES="baseline rtc rac rac2"
  YCSB_WORKLOADS="a b c d e f"
  YCSB_ENTRIES=200000000
  YCSB_OPERATIONS=32000000
  YCSB_UNIFORM="0 1"
  YCSB_READ_THEN_WRITE=true  # false restores randomized mixed ordering
  YCSB_REUSE_TEMPLATE_DB=/mnt/nvme/.../existing_template
  YCSB_CLEANUP_TEMPLATE_DB=1
  YCSB_RAC_BENEFIT_EFFICIENCY_THRESHOLD=0.01
  YCSB_RAC2_BENEFIT_EFFICIENCY_THRESHOLD=0.15
EOF
}

while (($#)); do
  case $1 in
    --dry-run) dry_run=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

die() {
  echo "[ERROR] $*" >&2
  exit 1
}

require_positive_integer() {
  local name=$1 value=$2
  [[ ${value} =~ ^[1-9][0-9]*$ ]] || die "${name} must be a positive integer: ${value}"
}

require_positive_number() {
  local name=$1 value=$2
  [[ ${value} =~ ^[0-9]+([.][0-9]+)?$ ]] || \
    die "${name} must be a positive number: ${value}"
  [[ ! ${value} =~ ^0+([.]0+)?$ ]] || \
    die "${name} must be greater than zero: ${value}"
}

safe_remove_db() {
  local path=$1
  [[ ${path} == "${run_db_root}/"* ]] || \
    die "Refusing to remove DB outside run root: ${path}"
  [[ ${path} != "${run_db_root}" ]] || \
    die "Refusing to remove run root itself: ${path}"
  rm -rf -- "${path}"
}

stop_active_monitors() {
  if [[ -n ${active_monitor_prefix} ]]; then
    stop_monitors "${active_monitor_prefix}" || true
    active_monitor_prefix=
  fi
}

trap stop_active_monitors EXIT
trap 'stop_active_monitors; exit 130' INT
trap 'stop_active_monitors; exit 143' TERM

drop_linux_caches() {
  [[ ${drop_caches} == 1 ]] || return 0
  sync
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches
  elif command -v sudo >/dev/null && sudo -n true 2>/dev/null; then
    sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'
  else
    echo "[WARN] Cannot drop Linux page cache without root/passwordless sudo" >&2
  fi
}

clear_db_logs() {
  local db_path=$1
  find "${db_path}" -maxdepth 1 -type f \
    \( -name LOG -o -name 'LOG.old.*' \) -delete
}

prepare_telemetry_dir() {
  local run_db=$1 raw_dir=$2
  local link_path="${run_db}/rac-telemetry"
  mkdir -p -- "${raw_dir}/rac-telemetry"
  if [[ -e ${link_path} && ! -L ${link_path} ]]; then
    die "Telemetry path exists and is not a symlink: ${link_path}"
  fi
  rm -f -- "${link_path}"
  ln -s -- "${raw_dir}/rac-telemetry" "${link_path}"
}

clone_template_db() {
  local template_db=$1 run_db=$2
  [[ -f ${template_db}/CURRENT ]] || \
    die "Template DB is missing CURRENT: ${template_db}"
  if [[ -e ${run_db} ]]; then
    safe_remove_db "${run_db}"
  fi
  cp -a --reflink=auto -- "${template_db}" "${run_db}"
  [[ -f ${run_db}/CURRENT ]] || die "Cloned DB is missing CURRENT: ${run_db}"
  echo "[INFO] Cloned clean template: ${template_db} -> ${run_db}"
}

write_command() {
  local output=$1
  shift
  printf '%q ' "$@" > "${output}"
  printf '\n' >> "${output}"
}

load_template_db() {
  local template_db=$1
  local raw_dir="${result_dir}/raw/load_template"
  local log_file="${raw_dir}/db_bench.log"
  mkdir -p -- "${raw_dir}" "${run_db_root}"
  if [[ -e ${template_db} ]]; then
    safe_remove_db "${template_db}"
  fi

  local -a cmd=(
    "${db_bench}"
    "--db=${template_db}"
    "--benchmarks=ycsbfill,waitforcompaction,stats"
    "--num=${entries}"
    "--threads=${threads}"
    "--key_size=${key_size}"
    "--value_size=${value_size}"
    "--compression_type=${compression}"
    "--max_background_jobs=8"
    "--bloom_bits=0"
    "--statistics"
    "--histogram"
    "--seed=${seed}"
    "--enable_rac=false"
    "--read_triggered_compaction_threshold=0"
  )
  if [[ -n ${extra_args_text} ]]; then
    local -a extra_args
    read -r -a extra_args <<< "${extra_args_text}"
    cmd+=("${extra_args[@]}")
  fi

  write_command "${raw_dir}/command.sh" "${cmd[@]}"
  cp -- "${raw_dir}/command.sh" "${log_file}"
  echo "[INFO] Loading one YCSB template DB: ${template_db}"
  "${cmd[@]}" >> "${log_file}" 2>&1
  [[ -f ${template_db}/CURRENT ]] || die "YCSB load did not create CURRENT"
  if [[ -f ${template_db}/LOG ]]; then
    cp -- "${template_db}/LOG" "${raw_dir}/LOG"
  fi
  clear_db_logs "${template_db}"
}

mode_args=()
build_mode_args() {
  local mode=$1 observation_log=$2
  local eta=${rac_efficiency_threshold}
  mode_args=()
  case ${mode} in
    baseline)
      mode_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=0"
        "--rtc_motivation_sst_log=${observation_log}"
      )
      ;;
    rtc)
      mode_args=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=${rtc_threshold}"
        "--max_compaction_trigger_wakeup_seconds=${rtc_wakeup_seconds}"
        "--rtc_motivation_sst_log=${observation_log}"
      )
      ;;
    rac|rac2)
      local design_option=2
      if [[ ${mode} == rac2 ]]; then
        design_option=3
        eta=${rac2_efficiency_threshold}
      fi
      mode_args=(
        "--enable_rac=true"
        "--read_triggered_compaction_threshold=0"
        "--rac_design_option=${design_option}"
        "--rac2_hotspot_persistence=${rac2_hotspot_persistence}"
        "--rac2_prediction_horizon=${rac2_prediction_horizon}"
        "--rac2_compaction_bandwidth_mib_per_sec=${rac2_compaction_bandwidth_mib_per_sec}"
        "--rac_window_size=${rac_window_size}"
        "--rac_top_k=${rac_top_k}"
        "--rac_benefit_efficiency_threshold=${eta}"
        "--rac_high_miss_ratio_threshold=${rac_high_miss_ratio_threshold}"
        "--rac_option2_allow_l0_with_writes=${rac_allow_l0_with_writes}"
        "--rac_option2_l0_efficiency_threshold=${rac_l0_efficiency_threshold}"
        "--rac_option2_l0_cooldown_windows=${rac_l0_cooldown_windows}"
        "--rac_bottom_overlap_threshold=${rac_bottom_overlap_threshold}"
        "--rac_cooldown_windows=${rac_cooldown_windows}"
        "--rac_option2_dual_purpose_selection=${rac_dual_purpose}"
        "--rac_option2_read_benefit_slack=${rac_read_benefit_slack}"
        "--rac_option2_emerging_score_floor=${rac_emerging_score_floor}"
        "--compaction_pri=3"
        "--num_bottom_pri_threads=${rac_num_bottom_pri_threads}"
        "--rtc_motivation_sst_log=${observation_log}"
      )
      ;;
  esac
}

run_workload() {
  local mode=$1 workload=$2 sequence=$3 run_db=$4
  local benchmark workload_label manifest_workload raw_name
  local -a operation_args=()
  case ${workload} in
    [a-f])
      benchmark="ycsb${workload}"
      workload_label="YCSB-${workload^^}"
      manifest_workload=${workload^^}
      raw_name="ycsb${workload}"
      operation_args=(
        "--num=${operations}"
        "--ycsb_record_count=${entries}"
        "--use_existing_db=true"
      )
      ;;
    readrandom)
      benchmark=readrandom
      workload_label=readrandom
      manifest_workload=readrandom
      raw_name="${sequence}_readrandom"
      operation_args=(
        "--num=${entries}"
        "--reads=${operations}"
        "--use_existing_db=true"
      )
      ;;
    overwrite)
      benchmark=overwrite
      workload_label=overwrite
      manifest_workload=overwrite
      raw_name="${sequence}_overwrite"
      operation_args=(
        "--num=${entries}"
        "--writes=${operations}"
        "--use_existing_db=true"
      )
      ;;
    fillrandom)
      benchmark=fillrandom
      workload_label=fillrandom
      manifest_workload=fillrandom
      raw_name="${sequence}_fillrandom"
      operation_args=(
        "--num=${entries}"
        "--writes=${operations}"
      )
      ;;
    *)
      die "Unsupported workload: ${workload}"
      ;;
  esac

  local raw_dir="${result_dir}/raw/uni${uniform}/${mode}/${raw_name}"
  local log_file="${raw_dir}/db_bench.log"
  local rocksdb_log="${raw_dir}/LOG"
  local monitor_prefix="${raw_dir}/monitor"
  local latency_log="${raw_dir}/ycsb_latency.log"
  local observation_log="${raw_dir}/sst_observations.csv"
  local status=0

  mkdir -p -- "${raw_dir}"
  clear_db_logs "${run_db}"
  prepare_telemetry_dir "${run_db}" "${raw_dir}"
  drop_linux_caches
  build_mode_args "${mode}" "${observation_log}"

  local -a cmd=(
    "${db_bench}"
    "--db=${run_db}"
    "--benchmarks=${benchmark},waitforcompaction,stats"
    "${operation_args[@]}"
    "--threads=${threads}"
    "--key_size=${key_size}"
    "--value_size=${value_size}"
    "--compression_type=${compression}"
    "--max_background_jobs=${max_background_jobs}"
    "--uni=${uniform}"
    "--ycsb_read_then_write=${ycsb_read_then_write}"
    "--bloom_bits=0"
    "--statistics"
    "--histogram"
    "--perf_level=3"
    "--level0_file_num_compaction_trigger=4"
    "--report_bg_io_stats=true"
    "--seed=${seed}"
    "--ycsb_latency_log=${latency_log}"
    "${mode_args[@]}"
  )
  if [[ -n ${extra_args_text} ]]; then
    local -a extra_args
    read -r -a extra_args <<< "${extra_args_text}"
    cmd+=("${extra_args[@]}")
  fi

  write_command "${raw_dir}/command.sh" "${cmd[@]}"
  cp -- "${raw_dir}/command.sh" "${log_file}"
  echo "[RUN] uniform=${uniform} mode=${mode} sequence=${sequence} workload=${workload_label} db=${run_db}"

  "${cmd[@]}" >> "${log_file}" 2>&1 &
  local bench_pid=$!
  active_monitor_prefix=${monitor_prefix}
  start_monitors "${bench_pid}" "${monitor_prefix}" "${run_db}"
  set +e
  wait "${bench_pid}"
  status=$?
  set -e
  stop_active_monitors

  if [[ -f ${run_db}/LOG ]]; then
    cp -- "${run_db}/LOG" "${rocksdb_log}"
  else
    : > "${rocksdb_log}"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${sequence}" "${manifest_workload}" "${mode}" "${status}" "${entries}" \
    "${operations}" "${threads}" "${key_size}" "${value_size}" \
    "${uniform}" "${run_db}" "${raw_dir}" "${log_file}" \
    >> "${result_dir}/manifest.csv"

  [[ ${status} == 0 ]] || \
    die "${workload_label} failed for ${mode}; see ${log_file}"
}

validate_config() {
  [[ -x ${db_bench} ]] || die "db_bench is missing or not executable: ${db_bench}"
  require_positive_integer YCSB_ENTRIES "${entries}"
  require_positive_integer YCSB_OPERATIONS "${operations}"
  require_positive_integer YCSB_THREADS "${threads}"
  require_positive_integer YCSB_KEY_SIZE "${key_size}"
  require_positive_integer YCSB_VALUE_SIZE "${value_size}"
  require_positive_integer YCSB_MAX_BACKGROUND_JOBS "${max_background_jobs}"
  [[ ${ycsb_read_then_write} == true || ${ycsb_read_then_write} == false || ${ycsb_read_then_write} == 1 || ${ycsb_read_then_write} == 0 ]] || die "YCSB_READ_THEN_WRITE must be true, false, 1, or 0: ${ycsb_read_then_write}"
  require_positive_integer YCSB_RTC_WAKEUP_SECONDS "${rtc_wakeup_seconds}"
  require_positive_integer YCSB_RAC_WINDOW_SIZE "${rac_window_size}"
  require_positive_number YCSB_RAC_BENEFIT_EFFICIENCY_THRESHOLD "${rac_efficiency_threshold}"
  require_positive_number YCSB_RAC2_BENEFIT_EFFICIENCY_THRESHOLD "${rac2_efficiency_threshold}"
  local uniform_value uniform_count=0
  for uniform_value in ${uniforms}; do
    [[ ${uniform_value} == 0 || ${uniform_value} == 1 ]] || \
      die "Each YCSB_UNIFORM value must be 0 or 1: ${uniform_value}"
    ((uniform_count += 1))
  done
  ((uniform_count > 0)) || die "YCSB_UNIFORM must contain 0, 1, or both"
  [[ ${cleanup_run_db} == 0 || ${cleanup_run_db} == 1 ]] || die "YCSB_CLEANUP_RUN_DB must be 0 or 1"
  [[ ${cleanup_template_db} == 0 || ${cleanup_template_db} == 1 ]] || die "YCSB_CLEANUP_TEMPLATE_DB must be 0 or 1"
  [[ ${drop_caches} == 0 || ${drop_caches} == 1 ]] || die "YCSB_DROP_CACHES must be 0 or 1"

  local mode workload
  for mode in ${modes}; do
    [[ ${mode} == baseline || ${mode} == rtc || ${mode} == rac || ${mode} == rac2 ]] || \
      die "Unsupported YCSB mode: ${mode}"
  done
  for workload in ${workloads}; do
    [[ ${workload} =~ ^[a-f]$ || ${workload} == readrandom || ${workload} == overwrite || ${workload} == fillrandom ]] || \
      die "Workload must be one of a b c d e f readrandom overwrite fillrandom: ${workload}"
  done
}

print_plan() {
  local workload_count=0 mode_count=0 uniform_count=0 mode workload uniform_value
  for workload in ${workloads}; do ((workload_count += 1)); done
  for mode in ${modes}; do ((mode_count += 1)); done
  for uniform_value in ${uniforms}; do ((uniform_count += 1)); done
  local total_runs=$((uniform_count * mode_count * workload_count))
  echo "[PLAN] template_loads=1 distributions=${uniform_count} modes=${mode_count} workloads_per_mode=${workload_count} total_runs=${total_runs}"
  echo "[PLAN] entries=${entries} operations_per_workload=${operations} workloads=${workloads}"
  echo "[PLAN] uniform_values=${uniforms}"
  echo "[PLAN] ycsb_read_then_write=${ycsb_read_then_write}"
  echo "[PLAN] modes=${modes}"
  echo "[PLAN] total_operations=$((total_runs * operations))"
  echo "[PLAN] layout: template -> clone(uniform, mode) -> configured phases on same DB path -> delete clone"
  if [[ " ${workloads} " == *" fillrandom "* ]]; then
    echo "[PLAN] note: db_bench fillrandom recreates the DB before its random-write phase"
  fi
}

main() {
  validate_config
  print_plan
  ((dry_run)) && return 0

  mkdir -p -- "${result_dir}/raw" "${run_db_root}"
  cat > "${result_dir}/experiment.conf" <<EOF
execution_layout=one_template_then_one_clone_per_uniform_and_mode_then_sequential_configured_phases
db_bench=${db_bench}
run_db_root=${run_db_root}
entries=${entries}
operations_per_workload=${operations}
threads=${threads}
key_size=${key_size}
value_size=${value_size}
compression=${compression}
uniform_values=${uniforms}
ycsb_read_then_write=${ycsb_read_then_write}
seed=${seed}
modes=${modes}
workloads=${workloads}
cleanup_run_db=${cleanup_run_db}
cleanup_template_db=${cleanup_template_db}
reuse_template_db=${reuse_template_db}
EOF
  echo 'sequence,workload,mode,status,entries,operations,threads,key_size,value_size,uniform,db_path,raw_dir,benchmark_log' \
    > "${result_dir}/manifest.csv"

  local template_db template_owned=0
  if [[ -n ${reuse_template_db} ]]; then
    template_db=${reuse_template_db}
    [[ -f ${template_db}/CURRENT ]] || \
      die "Reused YCSB template is missing CURRENT: ${template_db}"
    echo "[INFO] Reusing immutable template DB: ${template_db}"
  else
    template_db="${run_db_root}/ycsb_template_e${entries}_v${value_size}_$$"
    template_owned=1
    load_template_db "${template_db}"
  fi

  local uniform mode workload sequence run_db
  for uniform in ${uniforms}; do
    echo "[INFO] Starting YCSB distribution: uniform=${uniform}"
    for mode in ${modes}; do
      run_db="${run_db_root}/ycsb_run_${mode}_e${entries}_v${value_size}_uni${uniform}_$$"
      clone_template_db "${template_db}" "${run_db}"
      sequence=0
      for workload in ${workloads}; do
        ((sequence += 1))
        run_workload "${mode}" "${workload}" "${sequence}" "${run_db}"
      done
      if [[ ${cleanup_run_db} == 1 ]]; then
        safe_remove_db "${run_db}"
        echo "[INFO] Deleted completed uniform=${uniform} ${mode} clone: ${run_db}"
      fi
    done
  done

  if [[ ${cleanup_template_db} == 1 && ${template_owned} == 1 ]]; then
    safe_remove_db "${template_db}"
    echo "[INFO] Deleted owned template DB: ${template_db}"
  else
    echo "[INFO] Template DB retained for reuse: ${template_db}"
  fi
  echo "[DONE] Results: ${result_dir}"
}

main "$@"

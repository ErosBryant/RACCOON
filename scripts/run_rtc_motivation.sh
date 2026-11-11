#!/usr/bin/env bash

set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "${script_dir}/.." && pwd)

db_bench=${DB_BENCH:-"${repo_dir}/db_bench"}
python_bin=${PYTHON:-python3}
results_root=${RTC_MOTIVATION_RESULTS_ROOT:-"${repo_dir}/trace-zhao/rtc-motivation"}
run_db_root=${RTC_MOTIVATION_RUN_DB_ROOT:-"/mnt/nvme/rtc_motivation_runs"}

entries=${RTC_MOTIVATION_ENTRIES:-64000000}
operations_values=${RTC_MOTIVATION_OPERATIONS:-16000000}
operations=
threads=${RTC_MOTIVATION_THREADS:-2}
key_size=${RTC_MOTIVATION_KEY_SIZE:-16}
value_size_values=${RTC_MOTIVATION_VALUE_SIZE:-1000}
compression=${RTC_MOTIVATION_COMPRESSION:-none}
uniform_values=${RTC_MOTIVATION_UNIFORM:-1}
max_background_jobs=${RTC_MOTIVATION_MAX_BACKGROUND_JOBS:-2}
read_ratios=${RTC_MOTIVATION_READ_RATIOS:-"100 90 80 70 60 50 40 30 20 10 1 0"}
modes=${RTC_MOTIVATION_MODES:-"baseline rtc rac"}
rtc_threshold=${RTC_MOTIVATION_RTC_THRESHOLD:-0.01}
rtc_wakeup_seconds_values=${RTC_MOTIVATION_RTC_WAKEUP_SECONDS:-1}
rtc_read100_wakeup_seconds=${RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS:-}
rtc_wakeup_seconds=
rac_top_k=${RTC_MOTIVATION_RAC_TOP_K:-8}
rac_dual_purpose_values=${RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION:-0}
rac_dual_purpose=
rac_window_size_values=${RTC_MOTIVATION_RAC_WINDOW_SIZE:-100000}
rac_efficiency_threshold_values=${RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD:-1.0}
rac_efficiency_threshold=
rac_high_miss_ratio_threshold_values=${RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD:-0.95}
rac_allow_l0_with_writes=${RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES:-0}
rac_l0_efficiency_threshold=${RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD:-10}
rac_l0_cooldown_windows=${RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS:-8}
rac_bottom_overlap_threshold=${RTC_MOTIVATION_RAC_BOTTOM_OVERLAP_THRESHOLD:-8}
rac_cooldown_windows=${RTC_MOTIVATION_RAC_COOLDOWN_WINDOWS:-1}
rac_read_benefit_slack=${RTC_MOTIVATION_RAC_READ_BENEFIT_SLACK:-0.80}
rac_emerging_score_floor=${RTC_MOTIVATION_RAC_EMERGING_SCORE_FLOOR:-0.80}
rac_num_bottom_pri_threads=${RTC_MOTIVATION_RAC_NUM_BOTTOM_PRI_THREADS:-2}
rac2_hotspot_persistence=${RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE:-0.8}
rac2_prediction_horizon=${RTC_MOTIVATION_RAC2_PREDICTION_HORIZON:-4}
rac2_compaction_bandwidth_mib_per_sec=${RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC:-200}
seed=${RTC_MOTIVATION_SEED:-1000}
drop_caches=${RTC_MOTIVATION_DROP_CACHES:-1}
cleanup_run_db=${RTC_MOTIVATION_CLEANUP_RUN_DB:-1}
clone_per_ratio=${RTC_MOTIVATION_CLONE_PER_RATIO:-0}
reuse_rtc_db=${RTC_MOTIVATION_REUSE_RTC_DB:-}
result_dir=${RTC_MOTIVATION_RESULT_DIR:-}
extra_args_text=${RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS:-}

usage() {
  cat <<'EOF'
Usage: scripts/run_rtc_motivation.sh {load|A|summary|plot|all}

  load  Compatibility no-op. A loads one shared template DB per value-size/distribution.
  A       Clone the template for each workload case when
          RTC_MOTIVATION_CLONE_PER_RATIO=1; otherwise clone once per
          Baseline/RTC/RAC configuration. Generate CSV plus motivation_summary.md.
  summary Regenerate the concise CSV and motivation_summary.md from an existing result.
  plot    Compatibility alias for summary; it does not generate images.
  all     Run the complete experiment and generate the text summary.

Useful overrides:
  RTC_MOTIVATION_RESULT_DIR=/path/to/result
  RTC_MOTIVATION_ENTRIES=100000000
  RTC_MOTIVATION_OPERATIONS="32000000 64000000"
  RTC_MOTIVATION_VALUE_SIZE="100 500"
  RTC_MOTIVATION_UNIFORM="0 1"  # 0=Zipfian, 1=uniform
  RTC_MOTIVATION_READ_RATIOS="100 90 ... 1 0"
  RTC_MOTIVATION_MODES="baseline rtc rac rac2"
  RTC_MOTIVATION_RTC_WAKEUP_SECONDS=1
  RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS=600
  RTC_MOTIVATION_REUSE_RTC_DB=/path/to/already-loaded-rtc-db
  RTC_MOTIVATION_CLONE_PER_RATIO=1
  RTC_MOTIVATION_RAC_TOP_K=8
  RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION="1 0"
  RTC_MOTIVATION_RAC_WINDOW_SIZE=100000
  RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD="1.0 5.0"
  RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES=1
  RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD=10
  RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS=8
  RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE=0.8
  RTC_MOTIVATION_RAC2_PREDICTION_HORIZON=4
  RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC=200
  RTC_MOTIVATION_EXTRA_DB_BENCH_ARGS="--write_buffer_size=..."
EOF
}

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
    die "${name} must contain positive numbers: ${value}"
  [[ ! ${value} =~ ^0+([.]0+)?$ ]] || \
    die "${name} must contain values greater than zero: ${value}"
}

safe_remove_run_db() {
  local path=$1
  [[ ${path} == "${run_db_root}/"* ]] || die "Refusing to remove path outside run root: ${path}"
  [[ ${path} != "${run_db_root}" ]] || die "Refusing to remove protected path: ${path}"
  rm -rf -- "${path}"
}

ensure_binary() {
  [[ -x ${db_bench} ]] || die "db_bench is missing or not executable: ${db_bench}"
  command -v "${python_bin}" >/dev/null || die "Python is required: ${python_bin}"
}

create_result_dir() {
  if [[ -z ${result_dir} ]]; then
    result_dir="${results_root}/$(date +%Y%m%d-%H%M%S)"
  fi
  mkdir -p -- "${results_root}" "${result_dir}/raw"
  ln -sfn -- "${result_dir}" "${results_root}/latest"
  printf '%s\n' "${result_dir}" > "${results_root}/LATEST"
}

resolve_result_dir() {
  if [[ -n ${result_dir} ]]; then
    return
  fi
  if [[ -r ${results_root}/LATEST ]]; then
    result_dir=$(<"${results_root}/LATEST")
  elif [[ -L ${results_root}/latest ]]; then
    result_dir=$(readlink -f -- "${results_root}/latest")
  else
    die "No result directory found; run A first or set RTC_MOTIVATION_RESULT_DIR"
  fi
}

write_config() {
  local execution_layout="value_size_uniform_shared_template_mode_clone"
  if [[ ${clone_per_ratio} == 1 ]]; then
    execution_layout="value_size_uniform_shared_template_ratio_clone"
  fi
  {
    printf 'db_bench=%q\n' "${db_bench}"
    printf 'run_db_root=%q\n' "${run_db_root}"
    printf 'execution_layout=%q\n' "${execution_layout}"
    printf 'db_clone_method=%q\n' "cp_reflink_auto"
    printf 'clone_per_ratio=%q\n' "${clone_per_ratio}"
    printf 'entries=%q\n' "${entries}"
    printf 'operations=%q\n' "${operations_values}"
    printf 'threads=%q\n' "${threads}"
    printf 'key_size=%q\n' "${key_size}"
    printf 'value_size_values=%q\n' "${value_size_values}"
    printf 'compression=%q\n' "${compression}"
    printf 'uniform_values=%q\n' "${uniform_values}"
    printf 'max_background_jobs=%q\n' "${max_background_jobs}"
    printf 'read_ratios=%q\n' "${read_ratios}"
    printf 'modes=%q\n' "${modes}"
    printf 'rtc_threshold=%q\n' "${rtc_threshold}"
    printf 'rtc_wakeup_seconds=%q\n' "${rtc_wakeup_seconds_values}"
    printf 'rtc_read100_wakeup_seconds=%q\n' "${rtc_read100_wakeup_seconds}"
    printf 'rac_top_k=%q\n' "${rac_top_k}"
    printf 'rac_dual_purpose=%q\n' "${rac_dual_purpose_values}"
    printf 'rac_window_size_values=%q\n' "${rac_window_size_values}"
    printf 'rac_efficiency_threshold_values=%q\n' "${rac_efficiency_threshold_values}"
    printf 'rac_high_miss_ratio_threshold_values=%q\n' "${rac_high_miss_ratio_threshold_values}"
    printf 'rac_allow_l0_with_writes=%q\n' "${rac_allow_l0_with_writes}"
    printf 'rac_l0_efficiency_threshold=%q\n' "${rac_l0_efficiency_threshold}"
    printf 'rac_l0_cooldown_windows=%q\n' "${rac_l0_cooldown_windows}"
    printf 'rac_bottom_overlap_threshold=%q\n' "${rac_bottom_overlap_threshold}"
    printf 'rac_cooldown_windows=%q\n' "${rac_cooldown_windows}"
    printf 'rac_read_benefit_slack=%q\n' "${rac_read_benefit_slack}"
    printf 'rac_emerging_score_floor=%q\n' "${rac_emerging_score_floor}"
    printf 'rac_num_bottom_pri_threads=%q\n' "${rac_num_bottom_pri_threads}"
    printf 'rac2_hotspot_persistence=%q\n' "${rac2_hotspot_persistence}"
    printf 'rac2_prediction_horizon=%q\n' "${rac2_prediction_horizon}"
    printf 'rac2_compaction_bandwidth_mib_per_sec=%q\n' "${rac2_compaction_bandwidth_mib_per_sec}"
    printf 'seed=%q\n' "${seed}"
    printf 'reuse_rtc_db=%q\n' "${reuse_rtc_db}"
    printf 'extra_args=%q\n' "${extra_args_text}"
  } > "${result_dir}/experiment.conf"
}

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

device_for_path() {
  local path=$1 source
  source=$(findmnt -n -o SOURCE --target "${path}" 2>/dev/null || true)
  [[ -n ${source} ]] && basename -- "${source}"
}

start_iostat() {
  local output=$1 path=$2
  iostat_pid=""
  iostat_device=$(device_for_path "${path}")
  if command -v iostat >/dev/null; then
    if [[ -n ${iostat_device} ]]; then
      iostat -dx 1 "${iostat_device}" > "${output}" 2>&1 &
    else
      iostat -dx 1 > "${output}" 2>&1 &
    fi
    iostat_pid=$!
  else
    : > "${output}"
  fi
}

stop_iostat() {
  if [[ -n ${iostat_pid:-} ]]; then
    kill "${iostat_pid}" 2>/dev/null || true
    wait "${iostat_pid}" 2>/dev/null || true
  fi
}

clear_db_logs() {
  local db_path=$1
  find "${db_path}" -maxdepth 1 -type f \
    \( -name LOG -o -name 'LOG.old.*' \) -delete
}

clone_template_db() {
  local template_db=$1 run_db=$2
  [[ -d ${template_db} ]] || die "Template DB does not exist: ${template_db}"
  [[ -f ${template_db}/CURRENT ]] || \
    die "Template DB is missing CURRENT: ${template_db}"
  if [[ -e ${run_db} ]]; then
    safe_remove_run_db "${run_db}"
  fi
  cp -a --reflink=auto -- "${template_db}" "${run_db}"
  [[ -f ${run_db}/CURRENT ]] || die "Cloned DB is missing CURRENT: ${run_db}"
  echo "[INFO] Cloned template DB: ${template_db} -> ${run_db}"
}

load_template_db() {
  local load_label=$1 template_db=$2
  ensure_binary
  require_positive_integer RTC_MOTIVATION_ENTRIES "${entries}"
  mkdir -p -- "${run_db_root}"
  if [[ -e ${template_db} ]]; then
    safe_remove_run_db "${template_db}"
  fi
  local load_dir="${result_dir}/raw/load_${load_label}"
  local load_log="${load_dir}/db_bench.log"
  mkdir -p -- "${load_dir}"
  local -a cmd=(
    "${db_bench}"
    "--db=${template_db}"
    "--benchmarks=fillrandom,waitforcompaction,stats"
    "--num=${entries}"
    # "--threads=1"
    "--key_size=${key_size}"
    "--value_size=${value_size}"
    "--compression_type=${compression}"
    "--max_background_jobs=8"
    "--uni=${uniform}"
    "--bloom_bits=0"
    "--statistics"
    "--level0_file_num_compaction_trigger=8"
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
  printf '%q ' "${cmd[@]}" > "${load_dir}/command.sh"
  printf '\n' >> "${load_dir}/command.sh"
  cp -- "${load_dir}/command.sh" "${load_log}"
  "${cmd[@]}" >> "${load_log}" 2>&1
  if [[ -f ${template_db}/LOG ]]; then
    cp -- "${template_db}/LOG" "${load_dir}/LOG"
  fi
  clear_db_logs "${template_db}"
  echo "[INFO] Loaded shared template DB: ${template_db}"
  echo "[INFO] Template load log: ${load_log}"
}

append_manifest_header() {
  cat > "${result_dir}/manifest.csv" <<'EOF'
read_ratio,write_ratio,mode,base_mode,status,entries,operations,threads,key_size,value_size,uniform,db_path,raw_dir,benchmark_log,rocksdb_log,iostat_log,rtc_sst_log,device,rtc_threshold,rtc_wakeup_seconds,rac_dual_purpose_selection,rac_window_size,rac_high_miss_ratio_threshold,rac_benefit_efficiency_threshold,rac_allow_l0_with_writes,rac_l0_efficiency_threshold,rac_l0_cooldown_windows,rac_design_option,rac2_hotspot_persistence,rac2_prediction_horizon,rac2_compaction_bandwidth_mib_per_sec
EOF
}

run_one() {
  local read_ratio=$1 mode=$2 run_db=$3 config_label=${4:-$2}
  local write_ratio=$((100 - read_ratio))
  local effective_rtc_wakeup_seconds=""
  if [[ ${mode} == rtc ]]; then
    effective_rtc_wakeup_seconds=${rtc_wakeup_seconds}
    if [[ ${read_ratio} == 100 && -n ${rtc_read100_wakeup_seconds} ]]; then
      effective_rtc_wakeup_seconds=${rtc_read100_wakeup_seconds}
    fi
    if [[ -n ${rtc_read100_wakeup_seconds} ]]; then
      config_label+="_wakeup${effective_rtc_wakeup_seconds}"
    fi
  fi
  local tag="read${read_ratio}_write${write_ratio}_${config_label}"
  local raw_dir="${result_dir}/raw/${tag}"
  local benchmark_log="${raw_dir}/db_bench.log"
  local rocksdb_log="${raw_dir}/LOG"
  local iostat_log="${raw_dir}/iostat.log"
  local rtc_sst_log=""
  local effective_rtc_threshold=0
  local effective_rac_dual_purpose=""
  local effective_rac_window_size=""
  local effective_rac_high_miss_ratio_threshold=""
  local effective_rac_efficiency_threshold=""
  local effective_rac_allow_l0_with_writes=""
  local effective_rac_l0_efficiency_threshold=""
  local effective_rac_l0_cooldown_windows=""
  local effective_rac_design_option=""
  local effective_rac2_hotspot_persistence=""
  local effective_rac2_prediction_horizon=""
  local effective_rac2_compaction_bandwidth_mib_per_sec=""
  local status=0

  mkdir -p -- "${raw_dir}"
  clear_db_logs "${run_db}"
  mkdir -p -- "${raw_dir}/rac-telemetry"
  rm -rf -- "${run_db}/rac-telemetry"
  ln -s -- "${raw_dir}/rac-telemetry" "${run_db}/rac-telemetry"
  drop_linux_caches

  local -a cmd=(
    "${db_bench}"
    "--db=${run_db}"
    "--benchmarks=readrandomwriterandom,waitforcompaction,stats"
    "--num=${entries}"
    "--reads=${operations}"
    "--writes=${operations}"
    "--readwritepercent=${read_ratio}"
    "--readwrite_random_interleave=1"
    # "--threads=${threads}"
    "--key_size=${key_size}"
    "--value_size=${value_size}"
    "--compression_type=${compression}"
    "--max_background_jobs=${max_background_jobs}"
    "--uni=${uniform}"
    "--bloom_bits=0"
    "--statistics"
    "--histogram"
    "--perf_level=3"
    "--level0_file_num_compaction_trigger=4"
    "--report_bg_io_stats=true"
    "--use_existing_db=true"
    "--seed=${seed}"
  )
  case ${mode} in
    baseline)
      rtc_sst_log="${raw_dir}/rtc_sst_observations.csv"
      cmd+=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=0"
        "--rtc_motivation_sst_log=${rtc_sst_log}"
      )
      ;;
    rtc)
      effective_rtc_threshold=${rtc_threshold}
      rtc_sst_log="${raw_dir}/rtc_sst_observations.csv"
      cmd+=(
        "--enable_rac=false"
        "--read_triggered_compaction_threshold=${rtc_threshold}"
        "--max_compaction_trigger_wakeup_seconds=${effective_rtc_wakeup_seconds}"
        "--rtc_motivation_sst_log=${rtc_sst_log}"
      )
      ;;
    rac|rac2)
      if [[ ${mode} == rac2 ]]; then
        effective_rac_design_option=3
        effective_rac2_hotspot_persistence=${rac2_hotspot_persistence}
        effective_rac2_prediction_horizon=${rac2_prediction_horizon}
        effective_rac2_compaction_bandwidth_mib_per_sec=${rac2_compaction_bandwidth_mib_per_sec}
      else
        effective_rac_design_option=2
      fi
      effective_rac_dual_purpose=${rac_dual_purpose}
      effective_rac_window_size=${rac_window_size}
      effective_rac_high_miss_ratio_threshold=${rac_high_miss_ratio_threshold}
      effective_rac_efficiency_threshold=${rac_efficiency_threshold}
      effective_rac_allow_l0_with_writes=${rac_allow_l0_with_writes}
      effective_rac_l0_efficiency_threshold=${rac_l0_efficiency_threshold}
      effective_rac_l0_cooldown_windows=${rac_l0_cooldown_windows}
      rtc_sst_log="${raw_dir}/rac_sst_observations.csv"
      cmd+=(
        "--enable_rac=true"
        "--read_triggered_compaction_threshold=0"
        "--rac_design_option=${effective_rac_design_option}"
        "--rac2_hotspot_persistence=${rac2_hotspot_persistence}"
        "--rac2_prediction_horizon=${rac2_prediction_horizon}"
        "--rac2_compaction_bandwidth_mib_per_sec=${rac2_compaction_bandwidth_mib_per_sec}"
        "--rac_window_size=${rac_window_size}"
        "--rac_top_k=${rac_top_k}"
        "--rac_benefit_efficiency_threshold=${rac_efficiency_threshold}"
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
        "--rtc_motivation_sst_log=${rtc_sst_log}"
      )
      ;;
  esac
  if [[ -n ${extra_args_text} ]]; then
    local -a extra_args
    read -r -a extra_args <<< "${extra_args_text}"
    cmd+=("${extra_args[@]}")
  fi

  printf '%q ' "${cmd[@]}" > "${raw_dir}/command.sh"
  printf '\n' >> "${raw_dir}/command.sh"
  cp -- "${raw_dir}/command.sh" "${benchmark_log}"
  start_iostat "${iostat_log}" "${run_db}"
  set +e
  "${cmd[@]}" >> "${benchmark_log}" 2>&1
  status=$?
  set -e
  stop_iostat
  if [[ -f ${run_db}/LOG ]]; then
    cp -- "${run_db}/LOG" "${rocksdb_log}"
  else
    : > "${rocksdb_log}"
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "${read_ratio}" "${write_ratio}" "${config_label}" "${mode}" "${status}" "${entries}" \
    "${operations}" "${threads}" "${key_size}" "${value_size}" "${uniform}" "${run_db}" \
    "${raw_dir}" "${benchmark_log}" "${rocksdb_log}" "${iostat_log}" \
    "${rtc_sst_log}" "${iostat_device:-}" "${effective_rtc_threshold}" \
    "${effective_rtc_wakeup_seconds}" "${effective_rac_dual_purpose}" \
    "${effective_rac_window_size}" \
    "${effective_rac_high_miss_ratio_threshold}" \
    "${effective_rac_efficiency_threshold}" \
    "${effective_rac_allow_l0_with_writes}" \
    "${effective_rac_l0_efficiency_threshold}" \
    "${effective_rac_l0_cooldown_windows}" \
    "${effective_rac_design_option}" \
    "${effective_rac2_hotspot_persistence}" \
    "${effective_rac2_prediction_horizon}" \
    "${effective_rac2_compaction_bandwidth_mib_per_sec}" \
    >> "${result_dir}/manifest.csv"

  [[ ${status} == 0 ]] || die "db_bench failed for ${tag}; see ${benchmark_log}"
  echo "[INFO] Completed ${tag}"
}

run_mode_sweep() {
  local mode=$1 config_label=${2:-$1} template_db=$3
  local run_db read_ratio

  if [[ ${clone_per_ratio} == 1 ]]; then
    for read_ratio in ${read_ratios}; do
      run_db="${run_db_root}/rtc_motivation_${config_label}_read${read_ratio}_$$"
      clone_template_db "${template_db}" "${run_db}"
      run_one "${read_ratio}" "${mode}" "${run_db}" "${config_label}"
      if [[ ${cleanup_run_db} == 1 ]]; then
        safe_remove_run_db "${run_db}"
      fi
    done
  else
    run_db="${run_db_root}/rtc_motivation_${config_label}_$$"
    clone_template_db "${template_db}" "${run_db}"
    for read_ratio in ${read_ratios}; do
      run_one "${read_ratio}" "${mode}" "${run_db}" "${config_label}"
    done
    if [[ ${cleanup_run_db} == 1 ]]; then
      safe_remove_run_db "${run_db}"
    fi
  fi
}

run_sweep() {
  ensure_binary
  require_positive_integer RTC_MOTIVATION_THREADS "${threads}"
  [[ ${clone_per_ratio} == 0 || ${clone_per_ratio} == 1 ]] || \
    die "RTC_MOTIVATION_CLONE_PER_RATIO must be 0 or 1: ${clone_per_ratio}"
  if [[ -n ${reuse_rtc_db} && ( ! -d ${reuse_rtc_db} || ! -f ${reuse_rtc_db}/CURRENT ) ]]; then
    echo "[WARN] Ignoring unavailable RTC reuse DB and loading a fresh DB: ${reuse_rtc_db}" >&2
    reuse_rtc_db=
  fi
  create_result_dir
  write_config
  append_manifest_header
  local read_ratio mode value operations_label miss_tag config_label
  local uniform uniform_label value_size value_size_label
  local -a uniform_list value_size_list rtc_wakeup_list
  read -r -a uniform_list <<< "${uniform_values}"
  read -r -a value_size_list <<< "${value_size_values}"
  read -r -a rtc_wakeup_list <<< "${rtc_wakeup_seconds_values}"
  ((${#uniform_list[@]} > 0)) || die "RTC_MOTIVATION_UNIFORM must contain at least one value"
  ((${#value_size_list[@]} > 0)) || die "RTC_MOTIVATION_VALUE_SIZE must contain at least one value"
  for value in "${uniform_list[@]}"; do
    [[ ${value} == 0 || ${value} == 1 ]] || \
      die "RTC_MOTIVATION_UNIFORM must contain only 0 or 1: ${value}"
  done
  for value in "${value_size_list[@]}"; do
    require_positive_integer RTC_MOTIVATION_VALUE_SIZE "${value}"
  done
  for value in ${operations_values}; do
    require_positive_integer RTC_MOTIVATION_OPERATIONS "${value}"
  done
  if [[ -n ${reuse_rtc_db} && ( ${operations_values} == *" "* || ${rtc_wakeup_seconds_values} == *" "* || ${#uniform_list[@]} -ne 1 || ${#value_size_list[@]} -ne 1 ) ]]; then
    die "RTC_MOTIVATION_REUSE_RTC_DB requires exactly one operations value, one value-size value, one uniform value, and one RTC wakeup value"
  fi
  for read_ratio in ${read_ratios}; do
    [[ ${read_ratio} =~ ^(0|[1-9]|[1-9][0-9]|100)$ ]] || die "Invalid read ratio: ${read_ratio}"
  done
  for mode in ${modes}; do
    [[ ${mode} == baseline || ${mode} == rtc || ${mode} == rac || ${mode} == rac2 ]] || die "Invalid mode: ${mode}"
  done
  for value in "${rtc_wakeup_list[@]}"; do
    require_positive_integer RTC_MOTIVATION_RTC_WAKEUP_SECONDS "${value}"
  done
  if [[ -n ${rtc_read100_wakeup_seconds} ]]; then
    require_positive_integer RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS \
      "${rtc_read100_wakeup_seconds}"
    ((${#rtc_wakeup_list[@]} == 1)) || \
      die "RTC_MOTIVATION_RTC_READ100_WAKEUP_SECONDS requires exactly one default RTC wakeup value"
  fi
  for value in ${rac_dual_purpose_values}; do
    [[ ${value} == 0 || ${value} == 1 ]] || \
      die "RTC_MOTIVATION_RAC_DUAL_PURPOSE_SELECTION must contain only 0 or 1: ${value}"
  done
  for value in ${rac_window_size_values}; do
    require_positive_integer RTC_MOTIVATION_RAC_WINDOW_SIZE "${value}"
  done
  for value in ${rac_efficiency_threshold_values}; do
    require_positive_number RTC_MOTIVATION_RAC_BENEFIT_EFFICIENCY_THRESHOLD "${value}"
  done
  for value in ${rac_high_miss_ratio_threshold_values}; do
    [[ ${value} =~ ^(0([.][0-9]+)?|1([.]0+)?)$ ]] || \
      die "RTC_MOTIVATION_RAC_HIGH_MISS_RATIO_THRESHOLD must contain values from 0 to 1: ${value}"
  done
  [[ ${rac_allow_l0_with_writes} == 0 || ${rac_allow_l0_with_writes} == 1 ]] || \
    die "RTC_MOTIVATION_RAC_ALLOW_L0_WITH_WRITES must be 0 or 1: ${rac_allow_l0_with_writes}"
  require_positive_number RTC_MOTIVATION_RAC_L0_EFFICIENCY_THRESHOLD \
    "${rac_l0_efficiency_threshold}"
  require_positive_integer RTC_MOTIVATION_RAC_L0_COOLDOWN_WINDOWS \
    "${rac_l0_cooldown_windows}"
  [[ ${rac2_hotspot_persistence} =~ ^(0([.][0-9]+)?|1([.]0+)?)$ ]] || \
    die "RTC_MOTIVATION_RAC2_HOTSPOT_PERSISTENCE must be from 0 to 1: ${rac2_hotspot_persistence}"
  require_positive_integer RTC_MOTIVATION_RAC2_PREDICTION_HORIZON \
    "${rac2_prediction_horizon}"
  require_positive_number RTC_MOTIVATION_RAC2_COMPACTION_BANDWIDTH_MIB_PER_SEC \
    "${rac2_compaction_bandwidth_mib_per_sec}"
  for operations in ${operations_values}; do
    operations_label="ops${operations}"
    for value_size in "${value_size_list[@]}"; do
      value_size_label="v${value_size}"
      for uniform in "${uniform_list[@]}"; do
        uniform_label="uni${uniform}"
        local template_db="${run_db_root}/rtc_motivation_template_${operations_label}_${value_size_label}_${uniform_label}_$$"
        local template_owned=1
        if [[ -n ${reuse_rtc_db} ]]; then
          template_db=${reuse_rtc_db}
          template_owned=0
          echo "[INFO] Reusing DB as shared template: ${template_db}"
        else
          load_template_db "template_${operations_label}_${value_size_label}_${uniform_label}" "${template_db}"
        fi
        for mode in ${modes}; do
          if [[ ${mode} == baseline ]]; then
            run_mode_sweep "${mode}" "${mode}_${operations_label}_${value_size_label}_${uniform_label}" "${template_db}"
            continue
          fi
          if [[ ${mode} == rtc ]]; then
            if [[ -n ${rtc_read100_wakeup_seconds} ]]; then
              rtc_wakeup_seconds=${rtc_wakeup_list[0]}
              config_label="rtc_${operations_label}_${value_size_label}_${uniform_label}_ratio_wakeup"
              run_mode_sweep rtc "${config_label}" "${template_db}"
              continue
            fi
            for rtc_wakeup_seconds in ${rtc_wakeup_seconds_values}; do
              config_label="rtc_${operations_label}_${value_size_label}_${uniform_label}_wakeup${rtc_wakeup_seconds}"
              run_mode_sweep rtc "${config_label}" "${template_db}"
            done
            continue
          fi
          local rac_window_size rac_high_miss_ratio_threshold
          local rac_efficiency_threshold eta_tag
          for rac_dual_purpose in ${rac_dual_purpose_values}; do
            for rac_window_size in ${rac_window_size_values}; do
              for rac_high_miss_ratio_threshold in ${rac_high_miss_ratio_threshold_values}; do
                for rac_efficiency_threshold in ${rac_efficiency_threshold_values}; do
                  miss_tag=${rac_high_miss_ratio_threshold//./p}
                  eta_tag=${rac_efficiency_threshold//./p}
                  config_label="${mode}_${operations_label}_${value_size_label}_${uniform_label}_dual${rac_dual_purpose}_w${rac_window_size}_miss${miss_tag}_eta${eta_tag}_l0w${rac_allow_l0_with_writes}"
                  if [[ ${mode} == rac2 ]]; then
                    config_label+="_p${rac2_hotspot_persistence//./p}_h${rac2_prediction_horizon}_bw${rac2_compaction_bandwidth_mib_per_sec//./p}"
                  fi
                  run_mode_sweep "${mode}" "${config_label}" "${template_db}"
                done
              done
            done
          done
        done
        if [[ ${cleanup_run_db} == 1 && ${template_owned} == 1 ]]; then
          safe_remove_run_db "${template_db}"
        fi
      done
    done
  done
  "${python_bin}" "${repo_dir}/scripts/plot_rtc_motivation.py" \
    --result-dir "${result_dir}" --derive-only
  echo "[INFO] Results: ${result_dir}"
}

summarize_results() {
  resolve_result_dir
  "${python_bin}" "${repo_dir}/scripts/plot_rtc_motivation.py" \
    --result-dir "${result_dir}" --derive-only
  echo "[INFO] Summary: ${result_dir}/motivation_summary.md"
}

case ${1:-} in
  load)
    echo "[INFO] No standalone load is needed. Run A; it loads one fresh DB for each value-size/distribution/mode combination."
    ;;
  A)
    run_sweep
    ;;
  summary|plot)
    summarize_results
    ;;
  all)
    run_sweep
    ;;
  *)
    usage
    exit 2
    ;;
esac
